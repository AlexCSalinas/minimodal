package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc"
)

// configureLogger installs a slog handler as the default logger. Defaults to
// text for human readability; flip MINIMODAL_LOG_FORMAT=json for a structured
// stream that log aggregators can parse.
func configureLogger() {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if os.Getenv("MINIMODAL_LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}

func main() {
	configureLogger()
	cfg := LoadConfig()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		slog.Error("listen failed", "port", cfg.GRPCPort, "err", err)
		os.Exit(1)
	}

	srv, err := NewServer(cfg)
	if err != nil {
		slog.Error("init server failed", "err", err)
		os.Exit(1)
	}
	defer srv.Close()

	// Phase 4: WAL replay. Must run before the gRPC server starts accepting
	// new InvokeFunction calls, so recovered jobs queue up before fresh ones.
	if err := srv.RecoverUnfinishedJobs(); err != nil {
		slog.Error("WAL replay failed", "err", err)
		os.Exit(1)
	}

	// MaxRecvMsgSize set generously above the application-level
	// MaxPayloadBytes — the application check returns a clean InvalidArgument
	// to the client, whereas hitting the gRPC limit returns a less helpful
	// transport-level error.
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(recoveryUnaryInterceptor),
		grpc.MaxRecvMsgSize(cfg.MaxPayloadBytes+1024*1024),
	)
	pb.RegisterOrchestratorServer(grpcServer, srv)

	bgCtx, cancelBg := context.WithCancel(context.Background())
	defer cancelBg()

	go srv.workerPool.RunFaultDetector(bgCtx)
	go srv.RunPendingQueueLoop(bgCtx)

	// HTTP /metrics + dashboard server runs alongside the gRPC server on a
	// different port. Failures here don't kill the orchestrator.
	httpServer := NewHTTPServer(srv, cfg.HTTPPort)
	go func() {
		if err := httpServer.Run(bgCtx); err != nil {
			slog.Error("http server error", "err", err)
		}
	}()

	slog.Info("orchestrator listening",
		"grpc_port", cfg.GRPCPort,
		"http_port", cfg.HTTPPort,
		"db_path", cfg.DBPath,
		"max_payload_bytes", cfg.MaxPayloadBytes)

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		slog.Info("shutdown signal received", "signal", sig.String())
	case err := <-serveErr:
		slog.Warn("grpc server exited", "err", err)
	}

	// Order matters: flag shutdown first so new InvokeFunction RPCs that
	// land during the drain window don't try to enqueue into a channel whose
	// drainer is about to exit. Then GracefulStop to let in-flight RPCs
	// finish. Only then cancel the background goroutines.
	srv.BeginShutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		slog.Info("shut down cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("graceful shutdown deadline exceeded; forcing stop")
		grpcServer.Stop()
	}
	cancelBg()
}
