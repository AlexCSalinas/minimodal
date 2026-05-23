package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	pb "minimodal/orchestrator/pb"

	"google.golang.org/grpc"
)

func main() {
	cfg := LoadConfig()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen :%d: %v", cfg.GRPCPort, err)
	}

	srv, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}
	defer srv.Close()

	// Phase 4: WAL replay. Must run before the gRPC server starts accepting
	// new InvokeFunction calls, so recovered jobs queue up before fresh ones.
	if err := srv.RecoverUnfinishedJobs(); err != nil {
		log.Fatalf("WAL replay: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(recoveryUnaryInterceptor))
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
			log.Printf("http server error: %v", err)
		}
	}()

	log.Printf("orchestrator listening on :%d (db=%s)", cfg.GRPCPort, cfg.DBPath)

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		log.Printf("got signal %v, shutting down...", sig)
	case err := <-serveErr:
		log.Printf("grpc server exited: %v", err)
	}

	cancelBg()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("shut down cleanly")
	case <-shutdownCtx.Done():
		log.Printf("graceful shutdown deadline; forcing stop")
		grpcServer.Stop()
	}
}
