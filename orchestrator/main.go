package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

// watchRaftLeadership re-runs WAL replay each time this node becomes leader:
// jobs that were PENDING/RUNNING under the previous leader get requeued into
// this node's dispatch loop. Followers dispatch nothing.
func watchRaftLeadership(ctx context.Context, srv *Server, rs *RaftStore) {
	for {
		select {
		case <-ctx.Done():
			return
		case isLeader := <-rs.LeaderCh():
			if !isLeader {
				slog.Info("raft: lost leadership")
				continue
			}
			slog.Info("raft: became leader; replaying unfinished jobs")
			if err := srv.RecoverUnfinishedJobs(); err != nil {
				slog.Error("raft: leader replay failed", "err", err)
			}
		}
	}
}

// joinRaftCluster asks an existing node to add us as a voter, retrying while
// the target cluster elects a leader or the operator brings it up.
func joinRaftCluster(cfg Config) {
	body, _ := json.Marshal(map[string]string{"id": cfg.RaftID, "addr": cfg.RaftBind})
	url := strings.TrimRight(cfg.RaftJoin, "/") + "/raft/join"
	backoff := time.Second
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				slog.Info("raft: joined cluster", "via", url)
				return
			}
			slog.Warn("raft: join rejected; retrying", "status", resp.StatusCode, "attempt", attempt)
		} else {
			slog.Warn("raft: join request failed; retrying", "err", err, "attempt", attempt)
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	slog.Error("raft: gave up joining cluster", "via", url)
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
	// In raft mode, replay instead runs on leadership acquisition (see
	// watchRaftLeadership): a booting node is a follower with no business
	// enqueueing anything.
	if srv.RaftStore() == nil {
		if err := srv.RecoverUnfinishedJobs(); err != nil {
			slog.Error("WAL replay failed", "err", err)
			os.Exit(1)
		}
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

	if rs := srv.RaftStore(); rs != nil {
		go watchRaftLeadership(bgCtx, srv, rs)
		if cfg.RaftJoin != "" {
			go joinRaftCluster(cfg)
		}
		slog.Info("raft enabled",
			"id", cfg.RaftID,
			"bind", cfg.RaftBind,
			"dir", cfg.RaftDir,
			"join", cfg.RaftJoin)
	}

	// Autoscaler (opt-in): the orchestrator launches and reaps local worker
	// processes on demand. Composes with externally started workers — they
	// count toward capacity but are never stopped.
	var autoscaler *Autoscaler
	if cfg.AutoscaleEnabled {
		argv := strings.Fields(cfg.WorkerCmd)
		if len(argv) == 0 {
			slog.Error("MINIMODAL_AUTOSCALE=1 but MINIMODAL_WORKER_CMD is empty")
			os.Exit(1)
		}
		launcher := NewProcessLauncher(
			argv,
			cfg.WorkerDir,
			"127.0.0.1:"+strconv.Itoa(cfg.GRPCPort),
			cfg.WorkerBasePort,
		)
		autoscaler = NewAutoscaler(AutoscalerConfig{
			Min:             cfg.AutoscaleMin,
			Max:             cfg.AutoscaleMax,
			QueueThreshold:  cfg.AutoscaleQueueThreshold,
			IdleTimeout:     cfg.AutoscaleIdleTimeout,
			Cooldown:        cfg.AutoscaleCooldown,
			RegisterTimeout: cfg.AutoscaleRegisterTimeout,
			Tick:            cfg.AutoscaleTick,
		}, srv.workerPool, srv.QueueDepth, launcher)
		go autoscaler.Run(bgCtx)
		slog.Info("autoscaler enabled",
			"min", cfg.AutoscaleMin,
			"max", cfg.AutoscaleMax,
			"worker_cmd", cfg.WorkerCmd)
	}

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
	if autoscaler != nil {
		autoscaler.Shutdown()
	}
}
