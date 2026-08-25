package main

import (
	"os"
	"strconv"
	"time"
)

// Config is populated from environment variables. All fields have safe
// defaults so the orchestrator runs out of the box with `go run .`.
type Config struct {
	// gRPC listen port for SDK + worker connections.
	GRPCPort int
	// HTTP listen port for /metrics (Phase 5).
	HTTPPort int
	// BoltDB file path. Created on first boot.
	DBPath string
	// How long a worker can go silent before we mark it dead.
	WorkerTimeout time.Duration
	// Recommended interval; sent to workers in RegisterWorkerResponse.
	HeartbeatInterval time.Duration
	// Max retries before a job is marked FAILED (worker-death retries).
	MaxRetries int
	// Max dispatch attempts per call before a job is marked FAILED. Prevents
	// a job from spinning forever when no workers are available or all
	// workers keep NACKing.
	MaxDispatchAttempts int
	// Per-call gRPC timeout when sending ExecuteTask to a worker.
	ExecuteTaskTimeout time.Duration
	// Hard upper bound on len(function_bytes)+len(args_bytes) per
	// InvokeFunction call. Prevents a buggy or malicious client from
	// allocating arbitrary memory on the orchestrator.
	MaxPayloadBytes int

	// --- Autoscaler (opt-in via MINIMODAL_AUTOSCALE=1) ---
	// When enabled, the orchestrator launches and reaps local worker
	// processes on demand instead of relying solely on a static pool.
	AutoscaleEnabled bool
	// Bounds on the number of autoscaler-managed + external workers.
	AutoscaleMin int
	AutoscaleMax int
	// Queued jobs at/above this trigger a scale-up.
	AutoscaleQueueThreshold int
	// A managed worker idle this long (with an empty queue) is stopped.
	AutoscaleIdleTimeout time.Duration
	// Minimum gap between demand-driven launches.
	AutoscaleCooldown time.Duration
	// A launched worker must register within this or its slot is reclaimed.
	AutoscaleRegisterTimeout time.Duration
	// Policy evaluation interval.
	AutoscaleTick time.Duration
	// Command ProcessLauncher execs to start one worker (space-separated).
	WorkerCmd string
	// Directory the worker command runs in; must be the repo root so
	// `-m worker.worker` resolves.
	WorkerDir string
	// First worker gRPC port; each concurrent worker leases the next free one.
	WorkerBasePort int
}

func LoadConfig() Config {
	return Config{
		GRPCPort:            envInt("MINIMODAL_PORT", 50051),
		HTTPPort:            envInt("MINIMODAL_HTTP_PORT", 8080),
		DBPath:              envStr("MINIMODAL_DB_PATH", "minimodal.db"),
		WorkerTimeout:       envDuration("MINIMODAL_WORKER_TIMEOUT", 6*time.Second),
		HeartbeatInterval:   envDuration("MINIMODAL_HEARTBEAT_INTERVAL", 2*time.Second),
		MaxRetries:          envInt("MINIMODAL_MAX_RETRIES", 3),
		MaxDispatchAttempts: envInt("MINIMODAL_MAX_DISPATCH_ATTEMPTS", 30),
		ExecuteTaskTimeout:  envDuration("MINIMODAL_EXECUTE_TASK_TIMEOUT", 5*time.Second),
		MaxPayloadBytes:     envInt("MINIMODAL_MAX_PAYLOAD_BYTES", 16*1024*1024), // 16 MiB

		AutoscaleEnabled:         envBool("MINIMODAL_AUTOSCALE", false),
		AutoscaleMin:             envInt("MINIMODAL_AUTOSCALE_MIN", 0),
		AutoscaleMax:             envInt("MINIMODAL_AUTOSCALE_MAX", 4),
		AutoscaleQueueThreshold:  envInt("MINIMODAL_AUTOSCALE_QUEUE_THRESHOLD", 1),
		AutoscaleIdleTimeout:     envDuration("MINIMODAL_AUTOSCALE_IDLE_TIMEOUT", 30*time.Second),
		AutoscaleCooldown:        envDuration("MINIMODAL_AUTOSCALE_COOLDOWN", 3*time.Second),
		AutoscaleRegisterTimeout: envDuration("MINIMODAL_AUTOSCALE_REGISTER_TIMEOUT", 15*time.Second),
		AutoscaleTick:            envDuration("MINIMODAL_AUTOSCALE_TICK", 500*time.Millisecond),
		WorkerCmd:                envStr("MINIMODAL_WORKER_CMD", ".venv/bin/python -m worker.worker"),
		WorkerDir:                envStr("MINIMODAL_WORKER_DIR", "."),
		WorkerBasePort:           envInt("MINIMODAL_WORKER_BASE_PORT", 50100),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
