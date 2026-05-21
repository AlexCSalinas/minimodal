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
	// Max retries before a job is marked FAILED.
	MaxRetries int
}

func LoadConfig() Config {
	return Config{
		GRPCPort:          envInt("MINIMODAL_PORT", 50051),
		HTTPPort:          envInt("MINIMODAL_HTTP_PORT", 8080),
		DBPath:            envStr("MINIMODAL_DB_PATH", "minimodal.db"),
		WorkerTimeout:     envDuration("MINIMODAL_WORKER_TIMEOUT", 6*time.Second),
		HeartbeatInterval: envDuration("MINIMODAL_HEARTBEAT_INTERVAL", 2*time.Second),
		MaxRetries:        envInt("MINIMODAL_MAX_RETRIES", 3),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
