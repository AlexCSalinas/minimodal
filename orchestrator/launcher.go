package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// ProcessLauncher is the production Launcher: it exec's worker processes on
// the orchestrator's own host, wiring each one up with a unique worker ID and
// port via environment variables (worker.py reads WORKER_ID / WORKER_PORT /
// ORCH_ADDR). Suitable for single-box deployments; a container-based Launcher
// can implement the same interface later without touching the Autoscaler.
type ProcessLauncher struct {
	argv      []string // worker command, e.g. [".venv/bin/python", "-m", "worker.worker"]
	workDir   string   // directory the command runs in (repo root, so -m worker.worker resolves)
	orchAddr  string   // advertised to workers as ORCH_ADDR
	basePort  int      // first worker port; each live worker leases the next free one
	stopGrace time.Duration

	mu    sync.Mutex
	procs map[string]*workerProc
	ports map[int]bool // ports currently leased to live processes
}

type workerProc struct {
	cmd  *exec.Cmd
	port int
	done chan struct{} // closed by the reaper goroutine once Wait returns
}

func NewProcessLauncher(argv []string, workDir, orchAddr string, basePort int) *ProcessLauncher {
	return &ProcessLauncher{
		argv:      argv,
		workDir:   workDir,
		orchAddr:  orchAddr,
		basePort:  basePort,
		stopGrace: 5 * time.Second,
		procs:     make(map[string]*workerProc),
		ports:     make(map[int]bool),
	}
}

func (l *ProcessLauncher) LaunchWorker(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.procs[id]; exists {
		return fmt.Errorf("worker %s already running", id)
	}
	port := l.leasePortLocked()
	cmd := exec.Command(l.argv[0], l.argv[1:]...)
	cmd.Dir = l.workDir
	cmd.Env = append(os.Environ(),
		"WORKER_ID="+id,
		"WORKER_PORT="+strconv.Itoa(port),
		"ORCH_ADDR="+l.orchAddr,
		"WORKER_ADVERTISE_HOST=127.0.0.1",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		delete(l.ports, port)
		return fmt.Errorf("start worker %s: %w", id, err)
	}
	p := &workerProc{cmd: cmd, port: port, done: make(chan struct{})}
	l.procs[id] = p

	// Reaper goroutine: owns the exactly-once Wait call, releases the port
	// lease, and drops the bookkeeping entry when the process exits for any
	// reason — clean stop, crash, or kill.
	go func() {
		err := cmd.Wait()
		close(p.done)
		l.mu.Lock()
		delete(l.ports, port)
		if cur, ok := l.procs[id]; ok && cur == p {
			delete(l.procs, id)
		}
		l.mu.Unlock()
		if err != nil {
			slog.Warn("worker process exited with error", "worker", id, "err", err)
		}
	}()
	slog.Info("launched worker process", "worker", id, "port", port, "pid", cmd.Process.Pid)
	return nil
}

func (l *ProcessLauncher) leasePortLocked() int {
	port := l.basePort
	for l.ports[port] {
		port++
	}
	l.ports[port] = true
	return port
}

// StopWorker SIGTERMs the worker (it finishes in-flight tasks and exits) and
// escalates to SIGKILL if it hasn't exited within the grace period.
// Idempotent: unknown or already-exited workers are a no-op.
func (l *ProcessLauncher) StopWorker(id string) error {
	l.mu.Lock()
	p, ok := l.procs[id]
	l.mu.Unlock()
	if !ok {
		return nil
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	go func() {
		select {
		case <-p.done:
		case <-time.After(l.stopGrace):
			slog.Warn("worker ignored SIGTERM; killing", "worker", id)
			_ = p.cmd.Process.Kill()
		}
	}()
	return nil
}

// StopAll synchronously stops every live worker process; used at shutdown so
// managed workers don't outlive the orchestrator.
func (l *ProcessLauncher) StopAll() {
	l.mu.Lock()
	procs := make(map[string]*workerProc, len(l.procs))
	for id, p := range l.procs {
		procs[id] = p
	}
	l.mu.Unlock()
	for id, p := range procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(l.stopGrace):
			slog.Warn("worker ignored SIGTERM; killing", "worker", id)
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
}
