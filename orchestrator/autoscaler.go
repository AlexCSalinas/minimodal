package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Launcher abstracts how worker processes come into existence, so the
// Autoscaler's policy is testable without exec'ing real Python workers.
// ProcessLauncher (launcher.go) is the production implementation.
type Launcher interface {
	// LaunchWorker starts a new worker that will register itself with the
	// orchestrator over gRPC using the given worker ID.
	LaunchWorker(id string) error
	// StopWorker shuts down a previously launched worker. Must be idempotent:
	// stopping an unknown or already-exited worker is a no-op.
	StopWorker(id string) error
	// StopAll stops every worker this launcher has started. Called on
	// orchestrator shutdown so managed workers aren't orphaned.
	StopAll()
}

type AutoscalerConfig struct {
	Min             int           // never scale below this many workers
	Max             int           // never scale above this many workers
	QueueThreshold  int           // queued jobs at/above this trigger scale-up
	IdleTimeout     time.Duration // idle this long → candidate for scale-down
	Cooldown        time.Duration // min gap between demand-driven launches
	RegisterTimeout time.Duration // a launched worker must register within this
	Tick            time.Duration // policy evaluation interval
}

// Autoscaler grows and shrinks the worker pool based on demand. Scale-up
// fires when jobs are queueing with no capacity to absorb them (zero alive
// workers, or every alive worker at/over its declared capacity); scale-down
// reclaims one managed worker per tick once it has sat idle past IdleTimeout
// while nothing is queued.
//
// It only ever stops workers it launched itself ("managed" workers).
// Externally started workers (docker-compose replicas) count toward capacity
// but are never touched, so autoscaling composes with a static base pool.
type Autoscaler struct {
	cfg        AutoscalerConfig
	pool       *WorkerPool
	queueDepth func() int
	launcher   Launcher

	mu         sync.Mutex
	managed    map[string]*managedWorker
	lastLaunch time.Time
	seq        int
}

type managedWorker struct {
	launchedAt time.Time
	registered bool
	idleSince  time.Time // zero while busy or before first idle observation
}

func NewAutoscaler(cfg AutoscalerConfig, pool *WorkerPool, queueDepth func() int, launcher Launcher) *Autoscaler {
	if cfg.QueueThreshold < 1 {
		cfg.QueueThreshold = 1
	}
	if cfg.Max < cfg.Min {
		cfg.Max = cfg.Min
	}
	return &Autoscaler{
		cfg:        cfg,
		pool:       pool,
		queueDepth: queueDepth,
		launcher:   launcher,
		managed:    make(map[string]*managedWorker),
	}
}

func (a *Autoscaler) Run(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			a.tick(now)
		}
	}
}

// Shutdown stops every managed worker. Call after Run's context is canceled.
func (a *Autoscaler) Shutdown() {
	a.launcher.StopAll()
}

// ManagedCount reports how many workers the autoscaler currently tracks
// (registered plus launched-but-pending).
func (a *Autoscaler) ManagedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.managed)
}

// tick runs one policy evaluation. `now` is a parameter so tests can drive
// the policy with fabricated clocks instead of sleeping.
func (a *Autoscaler) tick(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	byID := make(map[string]WorkerMetrics)
	for _, w := range a.pool.Snapshot() {
		byID[w.ID] = w
	}
	a.reconcile(now, byID)

	alive, capacity, active := 0, 0, 0
	for _, w := range byID {
		if !w.Alive {
			continue
		}
		alive++
		capacity += w.Capacity
		active += w.ActiveTasks
	}
	pending := 0
	for _, m := range a.managed {
		if !m.registered {
			pending++
		}
	}
	effective := alive + pending
	queue := a.queueDepth()

	// Below the floor: launch without cooldown so a configured minimum pool
	// comes up promptly after boot (one per tick).
	if effective < a.cfg.Min {
		a.launch(now, "below min")
		return
	}

	// Demand-driven scale-up. Queue depth only builds when there are no alive
	// workers (workers ack ExecuteTask immediately), so saturation — every
	// alive worker at or over its declared capacity — is the load signal for
	// a nonzero pool.
	saturated := capacity > 0 && active >= capacity
	if (queue >= a.cfg.QueueThreshold || saturated) &&
		effective < a.cfg.Max &&
		now.Sub(a.lastLaunch) >= a.cfg.Cooldown {
		a.launch(now, fmt.Sprintf("queue=%d saturated=%v", queue, saturated))
		return
	}

	// Scale-down: nothing queued, capacity to spare, and a managed worker has
	// been idle past the timeout. One per tick so capacity drains gradually.
	// Stopping is safe even if a dispatch lands in the SIGTERM window: the
	// worker finishes in-flight tasks before exiting, and if it dies mid-task
	// anyway the reaper requeues the job (at-least-once holds).
	if queue == 0 && !saturated && effective > a.cfg.Min {
		if id, ok := a.pickIdle(now); ok {
			slog.Info("autoscaler: stopping idle worker",
				"worker", id, "idle_for", now.Sub(a.managed[id].idleSince))
			if err := a.launcher.StopWorker(id); err != nil {
				slog.Error("autoscaler: stop failed", "worker", id, "err", err)
				return
			}
			delete(a.managed, id)
		}
	}
}

// reconcile updates the managed set against what the pool actually sees:
// marks registrations, expires launches that never registered, forgets
// managed workers the fault detector declared dead, and tracks idleness.
func (a *Autoscaler) reconcile(now time.Time, byID map[string]WorkerMetrics) {
	for id, m := range a.managed {
		w, inPool := byID[id]
		if !m.registered {
			if inPool {
				m.registered = true
			} else if now.Sub(m.launchedAt) > a.cfg.RegisterTimeout {
				slog.Warn("autoscaler: worker never registered; giving up", "worker", id)
				_ = a.launcher.StopWorker(id)
				delete(a.managed, id)
			}
			continue
		}
		if !inPool || !w.Alive {
			// The fault detector declared it dead. StopWorker reaps the
			// process if it is somehow still running (e.g. wedged but not
			// heartbeating); either way we stop tracking it, which frees the
			// slot for a replacement launch on a later tick.
			slog.Warn("autoscaler: managed worker died", "worker", id)
			_ = a.launcher.StopWorker(id)
			delete(a.managed, id)
			continue
		}
		if w.ActiveTasks > 0 {
			m.idleSince = time.Time{}
		} else if m.idleSince.IsZero() {
			m.idleSince = now
		}
	}
}

// pickIdle returns the longest-idle managed worker past IdleTimeout, if any.
func (a *Autoscaler) pickIdle(now time.Time) (string, bool) {
	var bestID string
	var bestSince time.Time
	for id, m := range a.managed {
		if !m.registered || m.idleSince.IsZero() {
			continue
		}
		if now.Sub(m.idleSince) < a.cfg.IdleTimeout {
			continue
		}
		if bestID == "" || m.idleSince.Before(bestSince) {
			bestID = id
			bestSince = m.idleSince
		}
	}
	return bestID, bestID != ""
}

func (a *Autoscaler) launch(now time.Time, reason string) {
	a.seq++
	// PID in the ID keeps a fresh orchestrator's workers distinct from any
	// orphans of a previous run that are still heartbeating in.
	id := fmt.Sprintf("autoscale-%d-%d", os.Getpid(), a.seq)
	if err := a.launcher.LaunchWorker(id); err != nil {
		slog.Error("autoscaler: launch failed", "worker", id, "err", err)
		return
	}
	a.managed[id] = &managedWorker{launchedAt: now}
	a.lastLaunch = now
	slog.Info("autoscaler: launched worker", "worker", id, "reason", reason)
}
