package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// Reaper is what WorkerPool calls when a worker is declared dead. Implemented
// by Server in server.go — looks up in-flight jobs and re-enqueues them.
type Reaper interface {
	ReassignJobsOf(workerID string)
}

// WorkerPool tracks registered workers and their liveness.
type WorkerPool struct {
	timeout time.Duration
	mu      sync.RWMutex
	workers map[string]*workerState
	reaper  Reaper // set after Server is built; nil-safe in sweep()
}

type workerState struct {
	id            string
	address       string
	capacity      int
	activeTasks   int
	registeredAt  time.Time
	lastHeartbeat time.Time
	alive         bool
}

func NewWorkerPool(timeout time.Duration) *WorkerPool {
	return &WorkerPool{
		timeout: timeout,
		workers: make(map[string]*workerState),
	}
}

func (p *WorkerPool) SetReaper(r Reaper) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reaper = r
}

func (p *WorkerPool) Register(id, address string, capacity int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers[id] = &workerState{
		id:            id,
		address:       address,
		capacity:      capacity,
		registeredAt:  time.Now(),
		lastHeartbeat: time.Now(),
		alive:         true,
	}
}

// RecordHeartbeat returns true if the worker is known and still alive.
// Returns false if the worker is unknown (in which case caller should signal drain).
func (p *WorkerPool) RecordHeartbeat(id string, activeTasks int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[id]
	if !ok {
		return false
	}
	if !w.alive {
		w.alive = true
		log.Printf("worker %s resurrected (was marked dead)", id)
	}
	w.lastHeartbeat = time.Now()
	w.activeTasks = activeTasks
	return true
}

func (p *WorkerPool) LeastLoadedAlive() (id, address string, ok bool) {
	return p.LeastLoadedAliveExcluding(nil)
}

func (p *WorkerPool) LeastLoadedAliveExcluding(exclude map[string]bool) (id, address string, ok bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var best *workerState
	for _, w := range p.workers {
		if !w.alive {
			continue
		}
		if exclude[w.id] {
			continue
		}
		if best == nil || w.activeTasks < best.activeTasks {
			best = w
		}
	}
	if best == nil {
		return "", "", false
	}
	return best.id, best.address, true
}

// RunFaultDetector scans the pool every second and marks dead workers.
// For each newly-dead worker, it calls the reaper *outside* the pool lock —
// the reaper hits BoltDB and the dispatch queue, which would deadlock if it
// tried to re-acquire WorkerPool.mu.
func (p *WorkerPool) RunFaultDetector(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dead := p.sweep()
			for _, id := range dead {
				log.Printf("worker %s marked DEAD", id)
				if reaper := p.getReaper(); reaper != nil {
					reaper.ReassignJobsOf(id)
				}
			}
		}
	}
}

func (p *WorkerPool) sweep() []string {
	var dead []string
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for id, w := range p.workers {
		if !w.alive {
			continue
		}
		if now.Sub(w.lastHeartbeat) > p.timeout {
			w.alive = false
			dead = append(dead, id)
		}
	}
	return dead
}

func (p *WorkerPool) getReaper() Reaper {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.reaper
}

func (p *WorkerPool) AliveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, w := range p.workers {
		if w.alive {
			n++
		}
	}
	return n
}

// WorkerMetrics is a read-only snapshot of one worker's state, surfaced via
// the /metrics endpoint for the dashboard's heatmap.
type WorkerMetrics struct {
	ID              string  `json:"id"`
	Address         string  `json:"address"`
	Alive           bool    `json:"alive"`
	ActiveTasks     int     `json:"active_tasks"`
	Capacity        int     `json:"capacity"`
	Utilization     float64 `json:"utilization"`
	LastHeartbeatS  int64   `json:"last_heartbeat_seconds_ago"`
	RegisteredAtISO string  `json:"registered_at_iso"`
}

func (p *WorkerPool) Snapshot() []WorkerMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	out := make([]WorkerMetrics, 0, len(p.workers))
	for _, w := range p.workers {
		util := 0.0
		if w.capacity > 0 {
			util = float64(w.activeTasks) / float64(w.capacity)
		}
		out = append(out, WorkerMetrics{
			ID:              w.id,
			Address:         w.address,
			Alive:           w.alive,
			ActiveTasks:     w.activeTasks,
			Capacity:        w.capacity,
			Utilization:     util,
			LastHeartbeatS:  int64(now.Sub(w.lastHeartbeat) / time.Second),
			RegisteredAtISO: w.registeredAt.UTC().Format(time.RFC3339),
		})
	}
	return out
}
