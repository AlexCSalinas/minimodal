package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is the orchestrator's in-process telemetry. Counters use atomics so
// they can be touched from any goroutine without locking. The cold-start ring
// buffer (last N samples) is protected by a mutex — we only sort it on
// Snapshot, which happens on /metrics polls (~1/s), so contention is low.
type Metrics struct {
	totalInvocations atomic.Int64
	totalCompleted   atomic.Int64
	totalFailed      atomic.Int64

	coldStartsMu sync.Mutex
	coldStarts   []int64 // ring buffer of recent cold_start_ms samples
	coldHead     int     // next write index
	coldSize     int     // current number of valid entries (capped at len(coldStarts))

	startedAt time.Time
}

const coldStartRingSize = 1024

func NewMetrics() *Metrics {
	return &Metrics{
		coldStarts: make([]int64, coldStartRingSize),
		startedAt:  time.Now(),
	}
}

func (m *Metrics) RecordInvocation() { m.totalInvocations.Add(1) }
func (m *Metrics) RecordCompleted()  { m.totalCompleted.Add(1) }
func (m *Metrics) RecordFailed()     { m.totalFailed.Add(1) }

func (m *Metrics) RecordColdStart(ms int64) {
	if ms < 0 {
		return
	}
	m.coldStartsMu.Lock()
	defer m.coldStartsMu.Unlock()
	m.coldStarts[m.coldHead] = ms
	m.coldHead = (m.coldHead + 1) % len(m.coldStarts)
	if m.coldSize < len(m.coldStarts) {
		m.coldSize++
	}
}

// MetricsSnapshot is what the /metrics endpoint serializes.
type MetricsSnapshot struct {
	UptimeS          int64           `json:"uptime_s"`
	ActiveWorkers    int             `json:"active_workers"`
	TotalWorkers     int             `json:"total_workers"`
	QueuedJobs       int             `json:"queued_jobs"`
	TotalInvocations int64           `json:"total_invocations"`
	TotalCompleted   int64           `json:"total_completed"`
	TotalFailed      int64           `json:"total_failed"`
	InFlightJobs     int64           `json:"in_flight_jobs"`
	ErrorRate        float64         `json:"error_rate"`
	ColdStartSamples int             `json:"cold_start_samples"`
	P50ColdStartMs   int64           `json:"p50_cold_start_ms"`
	P95ColdStartMs   int64           `json:"p95_cold_start_ms"`
	P99ColdStartMs   int64           `json:"p99_cold_start_ms"`
	MeanColdStartMs  float64         `json:"mean_cold_start_ms"`
	Workers          []WorkerMetrics `json:"workers"`
}

func (m *Metrics) snapshotPercentiles() (p50, p95, p99 int64, mean float64, n int) {
	m.coldStartsMu.Lock()
	defer m.coldStartsMu.Unlock()
	if m.coldSize == 0 {
		return 0, 0, 0, 0, 0
	}
	sorted := make([]int64, m.coldSize)
	copy(sorted, m.coldStarts[:m.coldSize])
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	pick := func(p float64) int64 {
		k := int(float64(len(sorted)-1) * p)
		return sorted[k]
	}
	return pick(0.50), pick(0.95), pick(0.99), float64(sum) / float64(len(sorted)), len(sorted)
}
