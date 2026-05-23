package main

import (
	"sync"
	"testing"
)

func TestMetrics_CountersStartAtZero(t *testing.T) {
	m := NewMetrics()
	if got := m.totalInvocations.Load(); got != 0 {
		t.Errorf("invocations: want 0, got %d", got)
	}
	if got := m.totalCompleted.Load(); got != 0 {
		t.Errorf("completed: want 0, got %d", got)
	}
	if got := m.totalFailed.Load(); got != 0 {
		t.Errorf("failed: want 0, got %d", got)
	}
}

func TestMetrics_Counters_RaceFree(t *testing.T) {
	m := NewMetrics()
	const goroutines = 64
	const per = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				m.RecordInvocation()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				m.RecordCompleted()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				m.RecordFailed()
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * per)
	if got := m.totalInvocations.Load(); got != want {
		t.Errorf("invocations: want %d, got %d", want, got)
	}
	if got := m.totalCompleted.Load(); got != want {
		t.Errorf("completed: want %d, got %d", want, got)
	}
	if got := m.totalFailed.Load(); got != want {
		t.Errorf("failed: want %d, got %d", want, got)
	}
}

func TestMetrics_SnapshotPercentiles_NoSamples(t *testing.T) {
	m := NewMetrics()
	p50, p95, p99, mean, n := m.snapshotPercentiles()
	if p50 != 0 || p95 != 0 || p99 != 0 || mean != 0 || n != 0 {
		t.Errorf("empty snapshot should be zero, got p50=%d p95=%d p99=%d mean=%f n=%d",
			p50, p95, p99, mean, n)
	}
}

func TestMetrics_SnapshotPercentiles_BeforeWrap(t *testing.T) {
	m := NewMetrics()
	// 100 samples: 1..100 ms.
	for i := int64(1); i <= 100; i++ {
		m.RecordColdStart(i)
	}
	p50, p95, p99, mean, n := m.snapshotPercentiles()
	if n != 100 {
		t.Errorf("n: want 100, got %d", n)
	}
	// percentile k = round((p) * (len-1)). For len=100:
	//   p50 → k=49 → value 50
	//   p95 → k=94 → value 95
	//   p99 → k=98 → value 99
	if p50 != 50 {
		t.Errorf("p50: want 50, got %d", p50)
	}
	if p95 != 95 {
		t.Errorf("p95: want 95, got %d", p95)
	}
	if p99 != 99 {
		t.Errorf("p99: want 99, got %d", p99)
	}
	if mean != 50.5 {
		t.Errorf("mean: want 50.5, got %f", mean)
	}
}

// After the 1024-entry ring wraps, snapshotPercentiles must still report
// percentiles over the actual buffered samples, not garbage from the
// uninitialized-or-overwritten slots. (Since percentiles sort, what matters
// is that all 1024 buffer slots contain valid samples after wrap.)
func TestMetrics_SnapshotPercentiles_AfterWrap(t *testing.T) {
	m := NewMetrics()
	// Insert coldStartRingSize+500 samples. The buffer will retain the last
	// `coldStartRingSize` values, which are (501..1524).
	total := coldStartRingSize + 500
	for i := int64(1); i <= int64(total); i++ {
		m.RecordColdStart(i)
	}
	p50, p95, p99, _, n := m.snapshotPercentiles()
	if n != coldStartRingSize {
		t.Errorf("n: want %d (capped at ring size), got %d", coldStartRingSize, n)
	}
	// Retained: 501..1524.
	// p50 over 1024 sorted ints in [501, 1524]: k = round(0.5*1023) = 511 → 501+511 = 1012
	// p95: k = round(0.95*1023) = 972 → 501+972 = 1473
	// p99: k = round(0.99*1023) = 1013 → 501+1013 = 1514
	// (These hold so long as all 1024 retained slots are valid samples, i.e.,
	// the ring buffer didn't leave any zeros in place after wrap.)
	if p50 < 1000 || p50 > 1025 {
		t.Errorf("post-wrap p50 out of expected band [1000,1025]: %d", p50)
	}
	if p95 < 1450 || p95 > 1490 {
		t.Errorf("post-wrap p95 out of expected band [1450,1490]: %d", p95)
	}
	if p99 < 1500 || p99 > 1525 {
		t.Errorf("post-wrap p99 out of expected band [1500,1525]: %d", p99)
	}
}

func TestMetrics_RecordColdStart_IgnoresNegative(t *testing.T) {
	m := NewMetrics()
	m.RecordColdStart(-1)
	m.RecordColdStart(-100)
	_, _, _, _, n := m.snapshotPercentiles()
	if n != 0 {
		t.Errorf("negative samples should be dropped, got n=%d", n)
	}
}
