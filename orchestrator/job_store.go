package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// JobStore is a thin BoltDB wrapper used as the orchestrator WAL.
//
// Phase 4 design note: we persist the full payload (function_bytes + args_bytes)
// because the WAL needs to be able to re-dispatch a job whose worker died
// or whose orchestrator restarted. JSON marshal of []byte is base64, so a
// 1KB cloudpickle payload becomes ~1.4KB on disk — fine for a single-box
// build. A production system would put payloads in object storage and
// persist only the URI here; the JobRecord would hold {payload_uri,
// payload_size, payload_sha256} so recovery can re-fetch.

const (
	bucketJobs         = "jobs"
	bucketWorkers      = "workers"
	bucketIdempotency  = "idempotency"
)

type JobStatus string

const (
	StatusPending JobStatus = "PENDING"
	StatusRunning JobStatus = "RUNNING"
	StatusDone    JobStatus = "DONE"
	StatusFailed  JobStatus = "FAILED"
)

type JobRecord struct {
	ID           string    `json:"id"`
	Status       JobStatus `json:"status"`
	WorkerID     string    `json:"worker_id,omitempty"`
	FunctionName string    `json:"function_name"`

	// Payload — persisted so worker death or orchestrator restart can
	// re-dispatch. Cleared when the job reaches a terminal state to bound
	// WAL growth.
	FunctionBytes []byte `json:"function_bytes,omitempty"`
	ArgsBytes     []byte `json:"args_bytes,omitempty"`

	// Result bytes — persisted on terminal transition so GetJobStatus
	// returns the right answer after an orchestrator restart (important
	// for idempotency: a client retrying with the same key after a restart
	// should get back the original result, not None).
	ResultBytes []byte `json:"result_bytes,omitempty"`
	ColdStartMs int64  `json:"cold_start_ms,omitempty"`
	ExecutionMs int64  `json:"execution_ms,omitempty"`

	// Caller-supplied key for idempotency — see SubmitWithIdempotency.
	IdempotencyKey string `json:"idempotency_key,omitempty"`

	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	RetryCount int       `json:"retry_count"`
	Error      string    `json:"error,omitempty"`
}

type JobStore struct {
	db *bolt.DB
}

func NewJobStore(path string) (*JobStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open boltdb at %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{bucketJobs, bucketWorkers, bucketIdempotency} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &JobStore{db: db}, nil
}

func (s *JobStore) Close() error {
	return s.db.Close()
}

func (s *JobStore) PutJob(rec JobRecord) error {
	rec.UpdatedAt = time.Now()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = rec.UpdatedAt
	}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketJobs)).Put([]byte(rec.ID), buf)
	})
}

func (s *JobStore) GetJob(id string) (*JobRecord, error) {
	var rec *JobRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(bucketJobs)).Get([]byte(id))
		if raw == nil {
			return nil
		}
		rec = &JobRecord{}
		return json.Unmarshal(raw, rec)
	})
	return rec, err
}

// Transition atomically reads, mutates, and writes back a JobRecord in a
// single BoltDB transaction. Use this for any state change so concurrent
// writers (scheduler vs ReportTaskResult vs reaper) don't clobber each other.
//
// The mutator may return ErrSkipTransition to abort the write while leaving
// the existing record intact (e.g. ReportTaskResult sees the job is already
// terminal — leave it alone). The first return value is true iff the mutator
// ran cleanly AND the write committed; callers use this to detect "I lost
// the race" situations.
func (s *JobStore) Transition(jobID string, mutate func(*JobRecord) error) (bool, error) {
	var committed bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketJobs))
		raw := bucket.Get([]byte(jobID))
		if raw == nil {
			return fmt.Errorf("job %s not found", jobID)
		}
		var rec JobRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return err
		}
		if err := mutate(&rec); err != nil {
			if err == ErrSkipTransition {
				return nil
			}
			return err
		}
		rec.UpdatedAt = time.Now()
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = rec.UpdatedAt
		}
		buf, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(jobID), buf); err != nil {
			return err
		}
		committed = true
		return nil
	})
	return committed, err
}

var ErrSkipTransition = errors.New("skip transition")

// ListUnfinished returns all jobs not in DONE/FAILED state. Used by Phase 4
// WAL replay on orchestrator restart.
func (s *JobStore) ListUnfinished() ([]JobRecord, error) {
	var out []JobRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketJobs)).ForEach(func(k, v []byte) error {
			var rec JobRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if rec.Status == StatusPending || rec.Status == StatusRunning {
				out = append(out, rec)
			}
			return nil
		})
	})
	return out, err
}

// SubmitWithIdempotency atomically inserts a new JobRecord, or — if
// idempotencyKey is non-empty and an existing job already has that key —
// returns the existing job's ID without creating a new record. The whole
// check-and-set runs in a single BoltDB Update transaction, so two concurrent
// submits with the same key are linearizable: the first one wins, the second
// one sees the existing mapping and returns its ID.
//
// Return values:
//   - jobID:   the ID the caller should treat as authoritative
//   - created: true if a new job was inserted (caller should enqueue for
//              dispatch), false if the key matched an existing job (caller
//              should NOT enqueue — the existing job is already being
//              processed or has already finished)
func (s *JobStore) SubmitWithIdempotency(rec JobRecord, idempotencyKey string) (jobID string, created bool, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket([]byte(bucketJobs))
		idx := tx.Bucket([]byte(bucketIdempotency))
		if idempotencyKey != "" {
			if existing := idx.Get([]byte(idempotencyKey)); existing != nil {
				jobID = string(existing)
				created = false
				return nil
			}
		}
		rec.IdempotencyKey = idempotencyKey
		rec.UpdatedAt = time.Now()
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = rec.UpdatedAt
		}
		buf, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := jobs.Put([]byte(rec.ID), buf); err != nil {
			return err
		}
		if idempotencyKey != "" {
			if err := idx.Put([]byte(idempotencyKey), []byte(rec.ID)); err != nil {
				return err
			}
		}
		jobID = rec.ID
		created = true
		return nil
	})
	return
}

// ListRunningOnWorker returns all jobs currently RUNNING on a given worker.
// Used by the reaper when a worker is declared dead.
func (s *JobStore) ListRunningOnWorker(workerID string) ([]JobRecord, error) {
	var out []JobRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketJobs)).ForEach(func(k, v []byte) error {
			var rec JobRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if rec.Status == StatusRunning && rec.WorkerID == workerID {
				out = append(out, rec)
			}
			return nil
		})
	})
	return out, err
}
