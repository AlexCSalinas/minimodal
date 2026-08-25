package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// BoltStore is a thin BoltDB wrapper used as the orchestrator WAL.
//
// Phase 4 design note: we persist the full payload (function_bytes + args_bytes)
// because the WAL needs to be able to re-dispatch a job whose worker died
// or whose orchestrator restarted. JSON marshal of []byte is base64, so a
// 1KB cloudpickle payload becomes ~1.4KB on disk — fine for a single-box
// build. A production system would put payloads in object storage and
// persist only the URI here; the JobRecord would hold {payload_uri,
// payload_size, payload_sha256} so recovery can re-fetch.

const (
	bucketJobs        = "jobs"
	bucketWorkers     = "workers"
	bucketIdempotency = "idempotency"
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

	// Version increments on every write in Raft mode. RaftStore.Transition
	// runs the caller's mutator on the leader's local copy, then replicates a
	// compare-and-swap keyed on this version — the FSM rejects the write if
	// another command got there first, and the leader re-reads and retries.
	// Zero (and ignored) in plain BoltStore mode, where BoltDB transactions
	// provide the atomicity instead.
	Version int64 `json:"version,omitempty"`
}

// Store is the abstract interface every JobStore implementation satisfies.
// In single-node mode that's BoltStore (local BoltDB). In multi-node mode
// it's RaftStore, which wraps a hashicorp/raft node whose FSM is a BoltStore.
type Store interface {
	PutJob(rec JobRecord) error
	GetJob(id string) (*JobRecord, error)
	Transition(jobID string, mutate func(*JobRecord) error) (bool, error)
	SubmitWithIdempotency(rec JobRecord, idempotencyKey string) (string, bool, error)
	ListUnfinished() ([]JobRecord, error)
	ListRunningOnWorker(workerID string) ([]JobRecord, error)
	Ping() error
	Close() error
}

// BoltStore is the single-node Store implementation backed by a local BoltDB
// file. Used directly by single-node deployments and embedded inside the
// Raft FSM in multi-node deployments.
type BoltStore struct {
	db *bolt.DB
}

// Compile-time assertion that BoltStore satisfies Store.
var _ Store = (*BoltStore)(nil)

func NewBoltStore(path string) (*BoltStore, error) {
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
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

// Ping does a trivial read transaction to confirm the BoltDB handle is
// responsive. Used by /healthz so the health probe catches a wedged store
// rather than returning a hollow 200 OK.
func (s *BoltStore) Ping() error {
	return s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket([]byte(bucketJobs))
		return nil
	})
}

func (s *BoltStore) PutJob(rec JobRecord) error {
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

func (s *BoltStore) GetJob(id string) (*JobRecord, error) {
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
func (s *BoltStore) Transition(jobID string, mutate func(*JobRecord) error) (bool, error) {
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
func (s *BoltStore) ListUnfinished() ([]JobRecord, error) {
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
//     dispatch), false if the key matched an existing job (caller
//     should NOT enqueue — the existing job is already being
//     processed or has already finished)
func (s *BoltStore) SubmitWithIdempotency(rec JobRecord, idempotencyKey string) (jobID string, created bool, err error) {
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

// =============================================================================
// Raft FSM support — raw variants that write records verbatim.
//
// The public methods above stamp UpdatedAt with the local clock, which is
// fine for a single node but non-deterministic across a Raft cluster: every
// replica must end up with byte-identical state from the same command, so
// timestamps are stamped ONCE by the leader at propose time and applied
// verbatim here.
// =============================================================================

// putJobRaw writes a record exactly as given (no timestamp or version
// stamping). FSM use only.
func (s *BoltStore) putJobRaw(rec JobRecord) error {
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketJobs)).Put([]byte(rec.ID), buf)
	})
}

// casPut writes a record iff the stored version still matches expected.
// Returns conflict=true (and writes nothing) when another command won the
// race. A missing job is an error — Transition callers verified existence.
func (s *BoltStore) casPut(jobID string, expectedVersion int64, rec JobRecord) (conflict bool, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketJobs))
		raw := bucket.Get([]byte(jobID))
		if raw == nil {
			return fmt.Errorf("job %s not found", jobID)
		}
		var cur JobRecord
		if err := json.Unmarshal(raw, &cur); err != nil {
			return err
		}
		if cur.Version != expectedVersion {
			conflict = true
			return nil
		}
		buf, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(jobID), buf)
	})
	return conflict, err
}

// submitRaw is SubmitWithIdempotency without timestamp stamping — the record
// arrives fully stamped by the leader. FSM use only.
func (s *BoltStore) submitRaw(rec JobRecord, idempotencyKey string) (jobID string, created bool, err error) {
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

// exportState dumps everything a Raft snapshot needs to reconstruct this
// store: all job records plus the idempotency index.
func (s *BoltStore) exportState() (jobs []JobRecord, idem map[string]string, err error) {
	idem = make(map[string]string)
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte(bucketJobs)).ForEach(func(k, v []byte) error {
			var rec JobRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			jobs = append(jobs, rec)
			return nil
		}); err != nil {
			return err
		}
		return tx.Bucket([]byte(bucketIdempotency)).ForEach(func(k, v []byte) error {
			idem[string(k)] = string(v)
			return nil
		})
	})
	return jobs, idem, err
}

// importState replaces this store's contents with a snapshot's. Used by the
// Raft FSM's Restore when a node catches up from a snapshot.
func (s *BoltStore) importState(jobs []JobRecord, idem map[string]string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{bucketJobs, bucketIdempotency} {
			if err := tx.DeleteBucket([]byte(name)); err != nil {
				return err
			}
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		jobsBucket := tx.Bucket([]byte(bucketJobs))
		for _, rec := range jobs {
			buf, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if err := jobsBucket.Put([]byte(rec.ID), buf); err != nil {
				return err
			}
		}
		idxBucket := tx.Bucket([]byte(bucketIdempotency))
		for k, v := range idem {
			if err := idxBucket.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListRunningOnWorker returns all jobs currently RUNNING on a given worker.
// Used by the reaper when a worker is declared dead.
func (s *BoltStore) ListRunningOnWorker(workerID string) ([]JobRecord, error) {
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
