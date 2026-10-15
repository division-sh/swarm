package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type stopCommitForkInsert struct {
	RunID       string
	SourceRunID string
	BundleHash  string
	PointKind   runfork.RunForkPointKind
	Revision    int64
	EventID     string
}

// This is attempted-row evidence only: it makes no claim about COMMIT.
type stopCommitForkWitness struct {
	mu       sync.Mutex
	expected stopCommitForkInsert
	observed stopCommitForkInsert
	tx       *stopCommitTx
	err      error
}

func (c *stopCommitConnector) armForkInsert(sourceRunID string, point runfork.RunForkPoint, bundleHash string) *stopCommitForkWitness {
	witness := &stopCommitForkWitness{expected: stopCommitForkInsert{
		SourceRunID: sourceRunID, BundleHash: bundleHash,
		PointKind: point.Kind, Revision: point.Revision, EventID: point.EventID,
	}}
	parsed, err := uuid.Parse(sourceRunID)
	if err != nil || parsed == uuid.Nil || parsed.String() != sourceRunID || bundleHash == "" || point.Validate() != nil {
		witness.err = errors.New("fork witness requires an exact source/cut/bundle")
	}
	c.mu.Lock()
	c.fork = witness
	c.mu.Unlock()
	return witness
}

func (c *stopCommitConn) observeTransaction(native driver.Tx, readOnly bool) *stopCommitTx {
	c.owner.mu.Lock()
	witness := c.owner.fork
	c.owner.mu.Unlock()
	tx := &stopCommitTx{Tx: native, owner: c.owner, conn: c, fork: witness, readOnly: readOnly}
	c.tx = tx
	return tx
}

func (c *stopCommitConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Copy the selected bound values before the driver can reuse argument bytes.
	tx := c.tx
	var observed stopCommitForkInsert
	var candidate bool
	var observationErr error
	if tx != nil && tx.fork != nil {
		observed, candidate, observationErr = tx.fork.decode(query, args)
	}
	result, err := c.diagnosticSQLConn.ExecContext(ctx, query, args)
	if err == nil && candidate {
		if observationErr == nil {
			var affected int64
			affected, observationErr = result.RowsAffected()
			if observationErr == nil && affected != 1 {
				observationErr = fmt.Errorf("fork witness requires one successful inserted row, got %d", affected)
			}
		}
		tx.fork.record(tx, observed, observationErr)
	}
	return result, err
}

func stopCommitBoundText(value driver.Value) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func (w *stopCommitForkWitness) decode(query string, args []driver.NamedValue) (stopCommitForkInsert, bool, error) {
	query = strings.Join(strings.Fields(strings.ToLower(query)), " ")
	if !strings.HasPrefix(query, "insert into runs (") || !strings.Contains(query, "forked_from_run_id") {
		return stopCommitForkInsert{}, false, nil
	}
	if len(args) < 4 {
		return stopCommitForkInsert{}, true, errors.New("fork witness cannot identify INSERT source")
	}
	source, sourceOK := stopCommitBoundText(args[3].Value)
	if !sourceOK {
		return stopCommitForkInsert{}, true, errors.New("fork witness source has unexpected bound type")
	}
	if source != w.expected.SourceRunID {
		return stopCommitForkInsert{}, false, nil
	}
	const columns = "insert into runs ( run_id, status, origin_kind, forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, event_count, started_at, bundle_hash ) values ("
	if !strings.HasPrefix(query, columns) || len(args) != 9 {
		return stopCommitForkInsert{}, true, errors.New("fork witness INSERT binding shape changed")
	}
	for index, arg := range args {
		if arg.Ordinal != index+1 {
			return stopCommitForkInsert{}, true, errors.New("fork witness INSERT binding order changed")
		}
	}
	child, childOK := stopCommitBoundText(args[0].Value)
	origin, originOK := stopCommitBoundText(args[2].Value)
	kind, kindOK := stopCommitBoundText(args[4].Value)
	revision, revisionOK := args[5].Value.(int64)
	event, eventOK := stopCommitBoundText(args[6].Value)
	bundle, bundleOK := stopCommitBoundText(args[8].Value)
	parsed, parseErr := uuid.Parse(child)
	if !childOK || parseErr != nil || parsed == uuid.Nil || parsed.String() != child ||
		!originOK || origin != "fork_materialization" || !kindOK || !revisionOK || !eventOK || !bundleOK {
		return stopCommitForkInsert{}, true, errors.New("fork witness INSERT has invalid bound coordinates")
	}
	observed := stopCommitForkInsert{RunID: child, SourceRunID: source, BundleHash: bundle,
		PointKind: runfork.RunForkPointKind(kind), Revision: revision, EventID: event}
	comparison := observed
	comparison.RunID = ""
	if comparison != w.expected {
		return stopCommitForkInsert{}, true, errors.New("fork witness INSERT disagrees with its armed source/cut/bundle")
	}
	return observed, true, nil
}

func (w *stopCommitForkWitness) record(tx *stopCommitTx, observed stopCommitForkInsert, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx != nil {
		w.err = errors.Join(w.err, errors.New("fork witness observed duplicate INSERTs or transactions"))
		return
	}
	w.tx, w.observed, w.err = tx, observed, errors.Join(w.err, err)
}

func (w *stopCommitForkWitness) attempted() (stopCommitForkInsert, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return stopCommitForkInsert{}, w.err
	}
	if w.tx == nil {
		return stopCommitForkInsert{}, errors.New("fork witness has no successful INSERT")
	}
	return w.observed, nil
}

type stopCommitWitnessNativeTx struct {
	commits, rollbacks int
	commitErr          error
}

func (tx *stopCommitWitnessNativeTx) Commit() error   { tx.commits++; return tx.commitErr }
func (tx *stopCommitWitnessNativeTx) Rollback() error { tx.rollbacks++; return nil }

type stopCommitWitnessNativeConn struct {
	driver.Conn
	err   error
	rows  int64
	calls int
	query string
	args  []driver.NamedValue
}

func (c *stopCommitWitnessNativeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.calls++
	c.query, c.args = query, args
	return driver.RowsAffected(c.rows), c.err
}

func TestStopCommitForkWitnessObservesOnlySuccessfulExactTransaction(t *testing.T) {
	const query = `INSERT INTO runs (
		run_id, status, origin_kind, forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id,
		event_count, started_at, bundle_hash
	) VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), 0, ?, ?)`
	source, child, event := uuid.NewString(), uuid.NewString(), uuid.NewString()
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 7, EventID: event}
	const bundle = "witness-bundle"
	for _, name := range []string{"success", "unarmed", "outside_transaction", "armed_after_begin", "foreign_source", "failed_statement", "no_inserted_row", "duplicate", "duplicate_other_connection", "changed_cut", "changed_bundle", "changed_shape", "mutable_arguments", "rollback", "commit", "failed_commit", "lost_ack", "read_only_commit"} {
		t.Run(name, func(t *testing.T) {
			native := &stopCommitWitnessNativeConn{rows: 1}
			owner := &stopCommitConnector{}
			conn := &stopCommitConn{diagnosticSQLConn: diagnosticSQLConn{Conn: native}, owner: owner}
			var tx *stopCommitTx
			if name == "armed_after_begin" {
				tx = conn.observeTransaction(&stopCommitWitnessNativeTx{}, false)
			}
			witness := owner.armForkInsert(source, point, bundle)
			if name != "outside_transaction" && tx == nil {
				tx = conn.observeTransaction(&stopCommitWitnessNativeTx{}, false)
			}
			if name == "unarmed" {
				tx.fork = nil
			}
			values := []driver.Value{child, "paused", "fork_materialization", source, string(point.Kind), point.Revision, event, time.Now().UTC(), bundle}
			args := make([]driver.NamedValue, len(values))
			for index, value := range values {
				args[index] = driver.NamedValue{Ordinal: index + 1, Value: value}
			}
			statement := query
			switch name {
			case "foreign_source":
				args[3].Value = uuid.NewString()
			case "failed_statement":
				native.err = errors.New("native INSERT rejected")
			case "no_inserted_row":
				native.rows = 0
			case "changed_cut":
				args[5].Value = point.Revision + 1
			case "changed_bundle":
				args[8].Value = "foreign-bundle"
			case "changed_shape":
				args = args[:8]
			case "mutable_arguments":
				args[0].Value, args[3].Value, args[8].Value = []byte(child), []byte(source), []byte(bundle)
			}
			result, err := conn.ExecContext(context.Background(), statement, args)
			if !errors.Is(err, native.err) || result == nil || native.calls != 1 || native.query != statement || &native.args[0] != &args[0] {
				t.Fatalf("witness changed native SQL/result: calls=%d result=%v err=%v", native.calls, result, err)
			}
			if name == "mutable_arguments" {
				for _, arg := range args {
					if bytes, ok := arg.Value.([]byte); ok {
						bytes[0] = '!'
					}
				}
			}
			if name == "duplicate_other_connection" {
				conn = &stopCommitConn{diagnosticSQLConn: diagnosticSQLConn{Conn: native}, owner: owner}
				conn.observeTransaction(&stopCommitWitnessNativeTx{}, false)
			}
			if name == "duplicate" || name == "duplicate_other_connection" {
				if _, err := conn.ExecContext(context.Background(), statement, args); err != nil {
					t.Fatal(err)
				}
			}
			if name == "rollback" {
				if err := tx.Rollback(); err != nil || conn.tx != nil {
					t.Fatalf("rollback did not retire transaction witness: %v", err)
				}
			}
			if name == "commit" || name == "failed_commit" || name == "lost_ack" || name == "read_only_commit" {
				nativeTx := tx.Tx.(*stopCommitWitnessNativeTx)
				fault := errors.New("native commit acknowledgement unavailable")
				if name == "failed_commit" {
					nativeTx.commitErr = fault
				}
				if name == "lost_ack" {
					owner.arm(func(native driver.Tx) error {
						if _, err := witness.attempted(); err != nil {
							t.Fatal(err)
						}
						return errors.Join(fault, native.Commit())
					})
				}
				tx.readOnly = name == "read_only_commit"
				err := tx.Commit()
				wantWriteCommits := int32(1)
				if tx.readOnly {
					wantWriteCommits = 0
				}
				if wantFault := name == "failed_commit" || name == "lost_ack"; errors.Is(err, fault) != wantFault ||
					nativeTx.commits != 1 || owner.commits.Load() != 1 || owner.writeCommits.Load() != wantWriteCommits || conn.tx != nil {
					t.Fatalf("native commit delegation changed: err=%v calls=%d commit count=%d write count=%d", err, nativeTx.commits, owner.commits.Load(), owner.writeCommits.Load())
				}
			}
			got, observationErr := witness.attempted()
			want := stopCommitForkInsert{RunID: child, SourceRunID: source, BundleHash: bundle, PointKind: point.Kind, Revision: point.Revision, EventID: event}
			positive := name == "success" || name == "mutable_arguments" || name == "rollback" || name == "commit" || name == "failed_commit" || name == "lost_ack" || name == "read_only_commit"
			if positive && (observationErr != nil || got != want) || !positive && (observationErr == nil || got != (stopCommitForkInsert{})) {
				t.Fatalf("attempted INSERT witness: got=%+v err=%v positive=%t", got, observationErr, positive)
			}
		})
	}
}
