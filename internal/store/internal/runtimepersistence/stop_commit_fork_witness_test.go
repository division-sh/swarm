package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

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
	const columns = "insert into runs ( run_id, status, origin_kind, forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, started_at, bundle_hash, source_root_directory, flow_path ) select "
	wantArgs := 10
	if strings.HasSuffix(query, "from runs where run_id=?") {
		wantArgs = 11
	}
	if !strings.HasPrefix(query, columns) || len(args) != wantArgs {
		return stopCommitForkInsert{}, true, errors.New("fork witness INSERT binding shape changed")
	}
	if wantArgs == 11 {
		readSource, ok := stopCommitBoundText(args[10].Value)
		if !ok || readSource != source {
			return stopCommitForkInsert{}, true, errors.New("fork witness INSERT read source changed")
		}
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
	root, rootOK := stopCommitBoundText(args[9].Value)
	parsed, parseErr := uuid.Parse(child)
	if !childOK || parseErr != nil || parsed == uuid.Nil || parsed.String() != child ||
		!originOK || origin != "fork_materialization" || !kindOK || !revisionOK || !eventOK || !bundleOK || !rootOK || root == "" {
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
