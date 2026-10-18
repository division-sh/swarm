package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// This fixed-purpose fixture gates one exact attachment UPDATE on a real native
// transaction. It observes its terminal outcome, not transaction-count budgets.
type attachmentProgressNativeBarrier struct {
	mu                                     sync.Mutex
	runID, path, attemptID, previous, hash string
	armed                                  bool
	target                                 *attachmentProgressNativeTx
	entered                                chan context.Context
	release                                chan struct{}
	releaseOnce                            sync.Once
}

func (b *attachmentProgressNativeBarrier) arm(attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, hash string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runID, b.path, b.attemptID = attempt.RunID(), attempt.InstancePath(), attempt.ID()
	b.previous, b.hash, b.armed = string(previous), hash, true
}

func (b *attachmentProgressNativeBarrier) claim(query string, args []driver.NamedValue, tx *attachmentProgressNativeTx) bool {
	const postgres = `UPDATE flow_instance_runtime_readiness SET phase=$1,updated_at=$2 WHERE run_id=$3::uuid AND instance_path=$4 AND activation_attempt_id=$5 AND phase=$6 AND plan_hash=$7`
	const sqlite = `UPDATE flow_instance_runtime_readiness SET phase=?,updated_at=? WHERE run_id=? AND instance_path=? AND activation_attempt_id=? AND phase=? AND plan_hash=?`
	if tx == nil || (query != postgres && query != sqlite) || len(args) != 7 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.armed || b.target != nil || args[0].Value != string(pipeline.FlowAttachmentAgentsRegistered) ||
		args[2].Value != b.runID || args[3].Value != b.path || args[4].Value != b.attemptID ||
		args[5].Value != b.previous || args[6].Value != b.hash {
		return false
	}
	b.armed, b.target = false, tx
	return true
}

func (b *attachmentProgressNativeBarrier) releaseStatement() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *attachmentProgressNativeBarrier) matchedStatement() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.target != nil
}

func (b *attachmentProgressNativeBarrier) joinRollback(ctx context.Context) error {
	b.mu.Lock()
	tx := b.target
	b.mu.Unlock()
	if tx == nil {
		return errors.New("exact attachment transaction was not observed")
	}
	select {
	case <-tx.rolledBack:
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if tx.commitCalled {
			return errors.New("cancelled statement unexpectedly attempted COMMIT")
		}
		return tx.rollbackErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

type attachmentProgressConnector struct {
	driver.Connector
	barrier *attachmentProgressNativeBarrier
}

func (c attachmentProgressConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &attachmentProgressConn{diagnosticSQLConn: diagnosticSQLConn{Conn: conn}, barrier: c.barrier}, nil
}

type attachmentProgressConn struct {
	diagnosticSQLConn
	barrier *attachmentProgressNativeBarrier
	mu      sync.Mutex
	active  *attachmentProgressNativeTx
}

func (c *attachmentProgressConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.diagnosticSQLConn.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	wrapped := &attachmentProgressNativeTx{Tx: tx, rolledBack: make(chan struct{})}
	c.mu.Lock()
	c.active = wrapped
	c.mu.Unlock()
	return wrapped, nil
}

func (c *attachmentProgressConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	tx := c.active
	c.mu.Unlock()
	if c.barrier.claim(query, args, tx) {
		c.barrier.entered <- ctx
		<-c.barrier.release
	}
	return c.diagnosticSQLConn.ExecContext(ctx, query, args)
}

type attachmentProgressNativeTx struct {
	driver.Tx
	mu           sync.Mutex
	rolledBack   chan struct{}
	rollbackOnce sync.Once
	rollbackErr  error
	commitCalled bool
}

func (tx *attachmentProgressNativeTx) Commit() error {
	tx.mu.Lock()
	tx.commitCalled = true
	tx.mu.Unlock()
	return tx.Tx.Commit()
}

func (tx *attachmentProgressNativeTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.mu.Lock()
	tx.rollbackErr = err
	tx.mu.Unlock()
	tx.rollbackOnce.Do(func() { close(tx.rolledBack) })
	return err
}

func newAttachmentProgressBarrierStore(t *testing.T, backend string) (agentFixtureFlowStore, *attachmentProgressNativeBarrier) {
	t.Helper()
	var connector driver.Connector
	if backend == "postgres" {
		dsn, _, _ := testutil.StartPostgres(t)
		var err error
		connector, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		path := filepath.Join(t.TempDir(), "attachment-shutdown.sqlite")
		newBootstrappedSQLiteRuntimeStoreForPath(t, path)
		connector = diagnosticSQLiteConnector{path: "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"}
	}
	barrier := &attachmentProgressNativeBarrier{entered: make(chan context.Context, 1), release: make(chan struct{})}
	db := sql.OpenDB(attachmentProgressConnector{Connector: connector, barrier: barrier})
	db.SetMaxOpenConns(12)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	selected, ok := newStopCommitStoreOwner(t, backend, db).(agentFixtureFlowStore)
	if !ok {
		t.Fatal("attachment cancellation requires the original native fixture owner")
	}
	return selected, barrier
}
