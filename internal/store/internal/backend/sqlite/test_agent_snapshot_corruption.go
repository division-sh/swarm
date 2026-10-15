package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (b *Backend) EnableOperatorSnapshotWALForTest(ctx context.Context) error {
	if !b.Valid() {
		return errors.New("operator snapshot WAL requires the original backend")
	}
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("operator snapshot WAL requires a caller context")
	}
	if err := b.acquireMutation(ctx, mutationOperation{label: "operator snapshot WAL", phase: mutationPhaseFirstAttempt, attempt: 1}); err != nil {
		return err
	}
	defer b.releaseMutation()
	var mode string
	if err := b.db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode); err != nil {
		return err
	}
	if mode != "wal" {
		return fmt.Errorf("operator snapshot requires WAL, got %q", mode)
	}
	return nil
}

// These two fixture entry points keep connection-local corruption settings
// inside this backend's existing writer admission and transaction cleanup.
func (b *Backend) RunAgentSnapshotRouteCorruptionForTest(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
	return b.runAgentSnapshotCorruption(ctx, true, write)
}

func (b *Backend) RunAgentSnapshotStatusCorruptionForTest(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
	return b.runAgentSnapshotCorruption(ctx, false, write)
}

func (b *Backend) runAgentSnapshotCorruption(ctx context.Context, foreign bool, write func(context.Context, *sql.Tx) error) (bool, error) {
	if !b.Valid() || write == nil {
		return false, errors.New("agent snapshot corruption requires the original backend and a fixed owner operation")
	}
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writeObservation := b.agentRecoveryWrites.ObserveWriteStart()
	defer writeObservation.End()
	operation := mutationOperation{label: "agent snapshot corruption fixture", phase: mutationPhaseFirstAttempt, attempt: 1}
	started := time.Now()
	if err := b.acquireMutation(ctx, operation); err != nil {
		return false, err
	}
	defer b.releaseMutation()
	scope := &agentSnapshotConstraintScope{foreign: foreign}
	committed, err := b.runTransactionWithConnectionScope(ctx, nil, time.Since(started), scope, write)
	if committed {
		return true, err
	}
	return false, errors.Join(ctx.Err(), err)
}

type agentSnapshotConstraintScope struct {
	foreign bool
	armed   bool
}

func (s *agentSnapshotConstraintScope) prepare(ctx context.Context, conn *sql.Conn) error {
	if err := requireAgentSnapshotConstraints(ctx, conn); err != nil {
		return err
	}
	// Cleanup is already registered by the transaction owner, including a
	// partially acknowledged PRAGMA or cancellation before BeginTx.
	s.armed = true
	if s.foreign {
		if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
			return err
		}
	}
	_, err := conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=ON`)
	return err
}

func (s *agentSnapshotConstraintScope) cleanup(conn *sql.Conn) error {
	if !s.armed {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, checkErr := conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=OFF`)
	var foreignErr error
	if s.foreign {
		_, foreignErr = conn.ExecContext(ctx, `PRAGMA foreign_keys=ON`)
	}
	return errors.Join(checkErr, foreignErr, requireAgentSnapshotConstraints(ctx, conn))
}

func requireAgentSnapshotConstraints(ctx context.Context, conn *sql.Conn) error {
	var foreign, ignored int
	if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreign); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA ignore_check_constraints`).Scan(&ignored); err != nil {
		return err
	}
	if foreign != 1 || ignored != 0 {
		return fmt.Errorf("agent snapshot fixture requires enforced constraints: foreign_keys=%d ignore_check_constraints=%d", foreign, ignored)
	}
	return nil
}
