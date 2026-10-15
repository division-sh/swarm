package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

type operatorAgentSnapshotFault struct {
	selected any
	row      *agentpersistence.OperatorSnapshotCorruption
	route    bool
}

func captureOperatorAgentSnapshotFault(ctx context.Context, selected any, identity agentidentity.Identity, route bool) (*operatorAgentSnapshotFault, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("operator snapshot fault requires a caller context")
	}
	_, postgres := selected.(*PostgresStore)
	fault := &operatorAgentSnapshotFault{selected: selected, route: route}
	err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		capture := agentpersistence.CaptureOperatorSnapshotStatusCorruptionTx
		if route {
			capture = agentpersistence.CaptureOperatorSnapshotRouteCorruptionTx
		}
		var err error
		fault.row, err = capture(ctx, tx, identity, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *operatorAgentSnapshotFault) installForTest(ctx context.Context, selected any) error {
	if f == nil || f.row == nil || f.selected != selected {
		return errors.New("operator snapshot fault belongs to another native owner")
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("operator snapshot fault requires a caller context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return f.row.InstallTx(ctx, tx, true) })
	case *SQLiteRuntimeStore:
		run := owner.backend.RunAgentSnapshotStatusCorruptionForTest
		if f.route {
			run = owner.backend.RunAgentSnapshotRouteCorruptionForTest
		}
		_, err := run(ctx, func(ctx context.Context, tx *sql.Tx) error { return f.row.InstallTx(ctx, tx, false) })
		return err
	default:
		return fmt.Errorf("operator snapshot fault requires the original native owner")
	}
}

func (f *operatorAgentSnapshotFault) restore(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("operator snapshot restoration requires its captured receipt")
	}
	_, postgres := f.selected.(*PostgresStore)
	return runWorkflowProjectionFault(ctx, f.selected, func(ctx context.Context, tx *sql.Tx) error { return f.row.RestoreTx(ctx, tx, postgres) })
}

func (f *operatorAgentSnapshotFault) cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.restore(ctx); err != nil {
		var closeErr error
		switch owner := f.selected.(type) {
		case *PostgresStore:
			closeErr = owner.Close()
		case *SQLiteRuntimeStore:
			closeErr = owner.Close()
		}
		return errors.Join(fmt.Errorf("restore exact operator agent row and constraints: %w", err), closeErr)
	}
	return nil
}

func RequireMalformedOperatorAgentRouteForTest(t testing.TB, ctx context.Context, selected any, identity agentidentity.Identity) {
	t.Helper()
	requireOperatorAgentSnapshotFaultForTest(t, ctx, selected, identity, true)
}

func RequireUnknownOperatorAgentStatusForTest(t testing.TB, ctx context.Context, selected any, identity agentidentity.Identity) {
	t.Helper()
	requireOperatorAgentSnapshotFaultForTest(t, ctx, selected, identity, false)
}

func requireOperatorAgentSnapshotFaultForTest(t testing.TB, ctx context.Context, selected any, identity agentidentity.Identity, route bool) {
	t.Helper()
	fault, err := captureOperatorAgentSnapshotFault(ctx, selected, identity, route)
	if err != nil {
		t.Fatalf("capture operator agent snapshot fault: %v", err)
	}
	// Registered before the first connection setting or constraint can change.
	t.Cleanup(func() {
		if err := fault.cleanup(); err != nil {
			t.Error(err)
		}
	})
	if err := fault.installForTest(ctx, selected); err != nil {
		t.Fatalf("install fixed operator agent snapshot fault: %v", err)
	}
}
