package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func newOperatorSnapshotFaultFixture(t *testing.T, backend string) (context.Context, any, agentidentity.Identity) {
	t.Helper()
	ctx, selected := newAgentFixtureAuthorityStore(t, backend)
	runID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	identity := mustTestAgentIdentityForRun(runID, "snapshot-fault-agent", "global")
	if err := agentfixture.UpsertStatic(t, ctx, selected, agentFixtureStaticRecord(t, identity)); err != nil {
		t.Fatal(err)
	}
	state, found, err := selected.(manager.AgentLifecycleStateReader).LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		t.Fatalf("native lifecycle found=%v err=%v", found, err)
	}
	grantCtx := effects.WithLifecycleToken(ctx, effects.LifecycleToken{Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: state.RuntimeEpoch, Generation: state.Generation})
	registry := selected.(sessions.Registry)
	lease, err := registry.Acquire(grantCtx, identity, "operator-snapshot-fault")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := registry.ReleaseOutcome(grantCtx, lease); err != nil || !result.Acknowledged {
		t.Fatalf("native release=%#v err=%v", result, err)
	}
	return ctx, selected, identity
}

func restoreOperatorSnapshotFault(t *testing.T, fault *operatorAgentSnapshotFault) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := fault.restore(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOperatorSnapshotAgentCorruptionRestoresExactRowsAndConstraintsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected, identity := newOperatorSnapshotFaultFixture(t, backend)
			for _, route := range []bool{true, false} {
				name, column := "status", "status"
				if route {
					name, column = "route", "flow_scope_key"
				}
				t.Run(name, func(t *testing.T) {
					proveOperatorSnapshotRestoration(t, ctx, selected, identity, route, column)
				})
			}
		})
	}
}

func proveOperatorSnapshotRestoration(t *testing.T, ctx context.Context, selected any, identity agentidentity.Identity, route bool, column string) {
	t.Helper()

	before := forkReceiverFaultSnapshot(t, ctx, selected)
	// The existing delta assertion rewrites its expected rows. Keep
	// an independent immutable original for the restoration proof.
	deltaBefore := make(map[string]SelectedForkStorageTableSnapshot, len(before))
	for table, value := range before {
		value.Rows = slices.Clone(value.Rows)
		deltaBefore[table] = value
	}
	fault, err := captureOperatorAgentSnapshotFault(ctx, selected, identity, route)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restoreOperatorSnapshotFault(t, fault) })
	if err := fault.install(ctx, selected); err != nil {
		t.Fatal(err)
	}
	after := forkReceiverFaultSnapshot(t, ctx, selected)
	assertForkReceiverFaultOnlyColumns(t, deltaBefore, after, []forkReceiverFaultEdit{{table: "agents", key: "agent_id", value: identity.AgentID(), columns: []string{column}}})
	reader := selected.(interface {
		ListOperatorAgents(context.Context, operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error)
	})
	if _, err := reader.ListOperatorAgents(ctx, operatorread.OperatorAgentListOptions{}); err == nil {
		t.Fatal("actual operator reader accepted corrupted agent authority")
	}
	restoreOperatorSnapshotFault(t, fault)
	if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("agent restoration changed the full store, sibling rows or clocks")
	}
	if err := runWorkflowProjectionFault(ctx, selected, fault.row.VerifyEnforcementTx); err != nil {
		t.Fatal(err)
	}
	if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("constraint enforcement probe changed persisted data")
	}

}

func TestOperatorSnapshotAgentCleanupIgnoresCancelledCallerAndDisposesChangedReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveOperatorSnapshotIndependentCleanup(t, backend)
		})
	}
}

func proveOperatorSnapshotIndependentCleanup(t *testing.T, backend string) {
	t.Helper()

	ctx, selected, identity := newOperatorSnapshotFaultFixture(t, backend)
	for _, route := range []bool{true, false} {
		before := forkReceiverFaultSnapshot(t, ctx, selected)
		caller, cancel := context.WithCancel(ctx)
		fault, err := captureOperatorAgentSnapshotFault(caller, selected, identity, route)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err := fault.install(caller, selected); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if err := fault.cleanup(); err != nil {
			t.Fatal("cancelled caller prevented independent cleanup:", err)
		}
		if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
			t.Fatal("cancelled caller cleanup changed original data")
		}
	}
	fault, err := captureOperatorAgentSnapshotFault(ctx, selected, identity, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := fault.install(ctx, selected); err != nil {
		t.Fatal(err)
	}
	if err := CorruptOperatorAgentConfigForTest(ctx, selected, identity); err != nil {
		t.Fatal(err)
	}
	before := forkReceiverFaultSnapshot(t, ctx, selected)
	if err := fault.restore(ctx); err == nil {
		t.Fatal("restoration overwrote a changed replacement")
	}
	if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("refused restoration changed data")
	}
	if err := fault.cleanup(); err == nil {
		t.Fatal("unrestored disposable fixture reported success")
	}
	var pingErr error
	switch owner := selected.(type) {
	case *PostgresStore:
		pingErr = owner.Ping(ctx)
	case *SQLiteRuntimeStore:
		pingErr = owner.Ping(ctx)
	}
	if pingErr == nil {
		t.Fatal("unrestored disposable store was left usable")
	}

}

func TestOperatorSnapshotAgentCorruptionRefusesForeignStaleAndCancelledEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected, identity := newOperatorSnapshotFaultFixture(t, backend)
			_, foreign, _ := newOperatorSnapshotFaultFixture(t, backend)
			for _, route := range []bool{true, false} {
				for _, cut := range []string{"foreign", "absent", "cancelled", "stale"} {
					t.Run(map[bool]string{true: "route", false: "status"}[route]+"/"+cut, func(t *testing.T) {
						proveOperatorSnapshotFaultRefusal(t, ctx, selected, foreign, identity, route, cut)
					})
				}
			}
		})
	}
}

func proveOperatorSnapshotFaultRefusal(t *testing.T, ctx context.Context, selected, foreign any, identity agentidentity.Identity, route bool, cut string) {
	t.Helper()

	fault, err := captureOperatorAgentSnapshotFault(ctx, selected, identity, route)
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	target := selected
	switch cut {
	case "foreign":
		target = foreign
	case "cancelled":
		cancel()
	case "stale":
		if err := ApplyOperatorAgentTerminalizedForTest(ctx, selected, identity); err != nil {
			t.Fatal(err)
		}
	}
	before := forkReceiverFaultSnapshot(t, ctx, selected)
	foreignBefore := forkReceiverFaultSnapshot(t, ctx, foreign)
	if cut == "absent" {
		missing := mustTestAgentIdentityForRun(identity.RunID, "missing-agent", "global")
		_, err = captureOperatorAgentSnapshotFault(callCtx, selected, missing, route)
	} else {
		err = fault.install(callCtx, target)
	}
	if err == nil {
		t.Fatal("invalid evidence installed a corruption")
	}
	if cut == "cancelled" && !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
	if after := forkReceiverFaultSnapshot(t, ctx, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("refused fault changed the selected store")
	}
	if after := forkReceiverFaultSnapshot(t, ctx, foreign); !reflect.DeepEqual(foreignBefore, after) {
		t.Fatal("refused fault changed the foreign store")
	}
	if cut == "stale" {
		if err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			return agentpersistence.SetOperatorSnapshotAgentActiveTx(ctx, tx, identity, true)
		}); err != nil {
			t.Fatal(err)
		}
	}

}
