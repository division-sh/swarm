package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func projectionLifecycleFaultAt() time.Time {
	return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
}

func projectionLifecycleFaultCases() []projectionShapeFaultCase {
	return []projectionShapeFaultCase{
		{name: "MissingFields", apply: RemoveWorkflowProjectionFieldsForTest, entity: true},
		{name: "Draining", apply: SetWorkflowProjectionDrainingForTest},
		{name: "Terminated", apply: func(ctx context.Context, selected any, run, path string) (int64, error) {
			return SetWorkflowProjectionTerminatedForTest(ctx, selected, run, path, projectionLifecycleFaultAt())
		}},
	}
}

func TestWorkflowProjectionLifecycleFaultsKeepExactScopePayloadAndNativeTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, tc := range projectionLifecycleFaultCases() {
				t.Run(tc.name, func(t *testing.T) { verifyWorkflowProjectionLifecycleFault(t, backend, tc) })
			}
		})
	}
}

func verifyWorkflowProjectionLifecycleFault(t *testing.T, backend string, tc projectionShapeFaultCase) {
	t.Helper()
	f, plan := newWorkflowTargetConstructionFixture(t, backend)
	result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil || !result.Created || !result.Acknowledged {
		t.Fatalf("construct actual target: %+v %v", result, err)
	}
	run, key := correlation.RunIDFromContext(f.ctx), plan.Instance.StorageRef
	if tc.entity {
		key = plan.Instance.EntityID
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: run, Route: plan.Identity.Route()}
	reader := f.store.(pipeline.WorkflowTargetPersistenceReader)
	read := func() pipeline.WorkflowTargetPersistenceRecord {
		value, err := reader.LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(plan.Instance.EntityID))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := read()
	if before.Presence != pipeline.WorkflowTargetPersistenceComplete {
		t.Fatal("pre-fault target is not complete")
	}
	for _, scope := range [][2]string{{uuid.NewString(), key}, {run, uuid.NewString()}} {
		if changed, err := tc.apply(f.ctx, f.store, scope[0], scope[1]); err == nil || changed != 0 {
			t.Fatalf("fault changed absent scope: rows=%d err=%v", changed, err)
		}
	}
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	if changed, err := tc.apply(cancelled, f.store, run, key); !errors.Is(err, context.Canceled) || changed != 0 {
		t.Fatalf("cancelled fault invented success: rows=%d err=%v", changed, err)
	}
	if unchanged := read(); !reflect.DeepEqual(before, unchanged) {
		t.Fatal("refused lifecycle fault changed prestate")
	}
	probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if changed, err := tc.apply(f.ctx, f.store, run, key); err != nil || changed != 1 {
		t.Fatalf("exact native lifecycle cut: rows=%d err=%v", changed, err)
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
		t.Fatalf("lifecycle fault escaped original writer: %+v", counts)
	}
	after := read()
	expected := expectedWorkflowProjectionLifecycleFault(t, tc, before, after)
	if !reflect.DeepEqual(expected, after) {
		t.Fatalf("lifecycle fault changed unrelated field/header facts: expected=%+v after=%+v", expected, after)
	}
}

func expectedWorkflowProjectionLifecycleFault(t *testing.T, tc projectionShapeFaultCase, before, after pipeline.WorkflowTargetPersistenceRecord) pipeline.WorkflowTargetPersistenceRecord {
	t.Helper()
	expected := before
	switch tc.name {
	case "MissingFields":
		expected.Presence, expected.State = pipeline.WorkflowTargetPersistenceLifecycleOnly, pipeline.WorkflowEntityStatePersistenceRecord{}
	case "Draining":
		expected.Lifecycle.Status, expected.Lifecycle.TerminatedAt = "draining", time.Time{}
	case "Terminated":
		if !after.Lifecycle.TerminatedAt.Equal(projectionLifecycleFaultAt()) {
			t.Fatalf("terminated fault changed the specified instant: %v", after.Lifecycle.TerminatedAt)
		}
		expected.Lifecycle.Status, expected.Lifecycle.TerminatedAt = "terminated", after.Lifecycle.TerminatedAt
	default:
		t.Fatal("unknown lifecycle fault case")
	}
	return expected
}

func TestWorkflowProjectionLifecycleFaultsRefuseInvalidAndClosedOwnersBothStores(t *testing.T) {
	run, key := uuid.NewString(), uuid.NewString()
	for _, tc := range projectionLifecycleFaultCases() {
		for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
			requireWorkflowProjectionLifecycleFaultRefused(t, tc, context.Background(), selected, run, key)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, tc := range projectionLifecycleFaultCases() {
				for _, scope := range [][2]string{{"", key}, {"invalid", key}, {uuid.Nil.String(), key}, {run, ""}, {run, " spaced "}} {
					requireWorkflowProjectionLifecycleFaultRefused(t, tc, testAuthorActivityContext(), f.store, scope[0], scope[1])
				}
			}
			if changed, err := SetWorkflowProjectionTerminatedForTest(testAuthorActivityContext(), f.store, run, key, time.Time{}); err == nil || changed != 0 {
				t.Fatal("zero termination time was accepted")
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range projectionLifecycleFaultCases() {
				requireWorkflowProjectionLifecycleFaultRefused(t, tc, testAuthorActivityContext(), f.store, run, key)
			}
		})
	}
}

func requireWorkflowProjectionLifecycleFaultRefused(t *testing.T, tc projectionShapeFaultCase, ctx context.Context, selected any, run, key string) {
	t.Helper()
	if changed, err := tc.apply(ctx, selected, run, key); err == nil || changed != 0 {
		t.Fatalf("invalid %s owner/coordinate accepted: %T rows=%d err=%v", tc.name, selected, changed, err)
	}
}

func TestWorkflowProjectionLifecycleFaultsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	for _, tc := range projectionLifecycleFaultCases() {
		t.Run(tc.name, func(t *testing.T) { verifyWorkflowProjectionFaultSQLiteAdmission(t, tc) })
	}
}
