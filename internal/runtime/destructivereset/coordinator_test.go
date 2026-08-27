package destructivereset

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestInventoryPlannerCarriesImplementedContractsAndSplitResetSeams(t *testing.T) {
	reader := &recordingInventoryReader{}
	plan, err := (InventoryPlanner{Reader: reader}).BuildPlan(context.Background(), Request{})
	if err != nil {
		t.Fatalf("BuildPlan error = %v", err)
	}
	if !containsContractStatus(plan.DownstreamContracts, ContractRunDeliveryQuiescence, "implemented_internal_owner") ||
		!containsContractStatus(plan.DownstreamContracts, ContractRunScopedTruncation, "implemented_internal_owner") ||
		!containsContractStatus(plan.DownstreamContracts, ContractManagedContainers, "implemented_internal_owner") ||
		!containsContractStatus(plan.DownstreamContracts, ContractPublicAPIWrapper, "implemented_public_owner") ||
		!containsContractStatus(plan.DownstreamContracts, ContractLegacyResetMigration, "split") {
		t.Fatalf("downstream contracts = %#v, missing required contract state", plan.DownstreamContracts)
	}
	if !containsSeam(plan.ResetSeams, "startup_recovery_failed_reset") ||
		!containsSeam(plan.ResetSeams, "agent_manager_reset_runtime_state_with_source") {
		t.Fatalf("reset seams = %#v, missing required live seam classification", plan.ResetSeams)
	}
	if containsSeam(plan.ResetSeams, "dashboard_runtime_actions_reset_state") ||
		containsSeam(plan.ResetSeams, "builder_runtime_reset_state") ||
		containsSeam(plan.ResetSeams, "scripts_private_reset_dev") {
		t.Fatalf("reset seams = %#v, want retired dashboard/Builder and private reset helper seams omitted", plan.ResetSeams)
	}
	if !plan.IncludeBundles || !containsTableAction(plan.RunScopedTables, "bundles", CleanupDeleteAll) {
		t.Fatalf("include_bundles plan = include:%v tables:%#v, want bundle catalog delete table", plan.IncludeBundles, plan.RunScopedTables)
	}
	if !plan.Preserved.SchemaMigrations || !plan.Preserved.AuthTokens || plan.Preserved.BundleContracts {
		t.Fatalf("preserved resources = %#v, want schema/auth preserved and bundle contracts not preserved when include_bundles defaults true", plan.Preserved)
	}
	if !slices.Contains(plan.Preserved.SystemContainers, "swarm-scaffold") || !slices.Contains(plan.Preserved.SystemContainers, "swarm-system") {
		t.Fatalf("system containers = %#v, want scaffold/system preserved", plan.Preserved.SystemContainers)
	}
}

func TestInventoryPlannerMergesPreservedResourceDefaultsByField(t *testing.T) {
	reader := &recordingInventoryReader{inventory: Inventory{
		Preserved: PreservedResources{
			SystemContainers: []string{"custom-system"},
		},
	}}
	plan, err := (InventoryPlanner{Reader: reader}).BuildPlan(context.Background(), Request{IncludeBundles: false, IncludeBundlesSet: true})
	if err != nil {
		t.Fatalf("BuildPlan error = %v", err)
	}
	if !slices.Equal(plan.Preserved.SystemContainers, []string{"custom-system"}) {
		t.Fatalf("system containers = %#v, want caller-provided value", plan.Preserved.SystemContainers)
	}
	if plan.Preserved.OperatorManagedBoundary == "" {
		t.Fatalf("operator-managed boundary was not defaulted")
	}
	if !plan.Preserved.SchemaMigrations || !plan.Preserved.AuthTokens || !plan.Preserved.BundleContracts {
		t.Fatalf("preserved resources = %#v, want critical defaults merged", plan.Preserved)
	}
	if plan.IncludeBundles || containsTableAction(plan.RunScopedTables, "bundles", CleanupDeleteAll) {
		t.Fatalf("include_bundles=false plan = include:%v tables:%#v, want bundle catalog preserved", plan.IncludeBundles, plan.RunScopedTables)
	}
}

func TestQuiescerAppliesPlanResultThroughStore(t *testing.T) {
	now := time.Date(2026, 5, 15, 2, 30, 0, 0, time.UTC)
	store := &recordingQuiescenceStore{
		result: QuiescenceResult{
			OperationName: DefaultOperationName,
			DryRun:        false,
			AppliedAt:     now,
			ReasonCode:    QuiescenceReasonCode,
			ControlledBy:  QuiescenceControlledBy,
			Runs:          []QuiescedRun{{RunID: "run-1", PreviousStatus: "running", Status: "cancelled", Changed: true}},
		},
	}
	q := Quiescer{Store: store, Now: func() time.Time { return now }}
	plan := Result{
		OperationName: DefaultOperationName,
		PlannedAt:     now.Add(-time.Minute),
		Plan:          Plan{ActiveRuns: []RunRef{{RunID: "run-1", Status: "running"}}},
	}

	got, err := q.Apply(context.Background(), QuiescenceRequest{Result: plan, ActorTokenID: "operator-token"})
	if err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	if store.last.ActorTokenID != "operator-token" || store.last.RequestedAt.IsZero() {
		t.Fatalf("store request = %#v, want actor and requested_at", store.last)
	}
	if got.Runs[0].RunID != "run-1" {
		t.Fatalf("quiescence runs = %#v", got.Runs)
	}
	got.Runs[0].RunID = "tampered"
	if store.result.Runs[0].RunID != "run-1" {
		t.Fatal("Apply leaked mutable result slices")
	}
}

func TestQuiescerFailsClosedWithoutPlanResultOrStore(t *testing.T) {
	_, err := (Quiescer{}).Apply(context.Background(), QuiescenceRequest{ActorTokenID: "operator-token"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing plan error = %v, want ErrInvalidRequest", err)
	}
	_, err = (Quiescer{}).Apply(context.Background(), QuiescenceRequest{
		ActorTokenID: "operator-token",
		Result:       Result{PlannedAt: time.Date(2026, 5, 15, 2, 30, 0, 0, time.UTC)},
	})
	if err == nil || !strings.Contains(err.Error(), "quiescence store") {
		t.Fatalf("missing store error = %v, want quiescence store failure", err)
	}
}

func TestQuiescerPropagatesStoreFailure(t *testing.T) {
	storeErr := errors.New("store failed")
	_, err := (Quiescer{Store: &recordingQuiescenceStore{err: storeErr}}).Apply(context.Background(), QuiescenceRequest{
		ActorTokenID: "operator-token",
		Result: Result{
			OperationName: DefaultOperationName,
			PlannedAt:     time.Date(2026, 5, 15, 2, 30, 0, 0, time.UTC),
			Plan:          Plan{ActiveRuns: []RunRef{{RunID: "run-1", Status: "running"}}},
		},
	})
	if !errors.Is(err, storeErr) {
		t.Fatalf("Apply error = %v, want store failure", err)
	}
}

type recordingInventoryReader struct {
	inventory Inventory
	err       error
	reads     int
}

type recordingQuiescenceStore struct {
	result QuiescenceResult
	err    error
	calls  int
	last   QuiescenceRequest
}

func (s *recordingQuiescenceStore) ApplyDestructiveResetQuiescence(_ context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
	s.calls++
	s.last = req
	return copyQuiescenceResult(s.result), s.err
}

func (r *recordingInventoryReader) ReadResetInventory(context.Context) (Inventory, error) {
	r.reads++
	return r.inventory, r.err
}

type recordingLockManager struct {
	acquired   bool
	err        error
	releaseErr error
	acquires   int
	lease      *recordingLease
}

func (m *recordingLockManager) AcquireDestructiveReset(context.Context) (LockLease, bool, error) {
	m.acquires++
	if m.err != nil {
		return nil, false, m.err
	}
	if !m.acquired {
		return nil, false, nil
	}
	m.lease = &recordingLease{err: m.releaseErr}
	return m.lease, true, nil
}

type recordingLease struct {
	releases int
	err      error
}

func (l *recordingLease) Release(context.Context) error {
	l.releases++
	return l.err
}

func containsContractStatus(contracts []DownstreamContract, id, status string) bool {
	return slices.ContainsFunc(contracts, func(contract DownstreamContract) bool {
		return contract.ID == id && contract.Status == status
	})
}

func containsSeam(seams []ResetSeam, id string) bool {
	return slices.ContainsFunc(seams, func(seam ResetSeam) bool {
		return seam.ID == id
	})
}

func containsTableAction(tables []TableRef, name, action string) bool {
	return slices.ContainsFunc(tables, func(table TableRef) bool {
		return table.Name == name && table.Action == action
	})
}
