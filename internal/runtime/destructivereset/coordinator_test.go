package destructivereset

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorBuildPlanStoresAndReplaysIdempotentResult(t *testing.T) {
	now := time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC)
	reader := &recordingInventoryReader{inventory: Inventory{
		ActiveRuns:       []RunRef{{RunID: "run-1", Status: "running"}},
		ActiveDeliveries: []DeliveryRef{{DeliveryID: "delivery-1", RunID: "run-1", Status: "pending"}},
		RunScopedTables:  []TableRef{{Name: "runs", Owner: ContractRunScopedTruncation, Action: "planned_by_downstream_owner"}},
		EntityContainers: []ContainerRef{{Name: "swarm-entity-1", Kind: "entity", Action: "planned_by_downstream_owner"}},
	}}
	locks := &recordingLockManager{acquired: true}
	idempotency := newRecordingIdempotencyStore()
	coord := &Coordinator{
		Planner:     InventoryPlanner{Reader: reader},
		Locks:       locks,
		Idempotency: idempotency,
		Now:         func() time.Time { return now },
	}
	req := Request{
		ActorTokenID:   "operator-token",
		IdempotencyKey: "idem-1",
		RequestHash:    "hash-1",
		DryRun:         true,
	}

	result, replay, err := coord.BuildPlan(context.Background(), req)
	if err != nil {
		t.Fatalf("BuildPlan first call error = %v", err)
	}
	if replay {
		t.Fatal("first call replay = true, want false")
	}
	if result.OperationName != DefaultOperationName || !result.DryRun || !result.PlannedAt.Equal(now) {
		t.Fatalf("result metadata = %#v, want operation/dry-run/time", result)
	}
	if len(result.Plan.ActiveRuns) != 1 || result.Plan.ActiveRuns[0].RunID != "run-1" {
		t.Fatalf("active runs = %#v", result.Plan.ActiveRuns)
	}
	if reader.reads != 1 || locks.acquires != 1 || locks.lease.releases != 1 || idempotency.stores != 1 {
		t.Fatalf("first call counts reader=%d acquires=%d releases=%d stores=%d", reader.reads, locks.acquires, locks.lease.releases, idempotency.stores)
	}
	result.Plan.ActiveRuns[0].RunID = "tampered-return"
	result.Plan.Preserved.SystemContainers[0] = "tampered-container"

	replayed, replay, err := coord.BuildPlan(context.Background(), req)
	if err != nil {
		t.Fatalf("BuildPlan replay error = %v", err)
	}
	if !replay {
		t.Fatal("second call replay = false, want true")
	}
	if replayed.PlannedAt != result.PlannedAt || replayed.OperationName != result.OperationName {
		t.Fatalf("replayed result = %#v, want stored result %#v", replayed, result)
	}
	if replayed.Plan.ActiveRuns[0].RunID != "run-1" {
		t.Fatalf("replayed active run = %q, want original stored value", replayed.Plan.ActiveRuns[0].RunID)
	}
	if replayed.Plan.Preserved.SystemContainers[0] != "swarm-scaffold" {
		t.Fatalf("replayed preserved system container = %q, want original stored value", replayed.Plan.Preserved.SystemContainers[0])
	}
	if reader.reads != 1 || locks.acquires != 1 || idempotency.stores != 1 {
		t.Fatalf("replay should not re-plan/lock/store: reader=%d acquires=%d stores=%d", reader.reads, locks.acquires, idempotency.stores)
	}
	replayed.Plan.ActiveRuns[0].RunID = "tampered-replay"
	replayedAgain, replay, err := coord.BuildPlan(context.Background(), req)
	if err != nil {
		t.Fatalf("BuildPlan second replay error = %v", err)
	}
	if !replay {
		t.Fatal("third call replay = false, want true")
	}
	if replayedAgain.Plan.ActiveRuns[0].RunID != "run-1" {
		t.Fatalf("second replay active run = %q, want original stored value", replayedAgain.Plan.ActiveRuns[0].RunID)
	}
}

func TestCoordinatorRejectsIdempotencyConflictBeforePlanning(t *testing.T) {
	now := time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC)
	reader := &recordingInventoryReader{}
	locks := &recordingLockManager{acquired: true}
	idempotency := newRecordingIdempotencyStore()
	coord := &Coordinator{
		Planner:     InventoryPlanner{Reader: reader},
		Locks:       locks,
		Idempotency: idempotency,
		Now:         func() time.Time { return now },
	}
	req := Request{ActorTokenID: "operator-token", IdempotencyKey: "idem-1", RequestHash: "hash-1", DryRun: true}
	if _, _, err := coord.BuildPlan(context.Background(), req); err != nil {
		t.Fatalf("seed BuildPlan error = %v", err)
	}

	_, _, err := coord.BuildPlan(context.Background(), Request{
		ActorTokenID:   "operator-token",
		IdempotencyKey: "idem-1",
		RequestHash:    "different-hash",
		DryRun:         true,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict error = %v, want ErrIdempotencyConflict", err)
	}
	if reader.reads != 1 || locks.acquires != 1 || idempotency.stores != 1 {
		t.Fatalf("conflict should not re-plan/lock/store: reader=%d acquires=%d stores=%d", reader.reads, locks.acquires, idempotency.stores)
	}
}

func TestCoordinatorDoesNotStoreIdempotencyBeforeValidationOrPlanFailure(t *testing.T) {
	idempotency := newRecordingIdempotencyStore()
	coord := &Coordinator{
		Planner:     InventoryPlanner{Reader: &recordingInventoryReader{}},
		Locks:       &recordingLockManager{acquired: true},
		Idempotency: idempotency,
		Now:         func() time.Time { return time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC) },
	}
	_, _, err := coord.BuildPlan(context.Background(), Request{
		ActorTokenID:   "operator-token",
		IdempotencyKey: "idem-1",
		DryRun:         true,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing request hash error = %v, want ErrInvalidRequest", err)
	}
	if idempotency.loads != 0 || idempotency.stores != 0 {
		t.Fatalf("validation failure idempotency counts loads=%d stores=%d, want 0/0", idempotency.loads, idempotency.stores)
	}

	planErr := errors.New("inventory unavailable")
	locks := &recordingLockManager{acquired: true}
	idempotency = newRecordingIdempotencyStore()
	coord = &Coordinator{
		Planner:     InventoryPlanner{Reader: &recordingInventoryReader{err: planErr}},
		Locks:       locks,
		Idempotency: idempotency,
		Now:         func() time.Time { return time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC) },
	}
	_, _, err = coord.BuildPlan(context.Background(), Request{
		ActorTokenID:   "operator-token",
		IdempotencyKey: "idem-1",
		RequestHash:    "hash-1",
		DryRun:         true,
	})
	if !errors.Is(err, planErr) {
		t.Fatalf("plan error = %v, want %v", err, planErr)
	}
	if idempotency.stores != 0 {
		t.Fatalf("plan failure stored idempotency rows = %d, want 0", idempotency.stores)
	}
	if locks.lease.releases != 1 {
		t.Fatalf("plan failure lock releases = %d, want 1", locks.lease.releases)
	}
}

func TestCoordinatorLockConflictPreventsPlanning(t *testing.T) {
	reader := &recordingInventoryReader{}
	coord := &Coordinator{
		Planner: InventoryPlanner{Reader: reader},
		Locks:   &recordingLockManager{acquired: false},
		Now:     func() time.Time { return time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC) },
	}
	_, _, err := coord.BuildPlan(context.Background(), Request{ActorTokenID: "operator-token", DryRun: true})
	if !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("lock conflict error = %v, want ErrOperationInProgress", err)
	}
	if reader.reads != 0 {
		t.Fatalf("lock conflict reader reads = %d, want 0", reader.reads)
	}
}

func TestCoordinatorFailsClosedWhenLockLeaseIsMissing(t *testing.T) {
	reader := &recordingInventoryReader{}
	coord := &Coordinator{
		Planner: InventoryPlanner{Reader: reader},
		Locks:   lockManagerFunc(func(context.Context, string) (LockLease, bool, error) { return nil, true, nil }),
		Now:     func() time.Time { return time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC) },
	}
	_, _, err := coord.BuildPlan(context.Background(), Request{ActorTokenID: "operator-token", DryRun: true})
	if !errors.Is(err, ErrLockLeaseMissing) {
		t.Fatalf("missing lock lease error = %v, want ErrLockLeaseMissing", err)
	}
	if reader.reads != 0 {
		t.Fatalf("missing lease reader reads = %d, want 0", reader.reads)
	}
}

func TestInventoryPlannerCarriesSplitContractsAndResetSeams(t *testing.T) {
	reader := &recordingInventoryReader{}
	plan, err := (InventoryPlanner{Reader: reader}).BuildPlan(context.Background(), Request{})
	if err != nil {
		t.Fatalf("BuildPlan error = %v", err)
	}
	if !containsContract(plan.DownstreamContracts, ContractRunDeliveryQuiescence) ||
		!containsContract(plan.DownstreamContracts, ContractRunScopedTruncation) ||
		!containsContract(plan.DownstreamContracts, ContractManagedContainers) ||
		!containsContract(plan.DownstreamContracts, ContractPublicAPIWrapper) ||
		!containsContract(plan.DownstreamContracts, ContractLegacyResetMigration) {
		t.Fatalf("downstream contracts = %#v, missing required split contract", plan.DownstreamContracts)
	}
	if !containsSeam(plan.ResetSeams, "startup_recovery_failed_reset") ||
		!containsSeam(plan.ResetSeams, "scripts_run_clear_reset_dev") ||
		!containsSeam(plan.ResetSeams, "agent_manager_reset_runtime_state_with_source") {
		t.Fatalf("reset seams = %#v, missing required live seam classification", plan.ResetSeams)
	}
	if !plan.Preserved.SchemaMigrations || !plan.Preserved.AuthTokens || !plan.Preserved.BundleContracts {
		t.Fatalf("preserved resources = %#v, want schema/auth/bundle preserved", plan.Preserved)
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
	plan, err := (InventoryPlanner{Reader: reader}).BuildPlan(context.Background(), Request{})
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
	acquired bool
	err      error
	acquires int
	lease    *recordingLease
}

func (m *recordingLockManager) TryAcquire(context.Context, string) (LockLease, bool, error) {
	m.acquires++
	if m.err != nil {
		return nil, false, m.err
	}
	if !m.acquired {
		return nil, false, nil
	}
	m.lease = &recordingLease{}
	return m.lease, true, nil
}

type lockManagerFunc func(context.Context, string) (LockLease, bool, error)

func (f lockManagerFunc) TryAcquire(ctx context.Context, key string) (LockLease, bool, error) {
	return f(ctx, key)
}

type recordingLease struct {
	releases int
}

func (l *recordingLease) Release(context.Context) error {
	l.releases++
	return nil
}

type recordingIdempotencyStore struct {
	records map[IdempotencyKey]StoredResult
	loads   int
	stores  int
}

func newRecordingIdempotencyStore() *recordingIdempotencyStore {
	return &recordingIdempotencyStore{records: map[IdempotencyKey]StoredResult{}}
}

func (s *recordingIdempotencyStore) LoadResetResult(_ context.Context, key IdempotencyKey) (StoredResult, bool, error) {
	s.loads++
	record, ok := s.records[key.normalized()]
	return record, ok, nil
}

func (s *recordingIdempotencyStore) StoreResetResult(_ context.Context, result StoredResult) error {
	s.stores++
	s.records[result.Key.normalized()] = result
	return nil
}

func containsContract(contracts []DownstreamContract, id string) bool {
	return slices.ContainsFunc(contracts, func(contract DownstreamContract) bool {
		return contract.ID == id && contract.Status == "split"
	})
}

func containsSeam(seams []ResetSeam, id string) bool {
	return slices.ContainsFunc(seams, func(seam ResetSeam) bool {
		return seam.ID == id
	})
}
