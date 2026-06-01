package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/store"
)

func TestActivateSelectedContractRunForkDelegatesNonSelectedActivation(t *testing.T) {
	forkRunID := uuid.NewString()
	activation := store.RunForkActivation{
		SourceRunID:     uuid.NewString(),
		ForkRunID:       forkRunID,
		ForkRunStatus:   store.RunForkActivatedStatus,
		SourceRunStatus: store.RunForkSourceFrozenStatus,
		Activated:       true,
		SourceFrozen:    true,
	}
	fakeStore := &fakeSelectedContractActivationStore{activation: activation}

	result, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID: forkRunID,
		Store:     fakeStore,
	})
	if err != nil {
		t.Fatalf("ActivateSelectedContractRunFork: %v", err)
	}
	if !fakeStore.activateCalled || fakeStore.planCalled {
		t.Fatalf("store calls = activate:%v plan:%v, want delegate activate without selected plan", fakeStore.activateCalled, fakeStore.planCalled)
	}
	if fakeStore.activateRequest.HistoricalReplayExecutionAdmitter == nil {
		t.Fatal("non-selected activation did not receive historical replay execution admitter")
	}
	if result.SelectedContractExecutionAdmission != nil || result.ForkRunID != forkRunID || !result.Activated {
		t.Fatalf("result = %#v", result)
	}
}

func TestActivateSelectedContractRunForkConsumesAdmissionBeforeStateOnlyActivation(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	plan := testSelectedContractStateOnlyPlan(binding)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:    binding,
		bindingOK:  true,
		plan:       plan,
		activation: store.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkRunID, Activated: true, SourceFrozen: true},
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err != nil {
		t.Fatalf("ActivateSelectedContractRunFork: %v", err)
	}
	if !fakeStore.planCalled || !fakeStore.requireCalled || !fakeStore.activateCalled {
		t.Fatalf("store calls = plan:%v require:%v activate:%v, want admission before activation", fakeStore.planCalled, fakeStore.requireCalled, fakeStore.activateCalled)
	}
	if fakeStore.activateRequest.HistoricalReplayExecutionAdmitter == nil {
		t.Fatal("selected state-only activation did not receive historical replay execution admitter")
	}
	if result.Owner != store.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("owner = %q, want %q", result.Owner, store.RunForkSelectedContractExecutionActivationGateOwner)
	}
	if result.SelectedContractExecutionAdmission == nil ||
		result.SelectedContractExecutionAdmission.Owner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		result.SelectedContractExecutionAdmission.FrontierEventCount != 0 {
		t.Fatalf("selected admission = %#v", result.SelectedContractExecutionAdmission)
	}
	if result.ContractSwapBootResumeAdmission == nil ||
		result.ContractSwapBootResumeAdmission.Owner != store.RunForkContractSwapBootResumeAdmissionOwner ||
		result.ContractSwapBootResumeAdmission.BootResumeSupported ||
		!unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("contract-swap admission = %#v", result.ContractSwapBootResumeAdmission)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		result.HistoricalReplayExecutionAdmission.Owner != store.RunForkHistoricalReplayExecutionAdmissionOwner ||
		!result.HistoricalReplayExecutionAdmission.NonMutating ||
		result.HistoricalReplayExecutionAdmission.ExecutionSupported ||
		result.HistoricalReplayExecutionAdmission.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner {
		t.Fatalf("historical replay admission = %#v", result.HistoricalReplayExecutionAdmission)
	}
	if result.RunForkActivation.ForkRunID != forkRunID || !result.Activated {
		t.Fatalf("activation = %#v", result.RunForkActivation)
	}
}

func TestActivateSelectedContractRunForkRequiresConcreteStoreForReplayMutation(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	plan := testSelectedContractStateOnlyPlan(binding)
	plan.ExecutionReady = true
	plan.PendingWorkCount = 1
	plan.PendingWork = []store.RunForkPendingWork{{
		EventID:        uuid.NewString(),
		DeliveryID:     uuid.NewString(),
		EventName:      "work.begin",
		SubscriberType: "agent",
		SubscriberID:   "safe-agent",
		Status:         "pending",
		Classification: store.RunForkPendingClassificationPending,
		CreatedAt:      time.Unix(1700001000, 0).UTC(),
	}}
	plan.ReplayResumeAdmission = store.RunForkReplayResumeAdmission{
		Owner:                     store.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady:  true,
		HistoricalReplayRequired:  true,
		HistoricalReplaySupported: true,
	}
	fakeStore := &fakeSelectedContractActivationStore{binding: binding, bindingOK: true, plan: plan}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkHistoricalReplayContractSwapBootResumeOwner) {
		t.Fatalf("err = %v, want concrete store requirement for contract-swap historical replay execution", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
	if !fakeStore.requireCalled || result.SelectedContractExecutionAdmission == nil {
		t.Fatalf("admission not consumed before block; require:%v result:%#v", fakeStore.requireCalled, result)
	}
	if result.ContractSwapBootResumeAdmission == nil ||
		!unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("contract-swap admission = %#v, want non-mutating blocker before source replay block", result.ContractSwapBootResumeAdmission)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		!unsupportedBlockerHas(result.HistoricalReplayExecutionAdmission.UnsupportedBlockers, store.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) ||
		!historicalReplayFactHas(result.HistoricalReplayExecutionAdmission.FactAdmissions, store.RunForkHistoricalReplayFactEventDeliveries, store.RunForkHistoricalReplayAdmissionExecutableForkWork) {
		t.Fatalf("historical replay admission = %#v, want non-mutating replayable-source classification before source replay block", result.HistoricalReplayExecutionAdmission)
	}
}

func TestActivateSelectedContractRunForkPassesRecoveredRouteEvidenceToContractSwapAdmission(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	plan := testSelectedContractStateOnlyPlan(binding)
	routeRecovery := store.RunForkSelectedContractRouteRecovery{
		Owner:                  store.RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:   store.RunForkSelectedContractRouteRecoveryOwner,
		ForkRunID:              binding.ForkRunID,
		SourceRunID:            binding.SourceRunID,
		ForkEventID:            binding.ForkEventID,
		ContractSelection:      binding.ContractSelection,
		RouteTopologyOwner:     store.RunForkSelectedContractRouteTopologyOwner,
		RecipientPlanningOwner: store.RunForkSelectedContractRecipientPlanningOwner,
	}
	fakeStore := &fakeSelectedContractActivationStore{
		binding:       binding,
		bindingOK:     true,
		plan:          plan,
		routeRecovery: routeRecovery,
		routeOK:       true,
		activation:    store.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkRunID, Activated: true, SourceFrozen: true},
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err != nil {
		t.Fatalf("ActivateSelectedContractRunFork: %v", err)
	}
	if !fakeStore.loadRouteCalled {
		t.Fatal("LoadRunForkSelectedContractRouteRecovery was not called")
	}
	if result.ContractSwapBootResumeAdmission == nil {
		t.Fatalf("missing contract-swap admission: %#v", result)
	}
	if result.ContractSwapBootResumeAdmission.RouteRecoveryOwner != store.RunForkSelectedContractRoutePersistenceOwner ||
		result.ContractSwapBootResumeAdmission.RuntimeRouteRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("contract-swap route recovery owners = %#v", result.ContractSwapBootResumeAdmission)
	}
	if unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, store.RunForkBlockerContractSwapRouteRecoveryMissing) {
		t.Fatalf("unexpected missing-route blocker with route recovery evidence: %#v", result.ContractSwapBootResumeAdmission.UnsupportedBlockers)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		result.HistoricalReplayExecutionAdmission.RouteRecoveryOwner != store.RunForkSelectedContractRoutePersistenceOwner ||
		result.HistoricalReplayExecutionAdmission.RuntimeRouteRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("historical replay route recovery owners = %#v", result.HistoricalReplayExecutionAdmission)
	}
}

func TestActivateSelectedContractRunForkFailsBeforeMutationOnUnavailableSource(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:   binding,
		bindingOK: true,
		plan:      testSelectedContractStateOnlyPlan(binding),
	}
	loader := &fakeSelectedContractSourceLoader{err: errors.New("selected source unavailable")}

	_, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), "selected source unavailable") {
		t.Fatalf("err = %v, want selected source failure", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
}

func TestActivateSelectedContractRunForkFailsBeforeMutationOnStaleBindingAdmission(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:    binding,
		bindingOK:  true,
		requireErr: errors.New("selected contract binding disappeared"),
		plan:       testSelectedContractStateOnlyPlan(binding),
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	_, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), "selected contract binding disappeared") {
		t.Fatalf("err = %v, want stale binding failure", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
}

func TestActivateSelectedContractRunForkPreservesPlannerBlockersBeforeMutation(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	plan := testSelectedContractStateOnlyPlan(binding)
	plan.ExecutionReady = false
	plan.ReplayResumeAdmission.StateOnlyExecutionReady = false
	plan.ReplayResumeAdmission.HistoricalReplayRequired = true
	plan.UnsupportedBlockers = []store.RunForkUnsupportedBlocker{{
		Code:    store.RunForkBlockerSessionHistoryUnproven,
		Message: "session history is not reconstructable",
	}}
	fakeStore := &fakeSelectedContractActivationStore{binding: binding, bindingOK: true, plan: plan}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	_, err := ActivateSelectedContractRunFork(context.Background(), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkBlockerSessionHistoryUnproven) {
		t.Fatalf("err = %v, want preserved planner blocker", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
}

func historicalReplayFactHas(items []store.RunForkHistoricalReplayFactAdmission, fact, admission string) bool {
	for _, item := range items {
		if item.Fact == fact && item.Admission == admission {
			return true
		}
	}
	return false
}

type fakeSelectedContractActivationStore struct {
	binding         store.RunForkSelectedContractBinding
	bindingOK       bool
	bindingErr      error
	requireErr      error
	plan            store.RunForkPlan
	planErr         error
	routeRecovery   store.RunForkSelectedContractRouteRecovery
	routeOK         bool
	routeErr        error
	activation      store.RunForkActivation
	activationErr   error
	activateRequest store.RunForkActivateRequest

	loadCalled      bool
	requireCalled   bool
	loadRouteCalled bool
	planCalled      bool
	activateCalled  bool
}

func (s *fakeSelectedContractActivationStore) LoadRunForkSelectedContractBinding(_ context.Context, _ string) (store.RunForkSelectedContractBinding, bool, error) {
	s.loadCalled = true
	if s.bindingErr != nil {
		return store.RunForkSelectedContractBinding{}, false, s.bindingErr
	}
	return s.binding, s.bindingOK, nil
}

func (s *fakeSelectedContractActivationStore) RequireRunForkSelectedContractBinding(_ context.Context, _ string) (store.RunForkSelectedContractBinding, error) {
	s.requireCalled = true
	if s.requireErr != nil {
		return store.RunForkSelectedContractBinding{}, s.requireErr
	}
	if !s.bindingOK {
		return store.RunForkSelectedContractBinding{}, errors.New("selected contract binding not found")
	}
	return s.binding, nil
}

func (s *fakeSelectedContractActivationStore) LoadRunForkSelectedContractRouteRecovery(_ context.Context, _ string) (store.RunForkSelectedContractRouteRecovery, bool, error) {
	s.loadRouteCalled = true
	if s.routeErr != nil {
		return store.RunForkSelectedContractRouteRecovery{}, false, s.routeErr
	}
	return s.routeRecovery, s.routeOK, nil
}

func (s *fakeSelectedContractActivationStore) PlanRunFork(_ context.Context, _ store.RunForkPlanRequest) (store.RunForkPlan, error) {
	s.planCalled = true
	if s.planErr != nil {
		return store.RunForkPlan{}, s.planErr
	}
	return s.plan, nil
}

func (s *fakeSelectedContractActivationStore) ActivateRunFork(_ context.Context, req store.RunForkActivateRequest) (store.RunForkActivation, error) {
	s.activateCalled = true
	s.activateRequest = req
	if s.activationErr != nil {
		return store.RunForkActivation{}, s.activationErr
	}
	return s.activation, nil
}

func testSelectedContractStateOnlyPlan(binding store.RunForkSelectedContractBinding) store.RunForkPlan {
	return store.RunForkPlan{
		SourceRunID:      binding.SourceRunID,
		SourceRunStatus:  "running",
		ForkPoint:        store.RunForkPoint{Input: binding.ForkEventID, EventID: binding.ForkEventID, EventName: "work.ready", Timestamp: binding.CreatedAt},
		EventCountAtFork: 1,
		ExecutionReady:   true,
		ReplayResumeAdmission: store.RunForkReplayResumeAdmission{
			Owner:                   store.RunForkReplayResumeAdmissionOwner,
			StateOnlyExecutionReady: true,
		},
	}
}
