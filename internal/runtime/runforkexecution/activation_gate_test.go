package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestActivateSelectedContractRunForkDelegatesNonSelectedActivation(t *testing.T) {
	forkRunID := uuid.NewString()
	activation := runfork.RunForkActivation{
		SourceRunID:     uuid.NewString(),
		ForkRunID:       forkRunID,
		ForkRunStatus:   runfork.RunForkActivatedStatus,
		SourceRunStatus: runfork.RunForkSourceFrozenStatus,
		Activated:       true,
		SourceFrozen:    true,
	}
	fakeStore := &fakeSelectedContractActivationStore{activation: activation}

	result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
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
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               plan,
		activation:         runfork.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkRunID, Activated: true, SourceFrozen: true},
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
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
	if result.Owner != runfork.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("owner = %q, want %q", result.Owner, runfork.RunForkSelectedContractExecutionActivationGateOwner)
	}
	if result.SelectedContractExecutionAdmission == nil ||
		result.SelectedContractExecutionAdmission.Owner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		result.SelectedContractExecutionAdmission.FrontierEventCount != 0 {
		t.Fatalf("selected admission = %#v", result.SelectedContractExecutionAdmission)
	}
	if result.ContractSwapBootResumeAdmission == nil ||
		result.ContractSwapBootResumeAdmission.Owner != runfork.RunForkContractSwapBootResumeAdmissionOwner ||
		result.ContractSwapBootResumeAdmission.BootResumeSupported ||
		!unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("contract-swap admission = %#v", result.ContractSwapBootResumeAdmission)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		result.HistoricalReplayExecutionAdmission.Owner != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		!result.HistoricalReplayExecutionAdmission.NonMutating ||
		result.HistoricalReplayExecutionAdmission.ExecutionSupported ||
		result.HistoricalReplayExecutionAdmission.ReplayResumeAdmissionOwner != runfork.RunForkReplayResumeAdmissionOwner {
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
	plan.PendingWork = []runfork.RunForkPendingWork{{
		EventID:        uuid.NewString(),
		DeliveryID:     uuid.NewString(),
		EventName:      "work.begin",
		SubscriberType: "agent",
		SubscriberID:   "safe-agent",
		Status:         "pending",
		Classification: runfork.RunForkPendingClassificationPending,
		CreatedAt:      time.Unix(1700001000, 0).UTC(),
	}}
	plan.ReplayResumeAdmission = runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		DeliveryEventReplayReady: true,
		ReplayResumeFactsPresent: true,
		BoundedReplaySupported:   true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:           runfork.RunForkReplayResumeFactDeliveryPendingHistory,
			Disposition:    runfork.RunForkReplayResumeDispositionForkReplay,
			Classification: runfork.RunForkPendingClassificationPending,
			EventID:        plan.PendingWork[0].EventID,
			DeliveryID:     plan.PendingWork[0].DeliveryID,
			SubscriberType: plan.PendingWork[0].SubscriberType,
			SubscriberID:   plan.PendingWork[0].SubscriberID,
			Message:        "test pending source delivery is replayable fork-local work",
		}},
	}
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               plan,
		routeRecovery: runfork.RunForkSelectedContractRouteRecovery{
			Owner:                  runfork.RunForkSelectedContractRoutePersistenceOwner,
			RuntimeRecoveryOwner:   runfork.RunForkSelectedContractRouteRecoveryOwner,
			ForkRunID:              binding.ForkRunID,
			SourceRunID:            binding.SourceRunID,
			ForkEventID:            binding.ForkEventID,
			ContractSelection:      binding.ContractSelection,
			RouteTopologyOwner:     runfork.RunForkSelectedContractRouteTopologyOwner,
			RecipientPlanningOwner: runfork.RunForkSelectedContractRecipientPlanningOwner,
		},
		routeOK: true,
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkHistoricalReplayContractSwapBootResumeOwner) {
		t.Fatalf("err = %v, want concrete store requirement for contract-swap historical replay execution", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
	if !fakeStore.requireCalled || result.SelectedContractExecutionAdmission == nil {
		t.Fatalf("admission not consumed before block; require:%v result:%#v", fakeStore.requireCalled, result)
	}
	if result.ContractSwapBootResumeAdmission == nil ||
		!unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("contract-swap admission = %#v, want non-mutating blocker before source replay block", result.ContractSwapBootResumeAdmission)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		!unsupportedBlockerHas(result.HistoricalReplayExecutionAdmission.UnsupportedBlockers, runfork.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating) ||
		!historicalReplayFactHas(result.HistoricalReplayExecutionAdmission.FactAdmissions, runfork.RunForkHistoricalReplayFactEventDeliveries, runfork.RunForkHistoricalReplayAdmissionExecutableForkWork) {
		t.Fatalf("historical replay admission = %#v, want non-mutating replayable-source classification before source replay block", result.HistoricalReplayExecutionAdmission)
	}
}

func TestActivateSelectedContractRunForkPassesRecoveredRouteEvidenceToContractSwapAdmission(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	plan := testSelectedContractStateOnlyPlan(binding)
	routeRecovery := runfork.RunForkSelectedContractRouteRecovery{
		Owner:                  runfork.RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:   runfork.RunForkSelectedContractRouteRecoveryOwner,
		ForkRunID:              binding.ForkRunID,
		SourceRunID:            binding.SourceRunID,
		ForkEventID:            binding.ForkEventID,
		ContractSelection:      binding.ContractSelection,
		RouteTopologyOwner:     runfork.RunForkSelectedContractRouteTopologyOwner,
		RecipientPlanningOwner: runfork.RunForkSelectedContractRecipientPlanningOwner,
	}
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               plan,
		routeRecovery:      routeRecovery,
		routeOK:            true,
		activation:         runfork.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkRunID, Activated: true, SourceFrozen: true},
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
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
	if result.ContractSwapBootResumeAdmission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		result.ContractSwapBootResumeAdmission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("contract-swap route recovery owners = %#v", result.ContractSwapBootResumeAdmission)
	}
	if unsupportedBlockerHas(result.ContractSwapBootResumeAdmission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapRouteRecoveryMissing) {
		t.Fatalf("unexpected missing-route blocker with route recovery evidence: %#v", result.ContractSwapBootResumeAdmission.UnsupportedBlockers)
	}
	if result.HistoricalReplayExecutionAdmission == nil ||
		result.HistoricalReplayExecutionAdmission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		result.HistoricalReplayExecutionAdmission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("historical replay route recovery owners = %#v", result.HistoricalReplayExecutionAdmission)
	}
}

func TestActivateSelectedContractRunForkFailsBeforeMutationOnUnavailableSource(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               testSelectedContractStateOnlyPlan(binding),
	}
	loader := &fakeSelectedContractSourceLoader{err: errors.New("selected source unavailable")}

	_, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
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

func TestActivateSelectedContractRunForkFailsBeforePlanningOnPersistedIdentityMismatch(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               testSelectedContractStateOnlyPlan(binding),
	}
	loaded := testLoadedSelectedSource(binding.ContractSelection)
	loaded.BundleSourceFact = testEphemeralBundleSourceFact("bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	loader := &fakeSelectedContractSourceLoader{loaded: loaded}

	_, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), "bundle_hash mismatch") {
		t.Fatalf("err = %v, want persisted bundle identity mismatch", err)
	}
	if !fakeStore.loadBundleCalled || fakeStore.planCalled || fakeStore.requireCalled || fakeStore.activateCalled {
		t.Fatalf("store calls = identity:%v plan:%v require:%v activate:%v, want identity read only", fakeStore.loadBundleCalled, fakeStore.planCalled, fakeStore.requireCalled, fakeStore.activateCalled)
	}
}

func TestActivateSelectedContractRunForkFailsBeforeMutationOnStaleBindingAdmission(t *testing.T) {
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		requireErr:         errors.New("selected contract binding disappeared"),
		plan:               testSelectedContractStateOnlyPlan(binding),
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	_, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
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
	plan.ReplayResumeAdmission.ReplayResumeFactsPresent = true
	plan.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{{
		Code:    runfork.RunForkBlockerSessionHistoryUnproven,
		Message: "session history is not reconstructable",
	}}
	fakeStore := &fakeSelectedContractActivationStore{
		binding:            binding,
		bindingOK:          true,
		bundleAvailability: testSelectedContractBundleAvailability(forkRunID),
		plan:               plan,
	}
	loader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}

	_, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
		ForkRunID:    forkRunID,
		Store:        fakeStore,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerSessionHistoryUnproven) {
		t.Fatalf("err = %v, want preserved planner blocker", err)
	}
	if fakeStore.activateCalled {
		t.Fatal("ActivateRunFork called, want fail closed before mutation")
	}
}

func historicalReplayFactHas(items []runfork.RunForkHistoricalReplayFactAdmission, fact, admission string) bool {
	for _, item := range items {
		if item.Fact == fact && item.Admission == admission {
			return true
		}
	}
	return false
}

type fakeSelectedContractActivationStore struct {
	binding               runfork.RunForkSelectedContractBinding
	bindingOK             bool
	bindingErr            error
	requireErr            error
	bundleAvailability    runbundle.Availability
	bundleAvailabilityErr error
	plan                  runfork.RunForkPlan
	planErr               error
	routeRecovery         runfork.RunForkSelectedContractRouteRecovery
	routeOK               bool
	routeErr              error
	activation            runfork.RunForkActivation
	activationErr         error
	activateRequest       runfork.RunForkActivateRequest

	loadCalled       bool
	loadBundleCalled bool
	requireCalled    bool
	loadRouteCalled  bool
	planCalled       bool
	activateCalled   bool
}

func (s *fakeSelectedContractActivationStore) LoadRunBundleAvailability(_ context.Context, _ string) (runbundle.Availability, error) {
	s.loadBundleCalled = true
	if s.bundleAvailabilityErr != nil {
		return runbundle.Availability{}, s.bundleAvailabilityErr
	}
	return s.bundleAvailability, nil
}

func (s *fakeSelectedContractActivationStore) LoadRunForkSelectedContractBinding(_ context.Context, _ string) (runfork.RunForkSelectedContractBinding, bool, error) {
	s.loadCalled = true
	if s.bindingErr != nil {
		return runfork.RunForkSelectedContractBinding{}, false, s.bindingErr
	}
	return s.binding, s.bindingOK, nil
}

func (s *fakeSelectedContractActivationStore) RequireRunForkSelectedContractBinding(_ context.Context, _ string) (runfork.RunForkSelectedContractBinding, error) {
	s.requireCalled = true
	if s.requireErr != nil {
		return runfork.RunForkSelectedContractBinding{}, s.requireErr
	}
	if !s.bindingOK {
		return runfork.RunForkSelectedContractBinding{}, errors.New("selected contract binding not found")
	}
	return s.binding, nil
}

func (s *fakeSelectedContractActivationStore) LoadRunForkSelectedContractRouteRecovery(_ context.Context, _ string) (runfork.RunForkSelectedContractRouteRecovery, bool, error) {
	s.loadRouteCalled = true
	if s.routeErr != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, s.routeErr
	}
	return s.routeRecovery, s.routeOK, nil
}

func (s *fakeSelectedContractActivationStore) PlanRunFork(_ context.Context, _ runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	s.planCalled = true
	if s.planErr != nil {
		return runfork.RunForkPlan{}, s.planErr
	}
	return s.plan, nil
}

func (s *fakeSelectedContractActivationStore) ActivateRunFork(_ context.Context, req runfork.RunForkActivateRequest) (runfork.RunForkActivation, error) {
	s.activateCalled = true
	s.activateRequest = req
	if s.activationErr != nil {
		return runfork.RunForkActivation{}, s.activationErr
	}
	return s.activation, nil
}

func (s *fakeSelectedContractActivationStore) RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return nil, nil
}

func testSelectedContractStateOnlyPlan(binding runfork.RunForkSelectedContractBinding) runfork.RunForkPlan {
	return runfork.RunForkPlan{
		SourceRunID:      binding.SourceRunID,
		SourceRunStatus:  "running",
		ForkPoint:        runfork.RunForkPoint{Input: binding.ForkEventID, EventID: binding.ForkEventID, EventName: "work.ready", Timestamp: binding.CreatedAt},
		EventCountAtFork: 1,
		ExecutionReady:   true,
		ReplayResumeAdmission: runfork.RunForkReplayResumeAdmission{
			Owner:                   runfork.RunForkReplayResumeAdmissionOwner,
			StateOnlyExecutionReady: true,
		},
	}
}

func testSelectedContractBundleAvailability(forkRunID string) runbundle.Availability {
	return runbundle.Availability{
		RunID:        forkRunID,
		Status:       runfork.RunForkMaterializedStatus,
		BundleHash:   runForkTestBundleHash,
		BundleSource: runbundle.AvailabilitySourceEphemeral,
	}
}
