package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
)

type SelectedContractActivationStore interface {
	LoadRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, bool, error)
	RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	LoadRunForkSelectedContractRouteRecovery(context.Context, string) (runfork.RunForkSelectedContractRouteRecovery, bool, error)
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type SelectedContractActivationGateRequest struct {
	ForkRunID           string
	ConfirmSourceFreeze bool
	Store               SelectedContractActivationStore
	ExecutionOwner      SelectedContractExecutionOwner
	SourceLoader        SelectedContractSourceLoader
	AgentRuntime        SelectedContractAgentRuntimeOptions
}

type SelectedContractActivationGateResult struct {
	runfork.RunForkActivation
	Owner                              string                                                 `json:"selected_contract_activation_gate_owner,omitempty"`
	SelectedContractExecutionAdmission *runfork.RunForkSelectedContractExecutionAdmission     `json:"selected_contract_execution_admission,omitempty"`
	ContractSwapBootResumeAdmission    *runfork.RunForkContractSwapBootResumeAdmission        `json:"contract_swap_boot_resume_admission,omitempty"`
	HistoricalReplayExecutionAdmission *runfork.RunForkHistoricalReplayExecutionAdmission     `json:"historical_replay_execution_admission,omitempty"`
	ContractSwapBootResumeExecution    *runfork.RunForkHistoricalReplayContractSwapBootResume `json:"contract_swap_boot_resume_execution,omitempty"`
	ForkLocalRuntimeContainer          *SelectedContractForkLocalRuntimeContainer             `json:"fork_local_runtime_container,omitempty"`
	ExecutedEventCount                 int                                                    `json:"executed_event_count,omitempty"`
	ForkEvents                         []SelectedContractExecutionForkEvent                   `json:"fork_events,omitempty"`
}

func ActivateSelectedContractRunFork(ctx context.Context, req SelectedContractActivationGateRequest) (SelectedContractActivationGateResult, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate fork run_id must be a UUID: %w", err)
	}
	if req.Store == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires store")
	}

	binding, ok, err := req.Store.LoadRunForkSelectedContractBinding(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract binding for activation gate: %w", err)
	}
	if !ok {
		activation, err := req.Store.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
			ForkRunID:                         forkRunID,
			ConfirmSourceFreeze:               req.ConfirmSourceFreeze,
			HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
		})
		return SelectedContractActivationGateResult{RunForkActivation: activation}, err
	}
	if req.SourceLoader == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires selected source loader")
	}
	forkBundleIdentity, err := req.Store.LoadRunBundleAvailability(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract fork bundle identity: %w", err)
	}
	expectedBundleHash := strings.TrimSpace(forkBundleIdentity.BundleHash)
	if expectedBundleHash == "" {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: selected-contract fork run %s has no persisted bundle_hash", runbundle.CodeBundleDataIntegrityError, forkRunID)
	}
	expectedBundleSource := forkBundleIdentity.BundleSource
	if forkBundleIdentity.DataIntegrityError() {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: %s", runbundle.CodeBundleDataIntegrityError, forkBundleIdentity.DetailString())
	}
	if !expectedBundleSource.IsEphemeral() && !expectedBundleSource.IsPersisted() {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: selected-contract fork run %s has unavailable bundle_source %s", runbundle.CodeBundleUnavailable, forkRunID, expectedBundleSource)
	}
	var expectedSourceFact runtimecorrelation.BundleSourceFact
	if expectedBundleSource.IsPersisted() {
		expectedSourceFact, err = runtimecorrelation.NewPersistedBundleSourceFact(expectedBundleHash)
	} else {
		expectedSourceFact, err = runtimecorrelation.NewEphemeralBundleSourceFact(expectedBundleHash)
	}
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: selected-contract fork run %s has invalid bundle source fact: %w", runbundle.CodeBundleDataIntegrityError, forkRunID, err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, expectedSourceFact)

	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID:      binding.SourceRunID,
		BundleHash:       expectedBundleHash,
		BundleSourceFact: expectedSourceFact,
		Selection:        binding.ContractSelection,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected semantic source for activation gate: %w", err)
	}
	defer cleanupLoadedSelectedContractSource(loadedSource)
	if err := loadedSource.BundleSourceFact.Validate(); err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation source loader returned incomplete bundle identity")
	}
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loadedSource.BundleSourceFact.BundleHash())
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("resolve selected-contract activation author activity scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, scope)
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(loadedSource.Source)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("project selected-contract activation descriptors: %w", err)
	}
	lease, err := req.Store.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("register selected-contract activation descriptors: %w", err)
	}
	defer lease.Release()
	plan, err := req.Store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: binding.SourceRunID, At: binding.ForkEventID})
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("plan selected-contract activation gate: %w", err)
	}
	deferredWorkAdmission, err := admitSelectedContractDeferredWork(plan, loadedSource.Source)
	if err != nil {
		return SelectedContractActivationGateResult{Owner: runfork.RunForkSelectedContractExecutionActivationGateOwner}, err
	}
	replayAdmission := runfork.RunForkSelectedContractReplayResumeAdmission(plan)
	if len(plan.ReplayResumeAdmission.Dispositions) > 0 || len(plan.ReplayResumeAdmission.UnsupportedBlockers) > 0 {
		plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
		plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
	}
	plan.ReplayResumeAdmission = replayAdmission
	plan.ExecutionReady = replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: binding.ContractSelection,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: binding.ContractSelection,
		FrontierAdmission: frontier,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:             forkRunID,
		SourceRunID:           binding.SourceRunID,
		BundleSourceFact:      expectedSourceFact,
		BindingReader:         req.Store,
		SourceLoader:          req.SourceLoader,
		FrontierAdmission:     frontier,
		RouteAdmission:        routeAdmission,
		RouteTopology:         routeTopology,
		ExecutionModel:        model,
		DeferredWorkAdmission: deferredWorkAdmission,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	var routeRecovery *runfork.RunForkSelectedContractRouteRecovery
	recoveredRoute, ok, err := req.Store.LoadRunForkSelectedContractRouteRecovery(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract route recovery for contract-swap admission: %w", err)
	}
	if ok {
		routeRecovery = &recoveredRoute
		if err := validateContractSwapRouteRecovery(admission, recoveredRoute); err != nil {
			return SelectedContractActivationGateResult{}, err
		}
		replayAdmission = runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(replayAdmission)
		plan.ReplayResumeAdmission = replayAdmission
		plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
		plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
		plan.ExecutionReady = replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady
	}
	contractSwapAdmission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: admission,
		ReplayResumeAdmission:      replayAdmission,
		RouteRecovery:              routeRecovery,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	historicalReplayAdmission, err := BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
		ReplayResumeAdmission:      replayAdmission,
		SelectedExecutionAdmission: admission,
		ContractSwapAdmission:      contractSwapAdmission,
		RouteRecovery:              routeRecovery,
	})
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	result := SelectedContractActivationGateResult{
		Owner:                              runfork.RunForkSelectedContractExecutionActivationGateOwner,
		SelectedContractExecutionAdmission: &admission,
		ContractSwapBootResumeAdmission:    &contractSwapAdmission,
		HistoricalReplayExecutionAdmission: &historicalReplayAdmission,
	}
	if replayAdmission.DeliveryEventReplayReady && routeRecovery == nil {
		return result, fmt.Errorf("selected-contract activation gate requires persisted route recovery before delivery replay")
	}
	if !plan.ExecutionReady {
		return result, fmt.Errorf("selected-contract activation gate requires execution-ready plan before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if plan.ReplayResumeAdmission.DeliveryEventReplayReady {
		executionPorts, err := req.ExecutionOwner.require()
		if err != nil {
			return result, fmt.Errorf("%s: %w", runfork.RunForkHistoricalReplayContractSwapBootResumeOwner, err)
		}
		historicalReplayExecution, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
			Admission:             historicalReplayAdmission,
			ReplayResumeAdmission: plan.ReplayResumeAdmission,
			PendingWork:           plan.PendingWork,
		})
		if err != nil {
			return result, err
		}
		contractSwapExecution, err := BuildHistoricalReplayContractSwapBootResumeExecution(HistoricalReplayContractSwapBootResumeRequest{
			SelectedExecutionAdmission: admission,
			ContractSwapAdmission:      contractSwapAdmission,
			HistoricalReplayAdmission:  historicalReplayAdmission,
			HistoricalReplayExecution:  historicalReplayExecution,
			RouteRecovery:              routeRecovery,
		})
		if err != nil {
			return result, err
		}
		result.ContractSwapBootResumeExecution = &contractSwapExecution
		sourceEventIDs := contractSwapBootResumeSourceEvents(contractSwapExecution)
		workflowStates, err := selectedContractWorkflowStateProjection(plan, loadedSource.Source, *model.RecipientPlanning)
		if err != nil {
			return result, err
		}
		container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
			Owner:                 req.ExecutionOwner,
			Admission:             admission,
			LoadedSource:          loadedSource,
			RecipientPlanning:     *model.RecipientPlanning,
			SourceRunID:           binding.SourceRunID,
			ForkRunID:             forkRunID,
			ForkEventID:           plan.ForkPoint.EventID,
			SourceEvents:          sourceEventIDs,
			WorkflowStates:        workflowStates,
			ExecutionOwner:        runfork.RunForkHistoricalReplayContractSwapBootResumeOwner,
			DeferredWorkAdmission: deferredWorkAdmission,
			AgentRuntime:          selectedContractAgentRuntimePlan{Options: req.AgentRuntime},
		})
		if err != nil {
			return result, cleanupSelectedContractExecutionFailure(ctx, executionPorts.fork, forkRunID, err)
		}
		containerProof := container.Proof()
		result.ForkLocalRuntimeContainer = &containerProof
		published, err := container.Publish(ctx)
		result.ExecutedEventCount = len(published)
		result.ForkEvents = published
		if err != nil {
			if authorityErr := container.Fail(ctx, err); authorityErr != nil {
				err = errors.Join(err, authorityErr)
			} else {
				err = cleanupSelectedContractExecutionFailure(ctx, executionPorts.fork, forkRunID, err)
			}
			return result, err
		}
		if err := container.Quiesce(ctx); err != nil {
			if authorityErr := container.Fail(ctx, err); authorityErr != nil {
				return result, errors.Join(err, authorityErr)
			}
			return result, cleanupSelectedContractExecutionFailure(ctx, executionPorts.fork, forkRunID, err)
		}
		activation, err := executionPorts.fork.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
			ForkRunID:             forkRunID,
			ConfirmSourceFreeze:   req.ConfirmSourceFreeze,
			AllowedSourceEventIDs: sourceEventIDs,
			FrontierAdmission:     frontier,
			RouteTopology:         routeTopology,
			RecipientPlanning:     *model.RecipientPlanning,
		})
		result.RunForkActivation = activation
		if err != nil {
			if closeErr := container.Close(ctx); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return result, cleanupSelectedContractExecutionFailure(ctx, executionPorts.fork, forkRunID, err)
		}
		if err := container.Close(ctx); err != nil {
			return result, err
		}
		return result, nil
	}
	if plan.ReplayResumeAdmission.ReplayResumeFactsPresent {
		return result, fmt.Errorf("selected-contract activation gate blocks historical replay before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if frontier.FrontierEventCount > 0 {
		return result, fmt.Errorf("%s: selected-contract frontier execution remains non-mutating", runfork.RunForkBlockerContractFrontierExecutionUnsupported)
	}

	activation, err := req.Store.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
		ForkRunID:                         forkRunID,
		ConfirmSourceFreeze:               req.ConfirmSourceFreeze,
		HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
	})
	result.RunForkActivation = activation
	return result, err
}

func selectedContractBlockerCodes(blockers []runfork.RunForkUnsupportedBlocker) string {
	if len(blockers) == 0 {
		return "none"
	}
	codes := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if code := strings.TrimSpace(blocker.Code); code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "none"
	}
	return strings.Join(codes, ",")
}
