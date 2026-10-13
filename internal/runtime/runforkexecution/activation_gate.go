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
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
)

type SelectedContractActivationStore interface {
	LoadRunForkSourceRunID(context.Context, string) (string, error)
	LoadRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, bool, error)
	RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	LoadRunForkSelectedContractRouteRecovery(context.Context, string) (runfork.RunForkSelectedContractRouteRecovery, bool, error)
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type SelectedContractActivationGateRequest struct {
	ForkRunID         string
	AllowSourceFreeze bool
	Store             SelectedContractActivationStore
	ExecutionOwner    SelectedContractExecutionOwner
	SourceLoader      SelectedContractSourceLoader
	AgentRuntime      SelectedContractAgentRuntimeOptions
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

func ActivateSelectedContractRunFork(ctx context.Context, req SelectedContractActivationGateRequest) (out SelectedContractActivationGateResult, finalErr error) {
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
	if req.SourceLoader == nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation gate requires selected source loader")
	}
	operation, err := req.ExecutionOwner.beginPreparation(ctx)
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	resources := &PreparedSelectedFork{owner: req.ExecutionOwner, operation: operation}
	defer func() { finalErr = errors.Join(finalErr, req.ExecutionOwner.completePreparation(resources)) }()
	ctx = operation.PreparationContext()
	if !ok {
		sourceRunID, err := req.Store.LoadRunForkSourceRunID(ctx, forkRunID)
		if err != nil {
			return SelectedContractActivationGateResult{}, err
		}
		original, err := loadOriginalLoopCarriage(ctx, req.SourceLoader, sourceRunID, resources)
		if err != nil {
			return SelectedContractActivationGateResult{}, err
		}
		activation, err := req.Store.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
			OriginalLoopCarriage:              original,
			ForkRunID:                         forkRunID,
			AllowSourceFreeze:                 req.AllowSourceFreeze,
			HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
		})
		return SelectedContractActivationGateResult{RunForkActivation: activation}, err
	}
	if err := req.ExecutionOwner.bindStagedPreparation(operation, binding); err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	ctx = operation.PreparationContext()
	forkBundleIdentity, err := req.Store.LoadRunBundleAvailability(ctx, forkRunID)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected-contract fork bundle identity: %w", err)
	}
	expectedBundleHash := strings.TrimSpace(forkBundleIdentity.BundleHash)
	if expectedBundleHash == "" {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: selected-contract fork run %s has no persisted bundle_hash", runbundle.CodeBundleDataIntegrityError, forkRunID)
	}
	if forkBundleIdentity.DataIntegrityError() {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: %s", runbundle.CodeBundleDataIntegrityError, forkBundleIdentity.DetailString())
	}
	expectedSourceFact, err := runtimecorrelation.NewSourceArtifactFact(expectedBundleHash)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("%s: selected-contract fork run %s has invalid bundle source fact: %w", runbundle.CodeBundleDataIntegrityError, forkRunID, err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, expectedSourceFact)

	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID:        binding.SourceRunID,
		BundleHash:         expectedBundleHash,
		SourceArtifactFact: expectedSourceFact,
		Selection:          binding.ContractSelection,
	}, &resources.loadedSource)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("load selected semantic source for activation gate: %w", err)
	}
	original, err := loadOriginalLoopCarriage(ctx, req.SourceLoader, binding.SourceRunID, resources)
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	resources.originalLoopCarriage = original
	if err := loadedSource.SourceArtifactFact.Validate(); err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation source loader returned incomplete bundle identity")
	}
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loadedSource.SourceArtifactFact.BundleHash())
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("resolve selected-contract activation author activity scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, scope)
	operation.preparing = ctx
	operation.owned = runtimeauthoractivity.WithScope(runtimecorrelation.WithSourceArtifactFact(operation.owned, loadedSource.SourceArtifactFact), scope)
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(loadedSource.Source)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("project selected-contract activation descriptors: %w", err)
	}
	lease, err := req.Store.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("register selected-contract activation descriptors: %w", err)
	}
	resources.descriptorLease = lease
	planRequest := runfork.RunForkPlanRequest{SourceRunID: binding.SourceRunID}
	if err := binding.ForkPoint.Validate(); err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("selected-contract activation binding fork point: %w", err)
	}
	if binding.ForkEventID != binding.ForkPoint.EventID {
		return SelectedContractActivationGateResult{}, errors.New("selected-contract activation binding event differs from fork point")
	}
	if binding.ForkPoint.Kind == runfork.RunForkPointEvent {
		planRequest.At = binding.ForkPoint.EventID
	} else {
		planRequest.ResolvedPoint = &binding.ForkPoint
		planRequest.AtStart = binding.ForkPoint.Kind == runfork.RunForkPointRunStart
	}
	plan, err := req.Store.PlanRunFork(ctx, planRequest)
	if err != nil {
		return SelectedContractActivationGateResult{}, fmt.Errorf("plan selected-contract activation gate: %w", err)
	}
	if !sameSelectedForkPointIdentity(plan.ForkPoint, binding.ForkPoint) {
		return SelectedContractActivationGateResult{}, errors.New("selected-contract activation plan differs from durable fork point")
	}
	// Native preparation fingerprints the fixed plan, not resolved diagnostics.
	fixedPlan := plan
	plan.ReplayResumeAdmission.Dispositions = append([]runfork.RunForkReplayResumeDisposition(nil), plan.ReplayResumeAdmission.Dispositions...)
	plan.ReplayResumeAdmission.UnsupportedBlockers = append([]runfork.RunForkUnsupportedBlocker(nil), plan.ReplayResumeAdmission.UnsupportedBlockers...)
	deferredWorkAdmission, err := admitSelectedContractDeferredWork(plan, loadedSource.Source)
	if err != nil {
		return SelectedContractActivationGateResult{Owner: runfork.RunForkSelectedContractExecutionActivationGateOwner}, err
	}
	replayAdmission, err := runfork.RunForkSelectedContractReplayResumeAdmission(plan)
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
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
		SourceArtifactFact:    expectedSourceFact,
		BindingReader:         req.Store,
		LoadedSource:          loadedSource,
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
	readbackPending := selectedContractStagedReadbackPending(replayAdmission)
	if (replayAdmission.DeliveryEventReplayReady || readbackPending && frontier.FrontierEventCount > 0) && routeRecovery == nil {
		return result, fmt.Errorf("selected-contract activation gate requires persisted route recovery before delivery replay")
	}
	if !plan.ExecutionReady && !readbackPending {
		return result, fmt.Errorf("selected-contract activation gate requires execution-ready plan before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if err := req.ExecutionOwner.requirePreparationProcess(ctx, req.AgentRuntime.ProcessCapability); err != nil {
		return result, err
	}
	timerRuntimeRequired := len(plan.WorkflowTimers) > 0 || len(loadedSource.Source.WorkflowTimers()) > 0
	if replayAdmission.DeliveryEventReplayReady || readbackPending || timerRuntimeRequired {
		executionPorts, err := req.ExecutionOwner.require()
		if err != nil {
			return result, fmt.Errorf("%s: %w", runfork.RunForkHistoricalReplayContractSwapBootResumeOwner, err)
		}
		sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
		executionOwner := runfork.RunForkSelectedContractExecutionOwner
		if replayAdmission.DeliveryEventReplayReady {
			historicalReplayExecution, err := BuildHistoricalReplayExecution(HistoricalReplayExecutionRequest{
				Admission:             historicalReplayAdmission,
				ReplayResumeAdmission: plan.ReplayResumeAdmission,
				PendingWork:           plan.PendingWork,
				SourcePlan:            &fixedPlan,
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
			sourceEventIDs = contractSwapBootResumeSourceEvents(contractSwapExecution)
			executionOwner = runfork.RunForkHistoricalReplayContractSwapBootResumeOwner
		}
		agentRuntime, readiness, err := prepareSelectedContractWorkflowReadiness(
			ctx, executionPorts.replay, loadedSource, *model.RecipientPlanning, fixedPlan, frontier, sourceEventIDs, req.AgentRuntime,
		)
		if err != nil {
			return result, err
		}
		resources.agentRuntime = agentRuntime
		preparation, err := prepareSelectedFork(ctx, operation, executionPorts, loadedSource, fixedPlan, frontier, *model.RecipientPlanning, agentRuntime)
		if err != nil {
			return result, err
		}
		preparation.loadedSource, preparation.descriptorLease, preparation.agentRuntime = loadedSource, lease, agentRuntime
		preparation.originalSource = resources.originalSource
		preparation.owner = req.ExecutionOwner
		preparation.readiness = readiness
		preparation.originalLoopCarriage = original
		preparation.plan, preparation.frontier, preparation.routeAdmission = fixedPlan, frontier, routeAdmission
		preparation.routeTopology, preparation.model = routeTopology, model
		preparation.agentRuntime, preparation.deferredWorkAdmission = agentRuntime, deferredWorkAdmission
		resources = preparation
		materializationRequest, err := preparation.MaterializationRequest()
		if err != nil {
			return result, err
		}
		materialization, err := executionPorts.fork.RequireMaterializedRunForkForSelectedContractExecution(ctx, forkRunID, materializationRequest)
		if err != nil {
			return result, err
		}
		replayAdmission, err = selectedContractStagedMaterializationAdmission(materialization, forkRunID, binding.SourceRunID, binding.ForkPoint)
		if err != nil {
			return result, err
		}
		plan.ReplayResumeAdmission = replayAdmission
		plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
		plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
		contractSwapAdmission, err = BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
			SelectedExecutionAdmission: admission, ReplayResumeAdmission: replayAdmission, RouteRecovery: routeRecovery,
		})
		if err != nil {
			return result, err
		}
		historicalReplayAdmission, err = BuildHistoricalReplayExecutionAdmission(HistoricalReplayExecutionAdmissionRequest{
			ReplayResumeAdmission: replayAdmission, SelectedExecutionAdmission: admission,
			ContractSwapAdmission: contractSwapAdmission, RouteRecovery: routeRecovery,
		})
		if err != nil {
			return result, err
		}
		result.ContractSwapBootResumeAdmission = &contractSwapAdmission
		result.HistoricalReplayExecutionAdmission = &historicalReplayAdmission
		prepared, err := readiness.Projection()
		if err != nil {
			return result, err
		}
		workflowStates, err := prepared.MaterializationStates()
		if err != nil {
			return result, err
		}
		agentRuntime, err = bindRecoveredSelectedContractAgentRuntime(
			ctx, executionPorts.workflow, forkRunID, loadedSource, workflowStates, agentRuntime,
		)
		if err != nil {
			return result, err
		}
		ctx = operation.Context()
		container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
			OriginalLoopCarriage:  original,
			Prepared:              preparation,
			Operation:             operation,
			Owner:                 req.ExecutionOwner,
			Admission:             admission,
			LoadedSource:          loadedSource,
			RecipientPlanning:     *model.RecipientPlanning,
			SourceRunID:           binding.SourceRunID,
			ForkRunID:             forkRunID,
			ForkEventID:           plan.ForkPoint.EventID,
			SourceEvents:          sourceEventIDs,
			ExecutionOwner:        executionOwner,
			DeferredWorkAdmission: deferredWorkAdmission,
			SourcePlan:            plan,
			AgentRuntime:          agentRuntime,
		})
		containerProof := container.Proof()
		if containerProof.RuntimeExecutionID != "" {
			result.ForkLocalRuntimeContainer = &containerProof
		}
		if err != nil {
			if container.authority.Valid() {
				return result, err
			}
			return result, resources.cleanupExecutionFailure(ctx, executionPorts.fork, forkRunID, nil, err)
		}
		ctx = operation.Context()
		if err := container.PrepareAttachment(ctx); err != nil {
			acknowledged, authorityErr := container.Fail(ctx, err)
			err = errors.Join(err, authorityErr)
			if acknowledged {
				err = resources.cleanupExecutionFailure(ctx, executionPorts.fork, forkRunID, nil, err)
			}
			return result, err
		}
		ctx = runtimeeffects.WithAuthority(ctx, container.authority)
		activation, err := executionPorts.fork.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
			InheritedSchedules:    readiness,
			ExecutionSource:       loadedSource.Source,
			ForkRunID:             forkRunID,
			AllowSourceFreeze:     req.AllowSourceFreeze,
			AllowedSourceEventIDs: sourceEventIDs,
			FrontierAdmission:     frontier,
			RouteTopology:         routeTopology,
			RecipientPlanning:     *model.RecipientPlanning,
		})
		result.RunForkActivation = activation
		if activation.Activated {
			container.RetainActivated(resources, err)
			return result, nil
		}
		if err == nil {
			err = errors.New("selected staged activation was not acknowledged")
		}
		acknowledged, authorityErr := container.Fail(ctx, err)
		if !acknowledged {
			return result, errors.Join(err, authorityErr)
		}
		return result, resources.cleanupExecutionFailure(ctx, executionPorts.fork, forkRunID, nil, errors.Join(err, authorityErr))
	}
	if plan.ReplayResumeAdmission.ReplayResumeFactsPresent {
		return result, fmt.Errorf("selected-contract activation gate blocks historical replay before mutation; blockers: %s", selectedContractBlockerCodes(plan.UnsupportedBlockers))
	}
	if frontier.FrontierEventCount > 0 {
		return result, fmt.Errorf("%s: selected-contract frontier execution remains non-mutating", runfork.RunForkBlockerContractFrontierExecutionUnsupported)
	}

	if req.AgentRuntime.ProcessCapability == nil {
		return result, errors.New("selected-contract activation requires process capability before state-only mutation")
	}
	if err := req.AgentRuntime.ProcessCapability.ProveCurrent(ctx); err != nil {
		return result, fmt.Errorf("prove selected-contract state-only process: %w", err)
	}
	activation, err := req.Store.ActivateRunFork(operation.Context(), runfork.RunForkActivateRequest{
		OriginalLoopCarriage:              original,
		ForkRunID:                         forkRunID,
		AllowSourceFreeze:                 req.AllowSourceFreeze,
		HistoricalReplayExecutionAdmitter: HistoricalReplayExecutionAdmitter{},
	})
	result.RunForkActivation = activation
	return result, err
}

func selectedContractStagedMaterializationAdmission(materialization runfork.RunForkMaterialization, forkRunID, sourceRunID string, point runfork.RunForkPoint) (runfork.RunForkReplayResumeAdmission, error) {
	if materialization.ForkRunID != forkRunID || materialization.SourceRunID != sourceRunID ||
		!sameSelectedForkPointIdentity(materialization.ForkPoint, point) {
		return runfork.RunForkReplayResumeAdmission{}, errors.New("selected staged materialization readback differs from the exact child and cut")
	}
	admission := materialization.ReplayResumeAdmission
	if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner {
		return runfork.RunForkReplayResumeAdmission{}, errors.New("selected staged materialization readback lacks canonical replay admission")
	}
	if len(materialization.UnsupportedBlockers) != 0 {
		return runfork.RunForkReplayResumeAdmission{}, fmt.Errorf("selected staged materialization remains blocked before issuance: %s", selectedContractBlockerCodes(materialization.UnsupportedBlockers))
	}
	if len(admission.UnsupportedBlockers) != 0 {
		return runfork.RunForkReplayResumeAdmission{}, fmt.Errorf("selected staged replay admission remains blocked before issuance: %s", selectedContractBlockerCodes(admission.UnsupportedBlockers))
	}
	return admission, nil
}

// This permits reaching the native proof, not executing or discharging history.
// Timer history and reply rows remain blocked until their canonical readback.
func selectedContractStagedReadbackPending(admission runfork.RunForkReplayResumeAdmission) bool {
	if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner || !admission.ReplayResumeFactsPresent {
		return false
	}
	pending := make(map[string]string, 2)
	for _, blocker := range admission.UnsupportedBlockers {
		var fact string
		switch blocker.Code {
		case runfork.RunForkBlockerTimerHistoryUnproven:
			fact = runfork.RunForkReplayResumeFactTimerHistory
		case runfork.RunForkBlockerOpenReplyContextUnsupported:
			fact = runfork.RunForkReplayResumeFactOpenReplyContext
		default:
			return false
		}
		if _, duplicate := pending[fact]; duplicate {
			return false
		}
		pending[fact] = blocker.Code
	}
	if len(pending) == 0 {
		return false
	}
	seen := make(map[string]bool, len(pending))
	for _, disposition := range admission.Dispositions {
		if code, exists := pending[disposition.Fact]; exists {
			if seen[disposition.Fact] || disposition.Disposition != runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != code {
				return false
			}
			seen[disposition.Fact] = true
			continue
		}
		if disposition.Disposition == runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != "" {
			return false
		}
	}
	return len(seen) == len(pending)
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
