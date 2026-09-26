package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// SelectedContractForkLocalRuntimeContainer is the canonical live runtime
// container proof for selected-contract fork execution. It coordinates existing
// child owners; it does not authorize source-row replay or restart recovery.
type SelectedContractForkLocalRuntimeContainer struct {
	Owner                                          string                                             `json:"owner"`
	ExecutionOwner                                 string                                             `json:"execution_owner"`
	SourceRunID                                    string                                             `json:"source_run_id"`
	ForkRunID                                      string                                             `json:"fork_run_id"`
	ForkPoint                                      runfork.RunForkPoint                               `json:"fork_point"`
	ForkEventID                                    string                                             `json:"fork_event_id"`
	SourceEventIDs                                 []string                                           `json:"source_event_ids,omitempty"`
	RecipientPlanningOwner                         string                                             `json:"recipient_planning_owner"`
	DeferredWorkAdmissionOwner                     string                                             `json:"deferred_work_admission_owner"`
	AuthoritativeAgentDeliveryMaterializationOwner string                                             `json:"authoritative_agent_delivery_materialization_owner"`
	AgentRuntimeMaterializationOwner               string                                             `json:"agent_runtime_materialization_owner,omitempty"`
	RuntimePlatformEventLineagePolicyOwner         string                                             `json:"runtime_platform_event_lineage_policy_owner"`
	TypedRuntimeLineageOwner                       string                                             `json:"typed_runtime_lineage_owner"`
	RouteRecoveryOwner                             string                                             `json:"route_recovery_owner"`
	ActivationGateOwner                            string                                             `json:"activation_gate_owner"`
	EventBusRecipientPlanGuard                     bool                                               `json:"eventbus_recipient_plan_guard"`
	RuntimeActiveAgentDescriptorsEphemeral         bool                                               `json:"runtime_active_agent_descriptors_ephemeral"`
	EphemeralAgentRuntime                          bool                                               `json:"ephemeral_agent_runtime"`
	QuiescenceRequired                             bool                                               `json:"quiescence_required"`
	CleanupRequired                                bool                                               `json:"cleanup_required"`
	RuntimeExecutionID                             string                                             `json:"runtime_execution_id"`
	RuntimeGeneration                              uint64                                             `json:"runtime_generation"`
	AuthorityExecutionOwner                        string                                             `json:"authority_execution_owner"`
	AdmissionFingerprint                           string                                             `json:"admission_fingerprint"`
	ContainerPlanFingerprint                       string                                             `json:"container_plan_fingerprint"`
	ActorCensusFingerprint                         string                                             `json:"actor_census_fingerprint"`
	EffectiveConfigFingerprint                     string                                             `json:"effective_config_fingerprint"`
	InvalidPaths                                   []runfork.RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	SplitSiblings                                  []runfork.RunForkSelectedContractExecutionBoundary `json:"split_siblings,omitempty"`
}

type selectedContractForkLocalRuntimeContainer struct {
	proof             SelectedContractForkLocalRuntimeContainer
	req               publishSelectedContractForkEventsRequest
	ports             *selectedContractExecutionPorts
	authority         runtimeeffects.Authority
	admission         managedexecution.Admission
	runtimeInstanceID string
	diagnostics       *selectedForkCommitDiagnostics
}

func buildSelectedContractForkLocalRuntimeContainer(ctx context.Context, req publishSelectedContractForkEventsRequest) (out selectedContractForkLocalRuntimeContainer, finalErr error) {
	if err := ctx.Err(); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	if req.Operation == nil {
		return selectedContractForkLocalRuntimeContainer{}, errors.New("selected-contract runtime container requires its admitted operation")
	}
	if req.Prepared == nil || req.Prepared.operation != req.Operation {
		return selectedContractForkLocalRuntimeContainer{}, errors.New("selected-contract container requires its exact preparation")
	}
	ports, err := req.Owner.require()
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
	}
	sourceRunID, err := requireSelectedContractRuntimeContainerUUID("source run_id", req.SourceRunID)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	forkRunID, err := requireSelectedContractRuntimeContainerUUID("fork run_id", req.ForkRunID)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	point, err := validateSelectedContractRuntimeContainerForkPoint(req.SourcePlan, req.Admission, sourceRunID, forkRunID, req.ForkEventID)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	forkEventID := point.EventID
	if err := req.DeferredWorkAdmission.validate(sourceRunID, point, req.LoadedSource.Source); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	if req.Admission.DeferredWorkAdmissionOwner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires %s execution admission evidence",
			runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
		)
	}
	if strings.TrimSpace(req.RecipientPlanning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires %s; got %q",
			runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			runfork.RunForkSelectedContractRecipientPlanningOwner,
			req.RecipientPlanning.Owner,
		)
	}
	deliveryMaterialization, err := RequireSelectedContractAgentDeliveryMaterialization(ctx, SelectedContractAgentDeliveryMaterializationRequest{
		RunID:             forkRunID,
		RecipientPlanning: req.RecipientPlanning,
		AgentRuntime:      req.AgentRuntime.Proof,
	})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s consumes %s: %w",
			runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
			err,
		)
	}
	executionOwner := selectedContractRuntimeContainerExecutionOwner(req.ExecutionOwner)
	if err := validateSelectedContractRuntimeContainerExecutionOwner(executionOwner); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	sourceEventIDs := normalizeSelectedContractRuntimeContainerSourceEvents(req.SourceEvents)
	agentRuntimeOwner := strings.TrimSpace(req.AgentRuntime.Proof.Owner)
	if deliveryMaterialization.MaterializationRequired && agentRuntimeOwner != runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires %s for planned agent recipients; got %q",
			runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
			agentRuntimeOwner,
		)
	}
	req.ExecutionOwner = executionOwner
	proof := SelectedContractForkLocalRuntimeContainer{
		Owner:                      runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
		ExecutionOwner:             executionOwner,
		SourceRunID:                sourceRunID,
		ForkRunID:                  forkRunID,
		ForkPoint:                  point,
		ForkEventID:                forkEventID,
		SourceEventIDs:             sourceEventIDs,
		RecipientPlanningOwner:     req.RecipientPlanning.Owner,
		DeferredWorkAdmissionOwner: req.DeferredWorkAdmission.owner,
		AuthoritativeAgentDeliveryMaterializationOwner: deliveryMaterialization.Owner,
		AgentRuntimeMaterializationOwner:               agentRuntimeOwner,
		RuntimePlatformEventLineagePolicyOwner:         runfork.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
		TypedRuntimeLineageOwner:                       runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
		RouteRecoveryOwner:                             runfork.RunForkSelectedContractRouteRecoveryOwner,
		ActivationGateOwner:                            runfork.RunForkSelectedContractExecutionActivationGateOwner,
		EventBusRecipientPlanGuard:                     true,
		RuntimeActiveAgentDescriptorsEphemeral:         true,
		EphemeralAgentRuntime:                          true,
		QuiescenceRequired:                             true,
		CleanupRequired:                                true,
		InvalidPaths:                                   selectedContractRuntimeContainerInvalidPaths(),
		SplitSiblings:                                  selectedContractRuntimeContainerSplitSiblings(),
	}
	containerFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(struct {
		Proof             SelectedContractForkLocalRuntimeContainer
		RecipientPlanning runfork.RunForkSelectedContractRecipientPlanning
		SourceEvents      []string
	}{proof, req.RecipientPlanning, sourceEventIDs})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	actorFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(req.AgentRuntime.Records)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	configFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(req.AgentRuntime.Options.Config)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	posture := req.AgentRuntime.Options.ExecutionPosture
	if !posture.Valid() {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("selected-contract execution posture is invalid")
	}
	mode := runtimeeffects.ExecutionMode(posture.RootMode())
	var profile llmselection.Profile
	if cfg := req.AgentRuntime.Options.Config; cfg != nil {
		profile, err = cfg.LLMBackendProfile()
		if err != nil {
			return selectedContractForkLocalRuntimeContainer{}, err
		}
	}
	if err := validateSelectedContractAgentExecutionSelections(profile, req.AgentRuntime.Records); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	bundleHash := req.LoadedSource.SourceArtifactFact.BundleHash()
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(scope.RuntimeInstanceID) == "" || scope.BundleHash != bundleHash {
		return selectedContractForkLocalRuntimeContainer{}, errors.New("selected-contract runtime container requires exact selected bundle scope")
	}
	preparation, err := req.Prepared.bind(ctx, forkRunID, req.LoadedSource, req.AgentRuntime)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	issued, err := ports.runtimeExecution.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
		Preparation:             preparation,
		RecoveryFromExecutionID: req.Prepared.recoveryFromExecutionID,
		DeclarationPlan:         req.AgentRuntime.Declarations,
		Admission:               req.Admission, ContainerPlanFingerprint: containerFingerprint,
		ActorCensusFingerprint: actorFingerprint, EffectiveConfigFingerprint: configFingerprint, ExecutionMode: mode,
	})
	proof.RuntimeExecutionID = issued.ExecutionID
	proof.RuntimeGeneration = issued.Generation
	proof.AdmissionFingerprint = issued.AdmissionFingerprint
	proof.ContainerPlanFingerprint = issued.ContainerPlanFingerprint
	proof.ActorCensusFingerprint = issued.ActorCensusFingerprint
	proof.EffectiveConfigFingerprint = issued.EffectiveConfigFingerprint
	container := selectedContractForkLocalRuntimeContainer{proof: proof, req: req, ports: ports, diagnostics: &selectedForkCommitDiagnostics{}}
	if err != nil {
		return container, err
	}
	authorityOwner := executionOwner + ":" + uuid.NewString()
	authority, err := ports.runtimeExecution.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, authorityOwner, 2*time.Minute)
	// Construction owns failure settlement, including acknowledged claims with errors.
	container.authority = authority
	container.proof.AuthorityExecutionOwner = authority.ExecutionOwner
	defer func() {
		if finalErr != nil && out.authority.Valid() {
			if cleanupErr := out.Fail(ctx, finalErr); cleanupErr != nil {
				finalErr = errors.Join(finalErr, cleanupErr)
			} else {
				out.authority = runtimeeffects.Authority{}
			}
		}
	}()
	if err != nil {
		return container, err
	}
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation, authority.SelectedFork.ForkRunID, issued.ActorCensusFingerprint,
		bundleHash, preparation.SurfaceIDs())
	if err != nil {
		return container, err
	}
	if err := req.Operation.Bind(worklifetime.SelectedForkIdentity{
		ExecutionID: issued.ExecutionID, RunID: forkRunID, Generation: issued.Generation,
	}); err != nil {
		return container, err
	}
	container.admission = admission
	container.runtimeInstanceID = strings.TrimSpace(scope.RuntimeInstanceID)
	return container, nil
}

func validateSelectedContractAgentExecutionSelections(profile llmselection.Profile, records []runtimemanager.PersistedAgent) error {
	if profile.ID == "" {
		return nil
	}
	for _, record := range records {
		_, selectionErr := runtimellm.ValidateAgentExecutionDescriptor(profile, record.Config)
		if selectionErr != nil {
			return fmt.Errorf("selected-contract agent %s execution selection: %w", record.Config.ID, selectionErr)
		}
	}
	return nil
}

func (c selectedContractForkLocalRuntimeContainer) Proof() SelectedContractForkLocalRuntimeContainer {
	return c.proof
}

func (c selectedContractForkLocalRuntimeContainer) Publish(ctx context.Context) (published []SelectedContractExecutionForkEvent, finalErr error) {
	req := c.req
	req.RuntimeInstanceID = c.runtimeInstanceID
	forkOwner := req.Operation.selected
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok || forkOwner == nil || owner != forkOwner || forkOwner.Identity() != (worklifetime.SelectedForkIdentity{
		ExecutionID: c.proof.RuntimeExecutionID, RunID: req.ForkRunID, Generation: c.proof.RuntimeGeneration,
	}) {
		return nil, errors.New("selected-contract publication requires its exact bound operation context")
	}
	req.AgentRuntime.Options.AgentManagerOptions.WorkOwner = forkOwner
	controller := runtimeeffects.NewController(c.ports.effects).WithExecutionPosture(c.req.AgentRuntime.Options.ExecutionPosture)
	receiverExecution, err := eventreceiver.SelectedContractForkExecution(
		c.authority, c.admission, controller, selectedContractRuntimeContainerLineage(c.proof),
	)
	if err != nil {
		return nil, fmt.Errorf("construct selected-contract receiver execution: %w", err)
	}
	req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution = receiverExecution
	if req.SourcePlan.ForkPoint.Kind == runfork.RunForkPointEvent {
		if err := c.ports.replay.EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, req.SourceRunID, req.SourcePlan.ForkPoint.EventID); err != nil {
			return nil, err
		}
	}
	sourceEvents, err := c.ports.replay.LoadRunForkSelectedContractSourceEvents(ctx, req.SourceRunID, req.ForkRunID, req.SourceEvents, req.OriginalLoopCarriage)
	if err != nil {
		sourceRunID, forkRunID, committed, ok := isolatedSelectedForkSourceEventsCommit(err)
		if !ok {
			return nil, err
		}
		if sourceRunID != req.SourceRunID || forkRunID != req.ForkRunID {
			return nil, errors.Join(err, errors.New("selected-contract source event commit scope differs from admitted fork"))
		}
		sourceEvents = committed
	}
	if scopeErr := requireExactSelectedForkSourceEvents(req.SourceEvents, sourceEvents); scopeErr != nil {
		return nil, errors.Join(err, scopeErr)
	}
	if err != nil {
		c.diagnostics.add(err)
	}
	root, err := semanticview.AdmitRootExecutionCoordinate(req.LoadedSource.Source, req.ForkRunID)
	if err != nil {
		return nil, err
	}
	sourceEvents, workflowProjection, err := projectSelectedContractSourceEvents(req.SourceRunID, root, sourceEvents)
	if err != nil {
		return nil, err
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(req.RecipientPlanning, req.LoadedSource.Source, workflowProjection, c.proof.ExecutionOwner)
	if err != nil {
		return nil, err
	}
	var lifecycleManager *runtimemanager.AgentManager
	deliveryAuthority, err := runtimedelivery.NewSelectedExecutionAuthority(
		req.LoadedSource.SourceArtifactFact,
		c.authority.SelectedFork.ExecutionID,
		c.authority.SelectedFork.ForkRunID,
		c.authority.SelectedFork.Generation,
	)
	if err != nil {
		return nil, fmt.Errorf("create selected-contract delivery authority: %w", err)
	}
	if req.Prepared.recoveryFromExecutionID != "" {
		if err := c.ports.fork.ReconcileSelectedSuccessorDeliveryAuthority(ctx, req.Prepared.recoveryFromExecutionID, deliveryAuthority); err != nil {
			return nil, fmt.Errorf("reconcile selected successor deliveries: %w", err)
		}
	}
	payloadAdmitter := runtimepkg.NewRuntimePayloadAdmitter(nil, req.LoadedSource.Source, req.LoadedSource.SourceArtifactFact)
	bus, err := runtimebus.NewEventBusWithOptions(c.ports.events, runtimebus.EventBusOptions{
		ExecutionPosture:            req.AgentRuntime.Options.ExecutionPosture,
		WorkOwner:                   forkOwner,
		RuntimeInstanceID:           c.runtimeInstanceID,
		ReceiverExecution:           receiverExecution,
		Durable:                     c.ports.busDurable,
		PipelineObligations:         c.ports.pipelineObligations,
		SourceArtifactFact:          req.LoadedSource.SourceArtifactFact,
		DeliveryAuthority:           deliveryAuthority,
		ContractBundle:              req.LoadedSource.Source,
		PayloadAdmitter:             payloadAdmitter,
		Logger:                      selectedContractRuntimeContainerLogger(c.ports.logs, req.AgentRuntime.Options.ExecutionPosture, payloadAdmitter),
		RecipientPlanAdmissionGuard: guard.AuthorizeEvent,
		RecipientPlanMaterializer:   guard.MaterializeNodeDeliveryRoutes,
		RecipientPlanGuard:          guard.Authorize,
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, activation runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if lifecycleManager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
			}
			return lifecycleManager.PrepareFlowInstanceActivation(ctx, activation)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if lifecycleManager == nil {
				return fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
			}
			return lifecycleManager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("create selected-contract fork-local runtime container bus: %w", err)
	}
	pipeline := newSelectedContractPipeline(bus, c.ports, req.LoadedSource, req.AgentRuntime.Options, func(ctx context.Context, deactivation runtimepipeline.FlowInstanceDeactivationRequest) (runtimepipeline.PreparedFlowInstanceDeactivation, error) {
		if lifecycleManager == nil {
			return nil, fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
		}
		return lifecycleManager.PrepareFlowInstanceDeactivation(ctx, deactivation)
	})
	bus.SetInterceptors(pipeline)

	runCtx := selectedContractRuntimeContainerLineageContext(ctx, c.proof)
	runCtx = runtimeeffects.WithAuthority(runCtx, c.authority)
	runCtx = runtimeeffects.WithController(runCtx, controller)
	runCtx = managedexecution.WithAdmission(runCtx, c.admission)
	runCtx, cancelRuntime := context.WithCancel(runCtx)
	defer cancelRuntime()
	heartbeatErr := make(chan error, 1)
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan error, 1)
	var stopHeartbeatOnce sync.Once
	var heartbeatJoinErr error
	stopHeartbeatWork := func() {
		stopHeartbeatOnce.Do(func() {
			close(stopHeartbeat)
			heartbeatJoinErr = <-heartbeatDone
		})
	}
	heartbeatLease, err := forkOwner.BeginStanding(runCtx)
	if err != nil {
		return nil, fmt.Errorf("admit selected-fork heartbeat: %w", err)
	}
	defer func() {
		stopHeartbeatWork()
		finalErr = errors.Join(finalErr, heartbeatJoinErr)
	}()
	go func() {
		defer func() { heartbeatDone <- heartbeatLease.Done() }()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-heartbeatLease.Context().Done():
				return
			case <-ticker.C:
				if err := c.ports.runtimeExecution.HeartbeatRunForkSelectedContractRuntimeExecution(context.WithoutCancel(heartbeatLease.Context()), c.authority, 2*time.Minute); err != nil {
					heartbeatErr <- err
					cancelRuntime()
					return
				}
			}
		}
	}()
	agentRuntime, admission, err := startSelectedContractAgentRuntime(runCtx, req, bus, pipeline, c.diagnostics)
	if err != nil {
		return nil, err
	}
	runCtx = managedexecution.WithAdmission(runCtx, admission)
	if agentRuntime == nil || agentRuntime.manager == nil {
		return nil, fmt.Errorf("selected-contract fork-local lifecycle manager was not materialized")
	}
	lifecycleManager = agentRuntime.manager
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(lifecycleManager.FinalizeCommittedAgentReadiness))
	continuationFailures := make(chan error, 1)
	continuations, err := runtimedeliverycontinuation.NewSelected(
		c.ports.busDurable.DeliveryLifecycle, deliveryAuthority, forkOwner, bus,
		func(_ context.Context, cause error) {
			select {
			case continuationFailures <- cause:
			default:
			}
			cancelRuntime()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create selected delivery continuation owner: %w", err)
	}
	if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
		return nil, err
	}
	if err := continuations.Start(runCtx); err != nil {
		return nil, fmt.Errorf("start selected delivery continuations: %w", err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, continuations.Retire(context.WithoutCancel(runCtx)))
	}()
	if req.Prepared.recoveryFromExecutionID != "" {
		if err := bus.RecoverSelectedRunPipelineToExhaustion(runCtx, req.ForkRunID); err != nil {
			return nil, fmt.Errorf("recover selected child event pipeline: %w", err)
		}
		if err := c.ports.fork.ReconcileSelectedSuccessorDeliveryAuthority(runCtx, req.Prepared.recoveryFromExecutionID, deliveryAuthority); err != nil {
			return nil, fmt.Errorf("reconcile recovered selected child deliveries: %w", err)
		}
		if err := continuations.Synchronize(runCtx); err != nil {
			return nil, fmt.Errorf("synchronize recovered selected child deliveries: %w", err)
		}
	}
	agentRuntimeStopped := false
	if agentRuntime != nil {
		defer func() {
			if !agentRuntimeStopped {
				finalErr = errors.Join(finalErr, agentRuntime.Shutdown())
			}
		}()
	}
	out := make([]SelectedContractExecutionForkEvent, 0, len(sourceEvents))
	for _, sourceEvent := range sourceEvents {
		input := req.Prepared.inputs[sourceEvent.SourceEventID]
		_, hasPublication := sourceEvent.InputPublication.Event()
		if hasPublication != input.Present() {
			return out, fmt.Errorf("selected input publication changed after preparation")
		}
		if hasPublication {
			fingerprint, err := selectedPreparationFingerprint(sourceEvent.InputPublication.Coordinates())
			if err != nil || fingerprint != req.Prepared.inputCoordinates[sourceEvent.SourceEventID] {
				return out, fmt.Errorf("selected input evidence changed after preparation: %v", err)
			}
		}
		forkEventID := activityidentity.ForkLineageEventID(req.ForkRunID, sourceEvent.SourceEventID)
		evt, err := selectedContractForkEvent(req.SourceRunID, req.ForkRunID, forkEventID, sourceEvent, c.proof.ExecutionOwner)
		if err != nil {
			return out, err
		}
		guard.ExpectForkEvent(forkEventID, sourceEvent.SourceEventID)
		eventCtx := runtimecorrelation.WithRuntimeLineageSubject(runCtx, forkEventID, sourceEvent.EventName)
		prepared, err := bus.PrepareSelectedForkPublish(eventCtx, evt, input)
		if err != nil {
			return out, fmt.Errorf("%s execute selected-contract fork event %s as %s: %w",
				runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
				forkEventID,
				err,
			)
		}
		lineage := runfork.RunForkSelectedContractExecutionLineage{
			ForkRunID:          req.ForkRunID,
			SourceRunID:        req.SourceRunID,
			SourceEventID:      sourceEvent.SourceEventID,
			ForkEventID:        forkEventID,
			EventName:          sourceEvent.EventName,
			SelectionAuthority: c.proof.ExecutionOwner,
			CreatedAt:          prepared.Event.CreatedAt(),
		}
		committed, commitErr := c.ports.replay.CommitSelectedForkEvent(eventCtx, prepared.SelectedForkCommitRequest(lineage))
		if committed.AppendOutcome == 0 && commitErr != nil {
			return out, errors.Join(commitErr, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		if err := committed.Validate(); err != nil {
			return out, errors.Join(commitErr, err, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		committedPrepared, err := prepared.WithCommitOutcome(committed.AppendOutcome)
		if err != nil {
			return out, errors.Join(commitErr, err, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		committedPrepared = committedPrepared.WithCommittedDeliveryHandoffs(committed.DeliveryHandoffs)
		prepared = committedPrepared
		// The producer returns this evidence only after acknowledged COMMIT.
		// Complete its exact handoff once, even if transaction cleanup failed.
		out = append(out, SelectedContractExecutionForkEvent{
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
		})
		if err := bus.DispatchPreparedPublishAndWait(eventCtx, prepared); err != nil {
			return out, errors.Join(commitErr, fmt.Errorf("%s dispatch committed selected-contract fork event %s as %s: %w",
				runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
				forkEventID,
				err,
			))
		}
		if commitErr != nil {
			return out, commitErr
		}
		if err := runtimepkg.NewRuntimeLogger(c.ports.logs, req.AgentRuntime.Options.ExecutionPosture, payloadAdmitter).Log(eventCtx, runtimepkg.RuntimeLogEntry{
			Level:     diaglog.LevelInfo,
			Message:   "Selected-contract fork event completed local dispatch",
			Component: "run_fork",
			Action:    "selected_contract_event_dispatched",
			EventID:   prepared.Event.ID(),
			EventType: string(prepared.Event.Type()),
			EntityID:  prepared.Event.EntityID(),
			Detail: map[string]any{
				"source_run_id":   req.SourceRunID,
				"source_event_id": sourceEvent.SourceEventID,
			},
		}); err != nil {
			return out, fmt.Errorf("%s persist selected-contract fork event dispatch evidence %s: %w",
				runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				forkEventID,
				err,
			)
		}
	}
	if agentRuntime.generationGrant == nil {
		return out, errors.New("selected deployment serving requires the admitted runtime generation grant")
	}
	grant, err := agentRuntime.generationGrant.Evidence()
	if err != nil {
		return out, fmt.Errorf("load selected deployment generation grant: %w", err)
	}
	if err := c.serveSelectedDeploymentFeeds(runCtx, pipeline, grant); err != nil {
		return out, err
	}
	if req.Prepared.recoveryFromExecutionID != "" {
		if err := c.ports.fork.ReconcileSelectedSuccessorDeliveryAuthority(runCtx, req.Prepared.recoveryFromExecutionID, deliveryAuthority); err != nil {
			return out, fmt.Errorf("reconcile newly handed-off selected deliveries: %w", err)
		}
	}
	if err := continuations.Synchronize(runCtx); err != nil {
		return out, fmt.Errorf("synchronize selected delivery continuations: %w", err)
	}
	if agentRuntime != nil {
		timeout := req.AgentRuntime.Options.QuiescenceTimeout
		if timeout <= 0 {
			timeout = selectedContractAgentRuntimeDefaultQuiescenceTimeout
		}
		waitCtx, cancel := context.WithTimeout(runCtx, timeout)
		defer cancel()
		if err := agentRuntime.WaitForQuiescence(waitCtx, bus); err != nil {
			return out, fmt.Errorf("%s wait for selected-fork runtime quiescence: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
		select {
		case err := <-continuationFailures:
			return out, fmt.Errorf("selected delivery continuation: %w", err)
		default:
		}
		stopHeartbeatWork()
		if err := agentRuntime.Shutdown(); err != nil {
			return out, fmt.Errorf("%s stop selected-fork runtime after quiescence: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
		agentRuntimeStopped = true
	}
	select {
	case err := <-heartbeatErr:
		return out, fmt.Errorf("%s heartbeat selected-fork completion authority: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
	default:
	}
	return out, nil
}

func (c selectedContractForkLocalRuntimeContainer) serveSelectedDeploymentFeeds(ctx context.Context, coordinator *runtimepipeline.PipelineCoordinator, grant startupownership.GrantEvidence) error {
	if err := grant.Validate(); err != nil {
		return fmt.Errorf("selected deployment grant: %w", err)
	}
	if grant.SelectedFork == nil || grant.SelectedFork.ForkRunID != c.req.ForkRunID || grant.State != startupownership.GrantAdmitted {
		return errors.New("selected deployment grant differs from the executing fork")
	}
	feeds, err := c.ports.deploymentFanOut.ListSelectedDeploymentFeeds(ctx, grant)
	if err != nil {
		return fmt.Errorf("list selected deployment feeds: %w", err)
	}
	if err := validateSelectedDeploymentFeedAgreement(c.req.SourcePlan, selectedDeploymentFeedRequests(feeds), c.req.ForkRunID, c.req.LoadedSource.SourceArtifactFact.BundleHash()); err != nil {
		return err
	}
	if len(feeds) == 0 {
		return nil
	}
	owner, err := c.ports.deploymentFanOut.BindSelectedDeploymentFanOutGrant(grant)
	if err != nil {
		return fmt.Errorf("bind selected deployment feed owner: %w", err)
	}
	for _, feed := range feeds {
		for {
			if feed.Status == fanoutobligation.StatusClosed {
				break
			}
			if feed.Status != fanoutobligation.StatusOpen {
				return fmt.Errorf("selected deployment feed %s is %s before serving", feed.Request.Key.DeploymentFeedID, feed.Status)
			}
			turn, err := coordinator.ServeFanOutCandidate(ctx, owner, feed.Request.Key)
			if err != nil {
				return fmt.Errorf("serve selected deployment feed: %w", err)
			}
			observed, err := c.ports.deploymentFanOut.ListSelectedDeploymentFeeds(ctx, grant)
			if err != nil {
				return fmt.Errorf("observe selected deployment feed after serving: %w", err)
			}
			if err := validateSelectedDeploymentFeedAgreement(c.req.SourcePlan, selectedDeploymentFeedRequests(observed), c.req.ForkRunID, c.req.LoadedSource.SourceArtifactFact.BundleHash()); err != nil {
				return err
			}
			current, err := exactSelectedDeploymentFeed(observed, feed)
			if err != nil {
				return err
			}
			complete, err := selectedDeploymentTurnComplete(feed, current, turn.Refill)
			if err != nil {
				return err
			}
			if complete {
				break
			}
			feed = current
		}
	}
	return nil
}

func selectedDeploymentTurnComplete(previous, current fanoutobligation.Intent, refill bool) (bool, error) {
	if current.Status == fanoutobligation.StatusClosed {
		return true, nil
	}
	if current.Status != fanoutobligation.StatusOpen || !refill || current.Cursor <= previous.Cursor {
		return false, fmt.Errorf("selected deployment feed %s remains %s at %d/%d after serving", current.Request.Key.DeploymentFeedID, current.Status, current.Cursor, current.Request.Cardinality)
	}
	return false, nil
}

func exactSelectedDeploymentFeed(feeds []fanoutobligation.Intent, expected fanoutobligation.Intent) (fanoutobligation.Intent, error) {
	for _, feed := range feeds {
		if feed.Request.Key != expected.Request.Key {
			continue
		}
		if feed.Request.Deployment == nil || expected.Request.Deployment == nil ||
			*feed.Request.Deployment != *expected.Request.Deployment ||
			feed.Request.Source != expected.Request.Source || feed.Request.Cardinality != expected.Request.Cardinality {
			return fanoutobligation.Intent{}, errors.New("selected deployment feed changed its immutable source while serving")
		}
		return feed, nil
	}
	return fanoutobligation.Intent{}, errors.New("selected deployment feed disappeared while serving")
}

func selectedDeploymentFeedRequests(feeds []fanoutobligation.Intent) []fanoutobligation.IntentRequest {
	requests := make([]fanoutobligation.IntentRequest, 0, len(feeds))
	for _, feed := range feeds {
		requests = append(requests, feed.Request)
	}
	return requests
}

func validateSelectedDeploymentFeedAgreement(plan runfork.RunForkPlan, feeds []fanoutobligation.IntentRequest, forkRunID, bundleHash string) error {
	expected, err := selectedDeploymentSourceDeclarations(plan)
	if err != nil {
		return err
	}
	if len(feeds) != len(expected) {
		return fmt.Errorf("selected fork has %d deployment feeds, fixed revision requires %d", len(feeds), len(expected))
	}
	seen := make(map[durabledata.DeclarationRef]struct{}, len(feeds))
	for _, feed := range feeds {
		if err := feed.Validate(); err != nil {
			return fmt.Errorf("selected deployment feed: %w", err)
		}
		if feed.Key.RunID != forkRunID || feed.Deployment == nil || feed.Deployment.BundleHash != bundleHash {
			return errors.New("selected deployment feed differs from its fork or bundle")
		}
		declaration := feed.Deployment.Declaration
		if _, ok := expected[declaration]; !ok {
			return errors.New("selected fork has a deployment feed absent from its fixed source revision")
		}
		if _, duplicate := seen[declaration]; duplicate {
			return errors.New("selected fork has duplicate deployment feed declarations")
		}
		seen[declaration] = struct{}{}
	}
	return nil
}

func selectedDeploymentSourceDeclarations(plan runfork.RunForkPlan) (map[durabledata.DeclarationRef]struct{}, error) {
	expected := make(map[durabledata.DeclarationRef]struct{})
	for _, obligation := range plan.FanOutObligations {
		request := obligation.Intent.Request
		if request.Deployment == nil {
			continue
		}
		if err := request.Validate(); err != nil {
			return nil, fmt.Errorf("fixed-revision deployment feed: %w", err)
		}
		if request.Key.RunID != plan.SourceRunID {
			return nil, errors.New("fixed-revision deployment feed belongs to another source run")
		}
		declaration := request.Deployment.Declaration
		if _, duplicate := expected[declaration]; duplicate {
			return nil, errors.New("fixed revision has duplicate deployment feed declarations")
		}
		expected[declaration] = struct{}{}
	}
	if len(expected) > 0 {
		if err := plan.ForkPoint.Validate(); err != nil {
			return nil, fmt.Errorf("selected deployment feeds require an exact fixed source revision: %w", err)
		}
	}
	return expected, nil
}

func projectSelectedContractSourceEvents(
	sourceRunID string,
	root semanticview.RootExecutionCoordinate,
	eventsIn []runfork.RunForkSelectedContractSourceEvent,
) ([]runfork.RunForkSelectedContractSourceEvent, selectedContractWorkflowProjection, error) {
	projection := selectedContractWorkflowProjection{
		root: root, sourceEvents: make(map[string]struct{}, len(eventsIn)),
	}
	if err := projection.requireChildRun(root.RunID()); err != nil {
		return nil, selectedContractWorkflowProjection{}, err
	}
	out := append([]runfork.RunForkSelectedContractSourceEvent(nil), eventsIn...)
	for index := range out {
		projected, err := runfork.ProjectSelectedContractSourceEvent(sourceRunID, root.RunID(), out[index])
		if err != nil {
			return nil, selectedContractWorkflowProjection{}, err
		}
		if _, duplicate := projection.sourceEvents[projected.SourceEventID]; duplicate {
			return nil, selectedContractWorkflowProjection{}, fmt.Errorf("selected-contract source event %s is duplicated", projected.SourceEventID)
		}
		projection.sourceEvents[projected.SourceEventID] = struct{}{}
		out[index] = projected
	}
	return out, projection, nil
}

func (c selectedContractForkLocalRuntimeContainer) Quiesce(ctx context.Context) error {
	return c.ports.runtimeExecution.QuiesceRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority)
}

func (c selectedContractForkLocalRuntimeContainer) Close(ctx context.Context) error {
	return c.ports.runtimeExecution.CloseRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority.ID)
}

func (c selectedContractForkLocalRuntimeContainer) Fail(ctx context.Context, cause error) error {
	if c.proof.ForkPoint.Kind == runfork.RunForkPointDeploymentRevision && errors.Is(cause, worklifetime.ErrRetired) {
		// The process owner already withdrew execution. Startup recovery must
		// fence this generation and its open claims before issuing a successor.
		return fmt.Errorf("selected finite-feed predecessor awaits fenced recovery: %w", worklifetime.ErrRetired)
	}
	failure := runtimefailures.FromError(cause, runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, "execute")
	raw, err := json.Marshal(failure.Failure)
	if err != nil {
		return err
	}
	if err := c.ports.runtimeExecution.FailRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority, raw); err != nil {
		return err
	}
	return c.Close(ctx)
}

type selectedContractRuntimeContainerLoggerHook struct {
	logger *runtimepkg.RuntimeLogger
}

func (h selectedContractRuntimeContainerLoggerHook) ProjectLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic) error {
	return h.logger.ProjectLifecycleDiagnostic(ctx, item)
}

func selectedContractRuntimeContainerLogger(persistence runtimepkg.RuntimeLogPersistence, posture executionposture.Posture, payloadAdmitter runtimebus.PayloadAdmitter) runtimebus.LoggerHook {
	if persistence == nil {
		return nil
	}
	return selectedContractRuntimeContainerLoggerHook{logger: runtimepkg.NewRuntimeLogger(persistence, posture, payloadAdmitter)}
}

func (h selectedContractRuntimeContainerLoggerHook) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if h.logger == nil {
		return nil
	}
	return h.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:       level,
		Message:     message,
		Component:   component,
		Action:      action,
		EventID:     eventID,
		EventType:   eventType,
		AgentID:     agentID,
		EntityID:    strings.TrimSpace(entityID),
		SessionID:   sessionID,
		Correlation: correlation,
		Detail:      detail,
		Failure:     runtimefailures.CloneEnvelope(failure),
		DurationUS:  durationUS,
	})
}

func selectedContractRuntimeContainerLineageContext(ctx context.Context, proof SelectedContractForkLocalRuntimeContainer) context.Context {
	ctx = runtimecorrelation.WithRunID(ctx, proof.ForkRunID)
	return runtimecorrelation.WithRuntimeLineage(ctx, selectedContractRuntimeContainerLineage(proof))
}

func selectedContractRuntimeContainerLineage(proof SelectedContractForkLocalRuntimeContainer) runtimecorrelation.RuntimeLineage {
	return runtimecorrelation.RuntimeLineage{
		Owner:               proof.TypedRuntimeLineageOwner,
		RunID:               proof.ForkRunID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   proof.Owner,
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	}
}

func selectedContractRuntimeContainerExecutionOwner(owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return runfork.RunForkSelectedContractExecutionOwner
	}
	return owner
}

func validateSelectedContractRuntimeContainerExecutionOwner(owner string) error {
	switch strings.TrimSpace(owner) {
	case runfork.RunForkSelectedContractExecutionOwner, runfork.RunForkHistoricalReplayContractSwapBootResumeOwner:
		return nil
	default:
		return fmt.Errorf("%s cannot execute for owner %q", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, owner)
	}
}

func requireSelectedContractRuntimeContainerUUID(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s requires %s", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, name)
	}
	if _, err := uuid.Parse(value); err != nil {
		return "", fmt.Errorf("%s requires %s to be a UUID: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, name, err)
	}
	return value, nil
}

func validateSelectedContractRuntimeContainerForkPoint(plan runfork.RunForkPlan, admission runfork.RunForkSelectedContractExecutionAdmission, sourceRunID, forkRunID, forkEventID string) (runfork.RunForkPoint, error) {
	point := plan.ForkPoint
	if err := point.Validate(); err != nil {
		return runfork.RunForkPoint{}, fmt.Errorf("selected-contract runtime container fork point: %w", err)
	}
	if plan.SourceRunID != sourceRunID || admission.SourceRunID != sourceRunID || admission.ForkRunID != forkRunID ||
		admission.ForkPoint.Kind != point.Kind || admission.ForkPoint.Revision != point.Revision ||
		admission.ForkPoint.EventID != point.EventID || admission.ForkEventID != point.EventID || forkEventID != point.EventID {
		return runfork.RunForkPoint{}, errors.New("selected-contract runtime container fork point differs from admitted fixed revision")
	}
	return point, nil
}

func normalizeSelectedContractRuntimeContainerSourceEvents(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func selectedContractRuntimeContainerInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_row_copy_as_execution_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source events, deliveries, outcomes, routes, sessions, turns, audits, and runtime diagnostics remain lineage/blocker evidence; the container mints fresh fork-local runtime rows",
		},
		{
			Concept:     "eventbus_descriptor_as_semantic_owner",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "EventBus runtime descriptors are in-memory container evidence only and must not become selected-fork authority outside the container",
		},
		{
			Concept:     "normal_agent_manager_state_as_selected_fork_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "selected-fork agents are ephemeral fork-local handlers and must not persist ordinary current-runtime agent rows as selected-fork truth",
		},
		{
			Concept:     "readiness_or_operator_output_authorizes_runtime",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "readiness JSON, CLI, API, dashboard, and Builder are consumers only and cannot own runtime-container semantics",
		},
	}
}

func selectedContractRuntimeContainerSplitSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "restart_recovery",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
			Reason:      "the live selected-fork runtime container is not restart/recovery ownership",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "historical conversation reconstruction remains split; fresh fork-local rows may only come from normal selected-fork execution",
		},
		{
			Concept:     "non_agent_delivery_replay",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkHistoricalReplayExecutionAdmissionOwner,
			Reason:      "node/system/platform delivery replay needs separate handler/idempotency/recovery ownership",
		},
	}
}
