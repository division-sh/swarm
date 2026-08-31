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

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// SelectedContractForkLocalRuntimeContainer is the canonical live runtime
// container proof for selected-contract fork execution. It coordinates existing
// child owners; it does not authorize source-row replay or restart recovery.
type SelectedContractForkLocalRuntimeContainer struct {
	Owner                                          string                                             `json:"owner"`
	ExecutionOwner                                 string                                             `json:"execution_owner"`
	SourceRunID                                    string                                             `json:"source_run_id"`
	ForkRunID                                      string                                             `json:"fork_run_id"`
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
}

func buildSelectedContractForkLocalRuntimeContainer(ctx context.Context, req publishSelectedContractForkEventsRequest) (selectedContractForkLocalRuntimeContainer, error) {
	if err := ctx.Err(); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
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
	forkEventID, err := requireSelectedContractRuntimeContainerUUID("fork point event_id", req.ForkEventID)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	if err := req.DeferredWorkAdmission.validate(sourceRunID, forkEventID, req.LoadedSource.Source); err != nil {
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
	issued, err := ports.runtimeExecution.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
		Admission: req.Admission, ContainerPlanFingerprint: containerFingerprint,
		ActorCensusFingerprint: actorFingerprint, EffectiveConfigFingerprint: configFingerprint, ExecutionMode: mode,
	})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	authorityOwner := executionOwner + ":" + uuid.NewString()
	authority, err := ports.runtimeExecution.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, authorityOwner, 2*time.Minute)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	proof.RuntimeExecutionID = issued.ExecutionID
	proof.RuntimeGeneration = issued.Generation
	proof.AuthorityExecutionOwner = authorityOwner
	proof.AdmissionFingerprint = issued.AdmissionFingerprint
	proof.ContainerPlanFingerprint = issued.ContainerPlanFingerprint
	proof.ActorCensusFingerprint = issued.ActorCensusFingerprint
	proof.EffectiveConfigFingerprint = issued.EffectiveConfigFingerprint
	bundleHash := req.LoadedSource.BundleSourceFact.BundleHash()
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation, authority.SelectedFork.ForkRunID, issued.ActorCensusFingerprint,
		bundleHash, nil)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(scope.RuntimeInstanceID) == "" || scope.BundleHash != bundleHash {
		return selectedContractForkLocalRuntimeContainer{}, errors.New("selected-contract runtime container requires exact selected bundle scope")
	}
	return selectedContractForkLocalRuntimeContainer{
		proof: proof, req: req, ports: ports, authority: authority, admission: admission,
		runtimeInstanceID: strings.TrimSpace(scope.RuntimeInstanceID),
	}, nil
}

func validateSelectedContractAgentExecutionSelections(profile llmselection.Profile, records []runtimemanager.PersistedAgent) error {
	if profile.ID == "" {
		return nil
	}
	for _, record := range records {
		_, selectionErr := llmselection.ResolveAgentExecutionSelection(llmselection.AgentExecutionSelectionInput{
			ConfiguredDefault: profile,
			AuthoredBackend:   record.Config.LLMBackend,
			MockConfigured:    record.Config.Mock.Configured(),
		})
		if selectionErr != nil {
			return fmt.Errorf("selected-contract agent %s execution selection: %w", record.Config.ID, selectionErr)
		}
	}
	return nil
}

func (c selectedContractForkLocalRuntimeContainer) Proof() SelectedContractForkLocalRuntimeContainer {
	return c.proof
}

func (c selectedContractForkLocalRuntimeContainer) Publish(ctx context.Context) ([]SelectedContractExecutionForkEvent, error) {
	req := c.req
	req.RuntimeInstanceID = c.runtimeInstanceID
	parent, ok := worklifetime.RuntimeOccurrenceFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("selected-contract fork requires an acquired runtime occurrence")
	}
	forkOwner, err := parent.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{
		ExecutionID: c.proof.RuntimeExecutionID,
		RunID:       req.ForkRunID,
		Generation:  c.proof.RuntimeGeneration,
	})
	if err != nil {
		return nil, fmt.Errorf("create selected-fork process occurrence: %w", err)
	}
	defer func() { _ = forkOwner.RetireAndWait(context.Background()) }()
	ctx = worklifetime.WithOccurrence(ctx, forkOwner)
	req.AgentRuntime.Options.AgentManagerOptions.WorkOwner = forkOwner
	controller := runtimeeffects.NewController(c.ports.effects).WithExecutionPosture(c.req.AgentRuntime.Options.ExecutionPosture)
	receiverExecution, err := eventreceiver.SelectedContractForkExecution(
		c.authority, c.admission, controller, selectedContractRuntimeContainerLineage(c.proof),
	)
	if err != nil {
		return nil, fmt.Errorf("construct selected-contract receiver execution: %w", err)
	}
	req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution = receiverExecution
	if err := c.ports.replay.EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, req.SourceRunID, req.ForkEventID); err != nil {
		return nil, err
	}
	sourceEvents, err := c.ports.replay.LoadRunForkSelectedContractSourceEvents(ctx, req.SourceRunID, req.ForkRunID, req.SourceEvents, req.WorkflowStates)
	if err != nil {
		return nil, err
	}
	sourceEvents, err = projectSelectedContractSourceEventWorkflowStates(req.ForkRunID, req.WorkflowStates, sourceEvents)
	if err != nil {
		return nil, err
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(req.RecipientPlanning, req.LoadedSource.Source, c.proof.ExecutionOwner)
	if err != nil {
		return nil, err
	}
	var lifecycleManager *runtimemanager.AgentManager
	deliveryAuthority, err := runtimedelivery.NewSelectedExecutionAuthority(
		req.LoadedSource.BundleSourceFact,
		c.authority.SelectedFork.ExecutionID,
		c.authority.SelectedFork.ForkRunID,
		c.authority.SelectedFork.Generation,
	)
	if err != nil {
		return nil, fmt.Errorf("create selected-contract delivery authority: %w", err)
	}
	bus, err := runtimebus.NewEventBusWithOptions(c.ports.events, runtimebus.EventBusOptions{
		ExecutionPosture:            req.AgentRuntime.Options.ExecutionPosture,
		WorkOwner:                   forkOwner,
		RuntimeInstanceID:           c.runtimeInstanceID,
		ReceiverExecution:           receiverExecution,
		Durable:                     c.ports.busDurable,
		PipelineObligations:         c.ports.pipelineObligations,
		BundleSourceFact:            req.LoadedSource.BundleSourceFact,
		DeliveryAuthority:           deliveryAuthority,
		ContractBundle:              req.LoadedSource.Source,
		Logger:                      selectedContractRuntimeContainerLogger(c.ports.logs, req.AgentRuntime.Options.ExecutionPosture),
		RecipientPlanAdmissionGuard: guard.AuthorizeEvent,
		RecipientPlanMaterializer:   guard.MaterializeNodeDeliveryRoutes,
		RecipientPlanGuard:          guard.Authorize,
		TemplateInstanceActivator: func(ctx context.Context, activation runtimepipeline.FlowInstanceActivationRequest) error {
			if lifecycleManager == nil {
				return fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
			}
			return lifecycleManager.ActivateFlowInstance(ctx, activation)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create selected-contract fork-local runtime container bus: %w", err)
	}
	pipeline := newSelectedContractPipeline(bus, c.ports, req.LoadedSource, req.AgentRuntime.Options, func(ctx context.Context, activation runtimepipeline.FlowInstanceActivationRequest) error {
		if lifecycleManager == nil {
			return fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
		}
		return lifecycleManager.ActivateFlowInstance(ctx, activation)
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
	var stopHeartbeatOnce sync.Once
	stopHeartbeatWork := func() { stopHeartbeatOnce.Do(func() { close(stopHeartbeat) }) }
	defer stopHeartbeatWork()
	heartbeatLease, err := forkOwner.Begin(runCtx)
	if err != nil {
		return nil, fmt.Errorf("admit selected-fork heartbeat: %w", err)
	}
	go func() {
		defer func() { _ = heartbeatLease.Done() }()
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
	agentRuntime, admission, err := startSelectedContractAgentRuntime(runCtx, req, bus, pipeline)
	if err != nil {
		return nil, err
	}
	runCtx = managedexecution.WithAdmission(runCtx, admission)
	if agentRuntime == nil || agentRuntime.manager == nil {
		return nil, fmt.Errorf("selected-contract fork-local lifecycle manager was not materialized")
	}
	lifecycleManager = agentRuntime.manager
	agentRuntimeStopped := false
	if agentRuntime != nil {
		defer func() {
			if !agentRuntimeStopped {
				_ = agentRuntime.Shutdown()
			}
		}()
	}
	out := make([]SelectedContractExecutionForkEvent, 0, len(sourceEvents))
	for _, sourceEvent := range sourceEvents {
		forkEventID := activityidentity.ForkLineageEventID(req.ForkRunID, sourceEvent.SourceEventID)
		evt, err := selectedContractForkEvent(req.SourceRunID, req.ForkRunID, forkEventID, sourceEvent, c.proof.ExecutionOwner)
		if err != nil {
			return out, err
		}
		guard.ExpectForkEvent(forkEventID, sourceEvent.SourceEventID)
		eventCtx := runtimecorrelation.WithRuntimeLineageSubject(runCtx, forkEventID, sourceEvent.EventName)
		prepared, err := bus.PrepareSelectedForkPublish(eventCtx, evt)
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
		committed, err := c.ports.replay.CommitSelectedForkEvent(eventCtx, runtimebus.CommitSelectedForkEventRequest{
			Commit: prepared.CommitRequest(), Lineage: lineage,
		})
		if err != nil {
			return out, errors.Join(err, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		if err := committed.Validate(); err != nil {
			return out, errors.Join(err, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		committedPrepared, err := prepared.WithCommitOutcome(committed.AppendOutcome)
		if err != nil {
			return out, errors.Join(err, bus.AbandonPreparedPublish(eventCtx, prepared))
		}
		committedPrepared = committedPrepared.WithCommittedDeliveryHandoffs(committed.DeliveryHandoffs)
		prepared = committedPrepared
		if err := bus.DispatchPreparedPublishAndWait(eventCtx, prepared); err != nil {
			return out, fmt.Errorf("%s dispatch committed selected-contract fork event %s as %s: %w",
				runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
				forkEventID,
				err,
			)
		}
		if err := runtimepkg.NewRuntimeLogger(c.ports.logs, req.AgentRuntime.Options.ExecutionPosture).Log(eventCtx, runtimepkg.RuntimeLogEntry{
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
		out = append(out, SelectedContractExecutionForkEvent{
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
		})
	}
	if agentRuntime != nil {
		stopHeartbeatWork()
		if err := agentRuntime.Shutdown(); err != nil {
			return out, fmt.Errorf("%s stop selected-fork runtime before quiescence: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
		agentRuntimeStopped = true
		timeout := req.AgentRuntime.Options.QuiescenceTimeout
		if timeout <= 0 {
			timeout = selectedContractAgentRuntimeDefaultQuiescenceTimeout
		}
		waitCtx, cancel := context.WithTimeout(runCtx, timeout)
		defer cancel()
		if err := agentRuntime.WaitForQuiescence(waitCtx, bus); err != nil {
			return out, fmt.Errorf("%s wait for selected-fork runtime quiescence: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
	}
	select {
	case err := <-heartbeatErr:
		return out, fmt.Errorf("%s heartbeat selected-fork completion authority: %w", runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
	default:
	}
	return out, nil
}

func projectSelectedContractSourceEventWorkflowStates(
	forkRunID string,
	states []runfork.RunForkSelectedContractWorkflowState,
	eventsIn []runfork.RunForkSelectedContractSourceEvent,
) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	routesByEntity := make(map[string]string, len(states))
	for _, state := range states {
		entityID := strings.TrimSpace(state.EntityID)
		if entityID == "" {
			return nil, fmt.Errorf("selected-contract source event projection requires exact entity identity")
		}
		route := state.Route
		switch state.AddressKind {
		case runfork.RunForkSelectedContractWorkflowStateRunScope:
			route = runtimeflowidentity.StoredRoute(forkRunID, runtimeflowidentity.LogicalInstanceID(forkRunID), forkRunID)
		case runfork.RunForkSelectedContractWorkflowStateExact:
			route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
		default:
			return nil, fmt.Errorf("selected-contract source event projection has unsupported address kind %q", state.AddressKind)
		}
		if !route.Valid() {
			return nil, fmt.Errorf("selected-contract source event projection requires exact workflow route")
		}
		if existing := routesByEntity[entityID]; existing != "" && existing != route.InstancePath {
			return nil, fmt.Errorf("selected-contract source event entity %s has conflicting workflow routes", entityID)
		}
		routesByEntity[entityID] = route.InstancePath
	}
	out := append([]runfork.RunForkSelectedContractSourceEvent(nil), eventsIn...)
	for index := range out {
		route := routesByEntity[strings.TrimSpace(out[index].EntityID)]
		if route == "" {
			continue
		}
		out[index].FlowInstance = route
		if strings.TrimSpace(out[index].EventName) != "platform.activity_requested" {
			continue
		}
		var payload map[string]any
		if err := canonicaljson.DecodePreservingNumberLexemes(out[index].Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode selected-contract activity route projection for %s: %w", out[index].SourceEventID, err)
		}
		if payload == nil {
			return nil, fmt.Errorf("selected-contract activity route projection for %s requires object payload", out[index].SourceEventID)
		}
		payload["flow_instance"] = route
		raw, err := canonicaljson.MarshalPreservingNumberKinds(payload)
		if err != nil {
			return nil, fmt.Errorf("encode selected-contract activity route projection for %s: %w", out[index].SourceEventID, err)
		}
		out[index].Payload = raw
	}
	return out, nil
}

func (c selectedContractForkLocalRuntimeContainer) Quiesce(ctx context.Context) error {
	return c.ports.runtimeExecution.QuiesceRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority)
}

func (c selectedContractForkLocalRuntimeContainer) Close(ctx context.Context) error {
	return c.ports.runtimeExecution.CloseRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority.ID)
}

func (c selectedContractForkLocalRuntimeContainer) Fail(ctx context.Context, cause error) error {
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

func selectedContractRuntimeContainerLogger(persistence runtimepkg.RuntimeLogPersistence, posture executionposture.Posture) runtimebus.LoggerHook {
	if persistence == nil {
		return nil
	}
	return selectedContractRuntimeContainerLoggerHook{logger: runtimepkg.NewRuntimeLogger(persistence, posture)}
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
