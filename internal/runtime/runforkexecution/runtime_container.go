package runforkexecution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
)

// SelectedContractForkLocalRuntimeContainer is the canonical live runtime
// container proof for selected-contract fork execution. It coordinates existing
// child owners; it does not authorize source-row replay or restart recovery.
type SelectedContractForkLocalRuntimeContainer struct {
	Owner                                          string                                           `json:"owner"`
	ExecutionOwner                                 string                                           `json:"execution_owner"`
	SourceRunID                                    string                                           `json:"source_run_id"`
	ForkRunID                                      string                                           `json:"fork_run_id"`
	ForkEventID                                    string                                           `json:"fork_event_id"`
	SourceEventIDs                                 []string                                         `json:"source_event_ids,omitempty"`
	RecipientPlanningOwner                         string                                           `json:"recipient_planning_owner"`
	AuthoritativeAgentDeliveryMaterializationOwner string                                           `json:"authoritative_agent_delivery_materialization_owner"`
	AgentRuntimeMaterializationOwner               string                                           `json:"agent_runtime_materialization_owner,omitempty"`
	RuntimePlatformEventLineagePolicyOwner         string                                           `json:"runtime_platform_event_lineage_policy_owner"`
	TypedRuntimeLineageOwner                       string                                           `json:"typed_runtime_lineage_owner"`
	RouteRecoveryOwner                             string                                           `json:"route_recovery_owner"`
	ActivationGateOwner                            string                                           `json:"activation_gate_owner"`
	EventBusRecipientPlanGuard                     bool                                             `json:"eventbus_recipient_plan_guard"`
	RuntimeActiveAgentDescriptorsEphemeral         bool                                             `json:"runtime_active_agent_descriptors_ephemeral"`
	EphemeralAgentRuntime                          bool                                             `json:"ephemeral_agent_runtime"`
	QuiescenceRequired                             bool                                             `json:"quiescence_required"`
	CleanupRequired                                bool                                             `json:"cleanup_required"`
	RuntimeExecutionID                             string                                           `json:"runtime_execution_id"`
	RuntimeGeneration                              uint64                                           `json:"runtime_generation"`
	AuthorityExecutionOwner                        string                                           `json:"authority_execution_owner"`
	AdmissionFingerprint                           string                                           `json:"admission_fingerprint"`
	ContainerPlanFingerprint                       string                                           `json:"container_plan_fingerprint"`
	ActorCensusFingerprint                         string                                           `json:"actor_census_fingerprint"`
	EffectiveConfigFingerprint                     string                                           `json:"effective_config_fingerprint"`
	InvalidPaths                                   []store.RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	SplitSiblings                                  []store.RunForkSelectedContractExecutionBoundary `json:"split_siblings,omitempty"`
}

type selectedContractForkLocalRuntimeContainer struct {
	proof     SelectedContractForkLocalRuntimeContainer
	req       publishSelectedContractForkEventsRequest
	authority runtimeeffects.Authority
	admission managedexecution.Admission
}

func buildSelectedContractForkLocalRuntimeContainer(ctx context.Context, req publishSelectedContractForkEventsRequest) (selectedContractForkLocalRuntimeContainer, error) {
	if err := ctx.Err(); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	if req.Store == nil {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires postgres store", store.RunForkSelectedContractForkLocalRuntimeContainerOwner)
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
	if strings.TrimSpace(req.RecipientPlanning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires %s; got %q",
			store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			store.RunForkSelectedContractRecipientPlanningOwner,
			req.RecipientPlanning.Owner,
		)
	}
	deliveryMaterialization, err := RequireSelectedContractAgentDeliveryMaterialization(ctx, SelectedContractAgentDeliveryMaterializationRequest{
		RecipientPlanning: req.RecipientPlanning,
		AgentRuntime:      req.AgentRuntime.Proof,
	})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s consumes %s: %w",
			store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
			err,
		)
	}
	executionOwner := selectedContractRuntimeContainerExecutionOwner(req.ExecutionOwner)
	if err := validateSelectedContractRuntimeContainerExecutionOwner(executionOwner); err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	sourceEventIDs := normalizeSelectedContractRuntimeContainerSourceEvents(req.SourceEvents)
	agentRuntimeOwner := strings.TrimSpace(req.AgentRuntime.Proof.Owner)
	if deliveryMaterialization.MaterializationRequired && agentRuntimeOwner != store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires %s for planned agent recipients; got %q",
			store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
			agentRuntimeOwner,
		)
	}
	req.ExecutionOwner = executionOwner
	proof := SelectedContractForkLocalRuntimeContainer{
		Owner:                  store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
		ExecutionOwner:         executionOwner,
		SourceRunID:            sourceRunID,
		ForkRunID:              forkRunID,
		ForkEventID:            forkEventID,
		SourceEventIDs:         sourceEventIDs,
		RecipientPlanningOwner: req.RecipientPlanning.Owner,
		AuthoritativeAgentDeliveryMaterializationOwner: deliveryMaterialization.Owner,
		AgentRuntimeMaterializationOwner:               agentRuntimeOwner,
		RuntimePlatformEventLineagePolicyOwner:         store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
		TypedRuntimeLineageOwner:                       store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
		RouteRecoveryOwner:                             store.RunForkSelectedContractRouteRecoveryOwner,
		ActivationGateOwner:                            store.RunForkSelectedContractExecutionActivationGateOwner,
		EventBusRecipientPlanGuard:                     true,
		RuntimeActiveAgentDescriptorsEphemeral:         true,
		EphemeralAgentRuntime:                          true,
		QuiescenceRequired:                             true,
		CleanupRequired:                                true,
		InvalidPaths:                                   selectedContractRuntimeContainerInvalidPaths(),
		SplitSiblings:                                  selectedContractRuntimeContainerSplitSiblings(),
	}
	containerFingerprint, err := store.RunForkSelectedContractRuntimeFingerprint(struct {
		Proof             SelectedContractForkLocalRuntimeContainer
		RecipientPlanning store.RunForkSelectedContractRecipientPlanning
		SourceEvents      []string
	}{proof, req.RecipientPlanning, sourceEventIDs})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	actorFingerprint, err := store.RunForkSelectedContractRuntimeFingerprint(req.AgentRuntime.Records)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	configFingerprint, err := store.RunForkSelectedContractRuntimeFingerprint(req.AgentRuntime.Options.Config)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	mode := runtimeeffects.ExecutionModeLive
	if cfg := req.AgentRuntime.Options.Config; cfg != nil {
		profile, profileErr := cfg.LLMBackendProfile()
		if profileErr != nil {
			return selectedContractForkLocalRuntimeContainer{}, profileErr
		}
		mode, profileErr = llmselection.ExecutionModeForProfile(profile)
		if profileErr != nil {
			return selectedContractForkLocalRuntimeContainer{}, profileErr
		}
	}
	for _, record := range req.AgentRuntime.Records {
		if record.Config.ExecutionMode.Valid() && record.Config.ExecutionMode != mode {
			return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("selected-contract agent %s execution mode %s conflicts with selected backend mode %s", record.Config.ID, record.Config.ExecutionMode, mode)
		}
	}
	issued, err := req.Store.IssueRunForkSelectedContractRuntimeExecution(ctx, store.SelectedContractRuntimeExecutionIssueRequest{
		Admission: req.Admission, ContainerPlanFingerprint: containerFingerprint,
		ActorCensusFingerprint: actorFingerprint, EffectiveConfigFingerprint: configFingerprint,
	})
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	authorityOwner := executionOwner + ":" + uuid.NewString()
	authority, err := req.Store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, authorityOwner, 2*time.Minute)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	authority.ExecutionMode = mode
	proof.RuntimeExecutionID = issued.ExecutionID
	proof.RuntimeGeneration = issued.Generation
	proof.AuthorityExecutionOwner = authorityOwner
	proof.AdmissionFingerprint = issued.AdmissionFingerprint
	proof.ContainerPlanFingerprint = issued.ContainerPlanFingerprint
	proof.ActorCensusFingerprint = issued.ActorCensusFingerprint
	proof.EffectiveConfigFingerprint = issued.EffectiveConfigFingerprint
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation, authority.SelectedFork.ForkRunID, issued.ActorCensusFingerprint,
		issued.EffectiveConfigFingerprint, nil)
	if err != nil {
		return selectedContractForkLocalRuntimeContainer{}, err
	}
	return selectedContractForkLocalRuntimeContainer{proof: proof, req: req, authority: authority, admission: admission}, nil
}

func (c selectedContractForkLocalRuntimeContainer) Proof() SelectedContractForkLocalRuntimeContainer {
	return c.proof
}

func (c selectedContractForkLocalRuntimeContainer) Publish(ctx context.Context) ([]SelectedContractExecutionForkEvent, error) {
	req := c.req
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
	if err := req.Store.EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, req.SourceRunID, req.ForkEventID); err != nil {
		return nil, err
	}
	sourceEvents, err := req.Store.LoadRunForkSelectedContractSourceEvents(ctx, req.SourceRunID, req.ForkRunID, req.SourceEvents)
	if err != nil {
		return nil, err
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(req.RecipientPlanning, c.proof.ExecutionOwner)
	if err != nil {
		return nil, err
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(req.Store.DB)
	var lifecycleManager *runtimemanager.AgentManager
	bus, err := runtimebus.NewEventBusWithOptions(req.Store, runtimebus.EventBusOptions{
		WorkOwner:                   forkOwner,
		ContractBundle:              req.LoadedSource.Source,
		Logger:                      selectedContractRuntimeContainerLogger(req.Store),
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
	pipeline := newSelectedContractPipeline(bus, req.Store, req.LoadedSource, req.AgentRuntime.Options, workflowStore, func(ctx context.Context, activation runtimepipeline.FlowInstanceActivationRequest) error {
		if lifecycleManager == nil {
			return fmt.Errorf("selected-contract fork-local lifecycle manager is not initialized")
		}
		return lifecycleManager.ActivateFlowInstance(ctx, activation)
	})
	bus.SetInterceptors(pipeline)

	runCtx := selectedContractRuntimeContainerLineageContext(ctx, c.proof)
	runCtx = runtimeeffects.WithAuthority(runCtx, c.authority)
	runCtx = runtimeeffects.WithController(runCtx, runtimeeffects.NewController(req.Store))
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
				if err := req.Store.HeartbeatRunForkSelectedContractRuntimeExecution(context.WithoutCancel(heartbeatLease.Context()), c.authority, 2*time.Minute); err != nil {
					heartbeatErr <- err
					cancelRuntime()
					return
				}
			}
		}
	}()
	agentRuntime, admission, err := startSelectedContractAgentRuntime(runCtx, req, bus)
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
				store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
				forkEventID,
				err,
			)
		}
		lineage := store.RunForkSelectedContractExecutionLineage{
			ForkRunID:          req.ForkRunID,
			SourceRunID:        req.SourceRunID,
			SourceEventID:      sourceEvent.SourceEventID,
			ForkEventID:        forkEventID,
			EventName:          sourceEvent.EventName,
			SelectionAuthority: c.proof.ExecutionOwner,
			CreatedAt:          prepared.Event.CreatedAt(),
		}
		outcome, err := req.Store.CommitSelectedForkEvent(eventCtx, store.CommitSelectedForkEventRequest{
			Commit: prepared.CommitRequest(), Lineage: lineage,
		})
		if err != nil {
			bus.AbandonPreparedPublish(eventCtx, prepared)
			return out, err
		}
		committedPrepared, err := prepared.WithCommitOutcome(outcome)
		if err != nil {
			bus.AbandonPreparedPublish(eventCtx, prepared)
			return out, err
		}
		prepared = committedPrepared
		if err := bus.DispatchPreparedPublishAndWait(eventCtx, prepared); err != nil {
			return out, fmt.Errorf("%s dispatch committed selected-contract fork event %s as %s: %w",
				store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
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
			return out, fmt.Errorf("%s stop selected-fork runtime before quiescence: %w", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
		agentRuntimeStopped = true
		timeout := req.AgentRuntime.Options.QuiescenceTimeout
		if timeout <= 0 {
			timeout = selectedContractAgentRuntimeDefaultQuiescenceTimeout
		}
		waitCtx, cancel := context.WithTimeout(runCtx, timeout)
		defer cancel()
		if err := agentRuntime.WaitForQuiescence(waitCtx, bus); err != nil {
			return out, fmt.Errorf("%s wait for selected-fork runtime quiescence: %w", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
		}
	}
	select {
	case err := <-heartbeatErr:
		return out, fmt.Errorf("%s heartbeat selected-fork completion authority: %w", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, err)
	default:
	}
	return out, nil
}

func (c selectedContractForkLocalRuntimeContainer) Quiesce(ctx context.Context) error {
	return c.req.Store.QuiesceRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority)
}

func (c selectedContractForkLocalRuntimeContainer) Close(ctx context.Context) error {
	return c.req.Store.CloseRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority.ID)
}

func (c selectedContractForkLocalRuntimeContainer) Fail(ctx context.Context, cause error) error {
	failure := runtimefailures.FromError(cause, store.RunForkSelectedContractForkLocalRuntimeContainerOwner, "execute")
	raw, err := json.Marshal(failure.Failure)
	if err != nil {
		return err
	}
	if err := c.req.Store.FailRunForkSelectedContractRuntimeExecution(context.WithoutCancel(ctx), c.authority, raw); err != nil {
		return err
	}
	return c.Close(ctx)
}

type selectedContractRuntimeContainerLoggerHook struct {
	logger *runtimepkg.RuntimeLogger
}

func selectedContractRuntimeContainerLogger(pg *store.PostgresStore) runtimebus.LoggerHook {
	if pg == nil || pg.DB == nil {
		return nil
	}
	return selectedContractRuntimeContainerLoggerHook{logger: runtimepkg.NewRuntimeLogger(pg)}
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
	return runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
		Owner:               proof.TypedRuntimeLineageOwner,
		RunID:               proof.ForkRunID,
		RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   proof.Owner,
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})
}

func selectedContractRuntimeContainerExecutionOwner(owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return store.RunForkSelectedContractExecutionOwner
	}
	return owner
}

func validateSelectedContractRuntimeContainerExecutionOwner(owner string) error {
	switch strings.TrimSpace(owner) {
	case store.RunForkSelectedContractExecutionOwner, store.RunForkHistoricalReplayContractSwapBootResumeOwner:
		return nil
	default:
		return fmt.Errorf("%s cannot execute for owner %q", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, owner)
	}
}

func requireSelectedContractRuntimeContainerUUID(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s requires %s", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, name)
	}
	if _, err := uuid.Parse(value); err != nil {
		return "", fmt.Errorf("%s requires %s to be a UUID: %w", store.RunForkSelectedContractForkLocalRuntimeContainerOwner, name, err)
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

func selectedContractRuntimeContainerInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_row_copy_as_execution_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source events, deliveries, outcomes, routes, sessions, turns, audits, and runtime diagnostics remain lineage/blocker evidence; the container mints fresh fork-local runtime rows",
		},
		{
			Concept:     "eventbus_descriptor_as_semantic_owner",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "EventBus runtime descriptors are in-memory container evidence only and must not become selected-fork authority outside the container",
		},
		{
			Concept:     "normal_agent_manager_state_as_selected_fork_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "selected-fork agents are ephemeral fork-local handlers and must not persist ordinary current-runtime agent rows as selected-fork truth",
		},
		{
			Concept:     "readiness_or_operator_output_authorizes_runtime",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "readiness JSON, CLI, API, dashboard, and Builder are consumers only and cannot own runtime-container semantics",
		},
	}
}

func selectedContractRuntimeContainerSplitSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "restart_recovery",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkHistoricalReplayExecutionAdmissionOwner,
			Reason:      "the live selected-fork runtime container is not restart/recovery ownership",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "historical conversation reconstruction remains split; fresh fork-local rows may only come from normal selected-fork execution",
		},
		{
			Concept:     "non_agent_delivery_replay",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkHistoricalReplayExecutionAdmissionOwner,
			Reason:      "node/system/platform delivery replay needs separate handler/idempotency/recovery ownership",
		},
	}
}
