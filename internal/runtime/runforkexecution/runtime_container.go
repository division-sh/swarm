package runforkexecution

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/store"
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
	RouteRecoveryOwner                             string                                           `json:"route_recovery_owner"`
	ActivationGateOwner                            string                                           `json:"activation_gate_owner"`
	EventBusRecipientPlanGuard                     bool                                             `json:"eventbus_recipient_plan_guard"`
	RuntimeActiveAgentDescriptorsEphemeral         bool                                             `json:"runtime_active_agent_descriptors_ephemeral"`
	EphemeralAgentRuntime                          bool                                             `json:"ephemeral_agent_runtime"`
	QuiescenceRequired                             bool                                             `json:"quiescence_required"`
	CleanupRequired                                bool                                             `json:"cleanup_required"`
	InvalidPaths                                   []store.RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	SplitSiblings                                  []store.RunForkSelectedContractExecutionBoundary `json:"split_siblings,omitempty"`
}

type selectedContractForkLocalRuntimeContainer struct {
	proof SelectedContractForkLocalRuntimeContainer
	req   publishSelectedContractForkEventsRequest
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
	if req.ForkTime.IsZero() {
		return selectedContractForkLocalRuntimeContainer{}, fmt.Errorf("%s requires fork point timestamp", store.RunForkSelectedContractForkLocalRuntimeContainerOwner)
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
	return selectedContractForkLocalRuntimeContainer{proof: proof, req: req}, nil
}

func (c selectedContractForkLocalRuntimeContainer) Proof() SelectedContractForkLocalRuntimeContainer {
	return c.proof
}

func (c selectedContractForkLocalRuntimeContainer) Publish(ctx context.Context) ([]SelectedContractExecutionForkEvent, error) {
	req := c.req
	if err := req.Store.EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx, req.SourceRunID, req.ForkEventID, req.ForkTime); err != nil {
		return nil, err
	}
	sourceEvents, err := req.Store.LoadRunForkSelectedContractSourceEvents(ctx, req.SourceRunID, req.SourceEvents)
	if err != nil {
		return nil, err
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(req.RecipientPlanning, c.proof.ExecutionOwner)
	if err != nil {
		return nil, err
	}
	bus, err := runtimebus.NewEventBusWithOptions(req.Store, runtimebus.EventBusOptions{
		ContractBundle:              req.LoadedSource.Source,
		RecipientPlanAdmissionGuard: guard.AuthorizeEvent,
		RecipientPlanGuard:          guard.Authorize,
	})
	if err != nil {
		return nil, fmt.Errorf("create selected-contract fork-local runtime container bus: %w", err)
	}
	pipeline := newSelectedContractPipeline(bus, req.Store, req.LoadedSource)
	bus.SetInterceptors(pipeline)

	runCtx := runtimecorrelation.WithRunID(ctx, req.ForkRunID)
	agentRuntime, err := startSelectedContractAgentRuntime(runCtx, req, bus)
	if err != nil {
		return nil, err
	}
	if agentRuntime != nil {
		defer func() {
			_ = agentRuntime.Shutdown()
		}()
	}
	out := make([]SelectedContractExecutionForkEvent, 0, len(sourceEvents))
	for _, sourceEvent := range sourceEvents {
		forkEventID := uuid.NewString()
		evt := selectedContractForkEvent(req.ForkRunID, forkEventID, sourceEvent, c.proof.ExecutionOwner)
		guard.ExpectForkEvent(forkEventID, sourceEvent.SourceEventID)
		if err := bus.Publish(runCtx, evt); err != nil {
			return out, fmt.Errorf("%s execute selected-contract fork event %s as %s: %w",
				store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
				sourceEvent.SourceEventID,
				forkEventID,
				err,
			)
		}
		lineage := store.RunForkSelectedContractExecutionLineage{
			Owner:         c.proof.ExecutionOwner,
			ForkRunID:     req.ForkRunID,
			SourceRunID:   req.SourceRunID,
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
			CreatedAt:     time.Now().UTC(),
		}
		if err := req.Store.RecordRunForkSelectedContractExecutionLineage(ctx, lineage); err != nil {
			return out, err
		}
		out = append(out, SelectedContractExecutionForkEvent{
			SourceEventID: sourceEvent.SourceEventID,
			ForkEventID:   forkEventID,
			EventName:     sourceEvent.EventName,
		})
	}
	if agentRuntime != nil {
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
	return out, nil
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
			Concept:     "typed_runtime_lineage",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner,
			Reason:      "#708 remains a later typed-lineage sibling; this container consumes the current lineage policy without replacing it",
		},
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
