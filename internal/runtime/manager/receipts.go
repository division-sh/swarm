package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type ActiveRunDeliveryQuiescenceReader interface {
	ActiveRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (string, bool, error)
}

func (am *AgentManager) processEvent(ctx context.Context, agent Agent, evt events.Event) error {
	result := am.processEventDetailed(ctx, agent, evt)
	return result.err
}

type eventProcessResult struct {
	record startupManagerReplayRecord
	err    error
}

func failureEnvelope(err error, component, operation string) *runtimefailures.Envelope {
	failure := runtimefailures.FromError(err, component, operation)
	if failure == nil {
		return nil
	}
	return runtimefailures.CloneEnvelope(&failure.Failure)
}

func (am *AgentManager) processEventDetailed(ctx context.Context, agent Agent, evt events.Event) eventProcessResult {
	route, ok := runtimedelivery.RouteFromContext(ctx)
	if !ok || !route.Recipient.IsAgent() || route.Recipient.ID() != agent.ID() {
		err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_route_identity_missing", "agent-manager", "process_event", map[string]any{
			"agent_id": agent.ID(), "event_id": evt.ID(),
		})
		return eventProcessResult{record: startupManagerReplayRecord{
			Event: evt, AgentID: agent.ID(), Outcome: startupManagerReplayOutcomeDropped,
			ReasonCode: startupManagerReplayReasonDeliveryStartFailed,
			Failure:    failureEnvelope(err, "agent-manager", "acquire_claimed_attempt_lane"),
		}, err: err}
	}
	release, err := am.acquireClaimedAttemptLane(ctx, route.AgentIdentity)
	if err != nil {
		return eventProcessResult{record: startupManagerReplayRecord{
			Event: evt, AgentID: agent.ID(), Outcome: startupManagerReplayOutcomeDropped,
			ReasonCode: startupManagerReplayReasonDeliveryStartFailed,
			Failure:    failureEnvelope(err, "agent-manager", "acquire_claimed_attempt_lane"),
		}, err: err}
	}
	defer release()
	return am.processEventDetailedOwned(ctx, agent, evt)
}

func (am *AgentManager) processEventDetailedOwned(ctx context.Context, agent Agent, evt events.Event) eventProcessResult {
	record := startupManagerReplayRecord{
		Event:   evt,
		AgentID: agent.ID(),
	}
	if strings.EqualFold(strings.TrimSpace(agent.Type()), "llm") {
		if authority, managed := runtimeeffects.AuthorityFromContext(ctx); managed {
			admission, ok := managedexecution.FromContext(ctx)
			if !ok || (authority.Kind == runtimeeffects.AuthorityNormalAgent && !admission.AuthorizesNormal()) ||
				(authority.Kind == runtimeeffects.AuthoritySelectedContractFork && !admission.AuthorizesSelected(authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation)) {
				err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_execution_admission_missing", "agent-manager", "process_event", map[string]any{"agent_id": agent.ID(), "authority_kind": authority.Kind})
				record.Outcome = startupManagerReplayOutcomeDropped
				record.ReasonCode = startupManagerReplayReasonProcessFailed
				record.Failure = failureEnvelope(err, "agent-manager", "process_event")
				return eventProcessResult{record: record, err: err}
			}
		}
	}
	claim, claimed := runtimedelivery.ClaimFromContext(ctx)
	if !claimed {
		err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_claim_missing", "agent-manager", "process_event", map[string]any{"agent_id": agent.ID(), "event_id": evt.ID()})
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
		record.Failure = failureEnvelope(err, "agent-manager", "process_event")
		return eventProcessResult{record: record, err: err}
	}
	if claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != agent.ID() {
		err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_claim_subscriber_mismatch", "agent-manager", "process_event", map[string]any{"agent_id": agent.ID(), "delivery_id": claim.DeliveryID()})
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
		record.Failure = failureEnvelope(err, "agent-manager", "process_event")
		return eventProcessResult{record: record, err: err}
	}
	route, ok := runtimedelivery.RouteFromContext(ctx)
	if !ok || !route.Recipient.IsAgent() || route.Recipient.ID() != agent.ID() {
		err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_route_identity_missing", "agent-manager", "process_event", map[string]any{
			"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(),
		})
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
		record.Failure = failureEnvelope(err, "agent-manager", "process_event")
		return eventProcessResult{record: record, err: err}
	}
	if err := route.AgentIdentity.Validate(); err != nil {
		err = runtimefailures.WrapDetail("delivery_route_identity_invalid", "agent-manager", "process_event", map[string]any{
			"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(),
		}, err)
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
		record.Failure = failureEnvelope(err, "agent-manager", "process_event")
		return eventProcessResult{record: record, err: err}
	}
	am.notifyTestDeliveryStatus(ctx, evt, agent.ID(), runtimedelivery.StatusInProgress)
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), route); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	heartbeat, err := runtimedelivery.StartClaimHeartbeat(ctx, am.workOwner, am.deliveryStore, claim)
	if err != nil {
		claimFailure := runtimefailures.FromError(err, "agent-manager", "renew_delivery_claim")
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = runtimefailures.CloneEnvelope(&claimFailure.Failure)
		return eventProcessResult{record: record, err: claimFailure}
	}
	defer func() { _ = heartbeat.Stop() }()
	attemptCtx := heartbeat.Context()
	if suppress, _ := am.shouldSuppressForBudget(route.AgentIdentity, evt); suppress {
		budgetFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassBudgetExhausted, "spend_budget_emergency", "agent-manager", "delivery_budget_admission", map[string]any{
			"budget_kind": "spend", "agent_id": agent.ID(), "entity_id": evt.EntityID(),
		}), "agent-manager", "delivery_budget_admission")
		record.Failure = &budgetFailure.Failure
		if am.bus != nil {
			_ = am.bus.LogRuntime(attemptCtx, runtimepipeline.RuntimeLogEntry{
				Level: "warn", Component: "agent-manager", Action: "delivery_budget_suppressed", EventID: evt.ID(), AgentID: agent.ID(), EntityID: evt.EntityID(),
				Failure: &budgetFailure.Failure,
			})
		}
		if _, settleErr := am.writeReceipt(attemptCtx, evt, ReceiptStatusError, &budgetFailure.Failure, heartbeat); settleErr != nil {
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = failureEnvelope(settleErr, "agent-manager", "settle_budget_suppression")
			return eventProcessResult{record: record, err: settleErr}
		}
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonBudgetSuppressed
		return eventProcessResult{record: record}
	}
	if am.shouldInterceptDirective(agent.ID(), evt) {
		if _, settleErr := am.writeReceipt(attemptCtx, evt, ReceiptStatusProcessed, nil, heartbeat); settleErr != nil {
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = failureEnvelope(settleErr, "agent-manager", "settle_directive_intercept")
			return eventProcessResult{record: record, err: settleErr}
		}
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonDirectiveIntercepted
		return eventProcessResult{record: record}
	}
	attemptCtx, completionSettlement := runtimeeffects.WithCompletionSettlementObserver(attemptCtx)
	out, err := agent.OnEvent(attemptCtx, evt)
	if observation := completionSettlement(); observation.OriginSettled {
		if observation.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
			originErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_consumer_mismatch", "agent-manager", "process_event", map[string]any{
				"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(), "attempt_id": observation.AttemptID, "origin_kind": observation.Origin.Kind,
			})
			return eventProcessResult{record: record, err: originErr}
		}
		if observation.Disposition != runtimeeffects.CompletionSettlementDrained {
			dispositionErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_settled_without_drained_disposition", "agent-manager", "process_event", map[string]any{
				"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(), "attempt_id": observation.AttemptID, "disposition": observation.Disposition,
			})
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = failureEnvelope(dispositionErr, "agent-manager", "observe_completion_settlement")
			return eventProcessResult{record: record, err: dispositionErr}
		}
		if !claim.Same(observation.Origin.Delivery) {
			claimErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_settled_foreign_origin_delivery", "agent-manager", "process_event", map[string]any{
				"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(), "attempt_id": observation.AttemptID,
			})
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = failureEnvelope(claimErr, "agent-manager", "observe_completion_settlement")
			return eventProcessResult{record: record, err: claimErr}
		}
		if len(out) != 0 {
			outputErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "drained_completion_output_forbidden", "agent-manager", "process_event", map[string]any{
				"agent_id": agent.ID(), "delivery_id": claim.DeliveryID(), "attempt_id": observation.AttemptID,
			})
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = failureEnvelope(outputErr, "agent-manager", "observe_completion_settlement")
			return eventProcessResult{record: record, err: outputErr}
		}
		if observation.Finalization != nil {
			am.lifecycle.observeProviderDrainFinalization(*observation.Finalization)
		}
		if err != nil {
			agentFailure := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event")
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonProcessFailed
			record.Failure = runtimefailures.CloneEnvelope(&agentFailure.Failure)
			return eventProcessResult{record: record, err: agentFailure}
		}
		record.Outcome = startupManagerReplayOutcomeReplayed
		record.ReasonCode = startupManagerReplayReasonReplayed
		return eventProcessResult{record: record}
	}
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), route); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	if err != nil {
		status := receiptStatusForAgentFailure(err)
		agentFailure := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event")
		shutdownAfterSettlement := am.maybeTripAuthCircuitBreaker(ctx, route.AgentIdentity, evt, agentFailure.Failure)
		_, settleErr := am.writeReceipt(attemptCtx, evt, status, &agentFailure.Failure, heartbeat)
		if shutdownAfterSettlement {
			am.lifecycle.requestShutdownTransition()
		}
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = runtimefailures.CloneEnvelope(&agentFailure.Failure)
		return eventProcessResult{record: record, err: errors.Join(agentFailure, settleErr)}
	}
	for idx, e := range out {
		if e.ID() == "" {
			outputID, identityErr := deterministicOutputEventID(evt, route.AgentIdentity, idx, e)
			if identityErr == nil {
				e, identityErr = events.BindManagerOutputIdentity(e, outputID)
			}
			if identityErr != nil {
				pubErr := runtimefailures.WrapDetail("event_output_identity_failed", "agent-manager", "process_event.bind_output_identity", map[string]any{
					"event_type": e.Type(), "agent_id": agent.ID(), "output_index": idx,
				}, identityErr)
				failure := runtimefailures.FromError(pubErr, "agent-manager", "process_event.bind_output_identity")
				_, settleErr := am.writeReceipt(attemptCtx, evt, ReceiptStatusError, &failure.Failure, heartbeat)
				record.Outcome = startupManagerReplayOutcomeDropped
				record.ReasonCode = startupManagerReplayReasonPublishFailed
				record.Failure = runtimefailures.CloneEnvelope(&failure.Failure)
				return eventProcessResult{record: record, err: errors.Join(pubErr, settleErr)}
			}
		}
		if am.shouldSkipAlreadyPublishedOutput(ctx, e.ID()) {
			continue
		}
		if err := am.bus.Publish(attemptCtx, e); err != nil {
			pubErr := runtimefailures.WrapDetail("event_publish_failed", "agent-manager", "process_event.publish_output", map[string]any{
				"event_id": e.ID(), "event_type": e.Type(), "agent_id": agent.ID(),
			}, err)
			failure := runtimefailures.FromError(pubErr, "agent-manager", "process_event.publish_output")
			_, settleErr := am.writeReceipt(attemptCtx, evt, ReceiptStatusError, &failure.Failure, heartbeat)
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonPublishFailed
			record.Failure = runtimefailures.CloneEnvelope(&failure.Failure)
			return eventProcessResult{record: record, err: errors.Join(pubErr, settleErr)}
		}
	}
	if _, settleErr := am.writeReceipt(attemptCtx, evt, ReceiptStatusProcessed, nil, heartbeat); settleErr != nil {
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = failureEnvelope(settleErr, "agent-manager", "settle_delivery")
		return eventProcessResult{record: record, err: settleErr}
	}
	record.Outcome = startupManagerReplayOutcomeReplayed
	record.ReasonCode = startupManagerReplayReasonReplayed
	return eventProcessResult{record: record}
}

func receiptStatusForAgentFailure(err error) ReceiptStatus {
	switch runtimeengine.FailureDispositionFor(err) {
	case runtimeengine.FailureDispositionRetry:
		return ReceiptStatusError
	case runtimeengine.FailureDispositionDeadLetter:
		return ReceiptStatusDeadLetter
	default:
		return ReceiptStatusTerminal
	}
}

func (am *AgentManager) activeRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (startupManagerReplayReasonCode, bool) {
	reader := am.roles.DeliveryQuiescence
	if reader == nil {
		return "", false
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return "", false
	}
	route = route.Normalized()
	reason, ok, err := reader.ActiveRunDeliveryQuiesced(ctx, eventID, route)
	if err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "active_run_quiescence_check_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   route.Recipient.ID(),
				Failure:   failureEnvelope(err, "agent-manager", "check_active_run_quiescence"),
			})
		}
		return "active_run_quiescence_check_failed", true
	}
	return startupManagerReplayReasonCode(strings.TrimSpace(reason)), ok
}

func (am *AgentManager) shouldInterceptDirective(agentID string, evt events.Event) bool {
	_, _ = agentID, evt
	return false
}

func (am *AgentManager) shouldSuppressForBudget(identity runtimeagentidentity.Identity, evt events.Event) (bool, string) {
	execution, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	am.mu.RLock()
	tracker := am.budget
	am.mu.RUnlock()
	if !ok || tracker == nil {
		return false, ""
	}
	cfg := execution.Config
	eventType := strings.ToLower(strings.TrimSpace(string(evt.Type())))
	if eventType == "platform.budget_threshold_crossed" {
		return false, ""
	}
	entityID := strings.TrimSpace(evt.EntityID())
	if entityID == "" {
		entityID = cfg.EffectiveEntityID()
	}

	if tracker.IsEntityEmergency(entityID) {
		return true, "suppressed by budget emergency guardrail"
	}
	if tracker.IsEntityThrottle(entityID) {
		for _, prefix := range am.throttleSuppressPrefixes {
			if strings.HasPrefix(eventType, prefix) {
				return true, "suppressed by budget throttle"
			}
		}
	}
	return false, ""
}

func (am *AgentManager) maybeTripAuthCircuitBreaker(ctx context.Context, identity runtimeagentidentity.Identity, evt events.Event, failure runtimefailures.Envelope) (shutdownAfterSettlement bool) {
	identity = identity.Normalize()
	agentID := identity.AgentID()
	eventID := strings.TrimSpace(evt.ID())
	reason := ""
	authRequired := false
	switch {
	case failure.Class == runtimefailures.ClassAuthenticationNeeded:
		reason = "authentication_intervention_required"
		authRequired = true
	case failure.Class == runtimefailures.ClassConnectorFailure && failure.Detail.Code == "provider_credit_exhausted":
		reason = "provider_credit_intervention_required"
	default:
		return false
	}
	am.runMu.Lock()
	if am.authBreakerTripped {
		am.runMu.Unlock()
		return false
	}
	am.authBreakerTripped = true
	am.runMu.Unlock()
	_, _, running := am.lifecycle.runSnapshot()

	eventCtx := am.runtimePlatformControlEventContext(ctx)
	if am.runtimeIngressSafetyPause != nil {
		if pauseErr := am.runtimeIngressSafetyPause(eventCtx, reason, &failure); pauseErr != nil {
			if am.bus != nil {
				am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "runtime_pause_owner_failed",
					EventID:   strings.TrimSpace(eventID),
					AgentID:   strings.TrimSpace(agentID),
					Failure:   failureEnvelope(pauseErr, "agent-manager", "pause_runtime"),
					Detail: map[string]any{
						"reason": reason,
					},
				})
			}
		}
	} else if am.bus != nil {
		am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
			Level:     "warn",
			Component: "agent-manager",
			Action:    "runtime_pause_owner_missing",
			EventID:   strings.TrimSpace(eventID),
			AgentID:   strings.TrimSpace(agentID),
			Detail: map[string]any{
				"reason": reason,
			},
		})
	}
	if am.bus != nil {
		am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
			Level:     "error",
			Component: "agent-manager",
			Action:    "runtime_pause_breaker_tripped",
			EventID:   strings.TrimSpace(eventID),
			AgentID:   strings.TrimSpace(agentID),
			Detail: map[string]any{
				"reason": reason,
			},
			Failure: runtimefailures.CloneEnvelope(&failure),
		})
	}
	now := time.Now().UTC()
	entityID := ""
	flowInstance := ""
	if execution, ok := am.lifecycle.executionSnapshotByIdentity(identity); ok {
		cfg := execution.Config
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if authRequired {
		authEvt, constructErr := newPlatformCausalRuntimeControlEvent(events.EventLineage{RunID: evt.RunID(), ParentEventID: eventID, TaskID: evt.TaskID(), ExecutionMode: evt.ExecutionMode()}, events.EventType("platform.auth_required"), mustJSON(map[string]any{
			"agent_id":      strings.TrimSpace(agentID),
			"entity_id":     entityID,
			"flow_instance": flowInstance,
			"action":        "llm_call",
			"failure":       failure,
			"timestamp":     now.Format(time.RFC3339Nano),
		}), events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, now)
		if constructErr != nil {
			return running
		}
		if err := am.bus.Publish(eventCtx, authEvt); err != nil {
			if am.bus != nil {
				am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "publish_auth_required_failed",
					EventID:   strings.TrimSpace(eventID),
					AgentID:   strings.TrimSpace(agentID),
					EntityID:  entityID,
					Failure:   failureEnvelope(err, "agent-manager", "publish_auth_required"),
				})
			}
		}
	}
	return running
}

func (am *AgentManager) writeReceipt(ctx context.Context, evt events.Event, status ReceiptStatus, failure *runtimefailures.Envelope, heartbeats ...*runtimedelivery.ClaimHeartbeat) (runtimedelivery.Snapshot, error) {
	eventID := strings.TrimSpace(evt.ID())
	route, routeOK := runtimedelivery.RouteFromContext(ctx)
	if am.deliveryStore == nil || eventID == "" || !routeOK {
		return runtimedelivery.Snapshot{}, fmt.Errorf("delivery settlement requires store, event id, and exact route")
	}
	route = route.Normalized()
	agentID := route.Recipient.ID()
	routeIdentity, err := route.Identity()
	if err != nil {
		return runtimedelivery.Snapshot{}, fmt.Errorf("delivery settlement route: %w", err)
	}
	claim, ok := runtimedelivery.ClaimFromContext(ctx)
	if !ok || claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != agentID || claim.RouteIdentity() != events.EncodeDeliveryRouteIdentity(routeIdentity) {
		if am.bus != nil {
			_ = am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level: "error", Component: "agent-manager", Action: "delivery_settlement_claim_missing",
				EventID: eventID, AgentID: strings.TrimSpace(agentID),
			})
		}
		return runtimedelivery.Snapshot{}, fmt.Errorf("delivery settlement requires the exact agent claim")
	}
	var heartbeat *runtimedelivery.ClaimHeartbeat
	if len(heartbeats) > 0 {
		heartbeat = heartbeats[0]
	}
	if heartbeat == nil {
		var err error
		heartbeat, err = runtimedelivery.StartClaimHeartbeat(ctx, am.workOwner, am.deliveryStore, claim)
		if err != nil {
			return runtimedelivery.Snapshot{}, err
		}
		defer func() { _ = heartbeat.Stop() }()
	}
	settlementGuard, err := heartbeat.BeginSettlement()
	if err != nil {
		return runtimedelivery.Snapshot{}, err
	}
	var snapshot runtimedelivery.Snapshot
	writeCtx := heartbeat.Context()
	if admission, ok := managedexecution.FromContext(writeCtx); ok && admission.Kind == managedexecution.KindSelectedContractFork && status == ReceiptStatusError {
		// A bounded selected-fork runtime cannot hand retry ownership to the
		// store-wide manager backlog after it retires. Preserve the failure as a
		// terminal selected-execution outcome and let activation fail closed.
		status = ReceiptStatusTerminal
	}
	switch status {
	case ReceiptStatusProcessed:
		snapshot, err = am.deliveryStore.SettleSuccess(writeCtx, claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())
	case ReceiptStatusError:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry, ReasonCode: "handler_failure",
			Failure: failure, RetryBase: semanticview.HandlerRetryBase(am.semanticSource),
			RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		})
	case ReceiptStatusDeadLetter:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "dead_letter", Failure: failure,
			RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		})
	case ReceiptStatusTerminal:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "terminal_failure", Failure: failure,
			RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		})
	default:
		err = fmt.Errorf("delivery receipt status %q is invalid", status)
	}
	finishErr := settlementGuard.Finish(err == nil)
	postCtx := context.WithoutCancel(ctx)
	if err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(postCtx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "delivery_settlement_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
				Failure:   failureEnvelope(err, "agent-manager", "settle_delivery"),
				Detail: map[string]any{
					"status": strings.TrimSpace(string(status)),
				},
			})
		}
		return runtimedelivery.Snapshot{}, errors.Join(err, finishErr)
	}
	if snapshot.Status == runtimedelivery.StatusFailed {
		if finishErr != nil {
			return runtimedelivery.Snapshot{}, finishErr
		}
		ownerProvider := am.roles.DeliveryRuntime
		if ownerProvider == nil {
			return runtimedelivery.Snapshot{}, errors.New("retry settlement requires the normal generation continuation owner")
		}
		if err := ownerProvider.RetainDeliveryContinuation(snapshot); err != nil {
			return runtimedelivery.Snapshot{}, fmt.Errorf("transfer retry continuation: %w", err)
		}
	} else if snapshot.Terminal() {
		ownerProvider := am.roles.DeliveryRuntime
		if ownerProvider == nil {
			return runtimedelivery.Snapshot{}, errors.Join(
				finishErr,
				errors.New("terminal settlement requires the exact continuation owner"),
			)
		}
		if err := ownerProvider.ReleaseDeliveryContinuation(snapshot.DeliveryID); err != nil {
			return runtimedelivery.Snapshot{}, errors.Join(finishErr, fmt.Errorf("release terminal continuation: %w", err))
		}
	}
	if finishErr != nil {
		return runtimedelivery.Snapshot{}, finishErr
	}
	am.logDeliveryLifecycle(postCtx, snapshot)
	am.notifyTestDeliveryStatus(postCtx, evt, agentID, snapshot.Status)

	if snapshot.Status == runtimedelivery.StatusDeadLetter {
		am.maybeEscalateDeadLetter(postCtx, evt, route.AgentIdentity, snapshot)
	}
	return snapshot, nil
}

func (am *AgentManager) logDeliveryLifecycle(ctx context.Context, snapshot runtimedelivery.Snapshot) {
	if am == nil || am.bus == nil {
		return
	}
	detail := map[string]any{
		"delivery_id":     snapshot.DeliveryID,
		"subscriber_type": string(snapshot.SubscriberClass),
		"subscriber_id":   snapshot.SubscriberID,
		"retry_count":     snapshot.RetryCount,
	}
	entry := runtimepipeline.RuntimeLogEntry{
		Level:     "debug",
		Component: "agent-manager",
		Action:    "delivery_lifecycle_transition",
		EventID:   snapshot.EventID,
		AgentID:   snapshot.SubscriberID,
		Detail:    detail,
	}
	switch snapshot.Status {
	case runtimedelivery.StatusDelivered:
		detail["delivery_state"] = string(runtimedelivery.StateDelivered)
		detail["delivery_transition"] = string(runtimedelivery.StateDelivered)
		detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
		detail["delivery_reason"] = "agent_processed"
		entry.Message = "Delivery entered delivered state"
	case runtimedelivery.StatusFailed:
		detail["delivery_state"] = string(runtimedelivery.StateRetrying)
		detail["delivery_transition"] = string(runtimedelivery.StateRetrying)
		detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
		detail["delivery_reason"] = "handler_failure"
		entry.Message = "Delivery entered retrying state"
		if snapshot.Failure != nil {
			detail["failure"] = *snapshot.Failure
		}
	case runtimedelivery.StatusDeadLetter:
		detail["delivery_state"] = string(runtimedelivery.StateExhausted)
		detail["delivery_transition"] = string(runtimedelivery.StateExhausted)
		if snapshot.ReasonCode != "retry_exhausted" {
			detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
			detail["delivery_reason"] = "dead_letter"
			detail["delivery_terminal_outcome"] = "dead_letter"
			entry.Message = "Delivery entered dead-letter state"
		} else {
			detail["delivery_previous_state"] = string(runtimedelivery.StateRetrying)
			detail["delivery_reason"] = "retry_exhausted"
			detail["delivery_terminal_outcome"] = "retry_exhausted"
			entry.Message = "Delivery entered exhausted state"
		}
		if snapshot.Failure != nil {
			detail["failure"] = *snapshot.Failure
		}
	default:
		return
	}
	_ = am.bus.LogRuntime(ctx, entry)
}

func (am *AgentManager) maybeEscalateDeadLetter(ctx context.Context, evt events.Event, identity runtimeagentidentity.Identity, snapshot runtimedelivery.Snapshot) {
	eventID := strings.TrimSpace(evt.ID())
	execution, cfgOK := am.lifecycle.executionSnapshotByIdentity(identity)
	cfg := execution.Config
	agentID := identity.AgentID()
	entityID := ""
	flowInstance := ""
	if cfgOK {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	count, sampleEvents, shouldEmit := am.recordDeadLetterEscalation(flowInstance, deadLetterEscalationSample{
		at:         time.Now().UTC(),
		eventID:    strings.TrimSpace(eventID),
		agentID:    strings.TrimSpace(agentID),
		entityID:   entityID,
		retryCount: snapshot.RetryCount,
		failure:    runtimefailures.CloneEnvelope(snapshot.Failure),
	})
	if !shouldEmit {
		return
	}

	eventCtx := am.runtimePlatformControlEventContext(ctx)
	escalation, constructErr := newPlatformCausalRuntimeDiagnosticEvent(events.EventLineage{RunID: evt.RunID(), ParentEventID: eventID, TaskID: evt.TaskID(), ExecutionMode: evt.ExecutionMode()}, events.EventType("platform.dead_letter_escalation"), mustJSON(map[string]any{
		"flow_instance":     flowInstance,
		"dead_letter_count": count,
		"window_minutes":    int(deadLetterEscalationWindow / time.Minute),
		"sample_events":     sampleEvents,
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
	}), events.EventEnvelope{FlowInstance: flowInstance}, time.Now().UTC())
	if constructErr != nil {
		return
	}
	if err := am.bus.Publish(eventCtx, escalation); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "dead_letter_escalation_publish_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
				EntityID:  entityID,
				Failure:   failureEnvelope(err, "agent-manager", "publish_dead_letter_escalation"),
				Detail: map[string]any{
					"flow_instance": flowInstance,
				},
			})
		}
	}
}

func (am *AgentManager) recordDeadLetterEscalation(flowInstance string, sample deadLetterEscalationSample) (int, []map[string]any, bool) {
	flowInstance = strings.TrimSpace(flowInstance)
	key := flowInstance
	if key == "" {
		key = "__global__"
	}
	cutoff := sample.at.Add(-deadLetterEscalationWindow)

	am.deadLetterMu.Lock()
	defer am.deadLetterMu.Unlock()

	window := am.deadLetterWindows[key][:0]
	for _, item := range am.deadLetterWindows[key] {
		if item.at.Before(cutoff) {
			continue
		}
		window = append(window, item)
	}
	window = append(window, sample)
	am.deadLetterWindows[key] = window

	if len(window) < deadLetterEscalationThreshold {
		return len(window), nil, false
	}
	if last := am.deadLetterLastRaised[key]; !last.IsZero() && sample.at.Sub(last) < deadLetterEscalationWindow {
		return len(window), nil, false
	}
	am.deadLetterLastRaised[key] = sample.at

	sampleEvents := make([]map[string]any, 0, len(window))
	for _, item := range window {
		sampleEvents = append(sampleEvents, map[string]any{
			"event_id":    item.eventID,
			"agent_id":    item.agentID,
			"entity_id":   item.entityID,
			"retry_count": item.retryCount,
			"failure":     item.failure,
		})
	}
	return len(window), sampleEvents, true
}
