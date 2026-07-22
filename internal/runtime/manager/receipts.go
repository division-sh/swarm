package manager

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type deadLetterRecorder interface {
	RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error
}

type activeRunDeliveryQuiescenceReader interface {
	ActiveRunDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (string, bool, error)
}

type deliveryRunCompletionConverger interface {
	ConvergeDeliveryRunCompletion(ctx context.Context, evt events.Event) error
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
	if suppress, _ := am.shouldSuppressForBudget(agent.ID(), evt); suppress {
		budgetFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassBudgetExhausted, "spend_budget_emergency", "agent-manager", "delivery_budget_admission", map[string]any{
			"budget_kind": "spend", "agent_id": agent.ID(), "entity_id": evt.EntityID(),
		}), "agent-manager", "delivery_budget_admission")
		record.Failure = &budgetFailure.Failure
		if am.bus != nil {
			_ = am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level: "warn", Component: "agent-manager", Action: "delivery_budget_suppressed", EventID: evt.ID(), AgentID: agent.ID(), EntityID: evt.EntityID(),
				Failure: &budgetFailure.Failure,
			})
		}
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonBudgetSuppressed
		if _, claimed := runtimedelivery.ClaimFromContext(ctx); claimed {
			am.writeReceipt(ctx, evt, agent.ID(), ReceiptStatusError, &budgetFailure.Failure)
		}
		return eventProcessResult{record: record}
	}
	claim, claimed := runtimedelivery.ClaimFromContext(ctx)
	if !claimed {
		if am.deliveryStore == nil {
			err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_lifecycle_owner_missing", "agent-manager", "claim_delivery", map[string]any{"agent_id": agent.ID(), "event_id": evt.ID()})
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
			record.Failure = failureEnvelope(err, "agent-manager", "claim_delivery")
			return eventProcessResult{record: record, err: err}
		}
		route, ok := runtimedelivery.RouteFromContext(ctx)
		if !ok {
			err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_route_missing", "agent-manager", "claim_delivery", map[string]any{"agent_id": agent.ID(), "event_id": evt.ID()})
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
			record.Failure = failureEnvelope(err, "agent-manager", "claim_delivery")
			return eventProcessResult{record: record, err: err}
		}
		claimedDelivery, err := am.deliveryStore.ClaimAgentDelivery(ctx, evt, route)
		if err != nil {
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
			record.Failure = failureEnvelope(err, "agent-manager", "claim_delivery")
			return eventProcessResult{record: record, err: err}
		}
		claim = claimedDelivery.Claim
		ctx = runtimedelivery.WithClaim(ctx, claim)
	}
	if claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != agent.ID() {
		err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_claim_subscriber_mismatch", "agent-manager", "process_event", map[string]any{"agent_id": agent.ID(), "delivery_id": claim.DeliveryID()})
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
		record.Failure = failureEnvelope(err, "agent-manager", "process_event")
		return eventProcessResult{record: record, err: err}
	}
	am.notifyTestDeliveryStatus(ctx, evt, agent.ID(), runtimedelivery.StatusInProgress)
	if am.shouldInterceptDirective(agent.ID(), evt) {
		am.writeReceipt(ctx, evt, agent.ID(), ReceiptStatusProcessed, nil)
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonDirectiveIntercepted
		return eventProcessResult{record: record}
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), agent.ID()); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	out, err, heartbeatErr := am.executeAgentDeliveryHandler(ctx, claim, agent, evt)
	if heartbeatErr != nil {
		claimFailure := runtimefailures.FromError(heartbeatErr, "agent-manager", "renew_delivery_claim")
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = runtimefailures.CloneEnvelope(&claimFailure.Failure)
		return eventProcessResult{record: record, err: claimFailure}
	}
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), agent.ID()); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	if err != nil {
		status := receiptStatusForAgentFailure(err)
		agentFailure := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event")
		am.maybeTripAuthCircuitBreaker(ctx, agent.ID(), evt, agentFailure.Failure)
		am.writeReceipt(ctx, evt, agent.ID(), status, &agentFailure.Failure)
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = runtimefailures.CloneEnvelope(&agentFailure.Failure)
		return eventProcessResult{record: record, err: agentFailure}
	}
	for idx, e := range out {
		if e.ID() == "" {
			var identityErr error
			e, identityErr = events.BindManagerOutputIdentity(e, deterministicOutputEventID(evt, agent.ID(), idx, e))
			if identityErr != nil {
				pubErr := runtimefailures.WrapDetail("event_output_identity_failed", "agent-manager", "process_event.bind_output_identity", map[string]any{
					"event_type": e.Type(), "agent_id": agent.ID(), "output_index": idx,
				}, identityErr)
				failure := runtimefailures.FromError(pubErr, "agent-manager", "process_event.bind_output_identity")
				am.writeReceipt(ctx, evt, agent.ID(), ReceiptStatusError, &failure.Failure)
				record.Outcome = startupManagerReplayOutcomeDropped
				record.ReasonCode = startupManagerReplayReasonPublishFailed
				record.Failure = runtimefailures.CloneEnvelope(&failure.Failure)
				return eventProcessResult{record: record, err: pubErr}
			}
		}
		if am.shouldSkipAlreadyPublishedOutput(ctx, e.ID()) {
			continue
		}
		if err := am.bus.Publish(ctx, e); err != nil {
			pubErr := runtimefailures.WrapDetail("event_publish_failed", "agent-manager", "process_event.publish_output", map[string]any{
				"event_id": e.ID(), "event_type": e.Type(), "agent_id": agent.ID(),
			}, err)
			failure := runtimefailures.FromError(pubErr, "agent-manager", "process_event.publish_output")
			am.writeReceipt(ctx, evt, agent.ID(), ReceiptStatusError, &failure.Failure)
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonPublishFailed
			record.Failure = runtimefailures.CloneEnvelope(&failure.Failure)
			return eventProcessResult{record: record, err: pubErr}
		}
	}
	am.writeReceipt(ctx, evt, agent.ID(), ReceiptStatusProcessed, nil)
	record.Outcome = startupManagerReplayOutcomeReplayed
	record.ReasonCode = startupManagerReplayReasonReplayed
	return eventProcessResult{record: record}
}

func (am *AgentManager) executeAgentDeliveryHandler(ctx context.Context, claim runtimedelivery.Claim, agent Agent, evt events.Event) (out []events.Event, handlerErr error, heartbeatErr error) {
	heartbeat, err := runtimedelivery.StartClaimHeartbeat(ctx, am.workOwner, am.deliveryStore, claim)
	if err != nil {
		return nil, nil, err
	}
	defer func() { heartbeatErr = heartbeat.Stop() }()
	out, handlerErr = agent.OnEvent(heartbeat.Context(), evt)
	return out, handlerErr, nil
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

func (am *AgentManager) activeRunDeliveryQuiesced(ctx context.Context, eventID, agentID string) (startupManagerReplayReasonCode, bool) {
	reader, ok := am.store.(activeRunDeliveryQuiescenceReader)
	if !ok || reader == nil {
		return "", false
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return "", false
	}
	reason, ok, err := reader.ActiveRunDeliveryQuiesced(ctx, eventID, "agent", agentID)
	if err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "active_run_quiescence_check_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
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

func (am *AgentManager) shouldSuppressForBudget(agentID string, evt events.Event) (bool, string) {
	execution, ok := am.lifecycle.executionSnapshot(agentID)
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

func (am *AgentManager) maybeTripAuthCircuitBreaker(ctx context.Context, agentID string, evt events.Event, failure runtimefailures.Envelope) {
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
		return
	}
	am.runMu.Lock()
	if am.authBreakerTripped {
		am.runMu.Unlock()
		return
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
	if execution, ok := am.lifecycle.executionSnapshot(agentID); ok {
		cfg := execution.Config
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if authRequired {
		authEvt, constructErr := newPlatformCausalRuntimeControlEvent(events.EventLineage{RunID: evt.RunID(), ParentEventID: eventID, TaskID: evt.TaskID(), ExecutionMode: evt.ExecutionMode()}, events.EventType("platform.auth_required"), mustJSON(map[string]any{
			"agent_id":      strings.TrimSpace(agentID),
			"entity_id":     entityID,
			"flow_instance": flowInstance,
			"tool_name":     nil,
			"action":        "llm_call",
			"failure":       failure,
			"timestamp":     now.Format(time.RFC3339Nano),
		}), events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, now)
		if constructErr != nil {
			return
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
	if running {
		// The current delivery is part of the work this transition must join.
		// Request the shared shutdown and let its watcher execute the join.
		am.lifecycle.requestShutdownTransition()
	}
}

func (am *AgentManager) isAuthBreakerTripped() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.authBreakerTripped
}

func (am *AgentManager) writeReceipt(ctx context.Context, evt events.Event, agentID string, status ReceiptStatus, failure *runtimefailures.Envelope) {
	eventID := strings.TrimSpace(evt.ID())
	if am.deliveryStore == nil || eventID == "" || agentID == "" {
		return
	}
	claim, ok := runtimedelivery.ClaimFromContext(ctx)
	if !ok || claim.SubscriberClass() != runtimedelivery.SubscriberAgent || claim.SubscriberID() != strings.TrimSpace(agentID) {
		if am.bus != nil {
			_ = am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level: "error", Component: "agent-manager", Action: "delivery_settlement_claim_missing",
				EventID: eventID, AgentID: strings.TrimSpace(agentID),
			})
		}
		return
	}
	writeCtx := ctx
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	var snapshot runtimedelivery.Snapshot
	var err error
	switch status {
	case ReceiptStatusProcessed:
		snapshot, err = am.deliveryStore.SettleSuccess(writeCtx, claim, nil, 0)
	case ReceiptStatusError:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry, ReasonCode: "handler_failure",
			Failure: failure, RetryBase: time.Minute,
		})
	case ReceiptStatusDeadLetter:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "dead_letter", Failure: failure,
		})
	case ReceiptStatusTerminal:
		snapshot, err = am.deliveryStore.SettleFailure(writeCtx, claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "terminal_failure", Failure: failure,
		})
	default:
		return
	}
	if err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(writeCtx, runtimepipeline.RuntimeLogEntry{
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
		return
	}
	am.logDeliveryLifecycle(writeCtx, snapshot)
	am.notifyTestDeliveryStatus(writeCtx, evt, agentID, snapshot.Status)
	am.convergeDeliveryRunCompletion(writeCtx, evt, agentID)

	if snapshot.Status == runtimedelivery.StatusDeadLetter {
		am.maybeEscalateDeadLetter(writeCtx, evt, agentID, snapshot)
	}
}

func (am *AgentManager) convergeDeliveryRunCompletion(ctx context.Context, evt events.Event, agentID string) {
	if am == nil {
		return
	}
	if converger, ok := am.bus.(deliveryRunCompletionConverger); ok && converger != nil {
		if err := converger.ConvergeDeliveryRunCompletion(ctx, evt); err != nil && am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "delivery_run_completion_failed",
				EventID:   strings.TrimSpace(evt.ID()),
				AgentID:   strings.TrimSpace(agentID),
				Failure:   failureEnvelope(err, "agent-manager", "converge_delivery_run_completion"),
			})
		}
	}
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

func (am *AgentManager) maybeEscalateDeadLetter(ctx context.Context, evt events.Event, agentID string, snapshot runtimedelivery.Snapshot) {
	eventID := strings.TrimSpace(evt.ID())
	execution, cfgOK := am.lifecycle.executionSnapshot(agentID)
	cfg := execution.Config
	entityID := ""
	flowInstance := ""
	if cfgOK {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if recorder, ok := am.store.(deadLetterRecorder); ok && recorder != nil {
		if snapshot.Failure == nil {
			return
		}
		if err := recorder.RecordDeadLetter(ctx, runtimedeadletters.Record{
			OriginalEventID: eventID,
			Failure:         *snapshot.Failure,
			RetryCount:      snapshot.RetryCount,
			HandlerNode:     agentID,
		}); err != nil {
			if am.bus != nil {
				am.bus.LogRuntime(am.runtimeContext(), runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "record_dead_letter_failed",
					EventID:   strings.TrimSpace(eventID),
					AgentID:   strings.TrimSpace(agentID),
					EntityID:  entityID,
					Failure:   failureEnvelope(err, "agent-manager", "record_dead_letter"),
				})
			}
		}
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

func (am *AgentManager) resolveManagerAgentID(agentID string) string {
	execution, ok := am.lifecycle.executionSnapshot(agentID)
	cfg := execution.Config
	if ok {
		if p := strings.TrimSpace(cfg.ParentAgent); p != "" {
			return p
		}
		if managerID := normalizedManagerFallback(cfg, cfg.ManagerFallback); managerID != "" {
			return managerID
		}
	}
	return am.defaultManagerAgentID(cfg)
}
