package manager

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type eventReceiptReader interface {
	GetEventReceipt(ctx context.Context, eventID, agentID string) (EventReceipt, bool, error)
}

type deadLetterRecorder interface {
	RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error
}

type deliveryProgressWriter interface {
	MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error
}

type activeRunDeliveryQuiescenceReader interface {
	ActiveRunDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (string, bool, error)
}

type normalRunCompletionConverger interface {
	ConvergeNormalRunCompletionForEvent(ctx context.Context, eventID string) error
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
	if !am.markEventInFlight(agent.ID(), evt.ID()) {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonDuplicateInFlight
		return eventProcessResult{record: record}
	}
	defer am.unmarkEventInFlight(agent.ID(), evt.ID())
	if skip, reason := am.shouldSkipEventDetailed(agent.ID(), evt.ID()); skip {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
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
		return eventProcessResult{record: record}
	}
	if am.shouldInterceptDirective(agent.ID(), evt) {
		am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusProcessed, nil)
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonDirectiveIntercepted
		return eventProcessResult{record: record}
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
	ctx = events.WithDeliveryContext(ctx, evt.DeliveryContext())
	if writer, ok := am.store.(deliveryProgressWriter); ok && writer != nil {
		if err := writer.MarkEventDeliveryInProgress(ctx, evt.ID(), agent.ID(), ""); err != nil {
			if am.bus != nil {
				am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "mark_delivery_in_progress_failed",
					EventID:   strings.TrimSpace(evt.ID()),
					EventType: strings.TrimSpace(string(evt.Type())),
					AgentID:   agent.ID(),
					EntityID:  strings.TrimSpace(evt.EntityID()),
					Failure:   failureEnvelope(err, "agent-manager", "mark_delivery_in_progress"),
				})
			}
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
			record.Failure = failureEnvelope(err, "agent-manager", "mark_delivery_in_progress")
			return eventProcessResult{record: record, err: err}
		} else if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "debug",
				Component: "agent-manager",
				Action:    "delivery_lifecycle_transition",
				EventID:   strings.TrimSpace(evt.ID()),
				EventType: strings.TrimSpace(string(evt.Type())),
				AgentID:   agent.ID(),
				EntityID:  strings.TrimSpace(evt.EntityID()),
				Detail: map[string]any{
					"delivery_state":          string(runtimedelivery.StateLaunching),
					"delivery_transition":     string(runtimedelivery.StateLaunching),
					"delivery_previous_state": string(runtimedelivery.StateQueued),
					"delivery_reason":         "agent_processing",
					"subscriber_type":         "agent",
					"subscriber_id":           agent.ID(),
				},
			})
		}
	}
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), agent.ID()); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	out, err := agent.OnEvent(ctx, evt)
	if reason, ok := am.activeRunDeliveryQuiesced(ctx, evt.ID(), agent.ID()); ok {
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = reason
		return eventProcessResult{record: record}
	}
	if err != nil {
		status := receiptStatusForAgentFailure(err)
		agentFailure := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event")
		am.maybeTripAuthCircuitBreaker(ctx, agent.ID(), evt.ID(), agentFailure.Failure)
		am.writeReceipt(ctx, evt.ID(), agent.ID(), status, &agentFailure.Failure)
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.Failure = runtimefailures.CloneEnvelope(&agentFailure.Failure)
		return eventProcessResult{record: record, err: agentFailure}
	}
	for idx, e := range out {
		if strings.TrimSpace(e.ID()) == "" {
			e = events.NewProjectionEvent(
				deterministicOutputEventID(evt, agent.ID(), idx, e),
				e.Type(),
				e.SourceAgent(),
				e.TaskID(),
				e.Payload(),
				e.ChainDepth(),
				e.RunID(),
				e.ParentEventID(),
				e.Envelope(),
				e.CreatedAt(),
			)
		}
		if am.shouldSkipAlreadyPublishedOutput(ctx, e.ID()) {
			continue
		}
		if err := am.bus.Publish(ctx, e); err != nil {
			pubErr := runtimefailures.WrapDetail("event_publish_failed", "agent-manager", "process_event.publish_output", map[string]any{
				"event_id": e.ID(), "event_type": e.Type(), "agent_id": agent.ID(),
			}, err)
			failure := runtimefailures.FromError(pubErr, "agent-manager", "process_event.publish_output")
			am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusError, &failure.Failure)
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonPublishFailed
			record.Failure = runtimefailures.CloneEnvelope(&failure.Failure)
			return eventProcessResult{record: record, err: pubErr}
		}
	}
	am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusProcessed, nil)
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

func (am *AgentManager) markEventInFlight(agentID, eventID string) bool {
	agentID = strings.TrimSpace(agentID)
	eventID = strings.TrimSpace(eventID)
	if agentID == "" || eventID == "" {
		return true
	}
	key := agentID + "|" + eventID
	am.inFlightMu.Lock()
	defer am.inFlightMu.Unlock()
	if _, exists := am.inFlight[key]; exists {
		return false
	}
	am.inFlight[key] = struct{}{}
	return true
}

func (am *AgentManager) unmarkEventInFlight(agentID, eventID string) {
	agentID = strings.TrimSpace(agentID)
	eventID = strings.TrimSpace(eventID)
	if agentID == "" || eventID == "" {
		return
	}
	key := agentID + "|" + eventID
	am.inFlightMu.Lock()
	delete(am.inFlight, key)
	am.inFlightMu.Unlock()
}

func (am *AgentManager) shouldSkipEvent(agentID, eventID string) bool {
	skip, _ := am.shouldSkipEventDetailed(agentID, eventID)
	return skip
}

func (am *AgentManager) shouldSkipEventDetailed(agentID, eventID string) (bool, startupManagerReplayReasonCode) {
	reader, ok := am.store.(eventReceiptReader)
	if !ok || reader == nil {
		return false, ""
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return false, ""
	}
	receipt, found, err := reader.GetEventReceipt(am.runtimeContext(), eventID, agentID)
	if err != nil || !found {
		return false, ""
	}
	status := ReceiptStatus(strings.TrimSpace(string(receipt.Status)))
	switch status {
	case ReceiptStatusProcessed:
		return true, startupManagerReplayReasonReceiptProcessed
	case ReceiptStatusDeadLetter:
		return true, startupManagerReplayReasonReceiptDeadLettered
	default:
		return false, ""
	}
}

func (am *AgentManager) maybeTripAuthCircuitBreaker(ctx context.Context, agentID, eventID string, failure runtimefailures.Envelope) {
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
		authEvt := events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("platform.auth_required"), "runtime", "", mustJSON(map[string]any{
			"agent_id":      strings.TrimSpace(agentID),
			"entity_id":     entityID,
			"flow_instance": flowInstance,
			"tool_name":     nil,
			"action":        "llm_call",
			"failure":       failure,
			"timestamp":     now.Format(time.RFC3339Nano),
		}), 0, "", "", events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, now)
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
		_ = am.Shutdown()
	}
}

func (am *AgentManager) isAuthBreakerTripped() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.authBreakerTripped
}

func (am *AgentManager) writeReceipt(ctx context.Context, eventID, agentID string, status ReceiptStatus, failure *runtimefailures.Envelope) {
	if am.store == nil || eventID == "" || agentID == "" {
		return
	}
	writeCtx := ctx
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	receiptCtx := writeCtx
	var receiptCancel context.CancelFunc
	err := am.store.UpsertEventReceipt(writeCtx, eventID, agentID, status, failure)
	if err != nil {
		// Agent loop contexts are canceled aggressively during teardown; receipts
		// must still persist so pending deliveries do not get stuck indefinitely.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			retryCtx, cancel := context.WithTimeout(context.WithoutCancel(writeCtx), receiptWriteTimeout)
			retryErr := am.store.UpsertEventReceipt(retryCtx, eventID, agentID, status, failure)
			if retryErr == nil {
				receiptCtx = retryCtx
				receiptCancel = cancel
				err = nil
			} else {
				cancel()
				err = retryErr
			}
		}
	}
	if err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(writeCtx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "receipt_write_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
				Failure:   failureEnvelope(err, "agent-manager", "write_receipt"),
				Detail: map[string]any{
					"status": strings.TrimSpace(string(status)),
				},
			})
		}
		return
	}
	if receiptCancel != nil {
		defer receiptCancel()
	}
	am.logDeliveryLifecycle(receiptCtx, eventID, agentID, status, failure)
	if converger, ok := am.bus.(normalRunCompletionConverger); ok && converger != nil {
		if err := converger.ConvergeNormalRunCompletionForEvent(receiptCtx, eventID); err != nil && am.bus != nil {
			am.bus.LogRuntime(receiptCtx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "normal_run_completion_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
				Failure:   failureEnvelope(err, "agent-manager", "converge_run_completion"),
			})
		}
	}

	// Spec v2.0: dead-letter events are escalated to the agent's manager. The manager
	// decides whether to retry, skip, or escalate further.
	if status != ReceiptStatusProcessed {
		escalateCtx := ctx
		if receiptCancel != nil {
			escalateCtx = context.WithoutCancel(writeCtx)
		}
		am.maybeEscalateDeadLetter(escalateCtx, eventID, agentID)
	}
}

func (am *AgentManager) logDeliveryLifecycle(ctx context.Context, eventID, agentID string, requestedStatus ReceiptStatus, failure *runtimefailures.Envelope) {
	if am == nil || am.bus == nil || am.store == nil {
		return
	}
	reader, ok := am.store.(eventReceiptReader)
	if !ok || reader == nil {
		return
	}
	receipt, found, err := reader.GetEventReceipt(ctx, eventID, agentID)
	if err != nil || !found {
		return
	}
	detail := map[string]any{
		"subscriber_type": "agent",
		"subscriber_id":   strings.TrimSpace(agentID),
		"retry_count":     receipt.RetryCount,
	}
	entry := runtimepipeline.RuntimeLogEntry{
		Level:     "debug",
		Component: "agent-manager",
		Action:    "delivery_lifecycle_transition",
		EventID:   strings.TrimSpace(eventID),
		AgentID:   strings.TrimSpace(agentID),
		Detail:    detail,
	}
	status := receipt.Status
	if requestedStatus == ReceiptStatusTerminal {
		status = ReceiptStatusTerminal
	}
	switch status {
	case ReceiptStatusProcessed:
		detail["delivery_state"] = string(runtimedelivery.StateDelivered)
		detail["delivery_transition"] = string(runtimedelivery.StateDelivered)
		detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
		detail["delivery_reason"] = "agent_processed"
		entry.Message = "Delivery entered delivered state"
	case ReceiptStatusError:
		detail["delivery_state"] = string(runtimedelivery.StateRetrying)
		detail["delivery_transition"] = string(runtimedelivery.StateRetrying)
		detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
		detail["delivery_reason"] = "handler_failure"
		entry.Message = "Delivery entered retrying state"
		if receipt.Failure != nil {
			detail["failure"] = *receipt.Failure
		}
	case ReceiptStatusDeadLetter:
		detail["delivery_state"] = string(runtimedelivery.StateExhausted)
		detail["delivery_transition"] = string(runtimedelivery.StateExhausted)
		if requestedStatus == ReceiptStatusDeadLetter {
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
		if receipt.Failure != nil {
			detail["failure"] = *receipt.Failure
		}
	case ReceiptStatusTerminal:
		detail["delivery_state"] = string(runtimedelivery.StateExhausted)
		detail["delivery_transition"] = string(runtimedelivery.StateExhausted)
		detail["delivery_previous_state"] = string(runtimedelivery.StateActive)
		detail["delivery_reason"] = "terminal_failure"
		detail["delivery_terminal_outcome"] = "terminal_failure"
		entry.Message = "Delivery entered terminal state"
		if receipt.Failure != nil {
			detail["failure"] = *receipt.Failure
		}
	default:
		return
	}
	_ = failure
	_ = am.bus.LogRuntime(ctx, entry)
}

func (am *AgentManager) maybeEscalateDeadLetter(ctx context.Context, eventID, agentID string) {
	reader, ok := am.store.(eventReceiptReader)
	if !ok || reader == nil {
		return
	}
	receipt, found, err := reader.GetEventReceipt(ctx, eventID, agentID)
	if err != nil || !found {
		return
	}
	if ReceiptStatus(strings.TrimSpace(string(receipt.Status))) != ReceiptStatusDeadLetter {
		return
	}
	execution, cfgOK := am.lifecycle.executionSnapshot(agentID)
	cfg := execution.Config
	entityID := ""
	flowInstance := ""
	if cfgOK {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if recorder, ok := am.store.(deadLetterRecorder); ok && recorder != nil {
		if receipt.Failure == nil {
			return
		}
		if err := recorder.RecordDeadLetter(ctx, runtimedeadletters.Record{
			OriginalEventID: eventID,
			Failure:         *receipt.Failure,
			RetryCount:      receipt.RetryCount,
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
		retryCount: receipt.RetryCount,
		failure:    runtimefailures.CloneEnvelope(receipt.Failure),
	})
	if !shouldEmit {
		return
	}

	eventCtx := am.runtimePlatformControlEventContext(ctx)
	if err := am.bus.Publish(eventCtx, events.NewRuntimeDiagnosticEvent(uuid.NewString(), events.EventType("platform.dead_letter_escalation"), "runtime", "", mustJSON(map[string]any{
		"flow_instance":     flowInstance,
		"dead_letter_count": count,
		"window_minutes":    int(deadLetterEscalationWindow / time.Minute),
		"sample_events":     sampleEvents,
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
	}), 0, "", "", events.EventEnvelope{FlowInstance: flowInstance}, time.Now().UTC())); err != nil {
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
