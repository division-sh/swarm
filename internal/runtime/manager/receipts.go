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
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
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
	if suppress, reason := am.shouldSuppressForBudget(agent.ID(), evt); suppress {
		am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusProcessed, reason)
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonBudgetSuppressed
		return eventProcessResult{record: record}
	}
	if am.shouldInterceptDirective(agent.ID(), evt) {
		am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusProcessed, "intercepted simple directive (runtime-handled)")
		record.Outcome = startupManagerReplayOutcomeSkipped
		record.ReasonCode = startupManagerReplayReasonDirectiveIntercepted
		return eventProcessResult{record: record}
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
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
					Error:     strings.TrimSpace(err.Error()),
				})
			}
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonDeliveryStartFailed
			record.ErrorText = strings.TrimSpace(err.Error())
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
		if isTransientAgentError(err) {
			// Transient lock/contention errors should be retried without poisoning receipts.
			record.Outcome = startupManagerReplayOutcomeSkipped
			record.ReasonCode = transientReplayReason(err)
			return eventProcessResult{record: record}
		}
		agentErr := runtimerterr.WrapRuntimeError(
			"agent_on_event_failed",
			"agent-manager",
			"process_event.on_event",
			false,
			err,
			"agent %s failed processing event %s (%s)",
			agent.ID(),
			strings.TrimSpace(evt.ID()),
			strings.TrimSpace(string(evt.Type())),
		)
		am.maybeTripAuthCircuitBreaker(ctx, agent.ID(), evt.ID(), err)
		am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusError, runtimerterr.FormatRuntimeError(agentErr))
		record.Outcome = startupManagerReplayOutcomeDropped
		record.ReasonCode = startupManagerReplayReasonProcessFailed
		record.ErrorText = agentErr.Error()
		return eventProcessResult{record: record, err: agentErr}
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
			pubErr := runtimerterr.WrapRuntimeError(
				"event_publish_failed",
				"agent-manager",
				"process_event.publish_output",
				true,
				err,
				"failed publishing output event id=%s type=%s from agent=%s",
				strings.TrimSpace(e.ID()),
				strings.TrimSpace(string(e.Type())),
				agent.ID(),
			)
			am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusError, runtimerterr.FormatRuntimeError(pubErr))
			record.Outcome = startupManagerReplayOutcomeDropped
			record.ReasonCode = startupManagerReplayReasonPublishFailed
			record.ErrorText = pubErr.Error()
			return eventProcessResult{record: record, err: pubErr}
		}
	}
	am.writeReceipt(ctx, evt.ID(), agent.ID(), ReceiptStatusProcessed, "")
	record.Outcome = startupManagerReplayOutcomeReplayed
	record.ReasonCode = startupManagerReplayReasonReplayed
	return eventProcessResult{record: record}
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
				Error:     strings.TrimSpace(err.Error()),
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
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	tracker := am.budget
	am.mu.RUnlock()
	if !ok || tracker == nil {
		return false, ""
	}
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

func isTransientAgentError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "session currently leased") {
		return true
	}
	if strings.Contains(msg, "budget emergency") {
		return true
	}
	return false
}

func isClaudeAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.Join(strings.Fields(err.Error()), " "))
	return strings.Contains(msg, "not logged in") ||
		strings.Contains(msg, "please run /login") ||
		strings.Contains(msg, "/login") ||
		strings.Contains(msg, "claude auth required") ||
		(strings.Contains(msg, "claude") && strings.Contains(msg, "auth"))
}

func isClaudeCreditExhaustedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.Join(strings.Fields(err.Error()), " "))
	return strings.Contains(msg, "you've hit your limit") ||
		strings.Contains(msg, "you have hit your limit") ||
		strings.Contains(msg, "insufficient credit") ||
		strings.Contains(msg, "insufficient credits") ||
		strings.Contains(msg, "credit balance") ||
		strings.Contains(msg, "quota exceeded") ||
		(strings.Contains(msg, "resets") && strings.Contains(msg, "utc") && strings.Contains(msg, "limit"))
}

func (am *AgentManager) maybeTripAuthCircuitBreaker(ctx context.Context, agentID, eventID string, err error) {
	reason := ""
	authRequired := false
	switch {
	case isClaudeAuthError(err):
		reason = "claude_auth_required"
		authRequired = true
	case isClaudeCreditExhaustedError(err):
		reason = "claude_credit_exhausted"
	default:
		return
	}
	am.runMu.Lock()
	if am.authBreakerTripped {
		am.runMu.Unlock()
		return
	}
	am.authBreakerTripped = true
	running := am.running
	am.runMu.Unlock()

	eventCtx := am.runtimePlatformControlEventContext(ctx)
	if am.runtimeIngressSafetyPause != nil {
		if pauseErr := am.runtimeIngressSafetyPause(eventCtx, reason); pauseErr != nil {
			if am.bus != nil {
				am.bus.LogRuntime(eventCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "runtime_pause_owner_failed",
					EventID:   strings.TrimSpace(eventID),
					AgentID:   strings.TrimSpace(agentID),
					Error:     strings.TrimSpace(pauseErr.Error()),
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
			Error:     strings.TrimSpace(err.Error()),
			Detail: map[string]any{
				"reason": reason,
			},
		})
	}
	now := time.Now().UTC()
	entityID := ""
	flowInstance := ""
	am.mu.RLock()
	if cfg, ok := am.agentCfg[agentID]; ok {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	am.mu.RUnlock()
	if authRequired {
		authEvt := events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("platform.auth_required"), "runtime", "", mustJSON(map[string]any{
			"agent_id":      strings.TrimSpace(agentID),
			"entity_id":     entityID,
			"flow_instance": flowInstance,
			"tool_name":     nil,
			"action":        "llm_call",
			"reason":        reason,
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
					Error:     strings.TrimSpace(err.Error()),
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

func (am *AgentManager) writeReceipt(ctx context.Context, eventID, agentID string, status ReceiptStatus, errText string) {
	if am.store == nil || eventID == "" || agentID == "" {
		return
	}
	writeCtx := ctx
	if writeCtx == nil {
		writeCtx = context.Background()
	}
	receiptCtx := writeCtx
	var receiptCancel context.CancelFunc
	err := am.store.UpsertEventReceipt(writeCtx, eventID, agentID, status, errText)
	if err != nil {
		// Agent loop contexts are canceled aggressively during teardown; receipts
		// must still persist so pending deliveries do not get stuck indefinitely.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			retryCtx, cancel := context.WithTimeout(context.WithoutCancel(writeCtx), receiptWriteTimeout)
			retryErr := am.store.UpsertEventReceipt(retryCtx, eventID, agentID, status, errText)
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
				Error:     strings.TrimSpace(err.Error()),
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
	am.logDeliveryLifecycle(receiptCtx, eventID, agentID, status, errText)
	if converger, ok := am.bus.(normalRunCompletionConverger); ok && converger != nil {
		if err := converger.ConvergeNormalRunCompletionForEvent(receiptCtx, eventID); err != nil && am.bus != nil {
			am.bus.LogRuntime(receiptCtx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "normal_run_completion_failed",
				EventID:   strings.TrimSpace(eventID),
				AgentID:   strings.TrimSpace(agentID),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}

	// Spec v2.0: dead-letter events are escalated to the agent's manager. The manager
	// decides whether to retry, skip, or escalate further.
	if status == ReceiptStatusError {
		escalateCtx := ctx
		if receiptCancel != nil {
			escalateCtx = context.WithoutCancel(writeCtx)
		}
		am.maybeEscalateDeadLetter(escalateCtx, eventID, agentID)
	}
}

func (am *AgentManager) logDeliveryLifecycle(ctx context.Context, eventID, agentID string, requestedStatus ReceiptStatus, errText string) {
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
	switch receipt.Status {
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
		detail["delivery_reason"] = strings.TrimSpace(errText)
		entry.Message = "Delivery entered retrying state"
		if strings.TrimSpace(receipt.Error) != "" {
			entry.Error = strings.TrimSpace(receipt.Error)
		}
	case ReceiptStatusDeadLetter:
		detail["delivery_state"] = string(runtimedelivery.StateExhausted)
		detail["delivery_transition"] = string(runtimedelivery.StateExhausted)
		detail["delivery_previous_state"] = string(runtimedelivery.StateRetrying)
		detail["delivery_reason"] = strings.TrimSpace(errText)
		detail["delivery_terminal_outcome"] = "retry_exhausted"
		entry.Message = "Delivery entered exhausted state"
		if strings.TrimSpace(receipt.Error) != "" {
			entry.Error = strings.TrimSpace(receipt.Error)
		}
	default:
		return
	}
	_ = requestedStatus
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
	am.mu.RLock()
	cfg, cfgOK := am.agentCfg[agentID]
	am.mu.RUnlock()
	entityID := ""
	flowInstance := ""
	if cfgOK {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if recorder, ok := am.store.(deadLetterRecorder); ok && recorder != nil {
		if err := recorder.RecordDeadLetter(ctx, runtimedeadletters.Record{
			OriginalEventID: eventID,
			FailureType:     "retry_exhausted",
			ErrorMessage:    strings.TrimSpace(receipt.Error),
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
					Error:     strings.TrimSpace(err.Error()),
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
		errText:    strings.TrimSpace(receipt.Error),
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
				Error:     strings.TrimSpace(err.Error()),
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
			"error":       item.errText,
		})
	}
	return len(window), sampleEvents, true
}

func (am *AgentManager) resolveManagerAgentID(agentID string) string {
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
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
