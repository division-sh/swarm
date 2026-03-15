package manager

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimerterr "empireai/internal/runtime/rterrors"
	"github.com/google/uuid"
)

type eventReceiptReader interface {
	GetEventReceipt(ctx context.Context, eventID, agentID string) (EventReceipt, bool, error)
}

func (am *AgentManager) processEvent(ctx context.Context, agent Agent, evt events.Event) error {
	if !am.markEventInFlight(agent.ID(), evt.ID) {
		return nil
	}
	defer am.unmarkEventInFlight(agent.ID(), evt.ID)
	if am.shouldSkipEvent(agent.ID(), evt.ID) {
		return nil
	}
	if suppress, reason := am.shouldSuppressForBudget(agent.ID(), evt); suppress {
		am.writeReceipt(ctx, evt.ID, agent.ID(), ReceiptStatusProcessed, reason)
		return nil
	}
	if am.shouldInterceptDirective(agent.ID(), evt) {
		am.writeReceipt(ctx, evt.ID, agent.ID(), ReceiptStatusProcessed, "intercepted simple directive (runtime-handled)")
		return nil
	}
	out, err := agent.OnEvent(ctx, evt)
	if err != nil {
		if isTransientAgentError(err) {
			// Transient lock/contention errors should be retried without poisoning receipts.
			return nil
		}
		agentErr := runtimerterr.WrapRuntimeError(
			"agent_on_event_failed",
			"agent-manager",
			"process_event.on_event",
			false,
			err,
			"agent %s failed processing event %s (%s)",
			agent.ID(),
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
		)
		am.maybeTripAuthCircuitBreaker(agent.ID(), evt.ID, err)
		am.writeReceipt(ctx, evt.ID, agent.ID(), ReceiptStatusError, runtimerterr.FormatRuntimeError(agentErr))
		return agentErr
	}
	for idx, e := range out {
		if strings.TrimSpace(e.ID) == "" {
			e.ID = deterministicOutputEventID(evt, agent.ID(), idx, e)
		}
		if am.shouldSkipAlreadyPublishedOutput(ctx, e.ID) {
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
				strings.TrimSpace(e.ID),
				strings.TrimSpace(string(e.Type)),
				agent.ID(),
			)
			am.writeReceipt(ctx, evt.ID, agent.ID(), ReceiptStatusError, runtimerterr.FormatRuntimeError(pubErr))
			return pubErr
		}
	}
	am.writeReceipt(ctx, evt.ID, agent.ID(), ReceiptStatusProcessed, "")
	return nil
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
	eventType := strings.ToLower(strings.TrimSpace(string(evt.Type)))
	if strings.HasPrefix(eventType, "budget.") {
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
	reader, ok := am.store.(eventReceiptReader)
	if !ok || reader == nil {
		return false
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return false
	}
	receipt, found, err := reader.GetEventReceipt(am.runtimeContext(), eventID, agentID)
	if err != nil || !found {
		return false
	}
	status := ReceiptStatus(strings.TrimSpace(string(receipt.Status)))
	return status == ReceiptStatusProcessed || status == ReceiptStatusDeadLetter
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

func (am *AgentManager) maybeTripAuthCircuitBreaker(agentID, eventID string, err error) {
	reason := ""
	eventType := events.EventType("runtime.paused")
	instruction := ""
	switch {
	case isClaudeAuthError(err):
		reason = "claude_auth_required"
		eventType = events.EventType("runtime.auth_required")
		instruction = "Claude authentication is required. Runtime paused to prevent retry storm."
	case isClaudeCreditExhaustedError(err):
		reason = "claude_credit_exhausted"
		instruction = "Claude usage limit reached. Runtime paused globally until credits reset or billing is updated."
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

	runtimebus.PauseRuntimeIngress()
	log.Printf("runtime pause breaker tripped: reason=%s agent=%s event=%s err=%v", reason, agentID, eventID, err)
	payload := mustJSON(map[string]any{
		"agent_id":     strings.TrimSpace(agentID),
		"event_id":     strings.TrimSpace(eventID),
		"reason":       reason,
		"instruction":  instruction,
		"spec_version": runtimeSpecVersion,
	})
	if err := am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
		RuntimeWarn("agent-manager", "%s publish failed agent=%s event=%s err=%v", eventType, strings.TrimSpace(agentID), strings.TrimSpace(eventID), err)
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
	if err := am.store.UpsertEventReceipt(writeCtx, eventID, agentID, status, errText); err != nil {
		// Agent loop contexts are canceled aggressively during teardown; receipts
		// must still persist so pending deliveries do not get stuck indefinitely.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			retryCtx, cancel := context.WithTimeout(context.WithoutCancel(writeCtx), receiptWriteTimeout)
			retryErr := am.store.UpsertEventReceipt(retryCtx, eventID, agentID, status, errText)
			cancel()
			if retryErr == nil {
				if status == ReceiptStatusError {
					am.maybeEscalateDeadLetter(context.WithoutCancel(writeCtx), eventID, agentID)
				}
				return
			}
			err = retryErr
		}
		log.Printf("receipt write failed event=%s agent=%s status=%s err=%v", eventID, agentID, status, err)
		return
	}

	// Spec v2.0: dead-letter events are escalated to the agent's manager. The manager
	// decides whether to retry, skip, or escalate further.
	if status == ReceiptStatusError {
		am.maybeEscalateDeadLetter(ctx, eventID, agentID)
	}
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

	managerID := am.resolveManagerAgentID(agentID)
	if strings.TrimSpace(managerID) == "" || managerID == agentID {
		am.mu.RLock()
		cfg := am.agentCfg[agentID]
		am.mu.RUnlock()
		managerID = am.defaultManagerAgentID(cfg)
	}
	if managerID == agentID {
		// Prevent infinite self-escalation chains.
		log.Printf("dead-letter escalation suppressed for self-managed agent=%s event=%s", agentID, eventID)
		return
	}

	am.mu.RLock()
	cfg, cfgOK := am.agentCfg[agentID]
	am.mu.RUnlock()
	entityID := ""
	if cfgOK {
		entityID = cfg.EffectiveEntityID()
	}

	payload := mustJSON(map[string]any{
		"event_id":     eventID,
		"agent_id":     agentID,
		"manager_id":   managerID,
		"entity_id":    entityID,
		"retry_count":  receipt.RetryCount,
		"error":        strings.TrimSpace(receipt.Error),
		"instruction":  "Event delivery dead-lettered after 3 retries. Decide: retry (requeue), skip, or escalate.",
		"spec_version": runtimeSpecVersion,
	})

	if err := am.bus.PublishDirect(am.runtimeContext(), (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("ops.dead_letter_escalation"),
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}).WithEntityID(entityID), []string{managerID}); err != nil {
		RuntimeWarn("agent-manager", "ops.dead_letter_escalation publish failed agent=%s manager=%s event=%s err=%v", strings.TrimSpace(agentID), strings.TrimSpace(managerID), strings.TrimSpace(eventID), err)
	}
}

func (am *AgentManager) resolveManagerAgentID(agentID string) string {
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
	if ok {
		if p := strings.TrimSpace(cfg.ParentAgent); p != "" {
			return p
		}
		if managerID := normalizedManagerFallback(cfg, managerFallbackFromConfig(cfg)); managerID != "" {
			return managerID
		}
	}
	return am.defaultManagerAgentID(cfg)
}
