package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	runtimemanager "empireai/internal/runtime/manager"
	"github.com/google/uuid"
)

func (am *AgentManager) processEvent(ctx context.Context, agent Agent, evt events.Event) error {
	if !am.markEventInFlight(agent.ID(), evt.ID) {
		return nil
	}
	defer am.unmarkEventInFlight(agent.ID(), evt.ID)
	if am.shouldSkipEvent(agent.ID(), evt.ID) {
		return nil
	}
	corpusMeta, corpusMode := corpusTurnMetaFromEvent(evt)
	var corpusStartedAt time.Time
	if corpusMode {
		corpusMeta.AgentID = agent.ID()
		corpusStartedAt = time.Now().UTC()
		am.logCorpusTurnLifecycle(ctx, "corpus.turn_started", corpusMeta, 0, corpusEmitSnapshot{}, "")
	}
	if suppress, reason := am.shouldSuppressForBudget(agent.ID(), evt); suppress {
		if corpusMode {
			duration := time.Since(corpusStartedAt)
			am.logCorpusTurnLifecycle(ctx, "corpus.turn_suppressed", corpusMeta, duration, consumeCorpusEmitSnapshot(evt.ID), reason)
		}
		am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", reason)
		return nil
	}
	if strings.TrimSpace(agent.ID()) == "empire-coordinator" &&
		strings.TrimSpace(string(evt.Type)) == "system.directive" &&
		!directiveRequiresCoordinator(evt) {
		if corpusMode {
			duration := time.Since(corpusStartedAt)
			am.logCorpusTurnLifecycle(ctx, "corpus.turn_intercepted", corpusMeta, duration, consumeCorpusEmitSnapshot(evt.ID), "intercepted simple directive (runtime-handled)")
		}
		am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", "intercepted simple directive (runtime-handled)")
		return nil
	}
	out, err := agent.OnEvent(ctx, evt)
	if err != nil {
		if isTransientAgentError(err) {
			// Transient lock/contention errors should be retried without poisoning receipts.
			return nil
		}
		agentErr := WrapRuntimeError(
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
		if corpusMode {
			duration := time.Since(corpusStartedAt)
			am.logCorpusTurnLifecycle(ctx, "corpus.turn_failed", corpusMeta, duration, consumeCorpusEmitSnapshot(evt.ID), FormatRuntimeError(agentErr))
		}
		am.writeReceipt(ctx, evt.ID, agent.ID(), "error", FormatRuntimeError(agentErr))
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
			pubErr := WrapRuntimeError(
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
			if corpusMode {
				duration := time.Since(corpusStartedAt)
				am.logCorpusTurnLifecycle(ctx, "corpus.turn_failed", corpusMeta, duration, consumeCorpusEmitSnapshot(evt.ID), FormatRuntimeError(pubErr))
			}
			am.writeReceipt(ctx, evt.ID, agent.ID(), "error", FormatRuntimeError(pubErr))
			return pubErr
		}
	}
	if corpusMode {
		duration := time.Since(corpusStartedAt)
		am.logCorpusTurnLifecycle(ctx, "corpus.turn_completed", corpusMeta, duration, consumeCorpusEmitSnapshot(evt.ID), "")
	}
	am.writeReceipt(ctx, evt.ID, agent.ID(), "processed", "")
	return nil
}

func (am *AgentManager) logCorpusTurnLifecycle(
	ctx context.Context,
	action string,
	meta corpusTurnMeta,
	duration time.Duration,
	snapshot corpusEmitSnapshot,
	errText string,
) {
	if am == nil || am.bus == nil {
		return
	}
	if strings.TrimSpace(action) == "" {
		return
	}
	detail := map[string]any{
		"batch_size":          meta.BatchSize,
		"payload_bytes":       meta.PayloadBytes,
		"emit_count":          snapshot.EmitCount,
		"scan_complete_emits": snapshot.ScanCompleteEmits,
	}
	if duration > 0 {
		detail["duration_ms"] = int(duration / time.Millisecond)
	}
	if !snapshot.FirstEmitAt.IsZero() {
		detail["first_emit_at"] = snapshot.FirstEmitAt.UTC().Format(time.RFC3339Nano)
		if !meta.AssignedAt.IsZero() {
			ms := snapshot.FirstEmitAt.Sub(meta.AssignedAt).Milliseconds()
			if ms < 0 {
				ms = 0
			}
			detail["ms_to_first_emit"] = ms
		}
	}
	am.bus.logRuntime(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "agent-manager",
		Action:     strings.TrimSpace(action),
		EventID:    strings.TrimSpace(meta.EventID),
		EventType:  strings.TrimSpace(meta.EventType),
		AgentID:    firstNonEmptyString(strings.TrimSpace(meta.AgentID), "unknown-agent"),
		VerticalID: strings.TrimSpace(meta.VerticalID),
		CampaignID: strings.TrimSpace(meta.CampaignID),
		ScanID:     strings.TrimSpace(meta.ScanID),
		Detail:     detail,
		Error:      strings.TrimSpace(errText),
		DurationUS: int(duration / time.Microsecond),
	})
}

func directiveRequiresCoordinator(evt events.Event) bool {
	if strings.TrimSpace(evt.SourceAgent) == "scan-campaign-manager" {
		return true
	}
	text := runtimemanager.ExtractDirectiveText(evt.Payload)
	if text == "" {
		return false
	}
	return isComplexDirectiveText(text)
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
	role := strings.ToLower(strings.TrimSpace(cfg.Role))
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		verticalID = strings.TrimSpace(cfg.VerticalID)
	}

	if tracker.IsEmergency(verticalID) {
		if runtimemanager.IsEmergencyAllowedFlow(role, eventType) {
			return false, ""
		}
		return true, "suppressed by budget emergency guardrail"
	}
	if tracker.IsThrottle(verticalID) {
		if runtimemanager.IsGrowthRole(role) {
			return true, "suppressed by budget throttle: growth paused"
		}
		if runtimemanager.IsProactiveHeartbeat(role, eventType) {
			return true, "suppressed by budget throttle: proactive heartbeat paused"
		}
		if strings.HasPrefix(eventType, "scan.") {
			return true, "suppressed by budget throttle: scan work paused"
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
	reader, ok := am.store.(runtimemanager.EventReceiptReader)
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
	status := strings.TrimSpace(receipt.Status)
	return status == "processed" || status == "dead_letter"
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
	if errors.Is(err, ErrClaudeAuthRequired) {
		return true
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

	PauseRuntimeIngress()
	log.Printf("runtime pause breaker tripped: reason=%s agent=%s event=%s err=%v", reason, agentID, eventID, err)
	payload, _ := json.Marshal(map[string]any{
		"agent_id":     strings.TrimSpace(agentID),
		"event_id":     strings.TrimSpace(eventID),
		"reason":       reason,
		"instruction":  instruction,
		"spec_version": runtimeSpecVersion,
	})
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	_ = am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	})
	if running {
		_ = am.Shutdown()
	}
}

func (am *AgentManager) isAuthBreakerTripped() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.authBreakerTripped
}

func (am *AgentManager) writeReceipt(ctx context.Context, eventID, agentID, status, errText string) {
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
			retryCtx, cancel := context.WithTimeout(context.Background(), receiptWriteTimeout)
			retryErr := am.store.UpsertEventReceipt(retryCtx, eventID, agentID, status, errText)
			cancel()
			if retryErr == nil {
				if status == "error" {
					am.maybeEscalateDeadLetter(context.Background(), eventID, agentID)
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
	if status == "error" {
		am.maybeEscalateDeadLetter(ctx, eventID, agentID)
	}
}

func (am *AgentManager) maybeEscalateDeadLetter(ctx context.Context, eventID, agentID string) {
	reader, ok := am.store.(runtimemanager.EventReceiptReader)
	if !ok || reader == nil {
		return
	}
	receipt, found, err := reader.GetEventReceipt(ctx, eventID, agentID)
	if err != nil || !found {
		return
	}
	if strings.TrimSpace(receipt.Status) != "dead_letter" {
		return
	}

	managerID := am.resolveManagerAgentID(agentID)
	if strings.TrimSpace(managerID) == "" || managerID == agentID {
		managerID = "empire-coordinator"
	}
	if managerID == agentID {
		// Prevent infinite self-escalation chains.
		log.Printf("dead-letter escalation suppressed for self-managed agent=%s event=%s", agentID, eventID)
		return
	}

	am.mu.RLock()
	cfg, cfgOK := am.agentCfg[agentID]
	am.mu.RUnlock()
	verticalID := ""
	if cfgOK {
		verticalID = strings.TrimSpace(cfg.VerticalID)
	}

	payload, _ := json.Marshal(map[string]any{
		"event_id":     eventID,
		"agent_id":     agentID,
		"manager_id":   managerID,
		"vertical_id":  verticalID,
		"retry_count":  receipt.RetryCount,
		"error":        strings.TrimSpace(receipt.Error),
		"instruction":  "Event delivery dead-lettered after 3 retries. Decide: retry (requeue), skip, or escalate.",
		"spec_version": runtimeSpecVersion,
	})
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	_ = am.bus.PublishDirect(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("ops.dead_letter_escalation"),
		SourceAgent: "runtime",
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}, []string{managerID})
}

func (am *AgentManager) resolveManagerAgentID(agentID string) string {
	am.mu.RLock()
	cfg, ok := am.agentCfg[agentID]
	am.mu.RUnlock()
	if ok {
		if p := strings.TrimSpace(cfg.ParentAgent); p != "" {
			return p
		}
		if strings.TrimSpace(cfg.Mode) == "operating" && strings.TrimSpace(cfg.VerticalID) != "" && strings.TrimSpace(cfg.Role) != "opco-ceo" {
			return opCoAgentID("opco-ceo", cfg.VerticalID)
		}
	}
	return "empire-coordinator"
}
