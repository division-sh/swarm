package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
)

func (h *runHub) handleRuntimeLog(entry runtimepkg.RuntimeLogEntry) {
	if h == nil {
		return
	}
	entityID := strings.TrimSpace(entry.EffectiveEntityID())
	if entityID == "" {
		return
	}
	runIDs := make([]string, 0, 2)
	h.mu.RLock()
	for runID, session := range h.sessions {
		if _, ok := session.entityIDs[entityID]; ok {
			runIDs = append(runIDs, runID)
		}
	}
	h.mu.RUnlock()
	if len(runIDs) == 0 {
		return
	}
	event := h.toRunEvent(entry)
	for _, runID := range runIDs {
		if event != nil {
			h.emit(runID, event)
		}
		h.maybeEmitBreakpointHit(runID, strings.TrimSpace(entry.AgentID), entityID)
		h.maybeEmitHumanTaskWaiting(runID, entry)
		h.maybePauseAfterStep(runID, strings.TrimSpace(entry.AgentID), entityID)
	}
}

func (h *runHub) toRunEvent(entry runtimepkg.RuntimeLogEntry) RunEventEnvelope {
	if strings.TrimSpace(entry.Component) == "eventbus" && strings.TrimSpace(entry.Action) == "published" {
		event := map[string]any{
			"id":          strings.TrimSpace(entry.EventID),
			"type":        "event.fired",
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
			"instance_id": strings.TrimSpace(entry.EntityID),
			"payload": map[string]any{
				"event_name": strings.TrimSpace(entry.EventType),
				"source":     payloadMap(entry.Detail)["source"],
			},
		}
		if nodeID := strings.TrimSpace(entry.AgentID); nodeID != "" {
			event["node_id"] = nodeID
		}
		return event
	}
	event := map[string]any{
		"id":          nonEmptyOrUUID(strings.TrimSpace(entry.EventID)),
		"type":        "runtime.log",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"instance_id": strings.TrimSpace(entry.EntityID),
		"payload": map[string]any{
			"level":      strings.TrimSpace(entry.Level),
			"component":  strings.TrimSpace(entry.Component),
			"action":     strings.TrimSpace(entry.Action),
			"event_type": strings.TrimSpace(entry.EventType),
			"agent_id":   strings.TrimSpace(entry.AgentID),
			"detail":     payloadMap(entry.Detail),
			"error":      strings.TrimSpace(entry.Error),
		},
	}
	if nodeID := strings.TrimSpace(entry.AgentID); nodeID != "" {
		event["node_id"] = nodeID
	}
	return event
}

func (h *runHub) maybeEmitBreakpointHit(runID string, nodeID string, instanceID string) {
	if h == nil || strings.TrimSpace(nodeID) == "" {
		return
	}
	shouldPause := false
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session != nil {
		if _, ok := session.breakpoints[nodeID]; ok {
			if _, tripped := session.trippedBreakpoints[nodeID]; !tripped {
				session.trippedBreakpoints[nodeID] = struct{}{}
				shouldPause = true
			}
		}
	}
	h.mu.Unlock()
	if !shouldPause {
		return
	}
	if h.pauseRuntime != nil {
		_ = h.pauseRuntime()
	}
	event := map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.breakpoint_hit",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"node_id":   nodeID,
		"payload":   map[string]any{"reason": "node_breakpoint"},
	}
	if instanceID = strings.TrimSpace(instanceID); instanceID != "" {
		event["instance_id"] = instanceID
		event["payload"].(map[string]any)["instance_id"] = instanceID
	}
	h.emit(runID, event)
}

func (h *runHub) maybeEmitHumanTaskWaiting(runID string, entry runtimepkg.RuntimeLogEntry) {
	if h == nil || strings.TrimSpace(entry.Component) != "eventbus" || strings.TrimSpace(entry.Action) != "published" || strings.TrimSpace(entry.EventType) != "human_task.requested" {
		return
	}
	nodeID := strings.TrimSpace(entry.AgentID)
	instanceID := strings.TrimSpace(entry.EntityID)
	if nodeID == "" || instanceID == "" {
		return
	}
	shouldPause := false
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session != nil && session.pendingHuman == nil {
		session.pendingHuman = &pendingHumanDecision{nodeID: nodeID, instanceID: instanceID, requestingAgent: nodeID}
		shouldPause = true
	}
	h.mu.Unlock()
	if !shouldPause {
		return
	}
	if h.pauseRuntime != nil {
		_ = h.pauseRuntime()
	}
	h.emit(runID, map[string]any{
		"id":          uuid.NewString(),
		"type":        "human.task_waiting",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"node_id":     nodeID,
		"instance_id": instanceID,
		"payload":     map[string]any{"decision_options": []string{"approved", "rejected", "deferred"}},
	})
	h.emit(runID, map[string]any{
		"id":          uuid.NewString(),
		"type":        "run.paused",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"node_id":     nodeID,
		"instance_id": instanceID,
		"payload":     map[string]any{"run_id": runID, "reason": "human_task_waiting"},
	})
}

func (h *runHub) submitPendingHumanDecision(ctx context.Context, runID string, decision string) error {
	if h == nil {
		return nil
	}
	if decision = normalizeHumanDecision(decision); decision == "" {
		return nil
	}
	var pending *pendingHumanDecision
	var runtimeRef *runtimepkg.Runtime
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session != nil {
		pending = session.pendingHuman
		runtimeRef = session.runtime
		session.pendingHuman = nil
	}
	h.mu.Unlock()
	if pending == nil {
		return nil
	}
	if runtimeRef == nil || runtimeRef.Bus == nil {
		return fmt.Errorf("runtime bus is not configured")
	}
	eventType := "human_task." + decision
	encoded, err := json.Marshal(map[string]any{
		"requesting_agent": pending.requestingAgent,
		"entity_id":        pending.instanceID,
	})
	if err != nil {
		return err
	}
	if err := runtimeRef.Bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
		RunID:       runID,
		Type:        events.EventType(eventType),
		SourceAgent: "builder",
		Payload:     encoded,
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(pending.instanceID)); err != nil {
		return err
	}
	h.emit(runID, map[string]any{
		"id":          uuid.NewString(),
		"type":        "human.task_submitted",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"node_id":     pending.nodeID,
		"instance_id": pending.instanceID,
		"payload":     map[string]any{"decision": decision},
	})
	return nil
}

func (h *runHub) maybePauseAfterStep(runID string, nodeID string, instanceID string) {
	if h == nil {
		return
	}
	nodeID = strings.TrimSpace(nodeID)
	instanceID = strings.TrimSpace(instanceID)
	shouldPause := false
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session != nil && session.pendingStep != nil {
		pending := session.pendingStep
		if (pending.nodeID == "" || pending.nodeID == nodeID) && (pending.instanceID == "" || pending.instanceID == instanceID) {
			session.pendingStep = nil
			shouldPause = true
		}
	}
	h.mu.Unlock()
	if !shouldPause {
		return
	}
	if h.pauseRuntime != nil {
		_ = h.pauseRuntime()
	}
	event := map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.paused",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID, "reason": "step_complete"},
	}
	if nodeID != "" {
		event["node_id"] = nodeID
	}
	if instanceID != "" {
		event["instance_id"] = instanceID
		event["payload"].(map[string]any)["instance_id"] = instanceID
	}
	h.emit(runID, event)
}

func (h *runHub) attachRuntime(rt *runtimepkg.Runtime) {
	if h == nil || rt == nil || rt.Bus == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.attached == rt {
		return
	}
	rt.Bus.SetLoggerHook(runtimeLoggerHook{base: rt.Logger, hub: h})
	h.attached = rt
}

type runtimeLoggerHook struct {
	base *runtimepkg.RuntimeLogger
	hub  *runHub
}

func (h runtimeLoggerHook) Log(ctx context.Context, level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) {
	entry := runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action, EventID: eventID, EventType: eventType,
		AgentID: agentID, EntityID: entityID, SessionID: sessionID, Correlation: correlation,
		Detail: detail, Error: errText, DurationUS: durationUS,
	}
	if h.base != nil {
		h.base.Log(ctx, entry)
	}
	if h.hub != nil {
		h.hub.handleRuntimeLog(entry)
	}
}

var _ runtimebus.LoggerHook = runtimeLoggerHook{}
