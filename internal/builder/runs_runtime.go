package builder

import (
	"context"
	"strings"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func (h *runHub) handleRuntimeLog(entry runtimepkg.RuntimeLogEntry) {
	if h == nil {
		return
	}
	controlRunIDs := h.correlatedRunIDsForRuntimeEntry(entry)
	for _, runID := range controlRunIDs {
		h.maybeEmitBreakpointHit(runID, entry.AgentID, entry.EffectiveEntityID())
		h.maybePauseAfterStep(runID, entry.AgentID, entry.EffectiveEntityID())
	}
	runIDs := h.runIDsForRuntimeEntry(entry)
	for _, runID := range runIDs {
		h.syncCanonical(context.Background(), runID)
	}
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
	h.emitControl(runID, event)
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
	h.emitControl(runID, event)
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

func (h runtimeLoggerHook) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	entry := runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action, EventID: eventID, EventType: eventType,
		AgentID: agentID, EntityID: entityID, SessionID: sessionID, Correlation: correlation,
		Detail: detail, Failure: runtimefailures.CloneEnvelope(failure), DurationUS: durationUS,
	}
	var persistErr error
	if h.base != nil {
		persistErr = h.base.Log(ctx, entry)
	}
	if h.hub != nil {
		h.hub.handleRuntimeLog(entry)
	}
	return persistErr
}

var _ runtimebus.LoggerHook = runtimeLoggerHook{}
