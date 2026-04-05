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

const runCompletionTimeout = 30 * time.Second

func newRunHub(runtimeProvider func() *runtimepkg.Runtime, resetRuntime func() error, pauseRuntime func() error, resumeRuntime func() error) *runHub {
	if runtimeProvider == nil {
		return nil
	}
	return &runHub{
		runtimeProvider: runtimeProvider,
		resetRuntime:    resetRuntime,
		pauseRuntime:    pauseRuntime,
		resumeRuntime:   resumeRuntime,
		sessions:        map[string]*runSession{},
	}
}

func (h *runHub) startRun(ctx context.Context, runID string, inputs map[string]any, breakpoints []string) error {
	if h == nil {
		return fmt.Errorf("runtime bus is not configured")
	}
	rt := h.currentRuntime()
	if rt == nil || rt.Bus == nil {
		return fmt.Errorf("runtime bus is not configured")
	}
	h.attachRuntime(rt)
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	session := &runSession{
		runID:              runID,
		runtime:            rt,
		entityIDs:          map[string]struct{}{},
		breakpoints:        stringSet(breakpoints),
		trippedBreakpoints: map[string]struct{}{},
		subs:               map[string]func(RunEventEnvelope){},
		events:             []RunEventEnvelope{},
	}
	h.mu.Lock()
	if existing := h.sessions[runID]; existing != nil {
		for subID, listener := range existing.subs {
			session.subs[subID] = listener
		}
		for entityID := range existing.entityIDs {
			session.entityIDs[entityID] = struct{}{}
		}
		session.events = append(session.events, existing.events...)
	}
	h.sessions[runID] = session
	h.mu.Unlock()
	for eventName, rawPayload := range inputs {
		eventName = strings.TrimSpace(eventName)
		if eventName == "" {
			continue
		}
		payload := coercePayload(rawPayload)
		entityID := strings.TrimSpace(asString(payload["entity_id"]))
		if entityID == "" {
			entityID = runID
			payload["entity_id"] = entityID
		}
		session.entityIDs[entityID] = struct{}{}
		encoded, err := json.Marshal(payload)
		if err != nil {
			h.deleteRun(runID)
			return err
		}
		evt := events.Event{
			ID:          uuid.NewString(),
			RunID:       runID,
			Type:        events.EventType(eventName),
			SourceAgent: "builder",
			Payload:     encoded,
			CreatedAt:   time.Now().UTC(),
		}.WithEntityID(entityID)
		if err := rt.Bus.Publish(ctx, evt); err != nil {
			h.emit(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload":   map[string]any{"run_id": runID, "error": err.Error()},
			})
			h.deleteRun(runID)
			return err
		}
		h.emit(runID, map[string]any{
			"id":          evt.ID,
			"type":        "event.fired",
			"timestamp":   evt.CreatedAt.Format(time.RFC3339),
			"instance_id": entityID,
			"payload": map[string]any{
				"event_name": eventName,
				"source":     evt.SourceAgent,
				"payload":    payload,
			},
		})
	}
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.started",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID},
	})
	go h.awaitCompletion(runID)
	return nil
}

func (h *runHub) stopRun(runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if h.session(runID) == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	h.markTerminal(runID)
	if h.resetRuntime != nil {
		if err := h.resetRuntime(); err != nil {
			return err
		}
	}
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.stopped",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID},
	})
	return nil
}

func (h *runHub) pauseRun(runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if h.session(runID) == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	if h.pauseRuntime == nil {
		return fmt.Errorf("pause runtime is not configured")
	}
	if err := h.pauseRuntime(); err != nil {
		return err
	}
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.paused",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID},
	})
	return nil
}

func (h *runHub) continueRun(runID string, instanceIDs []string, decision string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if h.session(runID) == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	if h.resumeRuntime == nil {
		return fmt.Errorf("resume runtime is not configured")
	}
	if err := h.submitPendingHumanDecision(context.Background(), runID, decision); err != nil {
		return err
	}
	if err := h.resumeRuntime(); err != nil {
		return err
	}
	payload := map[string]any{"run_id": runID}
	if len(instanceIDs) > 0 {
		payload["instance_ids"] = instanceIDs
	}
	if decision = strings.TrimSpace(decision); decision != "" {
		payload["decision"] = decision
	}
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.resumed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	})
	return nil
}

func (h *runHub) stepRun(runID string, nodeID string, instanceID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	nodeID = strings.TrimSpace(nodeID)
	instanceID = strings.TrimSpace(instanceID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if h.session(runID) == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	if h.resumeRuntime == nil {
		return fmt.Errorf("resume runtime is not configured")
	}
	h.mu.Lock()
	if session := h.sessions[runID]; session != nil {
		session.pendingStep = &pendingNodeAction{kind: "step", nodeID: nodeID, instanceID: instanceID}
	}
	h.mu.Unlock()
	if err := h.resumeRuntime(); err != nil {
		return err
	}
	payload := map[string]any{"run_id": runID, "mode": "step"}
	event := map[string]any{"id": uuid.NewString(), "type": "run.resumed", "timestamp": time.Now().UTC().Format(time.RFC3339), "payload": payload}
	if nodeID != "" {
		event["node_id"] = nodeID
	}
	if instanceID != "" {
		event["instance_id"] = instanceID
		payload["instance_ids"] = []string{instanceID}
	}
	h.emit(runID, event)
	return nil
}

func (h *runHub) retryRun(runID string, nodeID string, instanceID string) error {
	return h.resumeNodeAction(runID, "retry", nodeID, instanceID)
}

func (h *runHub) skipRun(runID string, nodeID string, instanceID string) error {
	return h.resumeNodeAction(runID, "skip", nodeID, instanceID)
}

func (h *runHub) resumeNodeAction(runID string, actionKind string, nodeID string, instanceID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	nodeID = strings.TrimSpace(nodeID)
	instanceID = strings.TrimSpace(instanceID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if h.session(runID) == nil {
		return fmt.Errorf("run not found: %s", runID)
	}
	if actionKind == "skip" {
		if err := h.submitPendingHumanDecision(context.Background(), runID, "deferred"); err != nil {
			return err
		}
	}
	if h.resumeRuntime == nil {
		return fmt.Errorf("resume runtime is not configured")
	}
	actionEventType, mode := "handler.retried", "retry"
	if actionKind == "skip" {
		actionEventType, mode = "handler.skipped", "skip"
	}
	actionEvent := map[string]any{"id": uuid.NewString(), "type": actionEventType, "timestamp": time.Now().UTC().Format(time.RFC3339), "payload": map[string]any{}}
	if nodeID != "" {
		actionEvent["node_id"] = nodeID
	}
	if instanceID != "" {
		actionEvent["instance_id"] = instanceID
		actionEvent["payload"].(map[string]any)["instance_id"] = instanceID
	}
	h.emit(runID, actionEvent)
	if err := h.resumeRuntime(); err != nil {
		return err
	}
	payload := map[string]any{"run_id": runID, "mode": mode}
	if instanceID != "" {
		payload["instance_ids"] = []string{instanceID}
	}
	resumeEvent := map[string]any{"id": uuid.NewString(), "type": "run.resumed", "timestamp": time.Now().UTC().Format(time.RFC3339), "payload": payload}
	if nodeID != "" {
		resumeEvent["node_id"] = nodeID
	}
	if instanceID != "" {
		resumeEvent["instance_id"] = instanceID
	}
	h.emit(runID, resumeEvent)
	return nil
}

func (h *runHub) subscribe(runID string, listener func(RunEventEnvelope)) func() {
	if h == nil || listener == nil {
		return func() {}
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return func() {}
	}
	subID := uuid.NewString()
	h.mu.Lock()
	session, ok := h.sessions[runID]
	if !ok {
		session = &runSession{runID: runID, entityIDs: map[string]struct{}{}, subs: map[string]func(RunEventEnvelope){}, events: []RunEventEnvelope{}}
		h.sessions[runID] = session
	}
	session.subs[subID] = listener
	replay := append([]RunEventEnvelope(nil), session.events...)
	h.mu.Unlock()
	for _, event := range replay {
		listener(event)
	}
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		session := h.sessions[runID]
		if session == nil {
			return
		}
		delete(session.subs, subID)
		if len(session.subs) == 0 && (len(session.entityIDs) == 0 || session.terminal) {
			delete(h.sessions, runID)
		}
	}
}

func (h *runHub) emit(runID string, event RunEventEnvelope) {
	h.mu.Lock()
	session := h.sessions[runID]
	if session == nil {
		h.mu.Unlock()
		return
	}
	session.events = append(session.events, cloneRunEvent(event))
	if len(session.events) > 128 {
		session.events = append([]RunEventEnvelope(nil), session.events[len(session.events)-128:]...)
	}
	listeners := make([]func(RunEventEnvelope), 0, len(session.subs))
	for _, listener := range session.subs {
		listeners = append(listeners, listener)
	}
	h.mu.Unlock()
	for _, listener := range listeners {
		listener(event)
	}
}

func (h *runHub) awaitCompletion(runID string) {
	session := h.session(runID)
	if session == nil || session.runtime == nil || session.runtime.Bus == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), runCompletionTimeout)
	defer cancel()
	if err := session.runtime.Bus.WaitForQuiescence(ctx); err != nil {
		if h.isTerminal(runID) {
			return
		}
		if persistErr := h.persistTerminalState(runID, "failed", err.Error(), time.Now().UTC()); persistErr != nil {
			h.markTerminal(runID)
			h.emit(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload": map[string]any{
					"run_id":            runID,
					"error":             err.Error(),
					"persistence_error": persistErr.Error(),
				},
			})
			return
		}
		h.markTerminal(runID)
		h.emit(runID, map[string]any{
			"id":        uuid.NewString(),
			"type":      "run.failed",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"payload":   map[string]any{"run_id": runID, "error": err.Error()},
		})
		return
	}
	if h.isTerminal(runID) {
		return
	}
	if err := h.persistTerminalState(runID, "completed", "", time.Now().UTC()); err != nil {
		h.markTerminal(runID)
		h.emit(runID, map[string]any{
			"id":        uuid.NewString(),
			"type":      "run.failed",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"payload": map[string]any{
				"run_id":            runID,
				"error":             "persisting canonical run completion failed",
				"persistence_error": err.Error(),
			},
		})
		return
	}
	h.markTerminal(runID)
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.completed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID, "summary": map[string]any{"duration_ms": 0, "total_events": 0}},
	})
}

func (h *runHub) deleteRun(runID string) {
	h.mu.Lock()
	delete(h.sessions, strings.TrimSpace(runID))
	h.mu.Unlock()
}

func (h *runHub) markTerminal(runID string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if session := h.sessions[strings.TrimSpace(runID)]; session != nil {
		session.terminal = true
	}
}

func (h *runHub) persistTerminalState(runID, status, errorSummary string, endedAt time.Time) error {
	session := h.session(runID)
	if session == nil || session.runtime == nil || session.runtime.Bus == nil {
		return fmt.Errorf("run terminal persistence is not configured")
	}
	writer, ok := session.runtime.Bus.Store().(runtimebus.RunLifecyclePersistence)
	if !ok || writer == nil {
		return fmt.Errorf("run terminal persistence is not supported")
	}
	return writer.MarkRunTerminal(context.Background(), runID, status, errorSummary, endedAt)
}

func (h *runHub) isTerminal(runID string) bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	session := h.sessions[strings.TrimSpace(runID)]
	return session != nil && session.terminal
}

func (h *runHub) currentRuntime() *runtimepkg.Runtime {
	if h == nil || h.runtimeProvider == nil {
		return nil
	}
	return h.runtimeProvider()
}

func (h *runHub) session(runID string) *runSession {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sessions[strings.TrimSpace(runID)]
}
