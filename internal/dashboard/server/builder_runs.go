package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	runtimepkg "empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	"github.com/google/uuid"
)

type builderRunHub struct {
	runtimeProvider func() *runtimepkg.Runtime
	resetRuntime    func() error
	pauseRuntime    func() error
	resumeRuntime   func() error

	mu       sync.RWMutex
	sessions map[string]*builderRunSession
	attached *runtimepkg.Runtime
}

type builderRunSession struct {
	runID              string
	runtime            *runtimepkg.Runtime
	entityIDs          map[string]struct{}
	breakpoints        map[string]struct{}
	trippedBreakpoints map[string]struct{}
	pendingHuman       *builderPendingHumanDecision
	pendingStep        *builderPendingNodeAction
	subs               map[string]func(RunEventEnvelope)
	events             []RunEventEnvelope
	terminal           bool
}

type builderPendingHumanDecision struct {
	nodeID          string
	instanceID      string
	requestingAgent string
}

type builderPendingNodeAction struct {
	kind       string
	nodeID     string
	instanceID string
}

type RunEventEnvelope = map[string]any

const builderRunCompletionTimeout = 30 * time.Second

func newBuilderRunHub(
	runtimeProvider func() *runtimepkg.Runtime,
	resetRuntime func() error,
	pauseRuntime func() error,
	resumeRuntime func() error,
) *builderRunHub {
	if runtimeProvider == nil {
		return nil
	}
	hub := &builderRunHub{
		runtimeProvider: runtimeProvider,
		resetRuntime:    resetRuntime,
		pauseRuntime:    pauseRuntime,
		resumeRuntime:   resumeRuntime,
		sessions:        map[string]*builderRunSession{},
	}
	return hub
}

func (h *builderRunHub) startRun(ctx context.Context, runID string, inputs map[string]any, breakpoints []string) error {
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
	session := &builderRunSession{
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
		payload := coerceBuilderPayload(rawPayload)
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
			Type:        events.EventType(eventName),
			SourceAgent: "builder",
			Payload:     encoded,
			CreatedAt:   time.Now().UTC(),
		}
		if err := rt.Bus.Publish(ctx, evt); err != nil {
			h.emit(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload": map[string]any{
					"run_id": runID,
					"error":  err.Error(),
				},
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
		"payload": map[string]any{
			"run_id": runID,
		},
	})
	go h.awaitCompletion(runID)
	return nil
}

func (h *builderRunHub) stopRun(runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	session := h.session(runID)
	if session == nil {
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
		"payload": map[string]any{
			"run_id": runID,
		},
	})
	return nil
}

func (h *builderRunHub) pauseRun(runID string) error {
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
		"payload": map[string]any{
			"run_id": runID,
		},
	})
	return nil
}

func (h *builderRunHub) continueRun(runID string, instanceIDs []string, decision string) error {
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
	payload := map[string]any{
		"run_id": runID,
	}
	if len(instanceIDs) > 0 {
		payload["instance_ids"] = instanceIDs
	}
	if strings.TrimSpace(decision) != "" {
		payload["decision"] = strings.TrimSpace(decision)
	}
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.resumed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	})
	return nil
}

func (h *builderRunHub) stepRun(runID string, nodeID string, instanceID string) error {
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
		session.pendingStep = &builderPendingNodeAction{
			kind:       "step",
			nodeID:     nodeID,
			instanceID: instanceID,
		}
	}
	h.mu.Unlock()
	if err := h.resumeRuntime(); err != nil {
		return err
	}
	payload := map[string]any{
		"run_id": runID,
		"mode":   "step",
	}
	event := map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.resumed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	}
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

func (h *builderRunHub) retryRun(runID string, nodeID string, instanceID string) error {
	return h.resumeNodeAction(runID, "retry", nodeID, instanceID)
}

func (h *builderRunHub) skipRun(runID string, nodeID string, instanceID string) error {
	return h.resumeNodeAction(runID, "skip", nodeID, instanceID)
}

func (h *builderRunHub) subscribe(runID string, listener func(RunEventEnvelope)) func() {
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
		session = &builderRunSession{
			runID:     runID,
			entityIDs: map[string]struct{}{},
			subs:      map[string]func(RunEventEnvelope){},
			events:    []RunEventEnvelope{},
		}
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

func (h *builderRunHub) handleRuntimeLog(entry runtimepkg.RuntimeLogEntry) {
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

func (h *builderRunHub) toRunEvent(entry runtimepkg.RuntimeLogEntry) RunEventEnvelope {
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

func (h *builderRunHub) maybeEmitBreakpointHit(runID string, nodeID string, instanceID string) {
	if h == nil {
		return
	}
	nodeID = strings.TrimSpace(nodeID)
	instanceID = strings.TrimSpace(instanceID)
	if nodeID == "" {
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
		"payload": map[string]any{
			"reason": "node_breakpoint",
		},
	}
	if instanceID != "" {
		event["instance_id"] = instanceID
		payload := event["payload"].(map[string]any)
		payload["instance_id"] = instanceID
	}
	h.emit(runID, event)
}

func (h *builderRunHub) maybeEmitHumanTaskWaiting(runID string, entry runtimepkg.RuntimeLogEntry) {
	if h == nil {
		return
	}
	if strings.TrimSpace(entry.Component) != "eventbus" || strings.TrimSpace(entry.Action) != "published" {
		return
	}
	if strings.TrimSpace(entry.EventType) != "human_task.requested" {
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
		session.pendingHuman = &builderPendingHumanDecision{
			nodeID:          nodeID,
			instanceID:      instanceID,
			requestingAgent: nodeID,
		}
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
		"payload": map[string]any{
			"decision_options": []string{"approved", "rejected", "deferred"},
		},
	})
	h.emit(runID, map[string]any{
		"id":          uuid.NewString(),
		"type":        "run.paused",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"node_id":     nodeID,
		"instance_id": instanceID,
		"payload": map[string]any{
			"run_id": runID,
			"reason": "human_task_waiting",
		},
	})
}

func (h *builderRunHub) submitPendingHumanDecision(ctx context.Context, runID string, decision string) error {
	if h == nil {
		return nil
	}
	decision = normalizeHumanDecision(decision)
	if decision == "" {
		return nil
	}

	var pending *builderPendingHumanDecision
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
	payload := map[string]any{
		"requesting_agent": pending.requestingAgent,
		"entity_id":        pending.instanceID,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := runtimeRef.Bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
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
		"payload": map[string]any{
			"decision": decision,
		},
	})
	return nil
}

func (h *builderRunHub) maybePauseAfterStep(runID string, nodeID string, instanceID string) {
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
		nodeMatches := pending.nodeID == "" || pending.nodeID == nodeID
		instanceMatches := pending.instanceID == "" || pending.instanceID == instanceID
		if nodeMatches && instanceMatches {
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
		"payload": map[string]any{
			"run_id": runID,
			"reason": "step_complete",
		},
	}
	if nodeID != "" {
		event["node_id"] = nodeID
	}
	if instanceID != "" {
		event["instance_id"] = instanceID
		payload := event["payload"].(map[string]any)
		payload["instance_id"] = instanceID
	}
	h.emit(runID, event)
}

func (h *builderRunHub) resumeNodeAction(runID string, actionKind string, nodeID string, instanceID string) error {
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

	actionEventType := "handler.retried"
	mode := "retry"
	if actionKind == "skip" {
		actionEventType = "handler.skipped"
		mode = "skip"
	}

	actionEvent := map[string]any{
		"id":        uuid.NewString(),
		"type":      actionEventType,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{},
	}
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

	payload := map[string]any{
		"run_id": runID,
		"mode":   mode,
	}
	if instanceID != "" {
		payload["instance_ids"] = []string{instanceID}
	}
	resumeEvent := map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.resumed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	}
	if nodeID != "" {
		resumeEvent["node_id"] = nodeID
	}
	if instanceID != "" {
		resumeEvent["instance_id"] = instanceID
	}
	h.emit(runID, resumeEvent)
	return nil
}

func (h *builderRunHub) emit(runID string, event RunEventEnvelope) {
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
	if len(session.subs) == 0 {
		h.mu.Unlock()
		return
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

func (h *builderRunHub) awaitCompletion(runID string) {
	session := h.session(runID)
	if session == nil || session.runtime == nil || session.runtime.Bus == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), builderRunCompletionTimeout)
	defer cancel()
	if err := session.runtime.Bus.WaitForQuiescence(ctx); err != nil {
		if h.isTerminal(runID) {
			return
		}
		h.markTerminal(runID)
		h.emit(runID, map[string]any{
			"id":        uuid.NewString(),
			"type":      "run.failed",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"payload": map[string]any{
				"run_id": runID,
				"error":  err.Error(),
			},
		})
		return
	}
	if h.isTerminal(runID) {
		return
	}
	h.markTerminal(runID)
	h.emit(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.completed",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload": map[string]any{
			"run_id": runID,
			"summary": map[string]any{
				"duration_ms":  0,
				"total_events": 0,
			},
		},
	})
}

func (h *builderRunHub) deleteRun(runID string) {
	h.mu.Lock()
	delete(h.sessions, strings.TrimSpace(runID))
	h.mu.Unlock()
}

func (h *builderRunHub) markTerminal(runID string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if session := h.sessions[strings.TrimSpace(runID)]; session != nil {
		session.terminal = true
	}
}

func (h *builderRunHub) isTerminal(runID string) bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	session := h.sessions[strings.TrimSpace(runID)]
	return session != nil && session.terminal
}

func (h *builderRunHub) currentRuntime() *runtimepkg.Runtime {
	if h == nil || h.runtimeProvider == nil {
		return nil
	}
	return h.runtimeProvider()
}

func (h *builderRunHub) attachRuntime(rt *runtimepkg.Runtime) {
	if h == nil || rt == nil || rt.Bus == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.attached == rt {
		return
	}
	rt.Bus.SetLoggerHook(builderRuntimeLoggerHook{
		base: rt.Logger,
		hub:  h,
	})
	h.attached = rt
}

func (h *builderRunHub) session(runID string) *builderRunSession {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sessions[strings.TrimSpace(runID)]
}

type builderRuntimeLoggerHook struct {
	base *runtimepkg.RuntimeLogger
	hub  *builderRunHub
}

func (h builderRuntimeLoggerHook) Log(ctx context.Context, level, component, action, eventID, eventType, agentID, entityID, campaignID, scanID, sessionID string, detail any, errText string, durationUS int) {
	if h.base != nil {
		h.base.Log(ctx, runtimepkg.RuntimeLogEntry{
			Level:      level,
			Component:  component,
			Action:     action,
			EventID:    eventID,
			EventType:  eventType,
			AgentID:    agentID,
			EntityID:   entityID,
			CampaignID: campaignID,
			ScanID:     scanID,
			SessionID:  sessionID,
			Detail:     detail,
			Error:      errText,
			DurationUS: durationUS,
		})
	}
	if h.hub != nil {
		h.hub.handleRuntimeLog(runtimepkg.RuntimeLogEntry{
			Level:      level,
			Component:  component,
			Action:     action,
			EventID:    eventID,
			EventType:  eventType,
			AgentID:    agentID,
			EntityID:   entityID,
			CampaignID: campaignID,
			ScanID:     scanID,
			SessionID:  sessionID,
			Detail:     detail,
			Error:      errText,
			DurationUS: durationUS,
		})
	}
}

func coerceBuilderPayload(raw any) map[string]any {
	switch typed := raw.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = value
		}
		return out
	default:
		if typed == nil {
			return map[string]any{}
		}
		return map[string]any{"value": typed}
	}
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func normalizeHumanDecision(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "approve", "approved":
		return "approved"
	case "reject", "rejected":
		return "rejected"
	case "defer", "deferred":
		return "deferred"
	default:
		return ""
	}
}

func payloadMap(v any) map[string]any {
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = value
		}
		return out
	default:
		return map[string]any{}
	}
}

func nonEmptyOrUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw != "" {
		return raw
	}
	return uuid.NewString()
}

func cloneRunEvent(event RunEventEnvelope) RunEventEnvelope {
	out := make(RunEventEnvelope, len(event))
	for key, value := range event {
		out[key] = value
	}
	return out
}

var _ runtimebus.LoggerHook = builderRuntimeLoggerHook{}
