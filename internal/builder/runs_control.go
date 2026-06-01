package builder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/google/uuid"
)

var runCompletionTimeout = 30 * time.Second

func newRunHub(runtimeProvider func() *runtimepkg.Runtime, pauseRuntime func() error, resumeRuntime func() error, runDebug RunDebugReader) *runHub {
	if runtimeProvider == nil {
		return nil
	}
	return &runHub{
		runtimeProvider: runtimeProvider,
		pauseRuntime:    pauseRuntime,
		resumeRuntime:   resumeRuntime,
		runDebug:        runDebug,
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
		controlEvents:      []RunEventEnvelope{},
		debug: runDebugStreamState{
			eventIDs:      map[string]struct{}{},
			runtimeLogIDs: map[string]struct{}{},
		},
	}
	h.mu.Lock()
	if existing := h.sessions[runID]; existing != nil {
		for subID, listener := range existing.subs {
			session.subs[subID] = listener
		}
		for entityID := range existing.entityIDs {
			session.entityIDs[entityID] = struct{}{}
		}
		session.controlEvents = append(session.controlEvents, existing.controlEvents...)
		session.debug = existing.debug.clone()
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
			h.emitControl(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload":   map[string]any{"run_id": runID, "error": err.Error()},
			})
			h.deleteRun(runID)
			return err
		}
	}
	h.syncCanonical(context.Background(), runID)
	go h.awaitCompletion(runID)
	return nil
}

func (h *runHub) stopRun(ctx context.Context, runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	ctrl, err := h.runControl(runID)
	if err != nil {
		return err
	}
	if _, err := ctrl.Stop(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "builder_rpc", ControlledBy: "builder.rpc"}); err != nil {
		return err
	}
	h.markTerminal(runID)
	h.emitControl(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.stopped",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID},
	})
	return nil
}

func (h *runHub) pauseRun(ctx context.Context, runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	ctrl, err := h.runControl(runID)
	if err != nil {
		return err
	}
	if _, err := ctrl.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "builder_rpc", ControlledBy: "builder.rpc"}); err != nil {
		return err
	}
	h.markPaused(runID, true)
	h.emitControl(runID, map[string]any{
		"id":        uuid.NewString(),
		"type":      "run.paused",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"run_id": runID},
	})
	return nil
}

func (h *runHub) continueRun(ctx context.Context, runID string) error {
	if h == nil {
		return fmt.Errorf("run hub is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	ctrl, err := h.runControl(runID)
	if err != nil {
		return err
	}
	if _, err := ctrl.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "builder_rpc", ControlledBy: "builder.rpc"}); err != nil {
		return err
	}
	h.markPaused(runID, false)
	payload := map[string]any{"run_id": runID}
	h.emitControl(runID, map[string]any{
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
	h.emitControl(runID, event)
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
	h.emitControl(runID, actionEvent)
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
	h.emitControl(runID, resumeEvent)
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
		session = &runSession{
			runID:         runID,
			entityIDs:     map[string]struct{}{},
			subs:          map[string]func(RunEventEnvelope){},
			controlEvents: []RunEventEnvelope{},
			debug: runDebugStreamState{
				eventIDs:      map[string]struct{}{},
				runtimeLogIDs: map[string]struct{}{},
			},
		}
		h.sessions[runID] = session
	}
	session.subs[subID] = listener
	controlReplay := append([]RunEventEnvelope(nil), session.controlEvents...)
	canonicalReplay, primedState := h.canonicalReplay(context.Background(), runID)
	session.debug = primedState
	h.mu.Unlock()
	for _, event := range canonicalReplay {
		listener(event)
	}
	for _, event := range controlReplay {
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

func (h *runHub) emitControl(runID string, event RunEventEnvelope) {
	h.mu.Lock()
	session := h.sessions[runID]
	if session == nil {
		h.mu.Unlock()
		return
	}
	session.controlEvents = append(session.controlEvents, cloneRunEvent(event))
	if len(session.controlEvents) > 128 {
		session.controlEvents = append([]RunEventEnvelope(nil), session.controlEvents[len(session.controlEvents)-128:]...)
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
	for {
		session := h.session(runID)
		if session == nil || session.runtime == nil || session.runtime.Bus == nil {
			return
		}
		if h.isPaused(runID) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), runCompletionTimeout)
		err := session.runtime.WaitForQuiescence(ctx)
		cancel()
		if err != nil {
			if h.isTerminal(runID) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// A local quiescence timeout is not authoritative proof that the
				// accepted run should become terminal while same-run work may still
				// be active. Keep waiting until authoritative quiescence is true.
				continue
			}
			if _, persistErr := h.persistTerminalState(runID, "failed", err.Error(), time.Now().UTC()); persistErr != nil {
				h.markTerminal(runID)
				h.emitControl(runID, map[string]any{
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
			h.syncCanonical(context.Background(), runID)
			return
		}
		if h.isTerminal(runID) {
			return
		}
		snapshot, err := h.persistTerminalState(runID, "completed", "", time.Now().UTC())
		if err != nil {
			h.markTerminal(runID)
			h.emitControl(runID, map[string]any{
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
		_ = snapshot
		h.syncCanonical(context.Background(), runID)
		return
	}
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
		session.paused = false
	}
}

func (h *runHub) markPaused(runID string, paused bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if session := h.sessions[strings.TrimSpace(runID)]; session != nil {
		session.paused = paused
	}
}

func (h *runHub) isPaused(runID string) bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	session := h.sessions[strings.TrimSpace(runID)]
	return session != nil && session.paused
}

func (h *runHub) runControl(runID string) (*runtimeruncontrol.Controller, error) {
	runID = strings.TrimSpace(runID)
	if runID != "" {
		if session := h.session(runID); session != nil && session.runtime != nil && session.runtime.RunControl != nil {
			return session.runtime.RunControl, nil
		}
	}
	if rt := h.currentRuntime(); rt != nil && rt.RunControl != nil {
		return rt.RunControl, nil
	}
	return nil, fmt.Errorf("run control owner is not configured")
}

func (h *runHub) persistTerminalState(runID, status, errorSummary string, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	session := h.session(runID)
	if session == nil || session.runtime == nil || session.runtime.Bus == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run terminal persistence is not configured")
	}
	writer, ok := session.runtime.Bus.Store().(runtimebus.RunLifecyclePersistence)
	if !ok || writer == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run terminal persistence is not supported")
	}
	if err := writer.MarkRunTerminal(context.Background(), runID, status, errorSummary, endedAt); err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	reader, ok := session.runtime.Bus.Store().(runtimebus.RunLifecycleReadPersistence)
	if !ok || reader == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run lifecycle snapshot persistence is not supported")
	}
	return reader.LoadRunLifecycleSnapshot(context.Background(), runID)
}

func durationMillis(snapshot runtimebus.RunLifecycleSnapshot) int64 {
	if snapshot.StartedAt.IsZero() {
		return 0
	}
	endedAt := time.Now().UTC()
	if snapshot.EndedAt != nil && !snapshot.EndedAt.IsZero() {
		endedAt = snapshot.EndedAt.UTC()
	}
	if endedAt.Before(snapshot.StartedAt) {
		return 0
	}
	return endedAt.Sub(snapshot.StartedAt).Milliseconds()
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
