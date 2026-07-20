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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/google/uuid"
)

var runCompletionTimeout = 30 * time.Second
var runCompletionObservationInterval = 50 * time.Millisecond

func newRunHub(runtimeAcquirer RuntimeAcquirer, pauseRuntime func() error, resumeRuntime func() error, runDebug RunDebugReader) *runHub {
	if runtimeAcquirer == nil {
		return nil
	}
	return &runHub{
		runtimeAcquirer: runtimeAcquirer,
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
	use, err := h.runtimeAcquirer.AcquireCurrentRuntime(context.WithoutCancel(ctx))
	if err != nil {
		return err
	}
	releaseUse := true
	defer func() {
		if releaseUse {
			_ = use.Done()
		}
	}()
	rt := use.Runtime()
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
		evt, err := events.NewRunCreatingRootIngressEvent(events.RunCreatingRootIngressEventInput{Facts: events.EventFacts{
			ID: uuid.NewString(), Type: events.EventType(eventName),
			Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "builder"},
			Payload:  encoded, Envelope: events.EventEnvelope{EntityID: entityID},
			CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
		}, RunID: runID})
		if err != nil {
			h.deleteRun(runID)
			return err
		}
		if err := rt.Bus.Publish(use.WorkContext(), evt); err != nil {
			failure := runtimefailures.Normalize(err, "builder.run_hub", "publish_run_input")
			h.emitControl(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload":   map[string]any{"run_id": runID, "failure": builderFailureValue(failure)},
			})
			h.deleteRun(runID)
			return err
		}
	}
	h.syncCanonical(use.WorkContext(), runID)
	releaseUse = false
	go func() {
		defer func() { _ = use.Done() }()
		h.awaitCompletion(use.WorkContext(), runID, rt)
	}()
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
	ctrl, use, err := h.runControl(ctx, runID)
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
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
	ctrl, use, err := h.runControl(ctx, runID)
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
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
	ctrl, use, err := h.runControl(ctx, runID)
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
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

func (h *runHub) awaitCompletion(ctx context.Context, runID string, rt *runtimepkg.Runtime) {
	for {
		session := h.session(runID)
		if session == nil || rt == nil || rt.Bus == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if h.isTerminal(runID) {
			return
		}
		if h.isPaused(runID) {
			if !waitBuilderObservation(ctx, 50*time.Millisecond) {
				return
			}
			continue
		}
		waitCtx, cancel := context.WithTimeout(ctx, runCompletionTimeout)
		waitForQuiescence := rt.WaitForQuiescence
		if session.waitForQuiescence != nil {
			waitForQuiescence = session.waitForQuiescence
		}
		err := waitForQuiescence(waitCtx)
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
			failure := runtimefailures.Normalize(err, "builder.run_hub", "wait_for_quiescence")
			if _, persistErr := h.persistTerminalState(ctx, rt, runID, "failed", &failure, time.Now().UTC()); persistErr != nil {
				h.markTerminal(runID)
				persistenceFailure := runTerminalPersistenceFailure("failed")
				h.emitControl(runID, map[string]any{
					"id":        uuid.NewString(),
					"type":      "run.failed",
					"timestamp": time.Now().UTC().Format(time.RFC3339),
					"payload": map[string]any{
						"run_id":  runID,
						"failure": builderFailureValue(persistenceFailure),
					},
				})
				return
			}
			h.markTerminal(runID)
			h.syncCanonical(ctx, runID)
			return
		}
		snapshot, err := h.loadCanonicalRunLifecycle(ctx, rt, runID)
		if err != nil {
			h.markTerminal(runID)
			observationFailure := runCompletionObservationFailure()
			h.emitControl(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload": map[string]any{
					"run_id":  runID,
					"failure": builderFailureValue(observationFailure),
				},
			})
			return
		}
		switch strings.TrimSpace(strings.ToLower(snapshot.Status)) {
		case "completed", "failed", "cancelled", "forked":
			h.markTerminal(runID)
			h.syncCanonical(ctx, runID)
			return
		case "running", "paused":
			h.syncCanonical(ctx, runID)
			if !waitBuilderObservation(ctx, runCompletionObservationInterval) {
				return
			}
			continue
		default:
			h.markTerminal(runID)
			observationFailure := runCompletionObservationFailure()
			h.emitControl(runID, map[string]any{
				"id":        uuid.NewString(),
				"type":      "run.failed",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"payload": map[string]any{
					"run_id":  runID,
					"failure": builderFailureValue(observationFailure),
				},
			})
			return
		}
	}
}

func waitBuilderObservation(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
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

func (h *runHub) runControl(ctx context.Context, runID string) (*runtimeruncontrol.Controller, RuntimeUse, error) {
	runID = strings.TrimSpace(runID)
	if h == nil || h.runtimeAcquirer == nil {
		return nil, nil, fmt.Errorf("runtime acquirer is not configured")
	}
	use, err := h.runtimeAcquirer.AcquireRunRuntime(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	rt := use.Runtime()
	if rt == nil || rt.RunControl == nil {
		_ = use.Done()
		return nil, nil, fmt.Errorf("run control owner is not configured")
	}
	return rt.RunControl, use, nil
}

func (h *runHub) persistTerminalState(ctx context.Context, rt *runtimepkg.Runtime, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	if rt == nil || rt.Bus == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run terminal persistence is not configured")
	}
	writer, ok := rt.Bus.Store().(runtimebus.RunLifecyclePersistence)
	if !ok || writer == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run terminal persistence is not supported")
	}
	return writer.MarkRunTerminal(ctx, runID, status, failure, endedAt)
}

func (h *runHub) loadCanonicalRunLifecycle(ctx context.Context, rt *runtimepkg.Runtime, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if rt == nil || rt.Bus == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run lifecycle observation is not configured")
	}
	reader, ok := rt.Bus.Store().(runtimebus.RunLifecycleReadPersistence)
	if !ok || reader == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("run lifecycle observation is not supported")
	}
	return reader.LoadRunLifecycleSnapshot(ctx, runID)
}

func runTerminalPersistenceFailure(attemptedStatus string) runtimefailures.Envelope {
	return runtimefailures.Normalize(
		runtimefailures.New(
			runtimefailures.ClassOutcomeUncertain,
			"run_terminal_persistence_unconfirmed",
			"builder.run_hub",
			"mark_run_terminal",
			map[string]any{"attempted_status": strings.TrimSpace(attemptedStatus)},
		),
		"builder.run_hub",
		"mark_run_terminal",
	)
}

func runCompletionObservationFailure() runtimefailures.Envelope {
	return runtimefailures.Normalize(
		runtimefailures.New(
			runtimefailures.ClassOutcomeUncertain,
			"run_completion_observation_unavailable",
			"builder.run_hub",
			"observe_run_completion",
			nil,
		),
		"builder.run_hub",
		"observe_run_completion",
	)
}

func builderFailureValue(failure runtimefailures.Envelope) map[string]any {
	value, err := runtimefailures.EnvelopeValue(failure)
	if err != nil {
		fallback := runtimefailures.Normalize(err, "builder.run_hub", "encode_failure")
		value, _ = runtimefailures.EnvelopeValue(fallback)
	}
	return value
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

func (h *runHub) session(runID string) *runSession {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sessions[strings.TrimSpace(runID)]
}
