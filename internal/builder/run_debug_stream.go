package builder

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

const builderRunDebugReplayLimit = 128

type runDebugCandidate struct {
	key   string
	at    time.Time
	order int
	event RunEventEnvelope
}

func (s runDebugStreamState) clone() runDebugStreamState {
	out := runDebugStreamState{
		startedKey:    s.startedKey,
		terminalKey:   s.terminalKey,
		eventIDs:      map[string]struct{}{},
		runtimeLogIDs: map[string]struct{}{},
	}
	for key := range s.eventIDs {
		out.eventIDs[key] = struct{}{}
	}
	for key := range s.runtimeLogIDs {
		out.runtimeLogIDs[key] = struct{}{}
	}
	return out
}

func (h *runHub) runIDsForRuntimeEntry(entry interface{ EffectiveEntityID() string }) []string {
	if h == nil {
		return nil
	}
	entityID := strings.TrimSpace(entry.EffectiveEntityID())
	h.mu.RLock()
	defer h.mu.RUnlock()
	runIDs := make([]string, 0, len(h.sessions))
	if entityID == "" {
		for runID, session := range h.sessions {
			if session != nil {
				runIDs = append(runIDs, runID)
			}
		}
		sort.Strings(runIDs)
		return runIDs
	}
	for runID, session := range h.sessions {
		if session == nil {
			continue
		}
		if _, ok := session.entityIDs[entityID]; ok {
			runIDs = append(runIDs, runID)
		}
	}
	sort.Strings(runIDs)
	return runIDs
}

func (h *runHub) correlatedRunIDsForRuntimeEntry(entry interface{ EffectiveEntityID() string }) []string {
	if h == nil {
		return nil
	}
	entityID := strings.TrimSpace(entry.EffectiveEntityID())
	if entityID == "" {
		return nil
	}
	return h.runIDsForRuntimeEntry(entry)
}

func (h *runHub) canonicalReplay(ctx context.Context, runID string) ([]RunEventEnvelope, runDebugStreamState) {
	snapshot, _ := h.loadRunLifecycleSnapshot(ctx, runID)
	events, _ := h.loadOperatorEvents(ctx, runID)
	runtimeLogs, _ := h.loadOperatorRuntimeLogs(ctx, runID)
	return projectCanonicalRunDebugReplay(snapshot, events, runtimeLogs)
}

func (h *runHub) syncCanonical(ctx context.Context, runID string) {
	snapshot, snapshotOK := h.loadRunLifecycleSnapshot(ctx, runID)
	events, eventsOK := h.loadOperatorEvents(ctx, runID)
	runtimeLogs, logsOK := h.loadOperatorRuntimeLogs(ctx, runID)
	if !snapshotOK && !eventsOK && !logsOK {
		return
	}
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session == nil {
		h.mu.Unlock()
		return
	}
	delta := projectCanonicalRunDebugDelta(snapshot, events, runtimeLogs, &session.debug)
	listeners := make([]func(RunEventEnvelope), 0, len(session.subs))
	for _, listener := range session.subs {
		listeners = append(listeners, listener)
	}
	h.mu.Unlock()
	for _, event := range delta {
		for _, listener := range listeners {
			listener(event)
		}
	}
}

func (h *runHub) loadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, bool) {
	if h == nil || h.runDebug == nil {
		return runtimebus.RunLifecycleSnapshot{}, false
	}
	snapshot, err := h.runDebug.LoadRunLifecycleSnapshot(ctx, strings.TrimSpace(runID))
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, false
	}
	return snapshot, true
}

func (h *runHub) loadOperatorEvents(ctx context.Context, runID string) ([]store.OperatorEventFull, bool) {
	if h == nil || h.runDebug == nil {
		return nil, false
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, false
	}
	result, err := h.runDebug.ListOperatorEvents(ctx, store.OperatorEventListOptions{
		Filter:             store.OperatorEventListFilter{RunID: runID},
		Limit:              builderRunDebugReplayLimit,
		Order:              "desc",
		ExcludeRuntimeLogs: true,
	})
	if err != nil {
		return nil, false
	}
	return result.Events, true
}

func (h *runHub) loadOperatorRuntimeLogs(ctx context.Context, runID string) ([]store.OperatorRuntimeLogEntry, bool) {
	if h == nil || h.runDebug == nil {
		return nil, false
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, false
	}
	result, err := h.runDebug.ListOperatorRuntimeLogs(ctx, store.OperatorRuntimeLogListOptions{
		RunID: runID,
		Limit: builderRunDebugReplayLimit,
		Order: "desc",
	})
	if err != nil {
		return nil, false
	}
	return result.Logs, true
}

func projectCanonicalRunDebugReplay(snapshot runtimebus.RunLifecycleSnapshot, events []store.OperatorEventFull, runtimeLogs []store.OperatorRuntimeLogEntry) ([]RunEventEnvelope, runDebugStreamState) {
	state := runDebugStreamState{
		eventIDs:      map[string]struct{}{},
		runtimeLogIDs: map[string]struct{}{},
	}
	return projectCanonicalRunDebugDelta(snapshot, events, runtimeLogs, &state), state
}

func projectCanonicalRunDebugDelta(snapshot runtimebus.RunLifecycleSnapshot, events []store.OperatorEventFull, runtimeLogs []store.OperatorRuntimeLogEntry, state *runDebugStreamState) []RunEventEnvelope {
	if state == nil {
		state = &runDebugStreamState{eventIDs: map[string]struct{}{}, runtimeLogIDs: map[string]struct{}{}}
	}
	if state.eventIDs == nil {
		state.eventIDs = map[string]struct{}{}
	}
	if state.runtimeLogIDs == nil {
		state.runtimeLogIDs = map[string]struct{}{}
	}
	candidates := make([]runDebugCandidate, 0, 2+len(events)+len(runtimeLogs))
	if started := canonicalRunStartedCandidate(snapshot); started.key != "" && started.key != state.startedKey {
		candidates = append(candidates, started)
		state.startedKey = started.key
	}
	for _, item := range events {
		if strings.TrimSpace(item.EventName) == "platform.runtime_log" {
			continue
		}
		key := strings.TrimSpace(item.EventID)
		if key == "" {
			continue
		}
		if _, seen := state.eventIDs[key]; seen {
			continue
		}
		state.eventIDs[key] = struct{}{}
		candidates = append(candidates, canonicalEventFiredCandidate(item, key))
	}
	for _, item := range runtimeLogs {
		key := strings.TrimSpace(item.LogID)
		if key == "" {
			key = item.TS.UTC().Format(time.RFC3339Nano) + ":" + item.Component + ":" + item.Message
		}
		if _, seen := state.runtimeLogIDs[key]; seen {
			continue
		}
		state.runtimeLogIDs[key] = struct{}{}
		candidates = append(candidates, canonicalRuntimeLogCandidate(item, key))
	}
	if terminal := canonicalRunTerminalCandidate(snapshot); terminal.key != "" && terminal.key != state.terminalKey {
		candidates = append(candidates, terminal)
		state.terminalKey = terminal.key
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].at.Equal(candidates[j].at) {
			if candidates[i].order == candidates[j].order {
				return candidates[i].key < candidates[j].key
			}
			return candidates[i].order < candidates[j].order
		}
		return candidates[i].at.Before(candidates[j].at)
	})
	out := make([]RunEventEnvelope, 0, len(candidates))
	for _, item := range candidates {
		out = append(out, item.event)
	}
	return out
}

func canonicalRunStartedCandidate(snapshot runtimebus.RunLifecycleSnapshot) runDebugCandidate {
	runID := strings.TrimSpace(snapshot.RunID)
	if runID == "" || snapshot.StartedAt.IsZero() {
		return runDebugCandidate{}
	}
	startedAt := snapshot.StartedAt.UTC()
	key := "run.started:" + runID + ":" + startedAt.Format(time.RFC3339Nano)
	return runDebugCandidate{
		key:   key,
		at:    startedAt,
		order: 0,
		event: RunEventEnvelope{
			"id":        key,
			"type":      "run.started",
			"timestamp": startedAt.Format(time.RFC3339),
			"payload":   map[string]any{"run_id": runID},
		},
	}
}

func canonicalEventFiredCandidate(item store.OperatorEventFull, key string) runDebugCandidate {
	payload := map[string]any{
		"event_name": strings.TrimSpace(item.EventName),
	}
	if source := strings.TrimSpace(item.Source); source != "" {
		payload["source"] = source
	}
	if len(item.Payload) > 0 {
		payload["payload"] = cloneStringMap(item.Payload)
	}
	event := RunEventEnvelope{
		"id":        key,
		"type":      "event.fired",
		"timestamp": item.CreatedAt.UTC().Format(time.RFC3339),
		"payload":   payload,
	}
	if entityID := strings.TrimSpace(item.EntityID); entityID != "" {
		event["instance_id"] = entityID
	}
	return runDebugCandidate{
		key:   key,
		at:    item.CreatedAt.UTC(),
		order: 1,
		event: event,
	}
}

func canonicalRuntimeLogCandidate(item store.OperatorRuntimeLogEntry, key string) runDebugCandidate {
	payload := map[string]any{
		"level":     strings.TrimSpace(item.Level),
		"component": strings.TrimSpace(item.Component),
	}
	if action := stringMapValue(item.Details, "action"); action != "" {
		payload["action"] = action
	}
	if eventType := firstNonEmpty(stringMapValue(item.Details, "event_type"), stringMapValue(item.Details, "event_name")); eventType != "" {
		payload["event_type"] = eventType
	}
	if source := strings.TrimSpace(item.Source); source != "" {
		payload["agent_id"] = source
	}
	if item.Failure != nil {
		payload["failure"] = builderFailureValue(*item.Failure)
	}
	if message := strings.TrimSpace(item.Message); message != "" {
		payload["message"] = message
	}
	if detail := builderRuntimeLogDetail(item.Details); len(detail) > 0 {
		payload["detail"] = detail
	}
	event := RunEventEnvelope{
		"id":        firstNonEmpty(strings.TrimSpace(item.LogID), key),
		"type":      "runtime.log",
		"timestamp": item.TS.UTC().Format(time.RFC3339),
		"payload":   payload,
	}
	if entityID := strings.TrimSpace(item.EntityID); entityID != "" {
		event["instance_id"] = entityID
	}
	if source := strings.TrimSpace(item.Source); source != "" {
		event["node_id"] = source
	}
	return runDebugCandidate{
		key:   key,
		at:    item.TS.UTC(),
		order: 2,
		event: event,
	}
}

func canonicalRunTerminalCandidate(snapshot runtimebus.RunLifecycleSnapshot) runDebugCandidate {
	runID := strings.TrimSpace(snapshot.RunID)
	status := strings.TrimSpace(snapshot.Status)
	if runID == "" || snapshot.EndedAt == nil || snapshot.EndedAt.IsZero() {
		return runDebugCandidate{}
	}
	endedAt := snapshot.EndedAt.UTC()
	key := "run." + status + ":" + runID + ":" + endedAt.Format(time.RFC3339Nano)
	if status == "failed" {
		if snapshot.Failure == nil {
			return runDebugCandidate{}
		}
		fingerprint, err := runtimefailures.SemanticFingerprint(*snapshot.Failure)
		if err != nil {
			return runDebugCandidate{}
		}
		key += ":" + fingerprint
	}
	switch status {
	case "completed":
		payload := map[string]any{
			"run_id": runID,
			"summary": map[string]any{
				"duration_ms":  runDurationMillis(snapshot.StartedAt, endedAt),
				"total_events": snapshot.EventCount,
				"entity_count": snapshot.EntityCount,
			},
		}
		return runDebugCandidate{
			key:   key,
			at:    endedAt,
			order: 3,
			event: RunEventEnvelope{
				"id":        key,
				"type":      "run.completed",
				"timestamp": endedAt.Format(time.RFC3339),
				"payload":   payload,
			},
		}
	case "failed":
		payload := map[string]any{"run_id": runID, "failure": builderFailureValue(*snapshot.Failure)}
		return runDebugCandidate{
			key:   key,
			at:    endedAt,
			order: 3,
			event: RunEventEnvelope{
				"id":        key,
				"type":      "run.failed",
				"timestamp": endedAt.Format(time.RFC3339),
				"payload":   payload,
			},
		}
	default:
		return runDebugCandidate{}
	}
}

func builderRuntimeLogDetail(details map[string]any) map[string]any {
	if len(details) == 0 {
		return nil
	}
	out := cloneStringMap(details)
	delete(out, "error")
	delete(out, "error_code")
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringMapValue(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, ok := values[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.Trim(strings.TrimSpace(firstJSONScalar(value)), `"`)
	}
}

func firstJSONScalar(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}

func cloneStringMap(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func runDurationMillis(startedAt, endedAt time.Time) int64 {
	if startedAt.IsZero() {
		return 0
	}
	if endedAt.Before(startedAt) {
		return 0
	}
	return endedAt.Sub(startedAt).Milliseconds()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
