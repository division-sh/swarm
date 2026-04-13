package builder

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"swarm/internal/store"
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

func (h *runHub) canonicalReplay(ctx context.Context, runID string) []RunEventEnvelope {
	report, ok := h.loadRunDebugReport(ctx, runID)
	if !ok {
		return nil
	}
	return projectCanonicalRunDebugReplay(report)
}

func (h *runHub) syncCanonical(ctx context.Context, runID string) {
	report, ok := h.loadRunDebugReport(ctx, runID)
	if !ok {
		return
	}
	h.mu.Lock()
	session := h.sessions[strings.TrimSpace(runID)]
	if session == nil {
		h.mu.Unlock()
		return
	}
	delta := projectCanonicalRunDebugDelta(report, &session.debug)
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

func (h *runHub) loadRunDebugReport(ctx context.Context, runID string) (store.RunDebugReport, bool) {
	if h == nil || h.runDebug == nil {
		return store.RunDebugReport{}, false
	}
	report, err := h.runDebug.LoadRunDebugReport(ctx, strings.TrimSpace(runID), store.RunDebugQueryOptions{
		LogsAllLevels:   true,
		EventLimit:      builderRunDebugReplayLimit,
		MutationLimit:   1,
		RuntimeLogLimit: builderRunDebugReplayLimit,
		DeadLetterLimit: 1,
	})
	if err != nil {
		return store.RunDebugReport{}, false
	}
	return report, true
}

func projectCanonicalRunDebugReplay(report store.RunDebugReport) []RunEventEnvelope {
	state := runDebugStreamState{
		eventIDs:      map[string]struct{}{},
		runtimeLogIDs: map[string]struct{}{},
	}
	return projectCanonicalRunDebugDelta(report, &state)
}

func projectCanonicalRunDebugDelta(report store.RunDebugReport, state *runDebugStreamState) []RunEventEnvelope {
	if state == nil {
		state = &runDebugStreamState{eventIDs: map[string]struct{}{}, runtimeLogIDs: map[string]struct{}{}}
	}
	if state.eventIDs == nil {
		state.eventIDs = map[string]struct{}{}
	}
	if state.runtimeLogIDs == nil {
		state.runtimeLogIDs = map[string]struct{}{}
	}
	candidates := make([]runDebugCandidate, 0, 2+len(report.Events)+len(report.RuntimeLogs))
	if started := canonicalRunStartedCandidate(report); started.key != "" && started.key != state.startedKey {
		candidates = append(candidates, started)
		state.startedKey = started.key
	}
	for i := len(report.Events) - 1; i >= 0; i-- {
		item := report.Events[i]
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
		candidates = append(candidates, canonicalEventFiredCandidate(item))
	}
	for i := len(report.RuntimeLogs) - 1; i >= 0; i-- {
		item := report.RuntimeLogs[i]
		key := strings.TrimSpace(item.EventID)
		if key == "" {
			key = item.CreatedAt.UTC().Format(time.RFC3339Nano) + ":" + item.Component + ":" + item.Action
		}
		if _, seen := state.runtimeLogIDs[key]; seen {
			continue
		}
		state.runtimeLogIDs[key] = struct{}{}
		candidates = append(candidates, canonicalRuntimeLogCandidate(item, key))
	}
	if terminal := canonicalRunTerminalCandidate(report); terminal.key != "" && terminal.key != state.terminalKey {
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

func canonicalRunStartedCandidate(report store.RunDebugReport) runDebugCandidate {
	if strings.TrimSpace(report.RunID) == "" || report.StartedAt.IsZero() {
		return runDebugCandidate{}
	}
	key := "run.started:" + strings.TrimSpace(report.RunID) + ":" + report.StartedAt.UTC().Format(time.RFC3339Nano)
	return runDebugCandidate{
		key:   key,
		at:    report.StartedAt.UTC(),
		order: 0,
		event: RunEventEnvelope{
			"id":        key,
			"type":      "run.started",
			"timestamp": report.StartedAt.UTC().Format(time.RFC3339),
			"payload":   map[string]any{"run_id": strings.TrimSpace(report.RunID)},
		},
	}
}

func canonicalEventFiredCandidate(item store.RunDebugEvent) runDebugCandidate {
	payload := map[string]any{
		"event_name": strings.TrimSpace(item.EventName),
	}
	if source := firstNonEmpty(strings.TrimSpace(item.Source), strings.TrimSpace(item.SourceType)); source != "" {
		payload["source"] = source
	}
	if raw := strings.TrimSpace(string(item.Payload)); raw != "" && raw != "{}" {
		var decoded map[string]any
		if json.Unmarshal(item.Payload, &decoded) == nil && decoded != nil {
			payload["payload"] = decoded
		}
	}
	event := RunEventEnvelope{
		"id":        strings.TrimSpace(item.EventID),
		"type":      "event.fired",
		"timestamp": item.CreatedAt.UTC().Format(time.RFC3339),
		"payload":   payload,
	}
	if entityID := strings.TrimSpace(item.EntityID); entityID != "" {
		event["instance_id"] = entityID
	}
	return runDebugCandidate{
		key:   strings.TrimSpace(item.EventID),
		at:    item.CreatedAt.UTC(),
		order: 1,
		event: event,
	}
}

func canonicalRuntimeLogCandidate(item store.RunDebugRuntimeLog, key string) runDebugCandidate {
	payload := map[string]any{
		"level":     strings.TrimSpace(item.Level),
		"component": strings.TrimSpace(item.Component),
		"action":    strings.TrimSpace(item.Action),
	}
	if eventType := strings.TrimSpace(item.EventType); eventType != "" {
		payload["event_type"] = eventType
	}
	if agentID := strings.TrimSpace(item.AgentID); agentID != "" {
		payload["agent_id"] = agentID
	}
	if errText := strings.TrimSpace(item.Error); errText != "" {
		payload["error"] = errText
	}
	if raw := strings.TrimSpace(string(item.Detail)); raw != "" && raw != "{}" {
		var decoded map[string]any
		if json.Unmarshal(item.Detail, &decoded) == nil && decoded != nil {
			payload["detail"] = decoded
		}
	}
	event := RunEventEnvelope{
		"id":        firstNonEmpty(strings.TrimSpace(item.EventID), key),
		"type":      "runtime.log",
		"timestamp": item.CreatedAt.UTC().Format(time.RFC3339),
		"payload":   payload,
	}
	if entityID := strings.TrimSpace(item.EntityID); entityID != "" {
		event["instance_id"] = entityID
	}
	if agentID := strings.TrimSpace(item.AgentID); agentID != "" {
		event["node_id"] = agentID
	}
	return runDebugCandidate{
		key:   key,
		at:    item.CreatedAt.UTC(),
		order: 2,
		event: event,
	}
}

func canonicalRunTerminalCandidate(report store.RunDebugReport) runDebugCandidate {
	runID := strings.TrimSpace(report.RunID)
	status := strings.TrimSpace(report.RunTableStatus)
	if runID == "" || report.EndedAt == nil || report.EndedAt.IsZero() {
		return runDebugCandidate{}
	}
	endedAt := report.EndedAt.UTC()
	key := "run." + status + ":" + runID + ":" + endedAt.Format(time.RFC3339Nano) + ":" + strings.TrimSpace(report.ErrorSummary)
	switch status {
	case "completed":
		payload := map[string]any{
			"run_id": runID,
			"summary": map[string]any{
				"duration_ms":  runDurationMillis(report.StartedAt, endedAt),
				"total_events": report.EventCount,
				"entity_count": report.EntityCount,
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
		payload := map[string]any{"run_id": runID}
		if errText := strings.TrimSpace(report.ErrorSummary); errText != "" {
			payload["error"] = errText
		}
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
