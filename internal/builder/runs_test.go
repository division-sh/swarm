package builder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	eventtypes "github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

type snapshotRunStore struct {
	runtimebus.InMemoryEventStore
	snapshot      runtimebus.RunLifecycleSnapshot
	events        []store.OperatorEventFull
	runtimeLogs   []store.OperatorRuntimeLogEntry
	appendErr     error
	terminalErr   error
	terminalCalls int
}

func (s *snapshotRunStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	if s.appendErr != nil {
		return runtimebus.PreparedPublish{}, s.appendErr
	}
	return s.InMemoryEventStore.CommitPublish(ctx, plan)
}

func (s *snapshotRunStore) MarkRunTerminal(_ context.Context, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	s.terminalCalls++
	if s.terminalErr != nil {
		return runtimebus.RunLifecycleSnapshot{}, s.terminalErr
	}
	s.snapshot.RunID = runID
	s.snapshot.Status = status
	s.snapshot.Failure = runtimefailures.CloneEnvelope(failure)
	ended := endedAt
	s.snapshot.EndedAt = &ended
	return s.snapshot, nil
}

func (s *snapshotRunStore) LoadRunLifecycleSnapshot(context.Context, string) (runtimebus.RunLifecycleSnapshot, error) {
	return s.snapshot, nil
}

func (s *snapshotRunStore) ListOperatorEvents(_ context.Context, opts store.OperatorEventListOptions) (store.OperatorEventListResult, error) {
	out := []store.OperatorEventFull{}
	for _, event := range s.events {
		if opts.Filter.RunID != "" && event.RunID != opts.Filter.RunID {
			continue
		}
		if opts.ExcludeRuntimeLogs && strings.TrimSpace(event.EventName) == "platform.runtime_log" {
			continue
		}
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			if opts.Order == "asc" {
				return out[i].EventID < out[j].EventID
			}
			return out[i].EventID > out[j].EventID
		}
		if opts.Order == "asc" {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	if opts.Limit > 0 && len(out) > opts.Limit {
		out = out[:opts.Limit]
	}
	return store.OperatorEventListResult{Events: out}, nil
}

func (s *snapshotRunStore) ListOperatorRuntimeLogs(_ context.Context, opts store.OperatorRuntimeLogListOptions) (store.OperatorRuntimeLogListResult, error) {
	out := []store.OperatorRuntimeLogEntry{}
	for _, entry := range s.runtimeLogs {
		if opts.RunID != "" && entry.RunID != opts.RunID {
			continue
		}
		out = append(out, entry)
	}
	return store.OperatorRuntimeLogListResult{Logs: out}, nil
}

type counterPause struct{ calls int }

func (c *counterPause) pause() error {
	c.calls++
	return nil
}

func TestRunHubStartRunPublishesTypedEntityEnvelope(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := newRunHub(func() *runtimepkg.Runtime { return rt }, nil, nil, nil)
	ch := eb.Subscribe("observer", "review.requested")

	if err := hub.startRun(context.Background(), "run-123", map[string]any{
		"review.requested": map[string]any{
			"entity_id": "ent-001",
			"name":      "Telemedicine",
		},
	}, nil); err != nil {
		t.Fatalf("startRun: %v", err)
	}

	select {
	case evt := <-ch:
		if got := evt.EntityID(); got != "ent-001" {
			t.Fatalf("event entity_id = %q, want ent-001", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected run input event to be published")
	}
}

func TestRunHubStartRunPublishFailureUsesCanonicalEnvelopeOnly(t *testing.T) {
	store := &snapshotRunStore{appendErr: errors.New("raw publish secret")}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	var observed RunEventEnvelope
	hub := newRunHub(func() *runtimepkg.Runtime { return rt }, nil, nil, nil)
	hub.sessions["run-123"] = &runSession{subs: map[string]func(RunEventEnvelope){"test": func(event RunEventEnvelope) { observed = cloneRunEvent(event) }}}

	if err := hub.startRun(context.Background(), "run-123", map[string]any{"review.requested": map[string]any{"entity_id": "ent-1"}}, nil); err == nil {
		t.Fatal("startRun succeeded, want planted publish failure")
	}
	payload, _ := observed["payload"].(map[string]any)
	failureValue, ok := payload["failure"].(map[string]any)
	if !ok || failureValue["class"] != string(runtimefailures.ClassInternalFailure) {
		t.Fatalf("publish failure payload = %#v", payload)
	}
	for _, retired := range []string{"error", "persistence_error"} {
		if _, exists := payload[retired]; exists {
			t.Fatalf("publish failure payload retained %s: %#v", retired, payload)
		}
	}
}

func TestRunHubAwaitCompletion_MarksSessionTerminalWhenCanonicalObservationIsUnavailable(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{
		sessions: map[string]*runSession{
			"run-123": {
				runID:         "run-123",
				runtime:       rt,
				subs:          map[string]func(RunEventEnvelope){},
				controlEvents: []RunEventEnvelope{},
			},
		},
	}

	hub.awaitCompletion("run-123")

	if !hub.isTerminal("run-123") {
		t.Fatal("expected run session to be marked terminal when canonical completion observation is unavailable")
	}
	session := hub.session("run-123")
	if session == nil {
		t.Fatal("expected run session to remain addressable")
	}
	if len(session.controlEvents) == 0 {
		t.Fatal("expected terminal failure event to be emitted")
	}
	last := session.controlEvents[len(session.controlEvents)-1]
	if got, _ := last["type"].(string); got != "run.failed" {
		t.Fatalf("last event type = %q, want run.failed", got)
	}
	payload, _ := last["payload"].(map[string]any)
	failureValue, ok := payload["failure"].(map[string]any)
	if !ok || failureValue["class"] != string(runtimefailures.ClassOutcomeUncertain) {
		t.Fatalf("last payload = %#v, want canonical outcome-uncertain failure", payload)
	}
	if _, ok := payload["persistence_error"]; ok {
		t.Fatalf("last payload = %#v, persistence_error must be retired", payload)
	}
}

func TestRunHubAwaitCompletionFailedTerminalPersistenceOmitsOriginalFailure(t *testing.T) {
	store := &snapshotRunStore{terminalErr: errors.New("raw persistence secret")}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{sessions: map[string]*runSession{"run-123": {
		runID:             "run-123",
		runtime:           rt,
		waitForQuiescence: func(context.Context) error { return errors.New("raw execution secret") },
		subs:              map[string]func(RunEventEnvelope){},
	}}}

	hub.awaitCompletion("run-123")
	session := hub.session("run-123")
	if session == nil || len(session.controlEvents) != 1 {
		t.Fatalf("control events = %#v", session)
	}
	payload, _ := session.controlEvents[0]["payload"].(map[string]any)
	failureValue, _ := payload["failure"].(map[string]any)
	detail, _ := failureValue["detail"].(map[string]any)
	attributes, _ := detail["attributes"].(map[string]any)
	if failureValue["class"] != string(runtimefailures.ClassOutcomeUncertain) || detail["code"] != "run_terminal_persistence_unconfirmed" || attributes["attempted_status"] != "failed" {
		t.Fatalf("terminal persistence failure = %#v", failureValue)
	}
	raw, _ := json.Marshal(payload)
	if strings.Contains(string(raw), "raw execution secret") || strings.Contains(string(raw), "raw persistence secret") {
		t.Fatalf("raw cause leaked: %s", raw)
	}
}

func TestRunHubAwaitCompletion_EmitsAuthoritativeRunSummary(t *testing.T) {
	endedAt := time.Now().UTC()
	store := &snapshotRunStore{
		snapshot: runtimebus.RunLifecycleSnapshot{
			RunID:       "run-123",
			Status:      "completed",
			EventCount:  3,
			EntityCount: 2,
			StartedAt:   time.Now().UTC().Add(-2 * time.Second),
			EndedAt:     &endedAt,
		},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	var observed []RunEventEnvelope
	hub := &runHub{
		runDebug: store,
		sessions: map[string]*runSession{
			"run-123": {
				runID:   "run-123",
				runtime: rt,
				subs: map[string]func(RunEventEnvelope){
					"test": func(event RunEventEnvelope) {
						observed = append(observed, cloneRunEvent(event))
					},
				},
				controlEvents: []RunEventEnvelope{},
				debug: runDebugStreamState{
					eventIDs:      map[string]struct{}{},
					runtimeLogIDs: map[string]struct{}{},
				},
			},
		},
	}

	hub.awaitCompletion("run-123")

	if len(observed) == 0 {
		t.Fatal("expected terminal event to be emitted")
	}
	last := observed[len(observed)-1]
	if got, _ := last["type"].(string); got != "run.completed" {
		t.Fatalf("last event type = %q, want run.completed", got)
	}
	payload, _ := last["payload"].(map[string]any)
	summary, _ := payload["summary"].(map[string]any)
	if got := int(summary["total_events"].(int)); got != 3 {
		t.Fatalf("summary.total_events = %d, want 3", got)
	}
	if got := int(summary["entity_count"].(int)); got != 2 {
		t.Fatalf("summary.entity_count = %d, want 2", got)
	}
	if got, ok := summary["duration_ms"].(int64); !ok || got <= 0 {
		t.Fatalf("summary.duration_ms = %#v, want positive int64", summary["duration_ms"])
	}
}

func TestRunHubAwaitCompletion_WaitingCanonicalRunDoesNotWriteCompleted(t *testing.T) {
	store := &snapshotRunStore{snapshot: runtimebus.RunLifecycleSnapshot{RunID: "run-123", Status: "running", StartedAt: time.Now().UTC()}}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{sessions: map[string]*runSession{"run-123": {
		runID:             "run-123",
		runtime:           rt,
		waitForQuiescence: func(context.Context) error { return nil },
		subs:              map[string]func(RunEventEnvelope){},
	}}}
	previousInterval := runCompletionObservationInterval
	runCompletionObservationInterval = 5 * time.Millisecond
	t.Cleanup(func() { runCompletionObservationInterval = previousInterval })

	done := make(chan struct{})
	go func() {
		hub.awaitCompletion("run-123")
		close(done)
	}()
	time.Sleep(25 * time.Millisecond)
	wasTerminal := hub.isTerminal("run-123")
	hub.markTerminal("run-123")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion observer did not stop after session terminalization")
	}
	if wasTerminal {
		t.Fatal("waiting canonical run was marked locally terminal")
	}
	if store.terminalCalls != 0 {
		t.Fatalf("MarkRunTerminal calls = %d, want zero", store.terminalCalls)
	}
	if events := hub.session("run-123").controlEvents; len(events) != 0 {
		t.Fatalf("waiting canonical run emitted synthetic control events: %#v", events)
	}
}

func TestRunHubHandleRuntimeLog_NoEntityDoesNotTriggerControlHooksAcrossRuns(t *testing.T) {
	pause := &counterPause{}
	hub := &runHub{
		pauseRuntime: pause.pause,
		sessions: map[string]*runSession{
			"run-a": {
				runID:       "run-a",
				breakpoints: map[string]struct{}{"node-1": {}},
				subs:        map[string]func(RunEventEnvelope){},
				debug: runDebugStreamState{
					eventIDs:      map[string]struct{}{},
					runtimeLogIDs: map[string]struct{}{},
				},
			},
			"run-b": {
				runID:       "run-b",
				breakpoints: map[string]struct{}{"node-1": {}},
				subs:        map[string]func(RunEventEnvelope){},
				debug: runDebugStreamState{
					eventIDs:      map[string]struct{}{},
					runtimeLogIDs: map[string]struct{}{},
				},
			},
		},
	}

	hub.handleRuntimeLog(runtimepkg.RuntimeLogEntry{
		Component: "scheduler",
		Action:    "checkpoint",
		AgentID:   "node-1",
	})

	if pause.calls != 0 {
		t.Fatalf("pause calls = %d, want 0", pause.calls)
	}
	for runID, session := range hub.sessions {
		if _, ok := session.trippedBreakpoints["node-1"]; ok {
			t.Fatalf("%s unexpectedly tripped breakpoint on no-entity log", runID)
		}
	}
}

func TestRunHubSubscribe_PrimesCanonicalReplayDedupeState(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	debugStore := &snapshotRunStore{
		snapshot: runtimebus.RunLifecycleSnapshot{
			RunID:     "run-123",
			StartedAt: now,
		},
		events: []store.OperatorEventFull{{
			EventID:       "evt-1",
			EventName:     "scan.requested",
			ExecutionMode: "live",
			RunID:         "run-123",
			EntityID:      "entity-1",
			CreatedAt:     now.Add(1 * time.Second),
			Source:        "builder",
			ProducerType:  eventtypes.EventProducerExternal,
			Payload:       map[string]any{"topic": "sample"},
		}},
	}
	hub := &runHub{
		runDebug: debugStore,
		sessions: map[string]*runSession{},
	}
	var seen []RunEventEnvelope
	cancel := hub.subscribe("run-123", func(event RunEventEnvelope) {
		seen = append(seen, cloneRunEvent(event))
	})
	defer cancel()

	if len(seen) != 2 {
		t.Fatalf("initial replay len = %d, want 2", len(seen))
	}

	hub.syncCanonical(context.Background(), "run-123")

	if len(seen) != 2 {
		t.Fatalf("post-sync len = %d, want 2", len(seen))
	}
}

func TestRunHubSyncCanonical_UsesLatestCanonicalEventWindow(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	events := make([]store.OperatorEventFull, 0, builderRunDebugReplayLimit+2)
	for i := 0; i < builderRunDebugReplayLimit+2; i++ {
		events = append(events, store.OperatorEventFull{
			EventID:       fmt.Sprintf("evt-%03d", i),
			EventName:     "scan.requested",
			ExecutionMode: "live",
			RunID:         "run-123",
			EntityID:      "entity-1",
			CreatedAt:     now.Add(time.Duration(i) * time.Second),
			Source:        "builder",
			ProducerType:  eventtypes.EventProducerExternal,
			Payload:       map[string]any{"index": i},
		})
	}
	events[len(events)-1].EventID = "evt-latest"
	debugStore := &snapshotRunStore{
		snapshot: runtimebus.RunLifecycleSnapshot{RunID: "run-123", StartedAt: now},
		events:   events,
	}
	hub := &runHub{
		runDebug: debugStore,
		sessions: map[string]*runSession{
			"run-123": {
				runID:         "run-123",
				subs:          map[string]func(RunEventEnvelope){},
				controlEvents: []RunEventEnvelope{},
				debug: runDebugStreamState{
					eventIDs:      map[string]struct{}{},
					runtimeLogIDs: map[string]struct{}{},
				},
			},
		},
	}
	var seen []RunEventEnvelope
	hub.sessions["run-123"].subs["test"] = func(event RunEventEnvelope) {
		seen = append(seen, cloneRunEvent(event))
	}

	hub.syncCanonical(context.Background(), "run-123")

	gotLatest := false
	for _, event := range seen {
		if event["id"] == "evt-latest" {
			gotLatest = true
			break
		}
	}
	if !gotLatest {
		t.Fatalf("latest event was not emitted from bounded canonical event window; got %#v", seen)
	}
}
