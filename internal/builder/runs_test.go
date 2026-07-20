package builder

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	eventtypes "github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

type snapshotRunStore struct {
	runtimebus.InMemoryEventStore
	snapshot      runtimebus.RunLifecycleSnapshot
	events        []store.OperatorEventFull
	runtimeLogs   []store.OperatorRuntimeLogEntry
	appendErr     error
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

type builderCommitCaptureStore struct {
	runtimebus.InMemoryEventStore
	requests []runtimebus.CommitPublishRequest
}

func (s *builderCommitCaptureStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, nil, func(_ context.Context, request runtimebus.CommitPublishRequest) error {
		s.requests = append(s.requests, request)
		return nil
	})
}

func TestRunHubStartRunPublishesTypedEntityEnvelope(t *testing.T) {
	runID := eventtest.UUID("builder-run-hub-typed-envelope-run")
	entityIDs := map[eventtypes.EventType]string{
		"analysis.requested": eventtest.UUID("builder-run-hub-analysis-entity"),
		"review.requested":   eventtest.UUID("builder-run-hub-review-entity"),
	}
	commitStore := &builderCommitCaptureStore{}
	rt, acquirer := newTestOwnedEventBus(t, commitStore, runtimebus.EventBusOptions{})
	eb := rt.Bus
	hub := newRunHub(acquirer, nil, nil, nil)
	analysisEvents := eb.Subscribe("analysis-observer", "analysis.requested")
	reviewEvents := eb.Subscribe("review-observer", "review.requested")

	if err := hub.startRun(context.Background(), runID, map[string]any{
		"analysis.requested": map[string]any{
			"entity_id": entityIDs["analysis.requested"],
			"topic":     "Feasibility",
		},
		"review.requested": map[string]any{
			"entity_id": entityIDs["review.requested"],
			"name":      "Telemedicine",
		},
	}, nil); err != nil {
		t.Fatalf("startRun: %v", err)
	}

	if got := len(commitStore.requests); got != 2 {
		t.Fatalf("sealed commit requests = %d, want 2", got)
	}
	committedEventIDs := map[string]struct{}{}
	committedRunIDs := map[string]struct{}{}
	for _, request := range commitStore.requests {
		admitted := request.Event
		event := admitted.Event()
		if admitted.RunDisposition() != eventtypes.AdmittedRunCreateAuthorized {
			t.Fatalf("%s run disposition = %q, want create_authorized", event.Type(), admitted.RunDisposition())
		}
		if event.AdmissionClass() != eventtypes.EventAdmissionRootIngress {
			t.Fatalf("%s class = %q, want root_ingress", event.Type(), event.AdmissionClass())
		}
		if event.RunID() != runID {
			t.Fatalf("%s run_id = %q, want caller run %q", event.Type(), event.RunID(), runID)
		}
		wantEntityID, ok := entityIDs[event.Type()]
		if !ok {
			t.Fatalf("unexpected committed root type %q", event.Type())
		}
		if event.EntityID() != wantEntityID {
			t.Fatalf("%s entity_id = %q, want %q", event.Type(), event.EntityID(), wantEntityID)
		}
		committedEventIDs[event.ID()] = struct{}{}
		committedRunIDs[event.RunID()] = struct{}{}
	}
	if len(committedEventIDs) != 2 {
		t.Fatalf("unique committed root event identities = %d, want 2", len(committedEventIDs))
	}
	if len(committedRunIDs) != 1 {
		t.Fatalf("unique committed run identities = %d, want 1", len(committedRunIDs))
	}

	for eventType, channel := range map[eventtypes.EventType]<-chan *runtimebus.LocalDelivery{
		"analysis.requested": analysisEvents,
		"review.requested":   reviewEvents,
	} {
		select {
		case delivery := <-channel:
			event := delivery.Event()
			_ = delivery.Complete()
			if got := event.EntityID(); got != entityIDs[eventType] {
				t.Fatalf("%s dispatched entity_id = %q, want %q", eventType, got, entityIDs[eventType])
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("expected %s run input event to be published", eventType)
		}
	}
}

func TestRunHubStartRunPublishFailureUsesCanonicalEnvelopeOnly(t *testing.T) {
	store := &snapshotRunStore{appendErr: errors.New("raw publish secret")}
	_, acquirer := newTestOwnedEventBus(t, store, runtimebus.EventBusOptions{})
	var observed RunEventEnvelope
	hub := newRunHub(acquirer, nil, nil, nil)
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

func TestBuilderRunCompletionUsesAcquiredGeneration(t *testing.T) {
	store := &snapshotRunStore{snapshot: runtimebus.RunLifecycleSnapshot{
		RunID: "run-owned", Status: "completed", StartedAt: time.Now().UTC(),
	}}
	rt, acquirer := newTestOwnedEventBus(t, store, runtimebus.EventBusOptions{})
	unrelated, err := acquirer.owner.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin persistent runtime work: %v", err)
	}
	use, err := acquirer.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire completion generation: %v", err)
	}
	hub := &runHub{runDebug: store, sessions: map[string]*runSession{"run-owned": {
		runID: "run-owned", runtime: rt, subs: map[string]func(RunEventEnvelope){},
		debug: runDebugStreamState{eventIDs: map[string]struct{}{}, runtimeLogIDs: map[string]struct{}{}},
	}}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = use.Done() }()
		hub.awaitCompletion(use.WorkContext(), "run-owned", rt)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canonical completion waited on its own or unrelated runtime work")
	}
	if !hub.isTerminal("run-owned") {
		t.Fatal("canonical terminal lifecycle was not observed")
	}
	if got := acquirer.owner.ActiveCount(); got != 1 {
		t.Fatalf("active runtime work after completion = %d, want only persistent lease", got)
	}
	if err := unrelated.Done(); err != nil {
		t.Fatalf("settle persistent runtime work: %v", err)
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

	hub.awaitCompletion(context.Background(), "run-123", rt)

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

	hub.awaitCompletion(context.Background(), "run-123", rt)

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
		runID:   "run-123",
		runtime: rt,
		subs:    map[string]func(RunEventEnvelope){},
	}}}
	previousInterval := runCompletionObservationInterval
	runCompletionObservationInterval = 5 * time.Millisecond
	t.Cleanup(func() { runCompletionObservationInterval = previousInterval })

	done := make(chan struct{})
	go func() {
		hub.awaitCompletion(context.Background(), "run-123", rt)
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
