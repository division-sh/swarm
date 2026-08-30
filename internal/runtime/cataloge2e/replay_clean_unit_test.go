package cataloge2e

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil/replayconformance"
)

func TestCatalogReplayTranscriptIdentityAdmissionFailsClosed(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.primitives", "test-advances-to")
	transcript := buildCatalogExecutionTranscript(t, fixture)
	bundle := loadFixtureBundle(t, fixture.Root)
	repo := repoRootFromCatalogE2E(t)
	if err := validateCatalogTranscriptIdentity(repo, bundle, transcript); err != nil {
		t.Fatalf("valid transcript rejected: %v", err)
	}

	cases := map[string]func(*catalogExecutionTranscript){
		"unrevisioned":        func(got *catalogExecutionTranscript) { got.version = "" },
		"unknown revision":    func(got *catalogExecutionTranscript) { got.version = "catalog-replay-transcript/v99" },
		"stale platform spec": func(got *catalogExecutionTranscript) { got.platformSpecDigest = "sha256:" + strings.Repeat("0", 64) },
		"wrong bundle":        func(got *catalogExecutionTranscript) { got.bundleHash = "bundle-v2:sha256:" + strings.Repeat("0", 64) },
		"wrong run":           func(got *catalogExecutionTranscript) { got.runID = eventtest.UUID("wrong-run") },
		"unknown input kind":  func(got *catalogExecutionTranscript) { got.groups[0].steps[0].inputKind = "database_snapshot" },
		"partial input":       func(got *catalogExecutionTranscript) { got.groups[0].steps[0].eventID = "" },
		"unknown barrier":     func(got *catalogExecutionTranscript) { got.groups[0].barrierBefore.Kind = "callback" },
		"partial event barrier": func(got *catalogExecutionTranscript) {
			got.groups[0].barrierBefore = catalogTranscriptBarrier{Kind: catalogBarrierAutomaticEventCount, Event: "timer.tick"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := cloneCatalogTranscriptForUnit(transcript)
			mutate(candidate)
			if err := validateCatalogTranscriptIdentity(repo, bundle, candidate); err == nil {
				t.Fatal("invalid transcript was admitted")
			}
		})
	}
	if err := validateCatalogTranscriptIdentity(repo, bundle, nil); err == nil {
		t.Fatal("nil transcript was admitted")
	}
}

func TestCatalogReplayProjectionPreservesExactInputAndSemanticFacts(t *testing.T) {
	createdAt := time.Date(2026, 8, 9, 12, 0, 0, 123000, time.UTC)
	rootID := eventtest.UUID("catalog-replay-root")
	entityID := eventtest.UUID("catalog-replay-entity")
	baseEnvelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
	base := eventtest.ExistingRunRootIngress(rootID, events.EventType("input.received"), "cataloge2e", "task-1", json.RawMessage(`{"value":1}`), 0, catalogRuntimeRunID, baseEnvelope, createdAt)
	transcript := catalogReplayUnitTranscript(t, base)
	want := catalogReplayUnitProjection(t, transcript, replayOperatorEvent(t, base))

	targetEnvelope := events.EnvelopeForTargetRoute(baseEnvelope, events.RouteIdentity{FlowInstance: "worker/one", EntityID: entityID})
	externalSource, err := events.NewExternalIngressRoutingSource("root", entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]events.Event{
		"timestamp":      createdRootEvent(rootID, "input.received", "cataloge2e", "task-1", `{"value":1}`, catalogRuntimeRunID, baseEnvelope, createdAt.Add(time.Second)),
		"payload":        createdRootEvent(rootID, "input.received", "cataloge2e", "task-1", `{"value":2}`, catalogRuntimeRunID, baseEnvelope, createdAt),
		"task":           createdRootEvent(rootID, "input.received", "cataloge2e", "task-2", `{"value":1}`, catalogRuntimeRunID, baseEnvelope, createdAt),
		"producer":       createdRootEvent(rootID, "input.received", "other-producer", "task-1", `{"value":1}`, catalogRuntimeRunID, baseEnvelope, createdAt),
		"run":            createdRootEvent(rootID, "input.received", "cataloge2e", "task-1", `{"value":1}`, eventtest.UUID("other-run"), baseEnvelope, createdAt),
		"route":          createdRootEvent(rootID, "input.received", "cataloge2e", "task-1", `{"value":1}`, catalogRuntimeRunID, targetEnvelope, createdAt),
		"routing source": eventtest.ExistingRunRootIngressWithRoutingSource(rootID, events.EventType("input.received"), "cataloge2e", "task-1", json.RawMessage(`{"value":1}`), 0, catalogRuntimeRunID, baseEnvelope, externalSource, createdAt),
		"class":          eventtest.OperatorInjected(rootID, events.EventType("input.received"), "cataloge2e", "task-1", json.RawMessage(`{"value":1}`), 0, catalogRuntimeRunID, nil, baseEnvelope, createdAt),
	}
	for name, candidate := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{rootID: replayOperatorEvent(t, candidate)}, transcript)
			if err == nil && bytes.Equal(got, want) {
				t.Fatal("semantic divergence was normalized")
			}
		})
	}
}

func TestCatalogReplayProjectionNormalizesOnlyGeneratedIdentityAndLifecycleAllocations(t *testing.T) {
	createdAt := time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC)
	root := createdRootEvent(eventtest.UUID("normalization-root"), "input.received", "cataloge2e", "", `{"value":1}`, catalogRuntimeRunID, events.EventEnvelope{}, createdAt)
	transcript := catalogReplayUnitTranscript(t, root)
	childA := eventtest.Child(eventtest.UUID("generated-a"), events.EventType("output.ready"), "worker", "task", json.RawMessage(`{"result":"ok"}`), 1, root, events.EventEnvelope{}, createdAt.Add(time.Second))
	childB := eventtest.Child(eventtest.UUID("generated-b"), events.EventType("output.ready"), "worker", "task", json.RawMessage(`{"result":"ok"}`), 1, root, events.EventEnvelope{}, createdAt.Add(10*time.Second))

	projectionA := catalogReplayUnitProjection(t, transcript, replayOperatorEvent(t, root), replayOperatorEvent(t, childA))
	projectionB := catalogReplayUnitProjection(t, transcript, replayOperatorEvent(t, root), replayOperatorEvent(t, childB))
	if !bytes.Equal(projectionA, projectionB) {
		t.Fatalf("generated identity/time were not normalized\nA: %s\nB: %s", projectionA, projectionB)
	}

	fullA := replayOperatorEvent(t, childA)
	fullB := replayOperatorEvent(t, childB)
	deliveryA := replayProjectionDelivery(t, "delivery-a")
	deliveryB := deliveryA
	deliveryB.DeliveryID = "delivery-b"
	deliveryA.SessionID = "session-a"
	deliveryB.SessionID = "session-b"
	first := createdAt.Add(2 * time.Second)
	second := createdAt.Add(20 * time.Second)
	deliveryA.CreatedAt = &first
	deliveryB.CreatedAt = &second
	fullA.Deliveries = []operatorread.OperatorEventDelivery{deliveryA}
	fullB.Deliveries = []operatorread.OperatorEventDelivery{deliveryB}
	projectionA = catalogReplayUnitProjection(t, transcript, replayOperatorEvent(t, root), fullA)
	projectionB = catalogReplayUnitProjection(t, transcript, replayOperatorEvent(t, root), fullB)
	if !bytes.Equal(projectionA, projectionB) {
		t.Fatalf("named delivery lifecycle allocations were compared\nA: %s\nB: %s", projectionA, projectionB)
	}
}

func TestCatalogReplayProjectionRejectsSemanticDeliveryAndDeadLetterDivergence(t *testing.T) {
	createdAt := time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC)
	root := createdRootEvent(eventtest.UUID("delivery-root"), "input.received", "cataloge2e", "", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, createdAt)
	transcript := catalogReplayUnitTranscript(t, root)
	base := replayOperatorEvent(t, root)
	base.Deliveries = []operatorread.OperatorEventDelivery{replayProjectionDelivery(t, "delivery-1")}
	base.DeadLetters = []operatorread.OperatorDeadLetterRecord{replayProjectionDeadLetter("dead-letter-1", "delivery-1", createdAt)}
	want := catalogReplayUnitProjection(t, transcript, base)

	cases := map[string]func(*operatorread.OperatorEventFull){
		"subscriber": func(got *operatorread.OperatorEventFull) { got.Deliveries[0].SubscriberID = "other-node" },
		"route": func(got *operatorread.OperatorEventFull) {
			target := got.Deliveries[0].Route.Target.Route()
			target.FlowInstance = "other-flow"
			got.Deliveries[0].Route.Target = events.MustEntitylessReceiverTarget(target)
		},
		"status": func(got *operatorread.OperatorEventFull) { got.Deliveries[0].Status = "failed" },
		"failure": func(got *operatorread.OperatorEventFull) {
			failure := replayFailure(runtimefailures.ClassInternalFailure, "other_failure")
			got.Deliveries[0].Failure = &failure
		},
		"retry count":     func(got *operatorread.OperatorEventFull) { got.Deliveries[0].RetryCount++ },
		"retry scheduled": func(got *operatorread.OperatorEventFull) { got.Deliveries[0].RetryScheduled = true },
		"terminal":        func(got *operatorread.OperatorEventFull) { got.Deliveries[0].Terminal = false },
		"dead-letter failure": func(got *operatorread.OperatorEventFull) {
			got.DeadLetters[0].Failure = replayFailure(runtimefailures.ClassInternalFailure, "other_failure")
		},
		"dead-letter retry":   func(got *operatorread.OperatorEventFull) { got.DeadLetters[0].RetryCount++ },
		"dead-letter chain":   func(got *operatorread.OperatorEventFull) { got.DeadLetters[0].ChainDepth++ },
		"dead-letter handler": func(got *operatorread.OperatorEventFull) { got.DeadLetters[0].HandlerNode = "other-handler" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.Deliveries = append([]operatorread.OperatorEventDelivery(nil), base.Deliveries...)
			candidate.DeadLetters = append([]operatorread.OperatorDeadLetterRecord(nil), base.DeadLetters...)
			mutate(&candidate)
			got, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{root.ID(): candidate}, transcript)
			if err == nil && bytes.Equal(got, want) {
				t.Fatal("delivery/dead-letter semantic divergence was normalized")
			}
		})
	}

	allocated := base
	allocated.Deliveries = append([]operatorread.OperatorEventDelivery(nil), base.Deliveries...)
	allocated.DeadLetters = append([]operatorread.OperatorDeadLetterRecord(nil), base.DeadLetters...)
	allocated.DeadLetters[0].DeadLetterID = "other-dead-letter"
	allocated.DeadLetters[0].ClaimVersion = 99
	allocated.DeadLetters[0].CreatedAt = createdAt.Add(time.Hour)
	if got := catalogReplayUnitProjection(t, transcript, allocated); !bytes.Equal(got, want) {
		t.Fatalf("dead-letter lifecycle allocations were compared\nwant: %s\ngot: %s", want, got)
	}

	malformed := base
	malformed.Deliveries = append([]operatorread.OperatorEventDelivery(nil), base.Deliveries...)
	malformed.DeadLetters = append([]operatorread.OperatorDeadLetterRecord(nil), base.DeadLetters...)
	malformed.DeadLetters[0].DeliveryID = "missing-delivery"
	if _, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{root.ID(): malformed}, transcript); err == nil {
		t.Fatal("malformed dead-letter linkage was admitted")
	}
}

func TestCatalogReplayProjectionRejectsMissingUnknownCyclicAndAmbiguousLineage(t *testing.T) {
	createdAt := time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC)
	rootID := eventtest.UUID("lineage-root")
	root := createdRootEvent(rootID, "input.received", "cataloge2e", "", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, createdAt)
	transcript := catalogReplayUnitTranscript(t, root)
	if _, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{}, transcript); err == nil {
		t.Fatal("missing accepted root page was admitted")
	}

	unknown := eventtest.PersistedProjection(rootID, events.EventType("input.received"), "cataloge2e", "", json.RawMessage(`{}`), 0, catalogRuntimeRunID, eventtest.UUID("unknown-parent"), events.EventEnvelope{}, createdAt)
	if _, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{rootID: replayOperatorEvent(t, unknown)}, transcript); err == nil {
		t.Fatal("unknown causal parent was admitted")
	}

	otherID := eventtest.UUID("cycle-other")
	cycleRoot := eventtest.PersistedProjection(rootID, events.EventType("input.received"), "cataloge2e", "", json.RawMessage(`{}`), 0, catalogRuntimeRunID, otherID, events.EventEnvelope{}, createdAt)
	cycleOther := eventtest.PersistedProjection(otherID, events.EventType("cycle.other"), "worker", "", json.RawMessage(`{}`), 1, catalogRuntimeRunID, rootID, events.EventEnvelope{}, createdAt.Add(time.Second))
	if _, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{rootID: replayOperatorEvent(t, cycleRoot), otherID: replayOperatorEvent(t, cycleOther)}, transcript); err == nil {
		t.Fatal("causal cycle was admitted")
	}

	childA := eventtest.Child(eventtest.UUID("ambiguous-a"), events.EventType("output.ready"), "worker", "", json.RawMessage(`{"same":true}`), 1, root, events.EventEnvelope{}, createdAt.Add(time.Second))
	childB := eventtest.Child(eventtest.UUID("ambiguous-b"), events.EventType("output.ready"), "worker", "", json.RawMessage(`{"same":true}`), 1, root, events.EventEnvelope{}, createdAt.Add(2*time.Second))
	fullA := replayOperatorEvent(t, childA)
	fullB := replayOperatorEvent(t, childB)
	fullA.Deliveries = []operatorread.OperatorEventDelivery{replayProjectionDelivery(t, "delivery-a")}
	if _, err := projectCatalogReplayUnit(map[string]operatorread.OperatorEventFull{rootID: replayOperatorEvent(t, root), childA.ID(): fullA, childB.ID(): fullB}, transcript); err == nil {
		t.Fatal("ambiguous non-isomorphic generated siblings were admitted")
	}
}

func TestCatalogReplayPaginationFailsClosedAndExcludesOnlyRuntimeLogs(t *testing.T) {
	lister := &catalogReplayPageLister{pages: map[string]operatorread.OperatorEventListResult{"": {}}}
	if _, err := loadCatalogOperatorEvents(context.Background(), lister); err != nil {
		t.Fatal(err)
	}
	if len(lister.options) != 1 || !lister.options[0].ExcludeRuntimeLogs || lister.options[0].Filter.RunID != catalogRuntimeRunID {
		t.Fatalf("operator projection options = %#v, want exact run and runtime-log exclusion", lister.options)
	}

	cycle := &catalogReplayPageLister{pages: map[string]operatorread.OperatorEventListResult{
		"":      {NextCursor: "again"},
		"again": {NextCursor: "again"},
	}}
	if _, err := loadCatalogOperatorEvents(context.Background(), cycle); err == nil {
		t.Fatal("cursor cycle was admitted")
	}

	createdAt := time.Date(2026, 8, 9, 16, 0, 0, 0, time.UTC)
	event := createdRootEvent(eventtest.UUID("page-event"), "input.received", "cataloge2e", "", `{}`, catalogRuntimeRunID, events.EventEnvelope{}, createdAt)
	full := replayOperatorEvent(t, event)
	mutating := &catalogReplayPageLister{pages: map[string]operatorread.OperatorEventListResult{
		"":     {Events: []operatorread.OperatorEventFull{full}, NextCursor: "next"},
		"next": {Events: []operatorread.OperatorEventFull{full}},
	}}
	if _, err := loadCatalogOperatorEvents(context.Background(), mutating); err == nil {
		t.Fatal("repeated/mutating page event was admitted")
	}
	if _, err := loadCatalogOperatorEvents(context.Background(), nil); err == nil {
		t.Fatal("missing selected-store read owner was admitted")
	}
}

type catalogReplayPageLister struct {
	pages   map[string]operatorread.OperatorEventListResult
	options []operatorread.OperatorEventListOptions
}

func (l *catalogReplayPageLister) ListOperatorEvents(_ context.Context, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	l.options = append(l.options, opts)
	return l.pages[opts.Cursor], nil
}

func cloneCatalogTranscriptForUnit(in *catalogExecutionTranscript) *catalogExecutionTranscript {
	out := *in
	out.groups = make([]catalogTranscriptGroup, len(in.groups))
	for index, group := range in.groups {
		out.groups[index] = group
		out.groups[index].steps = append([]catalogTriggerStep(nil), group.steps...)
	}
	return &out
}

func catalogReplayUnitTranscript(t testing.TB, root events.Event) *catalogExecutionTranscript {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(root.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	return &catalogExecutionTranscript{
		version: catalogReplayTranscriptVersion,
		runID:   root.RunID(),
		groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{{
			Event: string(root.Type()), Payload: payload, inputKind: catalogReplayInputRootIngress,
			eventID: root.ID(), createdAt: root.CreatedAt(), sourceAgent: root.SourceAgent(),
		}}}},
	}
}

func catalogReplayUnitProjection(t testing.TB, transcript *catalogExecutionTranscript, events ...operatorread.OperatorEventFull) []byte {
	t.Helper()
	byID := make(map[string]operatorread.OperatorEventFull, len(events))
	for _, event := range events {
		byID[event.EventID] = event
	}
	projection, err := projectCatalogReplayUnit(byID, transcript)
	if err != nil {
		t.Fatalf("project replay fixture: %v", err)
	}
	return projection
}

func projectCatalogReplayUnit(eventsByID map[string]operatorread.OperatorEventFull, transcript *catalogExecutionTranscript) ([]byte, error) {
	roots, err := catalogReplayRootInputs(transcript)
	if err != nil {
		return nil, err
	}
	return replayconformance.Project(eventsByID, transcript.runID, roots)
}

func replayOperatorEvent(t testing.TB, event events.Event) operatorread.OperatorEventFull {
	t.Helper()
	full, err := operatorread.NewOperatorEventFull(event)
	if err != nil {
		t.Fatal(err)
	}
	return full
}

func createdRootEvent(id, eventType, producer, taskID, payload, runID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return eventtest.ExistingRunRootIngress(id, events.EventType(eventType), producer, taskID, json.RawMessage(payload), 0, runID, envelope, createdAt)
}

func replayProjectionDelivery(t testing.TB, id string) operatorread.OperatorEventDelivery {
	t.Helper()
	recipient, err := events.NewNodeDeliveryRecipient(catalogRootNode(t, "node-1"))
	if err != nil {
		t.Fatal(err)
	}
	return operatorread.OperatorEventDelivery{
		DeliveryID: id, SubscriberType: "node", SubscriberID: "node-1",
		Route:  events.DeliveryRoute{Recipient: recipient, Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "root"})},
		Status: "dead_letter", ReasonCode: "platform.retry_exhausted", Failure: replayFailurePointer(runtimefailures.ClassRetryExhausted, "attempts_exhausted"),
		RetryCount: 2, Terminal: true,
	}
}

func replayProjectionDeadLetter(id, deliveryID string, createdAt time.Time) operatorread.OperatorDeadLetterRecord {
	return operatorread.OperatorDeadLetterRecord{
		DeadLetterID: id, DeliveryID: deliveryID, ClaimVersion: 1,
		Failure:    replayFailure(runtimefailures.ClassRetryExhausted, "attempts_exhausted"),
		RetryCount: 2, ChainDepth: 3, HandlerNode: "node-1", CreatedAt: createdAt,
	}
}

func replayFailurePointer(class runtimefailures.Class, code string) *runtimefailures.Envelope {
	failure := replayFailure(class, code)
	return &failure
}

func replayFailure(class runtimefailures.Class, code string) runtimefailures.Envelope {
	return runtimefailures.Envelope{
		SchemaVersion: runtimefailures.EnvelopeSchemaVersion,
		Class:         class, Detail: runtimefailures.Detail{Code: code},
		Deterministic: true, Message: code, Remediation: "inspect",
	}
}
