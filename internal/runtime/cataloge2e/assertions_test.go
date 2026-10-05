package cataloge2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)


func TestCatalogCausalOrderPreservesParentsAndIndependentOrder(t *testing.T) {
	rows := []catalogStoredEvent{
		{ID: "a-child", SourceEventID: "z-parent"},
		{ID: "b-independent"},
		{ID: "z-parent", SourceEventID: "outside-window"},
		{ID: "grandchild", SourceEventID: "a-child"},
	}
	got := catalogCausalOrder(t, rows)
	want := []string{"z-parent", "a-child", "b-independent", "grandchild"}
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("event[%d] = %s, want %s", i, got[i].ID, want[i])
		}
	}
	if rows[0].ID != "a-child" {
		t.Fatal("causal ordering mutated input")
	}
}


func TestCatalogCausalEntityIDs_FollowsSourceEventIDChain(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	rootID := "11111111-1111-1111-1111-111111111111"
	childID := "22222222-2222-2222-2222-222222222222"
	grandchildID := "33333333-3333-3333-3333-333333333333"
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	registerTestAuthorActivityCatalog(t, pg, "root.started", "child.started", "grandchild.done")
	ctx := catalogRuntimeContext()
	storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID})
	startedAt := time.Now().UTC().Add(-time.Second)

	for _, stmt := range []struct {
		entityID string
		flow     string
		state    string
	}{
		{entityID: rootID, flow: rootID, state: "done"},
		{entityID: childID, flow: "child", state: "completed"},
		{entityID: grandchildID, flow: "grandchild", state: "finished"},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3, 'default', $4, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
			)
		`, catalogRuntimeRunID, stmt.entityID, stmt.flow, stmt.state); err != nil {
			t.Fatalf("insert entity_state %s: %v", stmt.entityID, err)
		}
	}

	rootEventID := uuid.NewString()
	childEventID := uuid.NewString()
	grandchildEventID := uuid.NewString()
	fixtureCtx := testAuthorActivityContext(context.Background())
	storetest.CommitSemanticEvent(t, fixtureCtx, pg, eventtest.ExistingRunRootIngress(
		rootEventID,
		"root.started",
		"",
		"",
		[]byte(`{"entity_id":"`+rootID+`"}`),
		0,
		catalogRuntimeRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, rootID),
		time.Now().UTC(),
	))
	storetest.CommitSemanticEvent(t, fixtureCtx, pg, eventtest.ChildWithLineage(
		childEventID,
		"child.started",
		"",
		"",
		[]byte(`{"entity_id":"`+childID+`"}`),
		0,
		events.EventLineage{RunID: catalogRuntimeRunID, ParentEventID: rootEventID, ExecutionMode: executionmode.Live},
		events.EnvelopeForEntityID(events.EventEnvelope{}, childID),
		time.Now().UTC(),
	))
	storetest.CommitSemanticEvent(t, fixtureCtx, pg, eventtest.ChildWithLineage(
		grandchildEventID,
		"grandchild.done",
		"",
		"",
		[]byte(`{"entity_id":"`+grandchildID+`"}`),
		0,
		events.EventLineage{RunID: catalogRuntimeRunID, ParentEventID: childEventID, ExecutionMode: executionmode.Live},
		events.EnvelopeForEntityID(events.EventEnvelope{}, grandchildID),
		time.Now().UTC(),
	))

	got := catalogCausalEntityIDs(t, db, startedAt, map[string]struct{}{rootEventID: {}}, rootID)
	if len(got) != 3 {
		t.Fatalf("causal entity ids len = %d, want 3 (%v)", len(got), got)
	}
	for _, candidate := range []string{rootID, childID, grandchildID} {
		if _, ok := got[candidate]; !ok {
			t.Fatalf("causal entity ids missing %s (%v)", candidate, got)
		}
	}
}

func TestCatalogAssertsAuthoritativeHandlerOutcome_OnlySuccess(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "empty", raw: "", want: false},
		{name: "success", raw: "success", want: true},
		{name: "success trimmed case-insensitive", raw: " Success ", want: true},
		{name: "reject", raw: "reject", want: false},
		{name: "discard", raw: "discard", want: false},
		{name: "escalate", raw: "escalate", want: false},
		{name: "kill", raw: "kill", want: false},
		{name: "terminal reject", raw: "terminal_reject", want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := catalogAssertsAuthoritativeHandlerOutcome(tc.raw); got != tc.want {
				t.Fatalf("catalogAssertsAuthoritativeHandlerOutcome(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestCatalogRecognizesHandlerOutcome_RejectsTyposAndUnsupportedValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "empty", raw: "", want: true},
		{name: "success", raw: "success", want: true},
		{name: "reject", raw: "reject", want: true},
		{name: "blocked", raw: "blocked", want: true},
		{name: "terminal reject", raw: "terminal_reject", want: true},
		{name: "success typo", raw: "succes", want: false},
		{name: "unsupported", raw: "maybe", want: false},
		{name: "trimmed unsupported", raw: " waiting ", want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := catalogRecognizesHandlerOutcome(tc.raw); got != tc.want {
				t.Fatalf("catalogRecognizesHandlerOutcome(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestAssertCatalogRuntimeOutcome_IgnoresTopLevelNonSuccessPreviewProof(t *testing.T) {
	h := newCatalogAssertionHarness(t, "pending")
	entityID := catalogRuntimeRunID
	eventID := uuid.NewString()

	seedCatalogAssertionPublishedEvent(h, eventID, entityID, runtimepipeline.HandlerOutcomeCompleted)

	expected := catalogExpectedDocument{}
	expected.Trigger.Event = "task.started"
	expected.Trigger.Payload = map[string]any{"entity_id": entityID}
	expected.Expected.HandlerOutcome = "reject"
	expected.Expected.EntityState = "pending"
	expected.Expected.EmittedEvents = []string{}

	assertCatalogRuntimeOutcome(t, h, expected)
}

func TestAssertCatalogRuntimeOutcome_IgnoresEntityNonSuccessPreviewProof(t *testing.T) {
	h := newCatalogAssertionHarness(t, "active")
	entityID := catalogRuntimeRunID
	eventID := uuid.NewString()

	insertCatalogAssertionDeadLetterEvent(t, h, entityID)
	insertCatalogAssertionDeadLetterRelation(t, h, eventID, entityID)
	seedCatalogAssertionPublishedEvent(h, eventID, entityID, runtimepipeline.HandlerOutcomeCompleted)

	expected := catalogExpectedDocument{}
	expected.Expected.Entities = map[string]catalogEntityExpected{
		entityID: {
			HandlerOutcome: "kill",
			EntityState:    "active",
			DeadLetter:     true,
			EmittedEvents:  []string{"platform.dead_letter"},
		},
	}

	assertCatalogRuntimeOutcome(t, h, expected)
}

func TestCatalogDeadLetterRelation_DiagnosticAloneGetsNoCredit(t *testing.T) {
	h := newCatalogAssertionHarness(t, "pending")
	entityID := uuid.NewString()
	insertCatalogAssertionDeadLetterEvent(t, h, entityID)

	if catalogHasDeadLetterRelation(t, h.db, h.startedAt, entityID) {
		t.Fatal("platform.dead_letter diagnostic received persisted relation credit")
	}
	if assertEntityDeadLetterOutcome(t, h.db, h.startedAt, entityID) {
		t.Fatal("entity diagnostic received persisted relation credit")
	}
}

func TestAssertEmittedEvents_AcceptsCrossFlowInheritDispatcherEmission(t *testing.T) {
	h := newCatalogAssertionHarness(t, "dispatched")
	entityID := catalogRuntimeRunID
	bundle := loadFixtureBundle(t, filepath.Join(repoRootFromCatalogE2E(t), "tests", "tier11-flow-composition", "test-subject-id-cross-flow-inherit"))
	h.bundle = bundle

	storetest.CommitSemanticEvent(t, h.ctx, h.pg, eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		"score.requested",
		"runtime",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		catalogRuntimeRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))

	assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, []string{"score.requested"}, "", semanticview.Wrap(bundle))
}

func newCatalogAssertionHarness(t *testing.T, initial string) *runtimeHarness {
	t.Helper()
	root := t.TempDir()
	for name, contents := range map[string]string{
		"schema.yaml":   "name: catalog-assertion\npins:\n  inputs: [score.requested, assertion.finish]\nstages:\n  " + initial + ": {initial: true}\n  finished: {terminal: true}\n",
		"entities.yaml": "assertion:\n  note: {type: 'text?', _unused_reason: assertion fixture}\n",
		"events.yaml":   "score.requested:\n  entity_id: uuid?\nassertion.finish:\n",
		"nodes.yaml":    "finisher:\n  execution_type: system_node\n  event_handlers:\n    score.requested: {}\n    assertion.finish:\n      advances_to: finished\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h := newRuntimeHarnessForBackend(t, root, catalogBackendPostgres, false)
	ctx := runtimeeffects.WithExecutionMode(h.ctx, executionmode.Live)
	plan, err := h.rt.Manager.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(h.bundle),
		Instance:       flowidentity.Stored(semanticview.Wrap(h.bundle), ".", catalogRuntimeRunID, catalogRuntimeRunID, catalogRuntimeRunID, ""),
		OccurredAt:     h.startedAt,
	})
	if err != nil {
		t.Fatalf("prepare assertion constructor: %v", err)
	}
	committed, err := h.rt.Bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("commit assertion constructor: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
	if err := h.workflow.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
		t.Fatalf("finalize assertion construction: %v", err)
	}
	return h
}

func insertCatalogAssertionDeadLetterEvent(t *testing.T, h *runtimeHarness, entityID string) {
	t.Helper()
	storetest.CommitSemanticEvent(t, h.ctx, h.pg, eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		"platform.dead_letter",
		"runtime",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		catalogRuntimeRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))
}

func insertCatalogAssertionDeadLetterRelation(t *testing.T, h *runtimeHarness, eventID, entityID string) {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(
		eventID,
		"score.requested",
		"cataloge2e",
		"",
		[]byte(`{"entity_id":"`+entityID+`"}`),
		0,
		catalogRuntimeRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)
	storetest.CommitSemanticEvent(t, h.ctx, h.pg, event)
	failure := runtimefailures.FromError(
		errors.New("catalog assertion dead letter"),
		"cataloge2e",
		"assert_dead_letter_relation",
	).Failure
	if err := h.pg.RecordDeadLetter(h.ctx, runtimedeadletters.Record{
		OriginalEventID: event.ID(),
		OriginalEvent:   string(event.Type()),
		OriginalPayload: event.Payload(),
		EntityID:        entityID,
		FlowInstance:    "runtime",
		Failure:         failure,
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record catalog assertion dead letter relation: %v", err)
	}
}

func seedCatalogAssertionPublishedEvent(h *runtimeHarness, eventID, entityID string, status runtimepipeline.HandlerOutcomeStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.publishedIDs[eventID] = struct{}{}
	h.publishedOrder = append(h.publishedOrder, eventID)
	h.eventEntityIDs[eventID] = entityID
	h.previews[eventID] = runtimepipeline.HandlerPreview{Status: status}
}
