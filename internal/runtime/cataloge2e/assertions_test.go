package cataloge2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestCatalogCausalEntityIDs_FollowsSourceEventIDChain(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	rootID := "11111111-1111-1111-1111-111111111111"
	childID := "22222222-2222-2222-2222-222222222222"
	grandchildID := "33333333-3333-3333-3333-333333333333"
	pg := &store.PostgresStore{DB: db}
	ctx := catalogRuntimeContext()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, catalogRuntimeRunID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
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
	if err := pg.AppendEvent(context.Background(), (events.Event{
		ID:        rootEventID,
		Type:      "root.started",
		RunID:     catalogRuntimeRunID,
		Payload:   []byte(`{"entity_id":"` + rootID + `"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(rootID)); err != nil {
		t.Fatalf("append root event: %v", err)
	}
	if err := pg.AppendEvent(context.Background(), (events.Event{
		ID:            childEventID,
		Type:          "child.started",
		RunID:         catalogRuntimeRunID,
		Payload:       []byte(`{"entity_id":"` + childID + `"}`),
		ParentEventID: rootEventID,
		CreatedAt:     time.Now().UTC(),
	}).WithEntityID(childID)); err != nil {
		t.Fatalf("append child event: %v", err)
	}
	if err := pg.AppendEvent(context.Background(), (events.Event{
		ID:            grandchildEventID,
		Type:          "grandchild.done",
		RunID:         catalogRuntimeRunID,
		Payload:       []byte(`{"entity_id":"` + grandchildID + `"}`),
		ParentEventID: childEventID,
		CreatedAt:     time.Now().UTC(),
	}).WithEntityID(grandchildID)); err != nil {
		t.Fatalf("append grandchild event: %v", err)
	}

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
	h := newCatalogAssertionHarness(t)
	entityID := uuid.NewString()
	eventID := uuid.NewString()

	insertCatalogAssertionEntityState(t, h, entityID, "pending")
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
	h := newCatalogAssertionHarness(t)
	entityID := uuid.NewString()
	eventID := uuid.NewString()

	insertCatalogAssertionEntityState(t, h, entityID, "active")
	insertCatalogAssertionDeadLetterEvent(t, h, entityID)
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

func TestAssertEmittedEvents_AcceptsCrossFlowInheritDispatcherEmission(t *testing.T) {
	h := newCatalogAssertionHarness(t)
	entityID := "11111111-1111-1111-1111-111111111111"
	bundle := loadFixtureBundle(t, filepath.Join(repoRootFromCatalogE2E(t), "tests", "tier11-flow-composition", "test-subject-id-cross-flow-inherit"))
	h.bundle = bundle

	insertCatalogAssertionEntityState(t, h, entityID, "dispatched")
	if err := h.pg.AppendEvent(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		Type:        "score.requested",
		SourceAgent: "runtime",
		RunID:       catalogRuntimeRunID,
		Payload:     []byte(`{"entity_id":"` + entityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)); err != nil {
		t.Fatalf("append score.requested event: %v", err)
	}

	assertEmittedEvents(t, h.db, h.startedAt, h.publishedIDs, entityID, []string{"score.requested"}, "", semanticview.Wrap(bundle))
}

func newCatalogAssertionHarness(t *testing.T) *runtimeHarness {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := catalogRuntimeContext()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, catalogRuntimeRunID); err != nil {
		t.Fatalf("seed catalog assertion run: %v", err)
	}
	return &runtimeHarness{
		t:              t,
		ctx:            ctx,
		db:             db,
		pg:             &store.PostgresStore{DB: db},
		workflow:       runtimepipeline.NewWorkflowInstanceStore(db),
		startedAt:      time.Now().UTC(),
		publishedIDs:   map[string]struct{}{},
		publishedOrder: []string{},
		eventEntityIDs: map[string]string{},
		previews:       map[string]runtimepipeline.HandlerPreview{},
	}
}

func insertCatalogAssertionEntityState(t *testing.T, h *runtimeHarness, entityID, state string) {
	t.Helper()
	if _, err := h.db.ExecContext(h.ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'root', 'default', $3,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, catalogRuntimeRunID, entityID, state); err != nil {
		t.Fatalf("insert entity_state %s: %v", entityID, err)
	}
}

func insertCatalogAssertionDeadLetterEvent(t *testing.T, h *runtimeHarness, entityID string) {
	t.Helper()
	if err := h.pg.AppendEvent(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		Type:        "platform.dead_letter",
		SourceAgent: "runtime",
		RunID:       catalogRuntimeRunID,
		Payload:     []byte(`{"entity_id":"` + entityID + `"}`),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)); err != nil {
		t.Fatalf("append platform.dead_letter event: %v", err)
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
