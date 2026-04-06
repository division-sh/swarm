package cataloge2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestCatalogSubjectEntityIDs_UsesResolvedSubjectID(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	rootID := "11111111-1111-1111-1111-111111111111"
	childID := "22222222-2222-2222-2222-222222222222"
	grandchildID := "33333333-3333-3333-3333-333333333333"

	for _, stmt := range []struct {
		entityID  string
		subjectID string
		flow      string
		state     string
	}{
		{entityID: rootID, subjectID: rootID, flow: rootID, state: "done"},
		{entityID: childID, subjectID: rootID, flow: "child", state: "completed"},
		{entityID: grandchildID, subjectID: rootID, flow: "grandchild", state: "finished"},
	} {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO entity_state (
				entity_id, subject_id, flow_instance, entity_type, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3, 'default', $4, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
			)
		`, stmt.entityID, stmt.subjectID, stmt.flow, stmt.state); err != nil {
			t.Fatalf("insert entity_state %s: %v", stmt.entityID, err)
		}
	}

	gotFromRoot := catalogSubjectEntityIDs(t, db, rootID)
	if len(gotFromRoot) != 3 {
		t.Fatalf("root subject entity ids len = %d, want 3 (%v)", len(gotFromRoot), gotFromRoot)
	}
	gotFromChild := catalogSubjectEntityIDs(t, db, childID)
	if len(gotFromChild) != 3 {
		t.Fatalf("child subject entity ids len = %d, want 3 (%v)", len(gotFromChild), gotFromChild)
	}
	for _, candidate := range []string{rootID, childID, grandchildID} {
		if _, ok := gotFromChild[candidate]; !ok {
			t.Fatalf("child subject entity ids missing %s (%v)", candidate, gotFromChild)
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

func newCatalogAssertionHarness(t *testing.T) *runtimeHarness {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return &runtimeHarness{
		t:              t,
		ctx:            context.Background(),
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
	if _, err := h.db.ExecContext(context.Background(), `
		INSERT INTO entity_state (
			entity_id, subject_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $1::uuid, 'root', 'default', $2,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, entityID, state); err != nil {
		t.Fatalf("insert entity_state %s: %v", entityID, err)
	}
}

func insertCatalogAssertionDeadLetterEvent(t *testing.T, h *runtimeHarness, entityID string) {
	t.Helper()
	if err := h.pg.AppendEvent(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		Type:        "platform.dead_letter",
		SourceAgent: "runtime",
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
