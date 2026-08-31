package cataloge2e

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type catalogPersistenceBus struct{}

func (catalogPersistenceBus) Publish(context.Context, events.Event) error { return nil }
func (catalogPersistenceBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (catalogPersistenceBus) ResolveSubscribedRecipients(string) []string { return nil }
func (catalogPersistenceBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}
func (catalogPersistenceBus) EngineDispatcher() runtimeengine.PostCommitDispatcher { return nil }
func (catalogPersistenceBus) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	return runtimedelivery.ExecutionAuthority{}, nil
}
func (catalogPersistenceBus) AcquireDeliveryContinuation(string) (worklifetime.DeliveryContinuation, error) {
	return nil, nil
}
func (catalogPersistenceBus) ReleaseDeliveryContinuation(string) error { return nil }
func (catalogPersistenceBus) RetainDeliveryContinuation(runtimedelivery.Snapshot) error {
	return nil
}

func TestCatalogCausalEntityIDs_FollowsSourceEventIDChain(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	rootID := "11111111-1111-1111-1111-111111111111"
	childID := "22222222-2222-2222-2222-222222222222"
	grandchildID := "33333333-3333-3333-3333-333333333333"
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	registerTestAuthorActivityCatalog(t, pg, "root.started", "child.started", "grandchild.done")
	ctx := catalogRuntimeContext()
	storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID})
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
	h := newCatalogAssertionHarness(t)
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
	h := newCatalogAssertionHarness(t)
	entityID := "11111111-1111-1111-1111-111111111111"
	bundle := loadFixtureBundle(t, filepath.Join(repoRootFromCatalogE2E(t), "tests", "tier11-flow-composition", "test-subject-id-cross-flow-inherit"))
	h.bundle = bundle

	insertCatalogAssertionEntityState(t, h, entityID, "dispatched")
	storetest.CommitSemanticEvent(t, testAuthorActivityContext(context.Background()), h.pg, eventtest.ExistingRunRootIngress(
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

func newCatalogAssertionHarness(t *testing.T) *runtimeHarness {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := catalogRuntimeContext()
	storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID})
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	registerTestAuthorActivityCatalog(t, pg, "score.requested")
	bus := catalogPersistenceBus{}
	workflow := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture:        executionposture.Live,
		Module:                  &fixtureWorkflowModule{},
		Persistence:             runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:            pg,
		PipelineObligations:     pg.PipelineObligations(),
		DeliveryStore:           pg,
		DeadLetters:             pg,
		DecisionCards:           pg,
		ProposedEffects:         pg,
		HumanTasks:              pg,
		DecisionCardDraftExpiry: pg,
		HumanTaskExpiry:         pg,
		DeliveryRuntime:         bus, ReceiverExecution: eventreceiver.NormalExecution(),
	})

	return &runtimeHarness{
		t:              t,
		ctx:            ctx,
		db:             db,
		pg:             pg,
		workflow:       workflow,
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
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
		VALUES (
			$1::uuid, $1::text, 'catalog-assertion', 'static',
			jsonb_build_object(
				'workflow_version', '1',
				'instance_id', $1::text,
				'storage_ref', $1::text,
				'flow_path', $1::text
			),
			'active', now()
		)
		ON CONFLICT (run_id, instance_path) DO NOTHING
	`, catalogRuntimeRunID); err != nil {
		t.Fatalf("insert root flow instance: %v", err)
	}
	if _, err := h.db.ExecContext(h.ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $1, 'default', $3,
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, catalogRuntimeRunID, entityID, state); err != nil {
		t.Fatalf("insert entity_state %s: %v", entityID, err)
	}
}

func insertCatalogAssertionDeadLetterEvent(t *testing.T, h *runtimeHarness, entityID string) {
	t.Helper()
	storetest.CommitSemanticEvent(t, testAuthorActivityContext(context.Background()), h.pg, eventtest.ExistingRunRootIngress(
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
	storetest.CommitSemanticEvent(t, testAuthorActivityContext(context.Background()), h.pg, event)
	failure := runtimefailures.FromError(
		errors.New("catalog assertion dead letter"),
		"cataloge2e",
		"assert_dead_letter_relation",
	).Failure
	if err := h.pg.RecordDeadLetter(testAuthorActivityContext(context.Background()), runtimedeadletters.Record{
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
