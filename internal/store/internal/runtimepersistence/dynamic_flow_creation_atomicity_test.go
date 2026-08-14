package runtimepersistence_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type dynamicFlowCreationAtomicityStore interface {
	externalStoreTestDurableEventBusStore
	runtimepipeline.WorkflowPersistenceOwner
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	PipelineObligations() runtimepipelineobligation.Store
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type dynamicFlowCreationAtomicityFixture struct {
	db       *sql.DB
	workflow *runtimepipeline.PipelineCoordinator
	bus      *runtimebus.EventBus
	ctx      context.Context
	runID    string
	plan     runtimepipeline.DynamicFlowRuntimeReadinessPlan
	event    events.Event
	sqlite   bool
}

type dynamicFlowCreationWorkflowModule struct{ source semanticview.Source }

func (m dynamicFlowCreationWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (dynamicFlowCreationWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (dynamicFlowCreationWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode  { return nil }
func (dynamicFlowCreationWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry   { return nil }
func (dynamicFlowCreationWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

func TestDynamicFlowRuntimeCreationOccurrenceLinearizesWithTerminalizationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		for _, order := range []string{"terminal_wins", "creation_wins"} {
			order := order
			t.Run(backend+"/"+order, func(t *testing.T) {
				fixture := newDynamicFlowCreationAtomicityFixture(t, backend)
				switch order {
				case "terminal_wins":
					if err := fixture.workflow.MarkTerminated(fixture.ctx, fixture.plan.Identity.Route(), identity.NormalizeEntityID(fixture.plan.Identity.EntityID), time.Now().UTC()); err != nil {
						t.Fatalf("MarkTerminated: %v", err)
					}
					err := fixture.commit()
					if err == nil || !strings.Contains(err.Error(), "active eligible") {
						t.Fatalf("creation after terminal error = %v, want active-run/instance refusal", err)
					}
				case "creation_wins":
					if err := fixture.commit(); err != nil {
						t.Fatalf("commit creation occurrence: %v", err)
					}
					if err := fixture.workflow.MarkTerminated(
						fixture.ctx,
						fixture.plan.Identity.Route(),
						identity.NormalizeEntityID(fixture.plan.Identity.EntityID),
						time.Now().UTC(),
					); err != nil {
						t.Fatalf("terminalize after creation commit: %v", err)
					}
				default:
					t.Fatalf("unknown order %q", order)
				}
				fixture.assertResult(t, order == "creation_wins")
			})
		}
	}
}

func TestDynamicFlowRuntimeCreationOccurrenceRollsBackAppendedEventOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDynamicFlowCreationAtomicityFixture(t, backend)
			fixture.rejectCreationMark(t)
			err := fixture.commit()
			if err == nil {
				t.Fatal("commit creation occurrence succeeded across injected completion-mark failure")
			}
			fixture.assertOccurrenceCounts(t, 0)
		})
	}
}

func newDynamicFlowCreationAtomicityFixture(t *testing.T, backend string) dynamicFlowCreationAtomicityFixture {
	t.Helper()
	var (
		db       *sql.DB
		selected dynamicFlowCreationAtomicityStore
		workflow *runtimepipeline.PipelineCoordinator
		sqlite   bool
	)
	switch backend {
	case "sqlite":
		sqliteStore := storetest.StartSQLiteRuntimeStore(t)
		db = storetest.DatabaseForTest(sqliteStore)
		selected = sqliteStore
		sqlite = true
	case "postgres":
		_, postgresDB, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		postgresStore := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
		db = postgresDB
		selected = postgresStore
	default:
		t.Fatalf("unknown backend %q", backend)
	}

	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	sourceFact := mustExternalStoreTestBundleSourceFact()
	bundleHash, bundleSource := sourceFact.StorageValues()
	if sqlite {
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC(), BundleHash: bundleHash, BundleSource: bundleSource})
	} else {
		runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: bundleHash, BundleSource: bundleSource})
	}

	occurredAt := time.Now().UTC().Truncate(time.Microsecond)
	identity := runtimeflowidentity.Instance{
		TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
		InstancePath: "review/inst-1", EntityID: uuid.NewString(), HasStoredPath: true,
	}
	plan := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: identity, RunID: runID,
		BundleHash: bundleHash, BundleSource: bundleSource, WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
		CreationEvent: &runtimepipeline.DynamicFlowRuntimeCreationEventPlan{
			EventID: uuid.NewString(), EventType: "review/inst-1/task.started",
			RunID: runID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live,
			Payload: []byte(`{"name":"alpha"}`), CreatedAt: occurredAt,
		},
	}
	event, err := dynamicFlowCreationAtomicityEvent(plan)
	if err != nil {
		t.Fatalf("build creation event: %v", err)
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("creation atomicity context is missing author-activity scope")
	}
	lease, err := selected.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{
		{EventType: "test.dynamic_flow.triggered", Disposition: runtimeauthoractivity.StoryDifferent},
		{EventType: string(event.Type()), Disposition: runtimeauthoractivity.StoryAuthored},
	})
	if err != nil {
		t.Fatalf("register creation event descriptor: %v", err)
	}
	t.Cleanup(lease.Release)
	eventBus, err := newStoreTestEventBus(t, selected, runtimebus.EventBusOptions{
		RuntimeInstanceID: "11111111-1111-1111-1111-111111111111",
		ContractBundle:    semanticview.Wrap(dynamicFlowCreationAtomicityBundle()),
		BundleSourceFact:  sourceFact,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
	if sqlite {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
	}
	workflow = runtimepipeline.NewPipelineCoordinatorWithOptions(eventBus, runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture:        executionposture.Live,
		Module:                  dynamicFlowCreationWorkflowModule{source: semanticview.Wrap(dynamicFlowCreationAtomicityBundle())},
		Persistence:             workflowPersistence,
		RunLifecycle:            selected,
		DeliveryStore:           selected,
		DeadLetters:             selected,
		PipelineObligations:     selected.PipelineObligations(),
		DecisionCards:           selected,
		ProposedEffects:         selected,
		HumanTasks:              selected,
		DecisionCardDraftExpiry: selected,
		HumanTaskExpiry:         selected,
		DeliveryRuntime:         eventBus, ReceiverExecution: eventreceiver.NormalExecution(),
	})

	parent := eventtest.ExistingRunRootIngress(
		plan.CreationEvent.ParentEventID,
		events.EventType("test.dynamic_flow.triggered"),
		"",
		runID,
		[]byte(`{"name":"alpha"}`),
		0,
		runID,
		events.EventEnvelope{},
		occurredAt.Add(-time.Second),
	)
	if err := eventBus.Publish(ctx, parent); err != nil {
		t.Fatalf("publish causal parent: %v", err)
	}
	result, err := workflow.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "inst-1", StorageRef: identity.InstancePath, EntityID: identity.EntityID, WorkflowName: identity.TemplateID,
		WorkflowVersion: "1.0.0", RuntimeReadiness: &plan, CurrentState: "pending",
		Config: map[string]any{"name": "alpha"},
	}, occurredAt)
	if err != nil || result != runtimepipeline.WorkflowInitialMaterializationCreated {
		t.Fatalf("materialize readiness: result=%d err=%v", result, err)
	}
	if err := workflow.MarkDynamicFlowRuntimeTopologyReady(ctx, plan, occurredAt.Add(time.Second)); err != nil {
		t.Fatalf("mark topology ready: %v", err)
	}
	return dynamicFlowCreationAtomicityFixture{
		db: db, workflow: workflow, bus: eventBus, ctx: ctx,
		runID: runID, plan: plan, event: event, sqlite: sqlite,
	}
}

func dynamicFlowCreationAtomicityBundle() *runtimecontracts.WorkflowContractBundle {
	review := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.started": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{*review}},
			ByID: map[string]*runtimecontracts.FlowContractView{"review": review},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode:             "template",
				Pins:             runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{Events: []string{"task.started"}}},
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: "task.started"},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "1.0.0"},
	}
}

func dynamicFlowCreationAtomicityEvent(plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (events.Event, error) {
	creation := plan.CreationEvent
	return eventtest.PersistedChildForProducer(
		creation.EventID,
		events.EventType(creation.EventType),
		eventtest.Producer(events.EventProducerPlatform, "flow-instance-activator"),
		"",
		creation.Payload,
		0,
		creation.RunID,
		creation.ParentEventID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{
			EntityID: plan.Identity.EntityID, FlowInstance: plan.Identity.InstancePath,
		}, events.RouteIdentity{
			FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID,
		}),
		creation.CreatedAt,
	), nil
}

func (f dynamicFlowCreationAtomicityFixture) commit() error {
	return f.bus.CommitDynamicFlowRuntimeCreationOccurrence(f.ctx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest{
		RunID: f.runID, InstancePath: f.plan.Identity.InstancePath, Plan: f.plan,
		Event: f.event, OccurredAt: time.Now().UTC(),
	})
}

func (f dynamicFlowCreationAtomicityFixture) rejectCreationMark(t *testing.T) {
	t.Helper()
	statement := `
		CREATE TRIGGER reject_dynamic_flow_creation_mark
		BEFORE UPDATE OF creation_event_emitted_at ON flow_instance_runtime_readiness
		WHEN NEW.creation_event_emitted_at IS NOT NULL
		BEGIN
			SELECT RAISE(ABORT, 'injected dynamic flow creation mark failure');
		END
	`
	if !f.sqlite {
		statement = `
			ALTER TABLE flow_instance_runtime_readiness
			ADD CONSTRAINT reject_dynamic_flow_creation_mark
			CHECK (creation_event_emitted_at IS NULL)
		`
	}
	if _, err := f.db.ExecContext(context.Background(), statement); err != nil {
		t.Fatalf("install dynamic flow creation mark failure: %v", err)
	}
}

func (f dynamicFlowCreationAtomicityFixture) assertResult(t *testing.T, creationWon bool) {
	t.Helper()
	want := 0
	if creationWon {
		want = 1
	}
	f.assertOccurrenceCounts(t, want)

	statusQuery := `SELECT status FROM flow_instances WHERE instance_id = ?`
	if !f.sqlite {
		statusQuery = `SELECT status FROM flow_instances WHERE instance_id = $1`
	}
	var status string
	if err := f.db.QueryRowContext(f.ctx, statusQuery, f.plan.Identity.InstancePath).Scan(&status); err != nil {
		t.Fatalf("load terminal flow instance: %v", err)
	}
	if strings.TrimSpace(status) != "terminated" {
		t.Fatalf("flow instance status = %q, want terminated", status)
	}
}

func (f dynamicFlowCreationAtomicityFixture) assertOccurrenceCounts(t *testing.T, want int) {
	t.Helper()
	eventQuery := `SELECT COUNT(*) FROM events WHERE event_id = ?`
	readinessQuery := `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ? AND creation_event_emitted_at IS NOT NULL`
	if !f.sqlite {
		eventQuery = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`
		readinessQuery = `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_id = $2 AND creation_event_emitted_at IS NOT NULL`
	}
	var eventCount, markCount int
	if err := f.db.QueryRowContext(f.ctx, eventQuery, f.event.ID()).Scan(&eventCount); err != nil {
		t.Fatalf("count creation events: %v", err)
	}
	if err := f.db.QueryRowContext(f.ctx, readinessQuery, f.runID, f.plan.Identity.InstancePath).Scan(&markCount); err != nil {
		t.Fatalf("count creation completion marks: %v", err)
	}
	if eventCount != want || markCount != want {
		t.Fatalf("creation atomic result = events:%d marks:%d, want %d/%d", eventCount, markCount, want, want)
	}
}

var _ dynamicFlowCreationAtomicityStore = (*store.PostgresStore)(nil)
var _ dynamicFlowCreationAtomicityStore = (*store.SQLiteRuntimeStore)(nil)
