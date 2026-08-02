package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
)

type workflowTestSelectedStore interface {
	storeTestRuntimeMutationOwner
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	PipelineObligations() runtimepipelineobligation.Store
}

type workflowTestBus struct{}

func (workflowTestBus) Publish(context.Context, events.Event) error                 { return nil }
func (workflowTestBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (workflowTestBus) PublishInMutation(context.Context, events.Event) error       { return nil }
func (workflowTestBus) PublishDirectInMutation(context.Context, events.Event, []string) error {
	return nil
}
func (workflowTestBus) ResolveSubscribedRecipients(string) []string                       { return nil }
func (workflowTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error { return nil }
func (workflowTestBus) EngineOutbox() runtimeengine.OutboxWriter                          { return nil }
func (workflowTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher              { return nil }
func (workflowTestBus) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	return runtimedelivery.ExecutionAuthority{}, nil
}
func (workflowTestBus) AcquireDeliveryContinuation(string) (worklifetime.DeliveryContinuation, error) {
	return nil, nil
}
func (workflowTestBus) ReleaseDeliveryContinuation(string) error                  { return nil }
func (workflowTestBus) RetainDeliveryContinuation(runtimedelivery.Snapshot) error { return nil }

type workflowTestModule struct{}

func (workflowTestModule) SemanticSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "persistence-proof", Version: "1"},
	})
}
func (workflowTestModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition { return nil }
func (workflowTestModule) WorkflowNodes() []runtimepipeline.WorkflowNode           { return nil }
func (workflowTestModule) GuardRegistry() runtimepipeline.GuardRegistry            { return nil }
func (workflowTestModule) ActionRegistry() runtimepipeline.ActionRegistry          { return nil }

func newPostgresWorkflowTestCoordinator(
	t *testing.T,
	db *sql.DB,
	selected workflowTestSelectedStore,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	return newWorkflowTestCoordinator(t, db, runtimepipeline.NewPostgresWorkflowPersistence(db, selected), selected)
}

func newSQLiteWorkflowTestCoordinator(
	t *testing.T,
	db *sql.DB,
	selected workflowTestSelectedStore,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	return newWorkflowTestCoordinator(t, db, runtimepipeline.NewSQLiteWorkflowPersistence(db, selected), selected)
}

func newWorkflowTestCoordinator(
	t *testing.T,
	db *sql.DB,
	persistence runtimepipeline.WorkflowPersistence,
	selected workflowTestSelectedStore,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, db, completeWorkflowTestCoordinatorOptions(persistence, selected))
	if coordinator == nil {
		t.Fatal("construct workflow persistence test coordinator")
	}
	return coordinator
}

func completeWorkflowTestCoordinatorOptions(
	persistence runtimepipeline.WorkflowPersistence,
	selected workflowTestSelectedStore,
) runtimepipeline.PipelineCoordinatorOptions {
	bus := workflowTestBus{}
	return runtimepipeline.PipelineCoordinatorOptions{
		ReceiverExecution:       eventreceiver.NormalExecution(),
		Module:                  workflowTestModule{},
		Persistence:             persistence,
		RunLifecycle:            selected,
		DeliveryStore:           selected,
		DecisionCards:           selected,
		ProposedEffects:         selected,
		HumanTasks:              selected,
		DecisionCardDraftExpiry: selected,
		HumanTaskExpiry:         selected,
		GatePublisher:           bus,
		DirectDecisionPublisher: bus,
		DeliveryRuntime:         bus,
		PipelineObligations:     selected.PipelineObligations(),
	}
}

func TestWorkflowStoreRolesAreImmutableConstructorInputs(t *testing.T) {
	type roleCase struct {
		name string
		omit func(*runtimepipeline.PipelineCoordinatorOptions, runtimepipeline.WorkflowPersistence)
	}
	roles := []roleCase{
		{name: "mutation_executor", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, invalid runtimepipeline.WorkflowPersistence) {
			opts.Persistence = invalid
		}},
		{name: "delivery_lifecycle", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DeliveryStore = nil
		}},
		{name: "pipeline_obligations", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.PipelineObligations = nil
		}},
		{name: "decision_cards", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DecisionCards = nil
		}},
		{name: "proposed_effects", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.ProposedEffects = nil
		}},
		{name: "human_tasks", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.HumanTasks = nil
		}},
		{name: "decision_card_draft_expiry", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DecisionCardDraftExpiry = nil
		}},
		{name: "human_task_expiry", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.HumanTaskExpiry = nil
		}},
		{name: "gate_publisher", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.GatePublisher = nil
		}},
		{name: "direct_decision_publisher", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DirectDecisionPublisher = nil
		}},
		{name: "delivery_continuation_state", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DeliveryRuntime = nil
		}},
		{name: "run_lifecycle", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.RunLifecycle = nil
		}},
	}

	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var (
				db          *sql.DB
				selected    workflowTestSelectedStore
				persistence runtimepipeline.WorkflowPersistence
				invalid     runtimepipeline.WorkflowPersistence
			)
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, selected = store.DB, store
				persistence = runtimepipeline.NewSQLiteWorkflowPersistence(db, store)
				invalid = runtimepipeline.NewSQLiteWorkflowPersistence(db, nil)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := admitTestPostgresStore(t, opened)
				db, selected = opened, store
				persistence = runtimepipeline.NewPostgresWorkflowPersistence(db, store)
				invalid = runtimepipeline.NewPostgresWorkflowPersistence(db, nil)
			}

			if coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, db, completeWorkflowTestCoordinatorOptions(persistence, selected)); coordinator == nil {
				t.Fatal("complete durable workflow roles were rejected")
			}
			for _, role := range roles {
				t.Run(role.name, func(t *testing.T) {
					opts := completeWorkflowTestCoordinatorOptions(persistence, selected)
					role.omit(&opts, invalid)
					if coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, db, opts); coordinator != nil {
						t.Fatalf("durable workflow construction accepted missing %s", role.name)
					}
				})
			}
		})
	}
}
