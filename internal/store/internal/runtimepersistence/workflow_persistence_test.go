package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/testutil"
)

type workflowTestSelectedStore interface {
	runtimepipeline.WorkflowPersistenceOwner
	runtimebus.TargetFailureDeadLetterRecorder
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	runtimetimerobligation.Reader
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
	return newWorkflowTestCoordinator(t, db, runtimepipeline.NewWorkflowPersistence(selected), selected)
}

func newSQLiteWorkflowTestCoordinator(
	t *testing.T,
	db *sql.DB,
	selected workflowTestSelectedStore,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	return newWorkflowTestCoordinator(t, db, runtimepipeline.NewWorkflowPersistence(selected), selected)
}

func newWorkflowTestCoordinator(
	t *testing.T,
	db *sql.DB,
	persistence runtimepipeline.WorkflowPersistence,
	selected workflowTestSelectedStore,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, completeWorkflowTestCoordinatorOptions(persistence, selected))
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
		ExecutionPosture:        executionposture.Live,
		Module:                  workflowTestModule{},
		Persistence:             persistence,
		RunLifecycle:            selected,
		DeliveryStore:           selected,
		DeadLetters:             selected,
		DecisionCards:           selected,
		ProposedEffects:         selected,
		HumanTasks:              selected,
		DecisionCardDraftExpiry: selected,
		HumanTaskExpiry:         selected,
		TimerObligationReader:   selected,
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
		{name: "dead_letter_recorder", omit: func(opts *runtimepipeline.PipelineCoordinatorOptions, _ runtimepipeline.WorkflowPersistence) {
			opts.DeadLetters = nil
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
				selected    workflowTestSelectedStore
				persistence runtimepipeline.WorkflowPersistence
				invalid     runtimepipeline.WorkflowPersistence
			)
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected = store
				persistence = runtimepipeline.NewWorkflowPersistence(store)
				invalid = runtimepipeline.NewWorkflowPersistence(nil)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := admitTestPostgresStore(t, opened)
				selected = store
				persistence = runtimepipeline.NewWorkflowPersistence(store)
				invalid = runtimepipeline.NewWorkflowPersistence(nil)
			}

			if coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, completeWorkflowTestCoordinatorOptions(persistence, selected)); coordinator == nil {
				t.Fatal("complete durable workflow roles were rejected")
			}
			for _, role := range roles {
				t.Run(role.name, func(t *testing.T) {
					opts := completeWorkflowTestCoordinatorOptions(persistence, selected)
					role.omit(&opts, invalid)
					if coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, opts); coordinator != nil {
						t.Fatalf("durable workflow construction accepted missing %s", role.name)
					}
				})
			}
		})
	}
}
