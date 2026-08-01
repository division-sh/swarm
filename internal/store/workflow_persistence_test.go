package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type workflowTestSelectedStore interface {
	storeTestRuntimeMutationOwner
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	decisioncard.Store
	PipelineObligations() runtimepipelineobligation.Store
}

type workflowTestBus struct{}

func (workflowTestBus) Publish(context.Context, events.Event) error                       { return nil }
func (workflowTestBus) PublishDirect(context.Context, events.Event, []string) error       { return nil }
func (workflowTestBus) ResolveSubscribedRecipients(string) []string                       { return nil }
func (workflowTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error { return nil }
func (workflowTestBus) EngineOutbox() runtimeengine.OutboxWriter                          { return nil }
func (workflowTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher              { return nil }

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
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:              workflowTestModule{},
		Persistence:         persistence,
		RunLifecycle:        selected,
		DeliveryStore:       selected,
		DecisionCards:       selected,
		PipelineObligations: selected.PipelineObligations(),
	})
	if coordinator == nil {
		t.Fatal("construct workflow persistence test coordinator")
	}
	return coordinator
}
