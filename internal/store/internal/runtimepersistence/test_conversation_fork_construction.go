package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type ConversationForkConstruction struct {
	Parent        *pipeline.FlowInstanceActivationPlan
	Child         pipeline.FlowInstanceActivationPlan
	CreatingEvent events.Event
}

// PrepareConversationForkConstructionForTest derives, rather than fabricates,
// the fixed fixture's complete construction through its actual admitted manager.
func PrepareConversationForkConstructionForTest(ctx context.Context, selected any, source semanticview.Source, planner *manager.AgentManager, fixture ConversationForkSourceFixture) (ConversationForkConstruction, error) {
	if _, err := validateConversationForkFixture(ctx, fixture); err != nil {
		return ConversationForkConstruction{}, err
	}
	if planner == nil || source == nil {
		return ConversationForkConstruction{}, fmt.Errorf("conversation construction requires its admitted source and manager")
	}
	ctx = correlation.WithRunID(ctx, fixture.RunID)
	root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), fixture.RunID, fixture.RunID, fixture.RunID, "")
	child, err := flowidentity.KeyedChild(source, root, "flow", "forkchat")
	if err != nil {
		return ConversationForkConstruction{}, err
	}
	child.EntityID = fixture.EntityID
	id := uuid.NewSHA1(uuid.MustParse(fixture.EntityID), []byte("conversation-fork-construction")).String()
	event := eventtest.ExistingRunRootIngress(id, "flow/conversation.created", "conversation-construction-fixture", "", []byte(`{"name":"Before"}`), 0, fixture.RunID, events.EventEnvelope{}, fixture.Turn1At.Add(-30*time.Second))
	childPlan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: child, ConstructorInput: "conversation.created", ResolvedKey: "Before",
		TriggerEvent: event, OccurredAt: event.CreatedAt(),
	})
	if err != nil {
		return ConversationForkConstruction{}, err
	}
	reader, ok := selected.(interface {
		LoadWorkflowInstance(context.Context, flowidentity.RunScopedFlowInstance) (pipeline.WorkflowInstance, bool, error)
	})
	if !ok {
		return ConversationForkConstruction{}, fmt.Errorf("conversation construction requires the existing parent reader")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(fixture.RunID, root.Route())
	if err != nil {
		return ConversationForkConstruction{}, err
	}
	_, found, err := reader.LoadWorkflowInstance(ctx, owner)
	if err != nil {
		return ConversationForkConstruction{}, err
	}
	result := ConversationForkConstruction{Child: childPlan, CreatingEvent: event}
	if !found {
		parent, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: root, OccurredAt: event.CreatedAt()})
		if err != nil {
			return ConversationForkConstruction{}, err
		}
		result.Parent = &parent
	}
	return result, nil
}
