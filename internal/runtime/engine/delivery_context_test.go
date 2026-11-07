package engine

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

type deliveryContextActivityWriter struct {
	intents []ActivityIntent
}

func (r *deliveryContextActivityWriter) WriteActivityIntents(_ context.Context, intents []ActivityIntent) error {
	r.intents = append([]ActivityIntent(nil), intents...)
	return nil
}

func TestExecutorPersistPropagatesDeliveryContextToEveryContinuationIntent(t *testing.T) {
	publications := &recordingPublicationCommitter{}
	activities := &deliveryContextActivityWriter{}
	exec := &Executor{deps: RuntimeDependencies{
		StateRepo:     stubStateRepo{},
		MutationOwner: composedMutationOwner{publications: publications, activities: activities},
	}}
	life := events.InstanceLife{RunID: eventtest.UUID("continuation-intent-life"), FlowInstance: "worker/one", Life: 7}
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:intent-propagation"}, Life: &life}
	ctx := events.WithDeliveryContext(context.Background(), deliveryContext)
	frame := executionFrame{
		req: ExecutionRequest{
			EntityID: identity.EntityID("entity-a"),
			Handler:  runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "work.noted"}},
		},
		result: ExecutionResult{
			HandlerRuleSelection: handlerselection.Resolved(handlerselection.NotApplicable()),
			EmitIntents:          []EmitIntent{{}},
			ActivityIntents:      []ActivityIntent{{ActivityID: "provider-call", ExecutionMode: executionmode.Live}},
		},
	}
	if _, err := exec.persist(ctx, frame); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if len(publications.intents) != 1 || publications.intents[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() || publications.intents[0].Context.Life == nil || *publications.intents[0].Context.Life != life {
		t.Fatalf("emit context = %#v", publications.intents)
	}
	if len(activities.intents) != 1 || activities.intents[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() || activities.intents[0].Context.Life == nil || *activities.intents[0].Context.Life != life {
		t.Fatalf("activity context = %#v", activities.intents)
	}
}
