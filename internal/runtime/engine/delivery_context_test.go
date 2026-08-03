package engine

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
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
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:intent-propagation"}}
	ctx := events.WithDeliveryContext(context.Background(), deliveryContext)
	frame := executionFrame{
		req: ExecutionRequest{EntityID: identity.EntityID("entity-a")},
		result: ExecutionResult{
			EmitIntents:     []EmitIntent{{}},
			ActivityIntents: []ActivityIntent{{ActivityID: "provider-call", ExecutionMode: executionmode.Live}},
		},
	}
	if _, err := exec.persist(ctx, frame); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if len(publications.intents) != 1 || publications.intents[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("emit context = %#v", publications.intents)
	}
	if len(activities.intents) != 1 || activities.intents[0].Context.ReplyContextID() != deliveryContext.ReplyContextID() {
		t.Fatalf("activity context = %#v", activities.intents)
	}
}
