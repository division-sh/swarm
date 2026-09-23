package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type failingPublicationBoundary struct {
	calls int
	err   error
}

func (b *failingPublicationBoundary) flushBeforeNestedPublication() error {
	b.calls++
	return b.err
}

func TestPublicationNestedBoundaryFailurePrecedesNewWork(t *testing.T) {
	failure := errors.New("prior segment was not settled")
	for _, test := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"outbox", func(ctx context.Context) error {
			return (engineDispatcher{bus: &EventBus{}}).DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{}})
		}},
		{"interceptor", func(ctx context.Context) error {
			return (engineDispatcher{bus: &EventBus{}}).dispatchCommittedInterceptorPublications(ctx, []events.Event{{}})
		}},
		{"deferred", func(ctx context.Context) error {
			return (&EventBus{}).publishDeferred(ctx, events.Event{})
		}},
		{"decision_processed", func(ctx context.Context) error {
			_, err := (&pipelinePublicationClaim{}).MarkDecisionProcessedOutcome(ctx)
			return err
		}},
		{"canonical_publication_preparation", func(ctx context.Context) error {
			_, _, err := (&EventBus{}).prepareClosedPublication(ctx, eventBusCommitPublishPlan{})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			boundary := &failingPublicationBoundary{err: failure}
			ctx := context.WithValue(context.Background(), deliveryDispatchScopeKey{}, &deliveryDispatchScope{publicationSettlement: boundary})
			if err := test.run(ctx); !errors.Is(err, failure) {
				t.Fatalf("nested work did not preserve predecessor failure: %v", err)
			}
			if boundary.calls != 1 {
				t.Fatalf("boundary calls = %d, want 1", boundary.calls)
			}
		})
	}
}

func TestPublicationBoundaryAbsentDoesNotInventOwnership(t *testing.T) {
	for _, ctx := range []context.Context{nil, context.Background(), context.WithValue(context.Background(), deliveryDispatchScopeKey{}, &deliveryDispatchScope{})} {
		if err := flushEnclosingPublicationSettlement(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
