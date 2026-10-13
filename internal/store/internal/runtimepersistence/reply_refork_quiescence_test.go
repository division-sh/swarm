package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestReplyReforkQuiescenceIncludesZeroRecipientPublicationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(interface {
				semanticEventFixtureStore
				PipelineObligations() pipelineobligation.Store
				LoadRunTestQuiescence(context.Context, string, time.Time) (operatorread.RunTestQuiescence, error)
			})
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "reply.observed", "runtime", "", []byte(`{}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			handoffs, err := ReadServedIncompletePipelineHandoffCountForTest(ctx, selected, event.RunID())
			if err != nil || handoffs != 0 {
				t.Fatalf("zero-recipient event unexpectedly acquired a delivery: handoffs=%d error=%v", handoffs, err)
			}
			quiescence, err := selected.LoadRunTestQuiescence(ctx, event.RunID(), event.CreatedAt())
			if err != nil || quiescence.Ready || quiescence.UnsettledPipelineEvents != 1 || quiescence.ActiveDeliveries != 0 {
				t.Fatalf("delivery-only completion concealed an unsettled publication: quiescence=%+v error=%v", quiescence, err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("quiescence observation changed publication evidence: %v", err)
			}
			settlePipelineParityEvent(t, ctx, selected.PipelineObligations(), event.ID(), pipelineobligation.Acknowledged("reply_refork_fixture"))
			quiescence, err = selected.LoadRunTestQuiescence(ctx, event.RunID(), time.Now().UTC())
			if err != nil || !quiescence.Ready || quiescence.UnsettledPipelineEvents != 0 || quiescence.ActiveDeliveries != 0 {
				t.Fatalf("exact publication settlement did not clear quiescence: quiescence=%+v error=%v", quiescence, err)
			}
		})
	}
}
