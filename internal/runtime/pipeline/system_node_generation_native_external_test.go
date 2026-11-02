package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestSystemNodePredecessorCannotCommitAfterGenerationAdvanceBothStores(t *testing.T) {
	pipeline.VerifySystemNodePredecessorCannotCommitAfterGenerationAdvanceForTest(t, pipelineDeliveryNativeFixture, func(t *testing.T, ctx context.Context, store deliverylifecycle.Store) func() {
		selected := store.(agentfixture.Store)
		original, err := agentfixture.ProcessCapability(t, ctx, selected)
		if err != nil {
			t.Fatal(err)
		}
		before, err := original.Evidence()
		if err != nil {
			t.Fatal(err)
		}
		if err := original.Release(ctx); err != nil {
			t.Fatal(err)
		}
		acquireCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		successor, err := selected.AcquireProcessCapability(acquireCtx, startupownership.AcquireRequest{
			OwnerID: "system-node-successor", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := successor.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
		after, err := successor.Evidence()
		if err != nil || after.AuthorityGeneration != before.AuthorityGeneration+1 || after.LeaseStamp() == before.LeaseStamp() {
			t.Fatalf("native process did not advance: before=%+v after=%+v err=%v", before, after, err)
		}
		return func() {
			join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := successor.Release(join); err != nil {
				t.Fatal(err)
			}
		}
	})
}
