package pipeline

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func nativeFanOutIntentObservationForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, trigger events.Event, node identity.ExecutableNode) fanoutobligation.Intent {
	t.Helper()
	plans := pc.SemanticSource().FanOutPlansForHandler(node, string(trigger.Type()))
	if len(plans) != 1 {
		t.Fatalf("native fan-out observation requires one exact compiled site, got %d", len(plans))
	}
	plan := plans[0]
	rows, err := fixture.FanOutStorage(ctx, trigger.RunID(), trigger.ID(), node, plan.Ref.ElementRef)
	if err != nil || len(rows) != 1 {
		t.Fatalf("native fan-out rows=%d error=%v, want one exact occurrence", len(rows), err)
	}
	row := rows[0]
	if row.SemanticDigest != plan.Ref.SemanticDigest {
		t.Fatal("native fan-out storage contradicts its compiled digest")
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(row.Capsule, &capsule); err != nil {
		t.Fatal(err)
	}
	// This is bounded readback used as a typed pump-unit input, not a claim or
	// permission to execute the stored intent through a reconstructed owner.
	return fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{Key: fanoutobligation.IntentKey{RunID: trigger.RunID(), TriggeringDeliveryID: row.DeliveryID, ElementRef: plan.Ref.ElementRef}, PlanRef: plan.Ref, Source: row.Source, Cardinality: row.Cardinality, Capsule: capsule},
		Source:  row.Source, Cursor: row.Cursor, Status: fanoutobligation.Status(row.Status), NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: trigger.CreatedAt(), UpdatedAt: trigger.CreatedAt(),
	}
}

func nativeFanOutTriggerRouteForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, node identity.ExecutableNode) events.DeliveryRoute {
	t.Helper()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(event.TargetRoute())}
	if err := fixture.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	return route
}
