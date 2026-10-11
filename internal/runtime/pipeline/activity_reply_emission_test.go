package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

func TestJournaledActivityDeferredEmissionRetainsOwnReply(t *testing.T) {
	intent := testActivityIntent("https://example.test")
	intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:unrelated-intent"}}
	completed := time.Now().UTC().Truncate(time.Microsecond)
	record := ActivityAttemptRecord{Attempt: 1, Status: ActivityAttemptStatusSucceeded,
		ResultEventID: "00000000-0000-0000-0000-000000000321", ResultEventType: intent.SuccessEvent,
		ResultPayload: map[string]any{"result": "recorded"}, CompletedAt: &completed, ReplyContextID: "reply-v1:recorded-child"}
	emissions := &pipelineEmissionPlan{}
	if err := (pipelineActivityDispatcher{emissions: emissions}).publishJournaledActivityResult(context.Background(), intent, record); err != nil {
		t.Fatal(err)
	}
	out := emissions.immutableEvents()
	if len(out) != 1 || out[0].DeliveryContext().ReplyContextID() != record.ReplyContextID || out[0].ID() != record.ResultEventID || !out[0].CreatedAt().Equal(completed) {
		t.Fatalf("deferred result lost its own recorded return: %+v", out)
	}
}
