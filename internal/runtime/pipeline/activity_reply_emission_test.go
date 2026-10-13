package pipeline

import (
	"context"
	"fmt"
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

func TestJournaledActivityReplyAbsenceAndPrecedenceAcrossPublicationPaths(t *testing.T) {
	for _, journalReply := range []string{"", "reply-v1:journal-child"} {
		for _, inputReply := range []string{"", "reply-v1:unrelated-input"} {
			for _, deferred := range []bool{false, true} {
				t.Run(fmt.Sprintf("journal_%t/input_%t/deferred_%t", journalReply != "", inputReply != "", deferred), func(t *testing.T) {
					intent := testActivityIntent("https://example.test")
					if inputReply != "" {
						intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: inputReply}}
					}
					completed := time.Now().UTC().Truncate(time.Microsecond)
					record := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
					record.Status = ActivityAttemptStatusSucceeded
					record.ResultEventID = "00000000-0000-0000-0000-000000000321"
					record.ResultEventType = intent.SuccessEvent
					record.ResultPayload = map[string]any{"result": "recorded"}
					record.CompletedAt = &completed
					record.ReplyContextID = journalReply
					if err := validateActivityAttemptClaimIdentity(record, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); err != nil {
						t.Fatalf("otherwise matching existing attempt refused: %v", err)
					}
					bus := &recordingPipelineBus{}
					emissions := &pipelineEmissionPlan{}
					dispatcher := pipelineActivityDispatcher{coordinator: &PipelineCoordinator{bus: bus}}
					if deferred {
						dispatcher.emissions = emissions
					}
					ctx := events.WithDeliveryContext(context.Background(), events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:ambient"}})
					if err := dispatcher.publishExistingActivityAttempt(ctx, intent, record); err != nil {
						t.Fatal(err)
					}
					var published []events.Event
					var output events.DeliveryContext
					if deferred {
						published = emissions.immutableEvents()
						if len(published) == 1 {
							output = published[0].DeliveryContext()
						}
					} else {
						published = bus.publishes
						if len(bus.publishContexts) == 1 {
							output = bus.publishContexts[0]
						}
					}
					if len(published) != 1 || output.ReplyContextID() != journalReply || len(output.Joins) != 0 ||
						published[0].ID() != record.ResultEventID || !published[0].CreatedAt().Equal(completed) || string(published[0].Payload()) != `{"result":"recorded"}` {
						t.Fatalf("recorded absence/value or event facts changed: output=%+v events=%+v journal=%q", output, published, journalReply)
					}
				})
			}
		}
	}
}
