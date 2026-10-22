package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// This executes refusal publication and its terminal receipt at all three
// levels. Deadline, selected-child and recovery cells remain separate.
func TestIssue642DepthThreeExecutedReplyRefusalsBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyNestedNodeReplyDepth3(t), backend, true)
			step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "same-business-key"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
			h.publishAndWait(step, 20*time.Second)
			h.waitForCatalogStoreQuiescence(20 * time.Second)
			proveNestedReplyRowAndReturnBindings(t, h, 1)
			var selected interface {
				bus.PreparedPublishEventReader
				replycontext.Store
			} = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			rows, err := storetest.ReadReplyContextRecords(h.ctx, selected, catalogRuntimeRunID)
			if err != nil || len(rows) != 3 {
				t.Fatalf("read exact settled depth-three chain: rows=%+v error=%v", rows, err)
			}
			levels := map[string]replycontext.Record{}
			for _, row := range rows {
				level := map[string]string{"provider": "Q1", "t": "Q2", "u": "Q3"}[row.ProviderFlowID]
				if level == "" || levels[level].ID != "" || row.State != replycontext.StateTerminal || row.RunID != catalogRuntimeRunID {
					t.Fatalf("executed refusal lacks one actual terminal row per level: %+v", row)
				}
				levels[level] = row
			}
			for _, level := range []string{"Q1", "Q2", "Q3"} {
				accepted, found, err := selected.LoadPreparedPublishEvent(h.ctx, levels[level].AcceptedReplyEventID)
				if err != nil || !found {
					t.Fatalf("read actual accepted %s response: found=%t error=%v", level, found, err)
				}
				for _, target := range []string{"Q1", "Q2", "Q3"} {
					name, class, code := "NR08_executed_wrong_level/"+level+"_to_"+target, failures.ClassStaleArrival, "stale_arrival"
					if target == level {
						name, class, code = "NR06_executed_second_reply/"+level, failures.ClassReplyAlreadyTerminal, "reply_already_terminal"
					}
					t.Run(name, func(t *testing.T) {
						original := accepted.Event.Event()
						response := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), original.Type(), original.Producer(), "", original.Payload(), original.ChainDepth(),
							events.LineageFromEvent(original), events.EnvelopeForSourceRoute(events.EventEnvelope{}, original.Envelope().Source), original.RoutingSource(), time.Now().UTC())
						ctx, cancel := context.WithTimeout(events.WithDeliveryContext(h.ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: levels[target].ID}}), 20*time.Second)
						defer cancel()
						before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
						if err != nil {
							t.Fatal(err)
						}
						if err := h.rt.Bus.PublishAndWait(ctx, response); err != nil {
							t.Fatalf("execute %s response publication: %v", name, err)
						}
						stored, found, err := selected.LoadPreparedPublishEvent(ctx, response.ID())
						if err != nil || !found || len(stored.DeliveryRoutes) != 0 {
							t.Fatalf("refused response created executable recipients: %+v found=%t error=%v", stored, found, err)
						}
						receipt, receiptErr := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, selected, response.ID())
						count, countErr := storetest.CountDeadLettersForOriginalEvent(ctx, selected, response.ID())
						var failure failures.Envelope
						decodeErr := json.Unmarshal(receipt.Failure, &failure)
						if receiptErr != nil || countErr != nil || decodeErr != nil || !receipt.Found || receipt.Outcome != "dead_letter" ||
							failure.Class != class || failure.Detail.Code != code || count != 1 {
							t.Fatalf("response did not settle its exact refusal: receipt=%+v failure=%+v dead_letters=%d errors=%v/%v/%v want=%s/%s", receipt, failure, count, receiptErr, countErr, decodeErr, class, code)
						}
						afterRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, catalogRuntimeRunID)
						finished, finishedErr := storetest.ReadLifecycleEventCardinality(ctx, selected, catalogRuntimeRunID, "request.finished")
						after, domainErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
						if rowsErr != nil || finishedErr != nil || domainErr != nil || !reflect.DeepEqual(rows, afterRows) || finished != 1 ||
							!reflect.DeepEqual(before["entity_state"], after["entity_state"]) || !reflect.DeepEqual(before["entity_mutations"], after["entity_mutations"]) {
							t.Fatalf("refused response changed another level or business state: finished=%d errors=%v/%v/%v", finished, rowsErr, finishedErr, domainErr)
						}
					})
				}
			}
		})
	}
}
