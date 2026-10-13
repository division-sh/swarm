package pipelinepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutContinuationRequiresExactHandlerInputBeforeSQL(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, cell := range []struct{ stored, incoming string }{
			{"original-reply", "foreign-reply"}, {"original-reply", ""}, {"", "original-reply"},
		} {
			request := a2CollectionRequest(t, fanoutobligation.SourceEventPayloadField, rc.CatalogTypeReference{Type: "[text]"}, 1)
			ctx := context.Background()
			if cell.stored != "" {
				request.Capsule.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: cell.stored}}
			}
			if cell.incoming != "" {
				ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: cell.incoming}})
			}
			// A missing transaction is deliberate: refusal must precede all SQL.
			err := commitFanOutIntentTx(ctx, nil, postgres, nil, request, request.Key.RunID, nil, request.Capsule.Lineage.ParentEventID, time.Now())
			if err == nil || !strings.Contains(err.Error(), "differs from the admitted handler input") {
				t.Fatalf("substituted/erased/invented input was admitted: %+v postgres=%t error=%v", cell, postgres, err)
			}
		}
	}
}

func TestFanOutContinuationCodecRetainsOpaqueReplyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := a2CollectionDatabase(t, backend)
			request := a2CollectionRequest(t, fanoutobligation.SourceEventPayloadField, rc.CatalogTypeReference{Type: "[text]"}, 1)
			request.Capsule.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:captured-input"}}
			a2CollectionSeedRun(t, db, request)
			a2CollectionTrigger(t, db, request, []byte(`{"items":["one"]}`))
			stored := a2PersistCollection(t, db, backend, request, nil)
			if !stored.Request.Capsule.Equal(request.Capsule) || stored.Request.Capsule.Context.ReplyContextID() != request.Capsule.Context.ReplyContextID() {
				t.Fatalf("writer/codec/readback lost the frozen input: %+v", stored.Request.Capsule.Context)
			}
		})
	}
}
