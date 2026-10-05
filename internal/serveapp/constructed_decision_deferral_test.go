package serveapp

import (
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedFieldlessGateDeferralBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, fields := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fields=%t", backend, fields), func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyConstructedGateMutationControl(t, fields))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "item.received", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{}, "idempotency_key": "fieldless-gate-seed",
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				var cardID string
				if err := rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{
					"card_id": cardID, "idempotency_key": "fieldless-gate-deferral",
					"until": time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano),
				}
				req := mailboxPrincipalRequest(t, rt, "mailbox.defer", params)
				mutation, err := mailboxCardMutation(req, params)
				if err != nil {
					t.Fatal(err)
				}
				ctx := servedControlProofAuthorActivityContext(t, rt)
				if _, replayed, err := rt.Runtime.Pipeline.CommitDecisionCardMutation(ctx, req, mutation); err != nil || replayed {
					t.Fatalf("real constructed gate deferral: replay=%t err=%v", replayed, err)
				}
				var replay map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", params, &replay)
				if replay["idempotency_replayed"] != true {
					t.Fatalf("served replay repeated gate deferral: %v", replay)
				}
				requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "mailbox.defer", "fieldless-gate-deferral", 1)
				var fieldRows int
				wantRows := 0
				if fields {
					wantRows = 1
				}
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&fieldRows); err != nil || fieldRows != wantRows {
					t.Fatalf("deferral changed declared field-companion census: rows=%d want=%d err=%v", fieldRows, wantRows, err)
				}
			})
		}
	}
}

func TestServedConstructedDecisionCardDeferralBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string {
				return canonicalrouting.CopyRootIngressServedDecisionControl(t)
			})
			fixture := seedServedDecisionCardFixture(t, rt)
			params := map[string]any{
				"card_id": fixture.CardID, "idempotency_key": "constructor-deferral",
				"until": time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano),
			}
			req := mailboxPrincipalRequest(t, rt, "mailbox.defer", params)
			mutation, err := mailboxCardMutation(req, params)
			if err != nil {
				t.Fatal(err)
			}
			ctx := servedControlProofAuthorActivityContext(t, rt)
			if _, replayed, err := rt.Runtime.Pipeline.CommitDecisionCardMutation(ctx, req, mutation); err != nil || replayed {
				t.Fatalf("constructed decision deferral: replay=%t err=%v", replayed, err)
			}
			var replay map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", params, &replay)
			if replay["idempotency_replayed"] != true {
				t.Fatalf("served replay repeated committed deferral: %v", replay)
			}
			requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "mailbox.defer", "constructor-deferral", 1)
		})
	}
}
