package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// The target is admitted against a real receiving row. Only after the durable
// claim is observed does the test make that exact row unavailable.
func TestReceiverCompositionFailureSettlementBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend)+"/unavailable_after_publish", func(t *testing.T) {
			root := canonicalrouting.CopyReceiverUnavailableAfterPublish(t)
			reached := make(chan runtimedelivery.Claim, 1)
			release := make(chan struct{})
			var once sync.Once
			hook := func(ctx context.Context, _ string, evt events.Event) error {
				if evt.Type() != "work.requested" {
					return nil
				}
				claim, ok := runtimedelivery.ClaimFromContext(ctx)
				if !ok {
					return fmt.Errorf("receiver test barrier requires the actual claim")
				}
				reached <- claim
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root, hook)
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"seed": true}, "idempotency_key": "receiver-seed",
			})
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
			params := map[string]any{"event_name": "work.requested", "run_id": seed.RunID,
				"source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "receiver-reject"}
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			var claim runtimedelivery.Claim
			select {
			case claim = <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("receiver claim was never reached")
			}
			var rawTarget, status, beforeFields string
			if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT), status FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&rawTarget, &status); err != nil {
				t.Fatal(err)
			}
			var owner events.DeliveryTargetOwnership
			if err := json.Unmarshal([]byte(rawTarget), &owner); err != nil {
				t.Fatal(err)
			}
			if !owner.ExistingEntity() || status != "in_progress" {
				t.Fatalf("not a claimed existing receiver: %s %s", rawTarget, status)
			}
			if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, seed.RunID, owner.Route().EntityID).Scan(&beforeFields); err != nil {
				t.Fatal(err)
			}
			// Fault injection, not a producer fixture: invalidate an already-admitted
			// receiving state after publication, without altering its ownership stamp.
			result, err := rt.DB.Exec(`UPDATE entity_state SET current_state='done' WHERE run_id=$1 AND entity_id=$2`, seed.RunID, owner.Route().EntityID)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("invalidate exact receiver: %d %v", n, err)
			}
			once.Do(func() { close(release) })
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			if err := rt.DB.QueryRow(`SELECT status FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "dead_letter" {
				t.Fatalf("receiver rejection left claimed work: %s", status)
			}
			var outcomes int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM event_delivery_outcomes WHERE delivery_id=$1 AND outcome='dead_letter'`, claim.DeliveryID()).Scan(&outcomes); err != nil {
				t.Fatal(err)
			}
			if outcomes != 1 {
				t.Fatalf("terminal receiver outcomes = %d, want one", outcomes)
			}
			var afterFields, afterTarget string
			if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, seed.RunID, owner.Route().EntityID).Scan(&afterFields); err != nil {
				t.Fatal(err)
			}
			if beforeFields != afterFields {
				t.Fatalf("rejected receiver mutated business fields: %s -> %s", beforeFields, afterFields)
			}
			if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&afterTarget); err != nil {
				t.Fatal(err)
			}
			if afterTarget != rawTarget {
				t.Fatal("receiver failure replaced its committed target")
			}
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if duplicate.EventID != published.EventID {
				t.Fatal("exact duplicate created another event")
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM event_delivery_outcomes WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&outcomes); err != nil {
				t.Fatal(err)
			}
			if outcomes != 1 {
				t.Fatalf("duplicate receiver settlement: %d outcomes", outcomes)
			}
			waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, published.EventID, "platform", "pipeline", "dead_letter", 1)
			requireReceiverPublicReadback(t, rt, seed.RunID)
		})
	}
}
