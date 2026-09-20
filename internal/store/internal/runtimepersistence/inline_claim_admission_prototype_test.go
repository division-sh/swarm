package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestInlineClaimPrototypeFreshAdmissionAndSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			route := testEntitylessNodeDeliveryRoute("inline-prototype")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "inline.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			counts := func() []int {
				t.Helper()
				var result []int
				for _, table := range []string{"run_fork_revisions", "run_fork_fact_revisions", "author_activity_occurrences", "event_delivery_outcomes"} {
					var n int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
						t.Fatal(err)
					}
					result = append(result, n)
				}
				return result
			}
			beforeCounts := counts()
			for i := 0; i < 3; i++ {
				remaining, err := selected.AdmitInlineClaim(ctx, claimed.Claim)
				if err != nil || remaining <= 0 || remaining > runtimedelivery.DefaultLeaseTTL {
					t.Fatalf("admission remaining=%s err=%v", remaining, err)
				}
			}
			after, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeCounts, counts()) {
				t.Fatalf("read-only admission changed delivery/history: err=%v before=%+v after=%+v", err, before, after)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := selected.AdmitInlineClaim(cancelled, claimed.Claim); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled admission=%v", err)
			}

			// A real short native lease expires AFTER successful admission. Its
			// historical fact is finalized; no clock override or malformed row.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			effects := privaterunforkrevision.NewEffects()
			if backend == "postgres" {
				_, err = postgresDeliveryAdapter.RenewClaim(ctx, tx, effects, claimed.Claim, 20*time.Millisecond)
				if err == nil {
					_, err = privaterunforkrevision.FinalizePostgres(ctx, tx, effects)
				}
			} else {
				_, err = sqliteDeliveryAdapter.RenewClaim(ctx, tx, effects, claimed.Claim, 20*time.Millisecond)
				if err == nil {
					_, err = privaterunforkrevision.FinalizeSQLite(ctx, tx, effects)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(30 * time.Millisecond)
			if _, err := selected.AdmitInlineClaim(ctx, claimed.Claim); !errors.Is(err, runtimedelivery.ErrConflict) {
				t.Fatalf("expired admission=%v", err)
			}
			expiredCounts := counts()
			if _, err := selected.SettleSuccess(ctx, claimed.Claim, []string{"forbidden"}, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); !errors.Is(err, runtimedelivery.ErrConflict) {
				t.Fatalf("stale admission authorized settlement: %v", err)
			}
			if !reflect.DeepEqual(expiredCounts, counts()) {
				t.Fatal("rejected expired settlement mutated history/outcomes")
			}
		})
	}
}
