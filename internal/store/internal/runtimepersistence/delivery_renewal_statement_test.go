package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresDeliveryRenewalRetainsIndependentMutationFences(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	for _, statement := range []string{
		`CREATE FUNCTION test_renewal_suppress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`,
		`CREATE FUNCTION test_renewal_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'renewal storage failure'; END $$`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, table, function, wantError string
		commit                           bool
	}{
		{name: "commit", commit: true},
		{name: "rollback"},
		{name: "attempt_lost", table: "event_delivery_attempts", function: "test_renewal_suppress", wantError: "lost claim"},
		{name: "lifecycle_lost", table: "event_deliveries", function: "test_renewal_suppress", wantError: "lost lifecycle owner"},
		{name: "storage_failure", table: "event_deliveries", function: "test_renewal_refuse", wantError: "renewal storage failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := seedNormalRunCompletionFixture(t, db, "active", "renewal/instance", "renewal")
			route := testAgentDeliveryRoute(t, fixture.RunID, "renewal-agent", "fixture/renewal-agent")
			event := commitPostgresDeliveryFixture(t, ctx, db, fixture.EventID, route)
			claimed := claimPostgresDeliveryFixture(t, ctx, db, event, route)
			store := postgresDeliveryFixtureStore(db)
			before := loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if tc.table != "" {
				// Install the fault only after lawful claim acquisition. Returning
				// NULL simulates a lost write without changing the admitted before-state.
				if _, err := tx.ExecContext(ctx, `CREATE TRIGGER test_renewal_fault BEFORE UPDATE ON `+tc.table+` FOR EACH ROW EXECUTE FUNCTION `+tc.function+`() `); err != nil {
					t.Fatal(err)
				}
			}
			effects := privaterunforkrevision.NewEffects()
			updated, err := postgresDeliveryAdapter.RenewClaim(ctx, tx, effects, claimed.Claim, 2*time.Minute)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("renewal error=%v, want %q", err, tc.wantError)
				}
				if tc.function == "test_renewal_suppress" && !errors.Is(err, runtimedelivery.ErrConflict) {
					t.Fatalf("lost mutation did not retain typed conflict: %v", err)
				}
				if !reflect.DeepEqual(updated, runtimedelivery.Snapshot{}) {
					t.Fatalf("failed renewal returned partial authority: %+v", updated)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if updated.ClaimExpiresAt.Sub(updated.UpdatedAt) != 2*time.Minute || !updated.UpdatedAt.After(before.UpdatedAt) {
					t.Fatalf("renewal did not apply the requested lease from the canonical clock: before=%+v after=%+v", before, updated)
				}
				unchanged := updated
				unchanged.ClaimExpiresAt, unchanged.UpdatedAt = before.ClaimExpiresAt, before.UpdatedAt
				if !reflect.DeepEqual(unchanged, before) {
					t.Fatalf("renewal changed non-time facts: before=%+v after=%+v", before, unchanged)
				}
				if _, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
					t.Fatal(err)
				}
			}
			if tc.commit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			want := before
			if tc.commit {
				want = updated
			}
			got := loadDeliverySnapshotFixture(t, context.WithoutCancel(ctx), store, event.ID(), route)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("renewal atomicity/readback differs: got=%+v want=%+v", got, want)
			}
		})
	}
}
