package runtimepersistence

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func finalizeFanOutGroupHistorySeed(t *testing.T, f *fanOutGroupHistoryFixture, seed fanOutOwnerFixture) {
	t.Helper()
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// The canonical source event is already revisioned. The low-level delivery
	// seed still needs its own exact revision before publication.
	effects := runforkrevision.NewEffects()
	if err := effects.AddFact(seed.runID, runforkrevision.FamilyEventDeliveries, seed.deliveryID); err != nil {
		t.Fatal(err)
	}
	results, err := finalizeRunForkRevisionMatrix(f.ctx, tx, f.postgres, effects)
	if err != nil || len(results) != 1 || !results[seed.runID].Changed {
		t.Fatalf("finalize exact source-event/delivery seed: results=%+v err=%v", results, err)
	}
	var total, exact, source int
	if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2`, seed.runID, results[seed.runID].Revision).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 AND family='event_deliveries' AND fact_key=$3 AND present=TRUE`, seed.runID, results[seed.runID].Revision, seed.deliveryID).Scan(&exact); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 AND family='events' AND fact_key=$3 AND present=TRUE`, seed.runID, results[seed.runID].Revision, seed.eventID).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if total != 1 || exact != 1 || source != 0 {
		t.Fatalf("seed captured other or missing facts: total=%d delivery=%d source=%d", total, exact, source)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Keep the original raw-seed path as a negative control: a later exact group
// writer must not capture an unrelated, unrevisioned fixture delivery.
func TestFanOutPublicationGroupRawSeedHistoryBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, seed, bundle := seedFanOutGroupHistoryFixture(t, backend, 2)
			var sourceCurrent, sourceHistory int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, seed.runID, seed.eventID).Scan(&sourceCurrent); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2`, seed.runID, seed.eventID).Scan(&sourceHistory); err != nil {
				t.Fatal(err)
			}
			if sourceCurrent != 1 || sourceHistory != 1 {
				t.Fatalf("canonical source event current=%d history=%d, want1/1", sourceCurrent, sourceHistory)
			}
			t.Logf("before any group preparation: canonical source event=%s current=%d history=%d", seed.eventID, sourceCurrent, sourceHistory)
			f = prepareFanOutGroupHistoryFixture(t, f, seed, bundle, 2, "producer/mixed.none", func(int) []byte { return []byte(`{}`) })
			tx, err := f.db.BeginTx(f.ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var current, history, otherMissing int
			if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND delivery_id=$2`, f.runID, seed.deliveryID).Scan(&current); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2`, f.runID, seed.deliveryID).Scan(&history); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND d.delivery_id<>$2 AND NOT EXISTS(SELECT 1 FROM run_fork_fact_revisions r WHERE r.run_id=d.run_id AND r.family='event_deliveries' AND r.fact_key=CAST(d.delivery_id AS TEXT) AND r.present=TRUE)`, f.runID, seed.deliveryID).Scan(&otherMissing); err != nil {
				t.Fatal(err)
			}
			if current != 1 || history != 0 || otherMissing != 0 {
				t.Fatalf("raw delivery current=%d history=%d other missing=%d, want1/0/0", current, history, otherMissing)
			}
			err = validateRunForkRevisionMatrix(f.ctx, tx, f.postgres, f.runID)
			if err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned event_deliveries facts") {
				t.Fatalf("expected exact raw-delivery history refusal: %v", err)
			}
			t.Logf("raw trigger delivery=%s current=%d history=%d other missing=%d; full validator=%v; no dispatch or fork planner invoked", seed.deliveryID, current, history, otherMissing, err)
		})
	}
}
