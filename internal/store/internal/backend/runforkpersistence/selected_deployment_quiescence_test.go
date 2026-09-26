package runforkpersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedDeploymentQuiescenceRequiresFeedPipelineAndReceiverExhaustionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE fan_out_intents (run_id TEXT, origin_kind TEXT, status TEXT, cursor INTEGER, deployment_feed_id TEXT)`,
				`CREATE TABLE fan_out_outcomes (run_id TEXT, deployment_feed_id TEXT, outcome_kind TEXT, event_id TEXT)`,
				`CREATE TABLE committed_replay_scopes (run_id TEXT, event_id TEXT)`,
				`CREATE TABLE event_receipts (event_id TEXT, subscriber_type TEXT, subscriber_id TEXT, outcome TEXT)`,
				`CREATE TABLE event_deliveries (run_id TEXT, status TEXT, continuation_handoff_at TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			runID, eventID := uuid.NewString(), uuid.NewString()
			check := func(want string) {
				t.Helper()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				err = requireSelectedDeploymentDrainedTx(ctx, tx, runID)
				if (want == "" && err != nil) || (want != "" && (err == nil || !strings.Contains(err.Error(), want))) {
					t.Fatalf("quiescence error=%v, want %q", err, want)
				}
			}
			check("") // A selected fork with no deployment feed retains its existing policy.
			feedID := uuid.NewString()
			if _, err := db.Exec(`INSERT INTO fan_out_intents VALUES ($1,'deployment','open',0,$2)`, runID, feedID); err != nil {
				t.Fatal(err)
			}
			check("unfinished feed")
			if _, err := db.Exec(`UPDATE fan_out_intents SET status='closed',cursor=1 WHERE run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			check("missing ordinal evidence")
			if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,outcome_kind) VALUES ($1,$2,'semantic_rejected')`, runID, feedID); err != nil {
				t.Fatal(err)
			}
			check("noncommitted ordinal")
			if _, err := db.Exec(`UPDATE fan_out_outcomes SET outcome_kind='committed',event_id=$2 WHERE run_id=$1`, runID, eventID); err != nil {
				t.Fatal(err)
			}
			check("committed ordinal without event scope")
			if _, err := db.Exec(`INSERT INTO committed_replay_scopes VALUES ($1,$2)`, runID, eventID); err != nil {
				t.Fatal(err)
			}
			check("unfinished event pipeline")
			if _, err := db.Exec(`INSERT INTO event_receipts VALUES ($1,'platform','pipeline','dead_letter')`, eventID); err != nil {
				t.Fatal(err)
			}
			check("unfinished event pipeline")
			if _, err := db.Exec(`UPDATE event_receipts SET outcome='reject' WHERE event_id=$1`, eventID); err != nil {
				t.Fatal(err)
			}
			check("unfinished event pipeline")
			if _, err := db.Exec(`UPDATE event_receipts SET outcome='success' WHERE event_id=$1`, eventID); err != nil {
				t.Fatal(err)
			}
			check("") // A successful no-route publication has no receiver delivery.
			if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,'pending',NULL)`, runID); err != nil {
				t.Fatal(err)
			}
			check("unfinished receiver delivery")
			if _, err := db.Exec(`UPDATE event_deliveries SET status='delivered' WHERE run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			check("unfinished receiver delivery")
			if _, err := db.Exec(`UPDATE event_deliveries SET continuation_handoff_at='2026-09-26T00:00:00Z' WHERE run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			check("")
		})
	}
}
