package runforkpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestForkDeploymentCarriageKeepsInsertionTimestampBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			if _, err := db.Exec(`CREATE TABLE fan_out_intents (
				run_id TEXT, deployment_feed_id TEXT, cursor INTEGER, status TEXT,
				blocked_reason TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			insertedAt := time.Now().UTC().Add(time.Second)
			for _, cell := range []struct {
				name   string
				cursor int
				status fanoutobligation.Status
			}{
				{name: "empty", status: fanoutobligation.StatusClosed},
				{name: "inherited_prefix", cursor: 1, status: fanoutobligation.StatusOpen},
			} {
				t.Run(cell.name, func(t *testing.T) {
					key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
					if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents
						(run_id,deployment_feed_id,cursor,status,created_at,updated_at)
						VALUES ($1,$2,0,'open',$3,$3)`, key.RunID, key.DeploymentFeedID, insertedAt); err != nil {
						t.Fatal(err)
					}
					if err := carryRunForkDeploymentFeedPrefixTx(ctx, tx, key, forkDeploymentCarriage{
						cursor: cell.cursor, status: cell.status,
					}); err != nil {
						t.Fatal(err)
					}
					var cursor int
					var status string
					var createdRaw, updatedRaw any
					if err := tx.QueryRowContext(ctx, `SELECT cursor,status,created_at,updated_at FROM fan_out_intents
						WHERE run_id=$1 AND deployment_feed_id=$2`, key.RunID, key.DeploymentFeedID).
						Scan(&cursor, &status, &createdRaw, &updatedRaw); err != nil {
						t.Fatal(err)
					}
					created, present, err := sqliteTimeValue(createdRaw)
					if err != nil || !present {
						t.Fatalf("decode created_at: present=%t err=%v", present, err)
					}
					updated, present, err := sqliteTimeValue(updatedRaw)
					if err != nil || !present {
						t.Fatalf("decode updated_at: present=%t err=%v", present, err)
					}
					if cursor != cell.cursor || status != string(cell.status) || !created.Equal(updated) {
						t.Fatalf("carried feed cursor=%d status=%s created=%s updated=%s", cursor, status, created, updated)
					}
				})
			}
		})
	}
}
