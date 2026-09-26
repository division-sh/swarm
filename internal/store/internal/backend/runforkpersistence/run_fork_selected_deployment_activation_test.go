package runforkpersistence

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedForkExecutedCountUsesDurableOriginsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE run_fork_selected_contract_executions (fork_run_id TEXT, fork_event_id TEXT)`,
				`CREATE TABLE fan_out_intents (run_id TEXT, origin_kind TEXT, deployment_feed_id TEXT)`,
				`CREATE TABLE fan_out_outcomes (run_id TEXT, deployment_feed_id TEXT, outcome_kind TEXT, event_id TEXT, source_event_id TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			runID, feedID := uuid.NewString(), uuid.NewString()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if present, err := selectedDeploymentFeedPresentTx(ctx, tx, runID); err != nil || present {
				t.Fatalf("feed before materialization: present=%t err=%v", present, err)
			}
			if err := requireSelectedForkMaterializedWorkTx(ctx, tx, runID, 0); err == nil {
				t.Fatal("entityless selected fork without deployment work was admitted")
			}
			if err := requireSelectedForkMaterializedWorkTx(ctx, tx, runID, 1); err != nil {
				t.Fatalf("ordinary entity-backed selected fork was rejected: %v", err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents VALUES ($1,'deployment',$2)`, runID, feedID); err != nil {
				t.Fatal(err)
			}
			if present, err := selectedDeploymentFeedPresentTx(ctx, tx, runID); err != nil || !present {
				t.Fatalf("feed after materialization: present=%t err=%v", present, err)
			}
			if err := requireSelectedForkMaterializedWorkTx(ctx, tx, runID, 0); err != nil {
				t.Fatalf("entityless selected deployment fork was rejected: %v", err)
			}
			selectedID, feedEventID, inheritedID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_executions VALUES ($1,$2)`, runID, selectedID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES ($1,$2,'committed',$3,NULL)`, runID, feedID, feedEventID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES ($1,$2,'committed',NULL,$3)`, runID, feedID, inheritedID); err != nil {
				t.Fatal(err)
			}
			if count, err := selectedForkDurableExecutedEventCountTx(ctx, tx, runID); err != nil || count != 2 {
				t.Fatalf("durable executed count=%d want=2 err=%v", count, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_executions VALUES ($1,$2)`, runID, feedEventID); err != nil {
				t.Fatal(err)
			}
			if _, err := selectedForkDurableExecutedEventCountTx(ctx, tx, runID); err == nil {
				t.Fatal("one event assigned to both execution origins")
			}
		})
	}
}
