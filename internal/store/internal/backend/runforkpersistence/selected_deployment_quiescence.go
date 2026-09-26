package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

// requireSelectedDeploymentDrainedTx is the durable completion gate for an
// admitted selected deployment feed. Cursor completion alone cannot prove that
// the event pipeline or its receiver continuations have finished.
func requireSelectedDeploymentDrainedTx(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	if tx == nil || forkRunID == "" {
		return fmt.Errorf("selected deployment quiescence requires transaction and fork run")
	}
	var feeds int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, forkRunID).Scan(&feeds); err != nil {
		return fmt.Errorf("read selected deployment feed inventory: %w", err)
	}
	if feeds == 0 {
		return nil
	}
	checks := []struct {
		name  string
		query string
	}{
		{"unfinished feed", `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' AND status<>'closed'`},
		{"missing ordinal evidence", `SELECT COUNT(*) FROM fan_out_intents i
			WHERE i.run_id=$1 AND i.origin_kind='deployment' AND i.cursor<>
			(SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.deployment_feed_id=i.deployment_feed_id)`},
		{"noncommitted ordinal", `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND deployment_feed_id IS NOT NULL AND outcome_kind<>'committed'`},
	}
	for _, check := range checks {
		var count int
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&count); err != nil {
			return fmt.Errorf("read selected deployment %s: %w", check.name, err)
		}
		if count != 0 {
			if check.name == "unfinished feed" {
				var status string
				var cursor, cardinality int
				if err := tx.QueryRowContext(ctx, `SELECT status,cursor,cardinality FROM fan_out_intents
					WHERE run_id=$1 AND origin_kind='deployment' AND status<>'closed'
					ORDER BY CAST(deployment_feed_id AS TEXT) LIMIT 1`, forkRunID).Scan(&status, &cursor, &cardinality); err == nil {
					return fmt.Errorf("selected deployment %s: %d remaining (first status=%s cursor=%d cardinality=%d)", check.name, count, status, cursor, cardinality)
				}
			}
			return fmt.Errorf("selected deployment %s: %d remaining", check.name, count)
		}
	}
	missingScope, unfinishedPipeline, err := pipelinepersistence.SelectedDeploymentPipelineDrainCountsTx(ctx, tx, forkRunID)
	if err != nil {
		return err
	}
	if missingScope != 0 {
		return fmt.Errorf("selected deployment committed ordinal without event scope: %d remaining", missingScope)
	}
	if unfinishedPipeline != 0 {
		return fmt.Errorf("selected deployment unfinished event pipeline: %d remaining", unfinishedPipeline)
	}
	unfinished, err := storedelivery.UnfinishedRunDeliveryCountTx(ctx, tx, forkRunID)
	if err != nil {
		return err
	}
	if unfinished != 0 {
		return fmt.Errorf("selected deployment unfinished receiver delivery: %d remaining", unfinished)
	}
	return nil
}
