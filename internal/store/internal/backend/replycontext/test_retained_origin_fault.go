package replycontextstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func CorruptOpenReplyOriginForTest(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, runID, contextID, origin string) error {
	query := `UPDATE reply_contexts SET origin_route=? WHERE reply_context_id=? AND run_id=? AND state='open' AND accepted_reply_event_id IS NULL`
	if postgres {
		query = `UPDATE reply_contexts SET origin_route=$1::jsonb WHERE reply_context_id=$2 AND run_id=$3::uuid AND state='open' AND accepted_reply_event_id IS NULL`
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, query, origin, contextID, runID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return fmt.Errorf("corrupt exact retained reply entry: rows=%d err=%v", rows, err)
		}
		return nil
	}); err != nil {
		return err
	}
	return attempt.AddFact(runID, runforkrevision.FamilyReplyContexts, contextID)
}
