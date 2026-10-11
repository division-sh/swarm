package eventrecord

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

// CorruptReplayEventTargetRouteTx cuts store.event_record.canonical_readback:
// valid JSON which is not a route object must be refused by operator replay.
func CorruptReplayEventTargetRouteTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) error {
	return setReplayEventTargetRouteTx(ctx, tx, postgres, eventID, []byte(`"bad"`))
}

func RestoreReplayEventTargetRouteTx(ctx context.Context, tx *sql.Tx, postgres bool, original events.Event) error {
	route, err := json.Marshal(original.Envelope().Target)
	if err != nil {
		return err
	}
	return setReplayEventTargetRouteTx(ctx, tx, postgres, original.ID(), route)
}

func setReplayEventTargetRouteTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string, route []byte) error {
	query := `UPDATE events SET target_route = ? WHERE event_id = ?`
	if postgres {
		query = `UPDATE events SET target_route = $1::jsonb WHERE event_id = $2::uuid`
	}
	result, err := tx.ExecContext(ctx, query, string(route), eventID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("replay target-route fault changed %d rows, want one exact event", changed)
	}
	return nil
}
