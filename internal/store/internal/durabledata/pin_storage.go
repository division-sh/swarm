package durabledata

import (
	"context"
	"database/sql"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

func (o *Owner) insertPinTx(ctx context.Context, tx *sql.Tx, pin runtimedata.Pin, at time.Time) error {
	_, err := tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_version_pins
		(run_id, flow_path, event_name, schema_digest, version_id, selection, pinned_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s)
	`, 7), pin.RunID, pin.Declaration.FlowPath, pin.Declaration.EventName, pin.SchemaDigest, pin.VersionID, pin.Selection, at)
	return err
}
