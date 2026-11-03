package durabledata

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

type liveHeadDeclaration struct {
	flowPath  string
	eventName string
}

// A selected fork retains its immutable construction pin. Only its live read
// follows the declaration head; paused and terminal reads use the fork cut.
func (o *Owner) readRunResourceBindingTx(ctx context.Context, tx *sql.Tx, runID string, ref runtimedata.DeclarationRef) (runtimedata.SchemaDigest, runtimedata.VersionID, error) {
	var digest, version sql.NullString
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT CASE WHEN r.origin_kind='fork_materialization' AND r.status='live' THEN d.schema_digest ELSE p.schema_digest END,
		       CASE WHEN r.origin_kind='fork_materialization' AND r.status='live' THEN h.version_id ELSE p.version_id END
		FROM runs r
		LEFT JOIN resource_version_pins p ON p.run_id=r.run_id AND p.flow_path=%s AND p.event_name=%s
		LEFT JOIN resource_bundle_declarations d ON d.bundle_hash=r.bundle_hash AND d.flow_path=%s AND d.event_name=%s
		LEFT JOIN resource_heads h ON h.flow_path=d.flow_path AND h.event_name=d.event_name
		WHERE r.run_id=%s
	`, 5), ref.FlowPath, ref.EventName, ref.FlowPath, ref.EventName, runID).Scan(&digest, &version)
	if err != nil {
		return "", "", err
	}
	if !digest.Valid || !version.Valid {
		return "", "", sql.ErrNoRows
	}
	return runtimedata.SchemaDigest(digest.String), runtimedata.VersionID(version.String), nil
}

// BindLiveRunHeadsTx selects current declaration heads for a served run in the
// same transaction that creates or resumes it. Import locks the same head rows.
func BindLiveRunHeadsTx(ctx context.Context, tx *sql.Tx, runID, bundleHash string, postgres bool, now time.Time) error {
	var originKind, storedBundle, status string
	var createdWithData bool
	if err := tx.QueryRowContext(ctx, `SELECT r.origin_kind,r.bundle_hash,r.status,EXISTS (
		SELECT 1 FROM resource_run_creation_operations c WHERE c.run_id=r.run_id
	) FROM runs r WHERE r.run_id=$1`, runID).Scan(&originKind, &storedBundle, &status, &createdWithData); err != nil {
		return fmt.Errorf("read live run data binding origin: %w", err)
	}
	if originKind != "deployment" || createdWithData {
		return nil
	}
	if storedBundle != bundleHash || status != "live" {
		return fmt.Errorf("live run data binding requires its exact active source")
	}
	rows, err := tx.QueryContext(ctx, `SELECT flow_path,event_name FROM resource_bundle_declarations
		WHERE bundle_hash=$1 ORDER BY flow_path,event_name`, bundleHash)
	if err != nil {
		return fmt.Errorf("list live run data declarations: %w", err)
	}
	var declarations []liveHeadDeclaration
	for rows.Next() {
		var declaration liveHeadDeclaration
		if err := rows.Scan(&declaration.flowPath, &declaration.eventName); err != nil {
			rows.Close()
			return fmt.Errorf("read live run data declaration: %w", err)
		}
		declarations = append(declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("list live run data declarations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close live run data declarations: %w", err)
	}

	for _, declaration := range declarations {
		query := `SELECT version_id FROM resource_heads WHERE flow_path=$1 AND event_name=$2`
		if postgres {
			query += ` FOR UPDATE`
		}
		var versionID sql.NullString
		if err := tx.QueryRowContext(ctx, query, declaration.flowPath, declaration.eventName).Scan(&versionID); err != nil {
			return fmt.Errorf("lock live run data head: %w", err)
		}
		if !versionID.Valid {
			continue
		}
		var schemaDigest string
		var prunedAt sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT schema_digest,pruned_at FROM resource_versions
			WHERE version_id=$1 AND flow_path=$2 AND event_name=$3`, versionID.String,
			declaration.flowPath, declaration.eventName).Scan(&schemaDigest, &prunedAt); err != nil {
			return fmt.Errorf("read live run data head version: %w", err)
		}
		if prunedAt.Valid {
			return fmt.Errorf("live run data head references pruned version %s", versionID.String)
		}
		if err := upsertLiveRunHeadPinTx(ctx, tx, runID, declaration, schemaDigest, versionID.String, now); err != nil {
			return err
		}
	}
	return nil
}

func upsertLiveRunHeadPinTx(ctx context.Context, tx *sql.Tx, runID string, declaration liveHeadDeclaration, schemaDigest, versionID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO resource_version_pins
		(run_id,flow_path,event_name,schema_digest,version_id,selection,pinned_at)
		VALUES ($1,$2,$3,$4,$5,'explicit',$6)
		ON CONFLICT (run_id,flow_path,event_name) DO UPDATE SET
			schema_digest=excluded.schema_digest,version_id=excluded.version_id,pinned_at=excluded.pinned_at
		WHERE resource_version_pins.version_id<>excluded.version_id`, runID, declaration.flowPath,
		declaration.eventName, schemaDigest, versionID, now)
	if err != nil {
		return fmt.Errorf("bind live run data head: %w", err)
	}
	return nil
}

func advanceLiveRunHeadPinsTx(ctx context.Context, tx *sql.Tx, declaration liveHeadDeclaration, schemaDigest, versionID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO resource_version_pins
		(run_id,flow_path,event_name,schema_digest,version_id,selection,pinned_at)
		SELECT r.run_id,$1,$2,$3,$4,'explicit',$5 FROM runs r
		JOIN resource_bundle_declarations d ON d.bundle_hash=r.bundle_hash
			AND d.flow_path=$1 AND d.event_name=$2
		WHERE r.origin_kind='deployment' AND r.status='live'
			AND NOT EXISTS (SELECT 1 FROM resource_run_creation_operations c WHERE c.run_id=r.run_id)
		ON CONFLICT (run_id,flow_path,event_name) DO UPDATE SET
			schema_digest=excluded.schema_digest,version_id=excluded.version_id,pinned_at=excluded.pinned_at
		WHERE resource_version_pins.version_id<>excluded.version_id`, declaration.flowPath,
		declaration.eventName, schemaDigest, versionID, now)
	if err != nil {
		return fmt.Errorf("advance live run data head bindings: %w", err)
	}
	return nil
}
