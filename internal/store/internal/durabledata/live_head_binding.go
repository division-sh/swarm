package durabledata

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type liveHeadDeclaration struct {
	flowPath  string
	eventName string
}

// BindLiveRunHeadsTx selects current declaration heads for a served run in the
// same transaction that creates or resumes it. Import locks the same head rows.
func BindLiveRunHeadsTx(ctx context.Context, tx *sql.Tx, runID, bundleHash string, postgres bool, now time.Time) error {
	var originKind string
	var createdWithData bool
	if err := tx.QueryRowContext(ctx, `SELECT r.origin_kind,EXISTS (
		SELECT 1 FROM resource_run_creation_operations c WHERE c.run_id=r.run_id
	) FROM runs r WHERE r.run_id=$1`, runID).Scan(&originKind, &createdWithData); err != nil {
		return fmt.Errorf("read live run data binding origin: %w", err)
	}
	if originKind != "deployment" || createdWithData {
		return nil
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
