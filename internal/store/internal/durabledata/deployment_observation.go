package durabledata

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/durabledata"
)

func ReadRootReadyResourcePinStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&version)
	return version, err
}

func ReadPortfolioAccountResourcePinStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='portfolio' AND event_name='portfolio/account.registered'`, runID).Scan(&version)
	return version, err
}

func ReadPortfolioResourcePinStorageTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='portfolio' AND event_name=$2`, runID, eventName).Scan(&version)
	return version, err
}

func CountRunResourcePinsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_version_pins WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountRunDataCreationOperationsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_run_creation_operations WHERE run_id=$1`, runID).Scan(&count)
	return count, err
}

func CountResourceVersionStorageTx(ctx context.Context, tx *sql.Tx, versionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_versions WHERE version_id=$1`, versionID).Scan(&count)
	return count, err
}

type ResourceVersionMetadataStorage struct {
	BusinessKey string
	RowCount    int
}

func ReadResourceVersionMetadataStorageTx(ctx context.Context, tx *sql.Tx, versionID string) (ResourceVersionMetadataStorage, error) {
	var out ResourceVersionMetadataStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(business_key_field,''),row_count FROM resource_versions WHERE version_id=$1`, versionID).Scan(&out.BusinessKey, &out.RowCount)
	return out, err
}

func ReadResourceVersionJSONLStorageTx(ctx context.Context, tx *sql.Tx, versionID string) ([]byte, error) {
	var out []byte
	err := tx.QueryRowContext(ctx, `SELECT canonical_jsonl FROM resource_versions WHERE version_id=$1`, versionID).Scan(&out)
	return out, err
}

func CountRootReadyResourceVersionsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_versions WHERE flow_path='.' AND event_name='root.ready'`).Scan(&count)
	return count, err
}

func CountRootReadyResourceHeadHistoryStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_head_history WHERE flow_path='.' AND event_name='root.ready'`).Scan(&count)
	return count, err
}

type RootReadyResourceHeadStorage struct {
	Version  string
	Revision int
}

func ReadRootReadyResourceHeadStorageTx(ctx context.Context, tx *sql.Tx) (RootReadyResourceHeadStorage, error) {
	var out RootReadyResourceHeadStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(version_id, ''), revision FROM resource_heads WHERE flow_path='.' AND event_name='root.ready'`).Scan(&out.Version, &out.Revision)
	return out, err
}

func ReadResourceHeadVersionStorageTx(ctx context.Context, tx *sql.Tx, ref durabledata.DeclarationRef) (string, error) {
	var version string
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM resource_heads WHERE flow_path=$1 AND event_name=$2`, ref.FlowPath, ref.EventName).Scan(&version)
	return version, err
}

func CountResourceSourceInvocationStorageTx(ctx context.Context, tx *sql.Tx, sourceID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_source_invocations WHERE source_invocation_id=$1`, sourceID).Scan(&count)
	return count, err
}

func CountGlobalResourceSourceInvocationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_source_invocations`).Scan(&count)
	return count, err
}

func CountGlobalDataCreationOperationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_run_creation_operations`).Scan(&count)
	return count, err
}

func CountGlobalResourceVersionsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_versions`).Scan(&count)
	return count, err
}

func CountGlobalResourcePinsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_version_pins`).Scan(&count)
	return count, err
}
