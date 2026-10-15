package pipelinepersistence

import (
	"context"
	"database/sql"
)

type FanOutRunProgressStorage struct{ Total, Cursor int }

type FanOutRunClaimMetadataStorage struct {
	Cursor, Cardinality                       int
	Status, Owner, Generation, Lease, Capsule string
}

type FanOutRunBlockedClaimStorage struct {
	Diagnostic []byte
	Released   bool
}

type ServingLifetimeMetadataStorage struct {
	Cursor, Generation, Retry int
	Status                    string
}

func ReadServingLifetimeMetadataStorageTx(ctx context.Context, tx *sql.Tx, runID string) (ServingLifetimeMetadataStorage, error) {
	var out ServingLifetimeMetadataStorage
	err := tx.QueryRowContext(ctx, `SELECT cursor,status,claim_generation,CASE WHEN retry_ready_at IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Cursor, &out.Status, &out.Generation, &out.Retry)
	return out, err
}

func ReadServingLifetimeClaimOwnerStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(claim_owner,'') FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&owner)
	return owner, err
}

type FanOutSourceFootprintMetadataStorage struct {
	Cursor, Cardinality int
	Status              string
	Diagnostic          []byte
}

func ReadFanOutSourceFootprintMetadataStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutSourceFootprintMetadataStorage, error) {
	var out FanOutSourceFootprintMetadataStorage
	err := tx.QueryRowContext(ctx, `SELECT cursor,cardinality,status,blocked_reason FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Cursor, &out.Cardinality, &out.Status, &out.Diagnostic)
	return out, err
}

func ReadFanOutRunBlockedClaimStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutRunBlockedClaimStorage, error) {
	var out FanOutRunBlockedClaimStorage
	err := tx.QueryRowContext(ctx, `SELECT blocked_reason,
		claim_owner IS NULL AND lease_expires_at IS NULL FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Diagnostic, &out.Released)
	return out, err
}

func ReadFanOutRunCapsuleWireTx(ctx context.Context, tx *sql.Tx, runID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func ReadFanOutRunClaimMetadataStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutRunClaimMetadataStorage, error) {
	var out FanOutRunClaimMetadataStorage
	err := tx.QueryRowContext(ctx, `SELECT cursor,cardinality,status,claim_owner,
		CAST(claim_generation AS TEXT),CAST(lease_expires_at AS TEXT),CAST(capsule AS TEXT)
		FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Cursor, &out.Cardinality, &out.Status, &out.Owner, &out.Generation, &out.Lease, &out.Capsule)
	if err != nil {
		return FanOutRunClaimMetadataStorage{}, err
	}
	return out, nil
}

func ReadRunFanOutProgressStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutRunProgressStorage, error) {
	var out FanOutRunProgressStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),SUM(cursor) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&out.Total, &out.Cursor)
	if err != nil {
		return FanOutRunProgressStorage{}, err
	}
	return out, nil
}

func CountLeasedRunFanOutStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND claim_owner IS NOT NULL`, runID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
