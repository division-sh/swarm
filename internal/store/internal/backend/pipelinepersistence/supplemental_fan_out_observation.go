package pipelinepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type SupplementalOutcomeCardinalityStorage struct{ Outcomes, DistinctEvents int }
type SupplementalIntentCardinalityStorage struct{ Cardinality, Cursor int }
type SupplementalClaimCursorStorage struct{ Claimed, Cursor int }
type ServingCapacityStorage struct{ Leased, Untouched, Cursor int }
type SupplementalProgressStorage struct{ Intents, Cursor, Owed, Claimed, Retry, Blocked int }
type SupplementalPressureStorage struct{ Unfinished, Owed, Retry, Blocked int }
type SupplementalExpiredClaimStorage struct {
	Cursor, Generation, Outcomes int
	Status                       string
	Owner                        sql.NullString
	Until, Retry                 sql.NullTime
}

func ReadSupplementalCommittedOutcomeCardinalityTx(ctx context.Context, tx *sql.Tx, run string) (SupplementalOutcomeCardinalityStorage, error) {
	var out SupplementalOutcomeCardinalityStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT event_id) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, run).Scan(&out.Outcomes, &out.DistinctEvents)
	if err != nil {
		return SupplementalOutcomeCardinalityStorage{}, err
	}
	return out, nil
}

func CountSupplementalGlobalOutcomesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes`).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountServingGlobalIntentsTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents`).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadServingGlobalCapacityTx(ctx context.Context, tx *sql.Tx) (ServingCapacityStorage, error) {
	var out ServingCapacityStorage
	err := tx.QueryRowContext(ctx, `SELECT SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),SUM(CASE WHEN claim_generation=0 THEN 1 ELSE 0 END),SUM(cursor) FROM fan_out_intents`).Scan(&out.Leased, &out.Untouched, &out.Cursor)
	if err != nil {
		return ServingCapacityStorage{}, err
	}
	return out, nil
}

func ReadSupplementalGlobalIntentCardinalityTx(ctx context.Context, tx *sql.Tx) (SupplementalIntentCardinalityStorage, error) {
	var out SupplementalIntentCardinalityStorage
	err := tx.QueryRowContext(ctx, `SELECT SUM(cardinality),SUM(cursor) FROM fan_out_intents`).Scan(&out.Cardinality, &out.Cursor)
	if err != nil {
		return SupplementalIntentCardinalityStorage{}, err
	}
	return out, nil
}

func ReadSupplementalGlobalClaimCursorTx(ctx context.Context, tx *sql.Tx) (SupplementalClaimCursorStorage, error) {
	var out SupplementalClaimCursorStorage
	err := tx.QueryRowContext(ctx, `SELECT SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),SUM(cursor) FROM fan_out_intents`).Scan(&out.Claimed, &out.Cursor)
	if err != nil {
		return SupplementalClaimCursorStorage{}, err
	}
	return out, nil
}

func ReadSupplementalGlobalProgressTx(ctx context.Context, tx *sql.Tx) (SupplementalProgressStorage, error) {
	var out SupplementalProgressStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0),COALESCE(SUM(cardinality-cursor),0),COALESCE(SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN retry_ready_at IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents`).Scan(&out.Intents, &out.Cursor, &out.Owed, &out.Claimed, &out.Retry, &out.Blocked)
	if err != nil {
		return SupplementalProgressStorage{}, err
	}
	return out, nil
}

func ReadSupplementalGlobalPressureTx(ctx context.Context, tx *sql.Tx) (SupplementalPressureStorage, error) {
	var out SupplementalPressureStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN cursor<cardinality THEN 1 ELSE 0 END),0),COALESCE(SUM(cardinality-cursor),0),COALESCE(SUM(CASE WHEN retry_ready_at IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents`).Scan(&out.Unfinished, &out.Owed, &out.Retry, &out.Blocked)
	if err != nil {
		return SupplementalPressureStorage{}, err
	}
	return out, nil
}

func ReadSupplementalExpiredClaimTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (SupplementalExpiredClaimStorage, error) {
	var out SupplementalExpiredClaimStorage
	err := tx.QueryRowContext(ctx, `SELECT i.cursor,i.status,i.claim_owner,i.claim_generation,i.lease_expires_at,i.retry_ready_at,
        (SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id AND o.flow_path=i.flow_path AND o.declaration_family=i.declaration_family AND o.semantic_path=i.semantic_path)
        FROM fan_out_intents i WHERE i.run_id=$1 AND i.triggering_delivery_id=$2 AND i.flow_path=$3 AND i.declaration_family=$4 AND i.semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&out.Cursor, &out.Status, &out.Owner, &out.Generation, &out.Until, &out.Retry, &out.Outcomes)
	if err != nil {
		return SupplementalExpiredClaimStorage{}, err
	}
	return out, nil
}
