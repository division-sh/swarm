package entitystore

import (
	"context"
	"database/sql"
)

type PostgresScenarioSetupEntityStorage struct{ FlowInstance, EntityType, CurrentState, Note, ReviewReady string }

func ReadFanOutPortfolioFieldsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]byte, error) {
	var out []byte
	if err := tx.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND flow_instance='portfolio'`, runID).Scan(&out); err != nil {
		return nil, err
	}
	return append([]byte(nil), out...), nil
}

func ReadPostgresScenarioSetupEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (PostgresScenarioSetupEntityStorage, error) {
	var out PostgresScenarioSetupEntityStorage
	err := tx.QueryRowContext(ctx, `SELECT flow_instance, entity_type, current_state, fields->>'note', gates->>'review_ready' FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).
		Scan(&out.FlowInstance, &out.EntityType, &out.CurrentState, &out.Note, &out.ReviewReady)
	if err != nil {
		return PostgresScenarioSetupEntityStorage{}, err
	}
	return out, nil
}

func ReadScenarioSetupFieldsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) ([]byte, error) {
	var fields []byte
	if err := tx.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func CountPhysicalEntitiesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state`).Scan(&count)
	return count, err
}
