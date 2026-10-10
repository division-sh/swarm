package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
)

type ScenarioSetupAckStorage struct {
	Runs, Entities, SetupMutations, Completions int
	Response                                    json.RawMessage
}

func ReadScenarioSetupAckStorageForTest(ctx context.Context, selected any, runID, entityID string) (ScenarioSetupAckStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	for _, id := range []string{runID, entityID} {
		if err := validateSelectedForkStorageIdentity(id); err != nil {
			return ScenarioSetupAckStorage{}, err
		}
	}
	var out ScenarioSetupAckStorage
	var response string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runs WHERE CAST(run_id AS TEXT)=$1),
			(SELECT COUNT(*) FROM entity_state WHERE CAST(run_id AS TEXT)=$1 AND CAST(entity_id AS TEXT)=$2),
			(SELECT COUNT(*) FROM entity_mutations WHERE CAST(run_id AS TEXT)=$1 AND CAST(entity_id AS TEXT)=$2 AND writer_id='test.setup_entities'),
			(SELECT COUNT(*) FROM api_idempotency WHERE resource_id=$1)`, runID, entityID).
			Scan(&out.Runs, &out.Entities, &out.SetupMutations, &out.Completions); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT response FROM api_idempotency WHERE resource_id=$1`, runID).Scan(&response)
	})
	if err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	out.Response = append(json.RawMessage(nil), response...)
	return out, nil
}
