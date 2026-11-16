package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
)

type ScenarioSetupAckStorage struct {
	Runs, Entities, SetupMutations, Completions int
	Response                                    json.RawMessage
}

func (s *PipelinePostgresOwner) ReadScenarioSetupAckStorageForTest(ctx context.Context, runID, entityID string) (ScenarioSetupAckStorage, error) {
	var out ScenarioSetupAckStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readScenarioSetupAckStorageTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadScenarioSetupAckStorageForTest(ctx context.Context, runID, entityID string) (ScenarioSetupAckStorage, error) {
	var out ScenarioSetupAckStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readScenarioSetupAckStorageTx(ctx, tx, runID, entityID)
		return err
	})
	if err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	return out, nil
}

func readScenarioSetupAckStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (ScenarioSetupAckStorage, error) {
	var out ScenarioSetupAckStorage
	var response string
	if err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM runs WHERE CAST(run_id AS TEXT)=$1),
		(SELECT COUNT(*) FROM entity_state WHERE CAST(run_id AS TEXT)=$1 AND CAST(entity_id AS TEXT)=$2),
		(SELECT COUNT(*) FROM entity_mutations WHERE CAST(run_id AS TEXT)=$1 AND CAST(entity_id AS TEXT)=$2 AND writer_id='test.setup_entities'),
		(SELECT COUNT(*) FROM api_idempotency WHERE resource_id=$1)`, runID, entityID).
		Scan(&out.Runs, &out.Entities, &out.SetupMutations, &out.Completions); err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT response FROM api_idempotency WHERE resource_id=$1`, runID).Scan(&response); err != nil {
		return ScenarioSetupAckStorage{}, err
	}
	out.Response = append(json.RawMessage(nil), response...)
	return out, nil
}
