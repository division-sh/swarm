package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func (s *PipelinePostgresOwner) CorruptRegistryVerdictForTest(ctx context.Context, run, entity, verdict string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return corruptRegistryVerdictTx(ctx, tx, run, entity, verdict)
	})
}

func (s *PipelineSQLiteOwner) CorruptRegistryVerdictForTest(ctx context.Context, run, entity, verdict string) error {
	return s.backend.RunTransaction(ctx, "registry verdict corruption fixture", func(ctx context.Context, tx *sql.Tx) error {
		return corruptRegistryVerdictTx(ctx, tx, run, entity, verdict)
	})
}

func corruptRegistryVerdictTx(ctx context.Context, tx *sql.Tx, run, entity, verdict string) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, entity).Scan(&raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	var original string
	if err := json.Unmarshal(fields["verdict"], &original); err != nil {
		return fmt.Errorf("registry fixture requires an assigned text verdict: %w", err)
	}
	if original == verdict {
		return fmt.Errorf("registry fixture must change the existing verdict")
	}
	encoded, err := json.Marshal(verdict)
	if err != nil {
		return err
	}
	fields["verdict"] = encoded
	raw, err = json.Marshal(fields)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE entity_state SET fields=$1 WHERE run_id=$2 AND entity_id=$3`, string(raw), run, entity)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", changed)
	}
	return nil
}
