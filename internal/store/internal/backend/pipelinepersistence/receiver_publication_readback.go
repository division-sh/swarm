package pipelinepersistence

import (
	"context"
	"database/sql"
)

func ReadConnectionWorkerInstancePathsStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template='worker' ORDER BY instance_path`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	return out, rows.Err()
}

func CountOpenPublicationFanOutIntentsStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND status<>'closed'`, runID).Scan(&count)
	return count, err
}

func CountNoncommittedPublicationFanOutOutcomesStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind<>'committed'`, runID).Scan(&count)
	return count, err
}

func ReadPublicationFanOutFailureStorageTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind<>'committed' LIMIT 1`, runID).Scan(&raw)
	return raw, err
}

type PublicationTemplateIdentityStorage struct{ Path, Fields, Declaration string }

func ReadActivityPublicationTemplatePathStorageTx(ctx context.Context, tx *sql.Tx, runID, flow string) (string, error) {
	var path string
	err := tx.QueryRowContext(ctx, `SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template=$2`, runID, flow).Scan(&path)
	return path, err
}

func ReadActivityPublicationTemplateEntityStorageTx(ctx context.Context, tx *sql.Tx, runID, path, flow string) (string, error) {
	var entity string
	err := tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND flow_template=$3`, runID, path, flow).Scan(&entity)
	return entity, err
}

func ReadPublicationTemplateIdentityStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (PublicationTemplateIdentityStorage, error) {
	var out PublicationTemplateIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT e.flow_instance,CAST(e.fields AS TEXT),f.flow_template FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND e.entity_id=$2`, runID, entityID).Scan(&out.Path, &out.Fields, &out.Declaration)
	return out, err
}
