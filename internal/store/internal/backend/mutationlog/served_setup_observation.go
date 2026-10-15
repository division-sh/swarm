package mutationlog

import (
	"context"
	"database/sql"
)

type ServedSetupMutationAuthorityStorage struct {
	WriterType, WriterID, Domain, Path string
	Step                               sql.NullString
	Count                              int
}

type ServedSetupConstructorMutationStorage struct {
	Domain, Path, After string
	Before              sql.NullString
}

func CountAllRunEntityMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&count)
	return count, err
}

func ReadServedSetupMutationAuthoritiesStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) ([]ServedSetupMutationAuthorityStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT writer_type, writer_id, handler_step, domain, path, count(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 GROUP BY writer_type, writer_id, handler_step, domain, path ORDER BY writer_type, writer_id, handler_step, domain, path`, runID, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServedSetupMutationAuthorityStorage
	for rows.Next() {
		var row ServedSetupMutationAuthorityStorage
		if err := rows.Scan(&row.WriterType, &row.WriterID, &row.Step, &row.Domain, &row.Path, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func ReadServedSetupConstructorMutationsStorageTx(ctx context.Context, tx *sql.Tx, runID, entityID string) ([]ServedSetupConstructorMutationStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT domain, path, old_value, new_value FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND writer_type='platform' AND writer_id='workflow_engine' AND handler_step='create' ORDER BY domain, path`, runID, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServedSetupConstructorMutationStorage
	for rows.Next() {
		var row ServedSetupConstructorMutationStorage
		if err := rows.Scan(&row.Domain, &row.Path, &row.Before, &row.After); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
