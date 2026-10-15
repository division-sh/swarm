package mutationlog

import (
	"context"
	"database/sql"
)

type EntityToolMutationAuthorityStorage struct{ Domain, Path, WriterType, WriterID, Step string }

func CountEntityToolAgentStatusMutationsTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1::uuid AND entity_id=$2::uuid
		AND domain='authored_field' AND path='status' AND writer_type='agent' AND handler_step='save_entity_field'`, runID, entityID).Scan(&count)
	return count, err
}

func CountEntityToolAuthoredPathMutationsTx(ctx context.Context, tx *sql.Tx, entityID, path string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations
		WHERE entity_id=$1::uuid AND domain='authored_field' AND path=$2`, entityID, path).Scan(&count)
	return count, err
}

func ReadEntityToolInitialMutationAuthoritiesTx(ctx context.Context, tx *sql.Tx, entityID string) ([]EntityToolMutationAuthorityStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT domain, path, COALESCE(writer_type,''), COALESCE(writer_id,''), COALESCE(handler_step,'')
		FROM entity_mutations WHERE entity_id=$1::uuid ORDER BY created_at ASC, mutation_id ASC`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EntityToolMutationAuthorityStorage
	for rows.Next() {
		var row EntityToolMutationAuthorityStorage
		if err := rows.Scan(&row.Domain, &row.Path, &row.WriterType, &row.WriterID, &row.Step); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
