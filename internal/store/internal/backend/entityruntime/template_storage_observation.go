package entitystore

import (
	"context"
	"database/sql"
	"errors"
)

type TemplateAccountIdentityStorage struct{ Path, EntityID string }

type TemplateAccountDiagnosticStorage struct{ EntityID, Path, State, Fields string }

func ReadTemplateAccountDiagnosticsTx(ctx context.Context, tx *sql.Tx) (out []TemplateAccountDiagnosticStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(entity_id AS TEXT),flow_instance,current_state,CAST(fields AS TEXT) FROM entity_state WHERE flow_instance LIKE 'account/%' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var row TemplateAccountDiagnosticStorage
		if err := rows.Scan(&row.EntityID, &row.Path, &row.State, &row.Fields); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountTemplateAccountEntitiesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE flow_instance LIKE 'account/%'`).Scan(&count)
	return count, err
}

func CountPostgresReviewedTemplateAccountTx(ctx context.Context, tx *sql.Tx, fields string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE flow_instance LIKE 'account/%' AND current_state='reviewed' AND fields @> $1::jsonb`, fields).Scan(&count)
	return count, err
}

func ReadLatestTemplateAccountIdentityTx(ctx context.Context, tx *sql.Tx) (TemplateAccountIdentityStorage, error) {
	var out TemplateAccountIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT flow_instance,CAST(entity_id AS TEXT) FROM entity_state WHERE flow_instance LIKE 'account/%' ORDER BY created_at DESC LIMIT 1`).Scan(&out.Path, &out.EntityID)
	if err != nil {
		return TemplateAccountIdentityStorage{}, err
	}
	return out, nil
}
