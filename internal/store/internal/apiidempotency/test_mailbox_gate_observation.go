package apiidempotency

import (
	"context"
	"database/sql"
	"errors"
)

type MailboxGateCompletionRow struct {
	Method, ActorKind, Actor, Key, Hash, ResourceID, Response string
}

func (s *PostgresOwner) ReadMailboxGateCompletionDomainForTest(ctx context.Context, cardID, method, key string) (out []MailboxGateCompletionRow, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateCompletionDomainTx(ctx, tx, cardID, method, key)
		return err
	})
	return
}

func (s *SQLiteOwner) ReadMailboxGateCompletionDomainForTest(ctx context.Context, cardID, method, key string) (out []MailboxGateCompletionRow, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = readMailboxGateCompletionDomainTx(ctx, tx, cardID, method, key)
		return err
	})
	return
}

func readMailboxGateCompletionDomainTx(ctx context.Context, tx *sql.Tx, cardID, method, key string) ([]MailboxGateCompletionRow, error) {
	// The OR domain also witnesses completions with the wrong actor or hash.
	rows, err := tx.QueryContext(ctx, `SELECT method, actor_kind, actor_id, idempotency_key, request_hash, COALESCE(resource_id,''), CAST(response AS TEXT) FROM api_idempotency WHERE resource_id=$1 OR (method=$2 AND idempotency_key=$3) ORDER BY method, actor_kind, actor_id, idempotency_key`, cardID, method, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MailboxGateCompletionRow
	for rows.Next() {
		var row MailboxGateCompletionRow
		if err := rows.Scan(&row.Method, &row.ActorKind, &row.Actor, &row.Key, &row.Hash, &row.ResourceID, &row.Response); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, errors.Join(rows.Err(), rows.Close())
}
