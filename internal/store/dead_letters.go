package store

import (
	"context"
	"database/sql"

	runtimedeadletters "swarm/internal/runtime/deadletters"
)

func (s *PostgresStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return runtimedeadletters.Insert(ctx, s.DB, rec)
}

func (s *PostgresStore) RecordDeadLetterTx(ctx context.Context, tx *sql.Tx, rec runtimedeadletters.Record) error {
	return runtimedeadletters.InsertTx(ctx, tx, rec)
}
