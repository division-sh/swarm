package store

import (
	"context"

	runtimedeadletters "empireai/internal/runtime/deadletters"
)

func (s *PostgresStore) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	return runtimedeadletters.Insert(ctx, s.DB, rec)
}
