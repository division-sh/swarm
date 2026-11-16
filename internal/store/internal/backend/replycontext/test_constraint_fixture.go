package replycontextstore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// InsertConstraintFixtureForTest exercises physical constraints on the selected
// reply owner, without run admission, reply claim or history. Cleanup fixtures
// require a fresh insertion inside the same transaction.
func (s *ReplyPostgresOwner) InsertConstraintFixtureForTest(ctx context.Context, record replycontext.Record, requireInserted bool) (bool, error) {
	var inserted bool
	err := s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		inserted, err = insertConstraintFixtureTx(ctx, tx, true, record, requireInserted)
		return err
	})
	return inserted, err
}

func (s *ReplySQLiteOwner) InsertConstraintFixtureForTest(ctx context.Context, record replycontext.Record, requireInserted bool) (bool, error) {
	var inserted bool
	err := s.backend.RunTransaction(ctx, "unrevisioned event fixture", func(ctx context.Context, tx *sql.Tx) (err error) {
		inserted, err = insertConstraintFixtureTx(ctx, tx, false, record, requireInserted)
		return err
	})
	return inserted, err
}

func insertConstraintFixtureTx(ctx context.Context, tx *sql.Tx, postgres bool, record replycontext.Record, requireInserted bool) (bool, error) {
	if tx == nil {
		return false, errors.New("reply constraint fixture requires the selected transaction")
	}
	origin, err := record.EncodeOrigin()
	if err != nil {
		return false, err
	}
	result, err := insertReplyContextRow(ctx, tx, record, origin, postgres)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err == nil && requireInserted && count != 1 {
		return false, errors.New("reply cleanup constraint fixture was not newly inserted")
	}
	return count == 1, err
}
