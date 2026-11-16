package replycontextstore

import (
	"context"
	"database/sql"

	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
)

func (s *ReplyPostgresOwner) ReadReplyContextRecordsForTest(ctx context.Context, run string) ([]runtimereplycontext.Record, error) {
	var out []runtimereplycontext.Record
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadReplyContextRecords(ctx, tx, run, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadReplyContextRecordsForTest(ctx context.Context, run string) ([]runtimereplycontext.Record, error) {
	var out []runtimereplycontext.Record
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadReplyContextRecords(ctx, tx, run, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplyPostgresOwner) ReadReplyContextRequestIDsForTest(ctx context.Context, run string) ([]string, error) {
	var out []string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(request_event_id AS TEXT) FROM reply_contexts WHERE run_id=$1 ORDER BY request_event_id`, run)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadReplyContextRequestIDsForTest(ctx context.Context, run string) ([]string, error) {
	var out []string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(request_event_id AS TEXT) FROM reply_contexts WHERE run_id=$1 ORDER BY request_event_id`, run)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplyPostgresOwner) ReadFirstReplyContextStorageForTest(ctx context.Context, run string) (FirstReplyContextStorage, error) {
	var out FirstReplyContextStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT reply_context_id,request_event_id,accepted_reply_event_id
FROM reply_contexts WHERE run_id=$1 ORDER BY reply_context_id LIMIT 1`, run).
			Scan(&out.ContextID, &out.RequestEventID, &out.AcceptedReplyEventID)
	})
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadFirstReplyContextStorageForTest(ctx context.Context, run string) (FirstReplyContextStorage, error) {
	var out FirstReplyContextStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT reply_context_id,request_event_id,accepted_reply_event_id
FROM reply_contexts WHERE run_id=$1 ORDER BY reply_context_id LIMIT 1`, run).
			Scan(&out.ContextID, &out.RequestEventID, &out.AcceptedReplyEventID)
	})
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return out, nil
}

func (s *ReplyPostgresOwner) ReadReplyContextStorageCountsForTest(ctx context.Context, run string) (ReplyContextStorageCounts, error) {
	var out ReplyContextStorageCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*),count(accepted_reply_event_id),count(DISTINCT request_event_id),count(DISTINCT accepted_reply_event_id)
FROM reply_contexts WHERE run_id=$1`, run).Scan(&out.Total, &out.Accepted, &out.DistinctRequests, &out.DistinctReplies)
	})
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadReplyContextStorageCountsForTest(ctx context.Context, run string) (ReplyContextStorageCounts, error) {
	var out ReplyContextStorageCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*),count(accepted_reply_event_id),count(DISTINCT request_event_id),count(DISTINCT accepted_reply_event_id)
FROM reply_contexts WHERE run_id=$1`, run).Scan(&out.Total, &out.Accepted, &out.DistinctRequests, &out.DistinctReplies)
	})
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return out, nil
}
