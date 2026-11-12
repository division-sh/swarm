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
		var err error
		out, err = ReadReplyContextRequestIDs(ctx, tx, run)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadReplyContextRequestIDsForTest(ctx context.Context, run string) ([]string, error) {
	var out []string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadReplyContextRequestIDs(ctx, tx, run)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ReplyPostgresOwner) ReadFirstReplyContextStorageForTest(ctx context.Context, run string) (FirstReplyContextStorage, error) {
	var out FirstReplyContextStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadFirstReplyContextStorage(ctx, tx, run)
		return err
	})
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadFirstReplyContextStorageForTest(ctx context.Context, run string) (FirstReplyContextStorage, error) {
	var out FirstReplyContextStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadFirstReplyContextStorage(ctx, tx, run)
		return err
	})
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return out, nil
}

func (s *ReplyPostgresOwner) ReadReplyContextStorageCountsForTest(ctx context.Context, run string) (ReplyContextStorageCounts, error) {
	var out ReplyContextStorageCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadReplyContextStorageCounts(ctx, tx, run)
		return err
	})
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return out, nil
}

func (s *ReplySQLiteOwner) ReadReplyContextStorageCountsForTest(ctx context.Context, run string) (ReplyContextStorageCounts, error) {
	var out ReplyContextStorageCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = ReadReplyContextStorageCounts(ctx, tx, run)
		return err
	})
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return out, nil
}
