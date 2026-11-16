package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
)

type ReplyContextStorageCounts = replycontextstore.ReplyContextStorageCounts
type FirstReplyContextStorage = replycontextstore.FirstReplyContextStorage

func insertReplyConstraintForTest(ctx context.Context, selected any, record replycontext.Record, requireInserted bool) (bool, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil {
			return store.replyPostgresOwner.InsertConstraintFixtureForTest(ctx, record, requireInserted)
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil {
			return store.replySQLiteOwner.InsertConstraintFixtureForTest(ctx, record, requireInserted)
		}
	}
	return false, fmt.Errorf("reply constraint fixture requires its original native owner")
}

func ReadReplyContextRecordsForTest(ctx context.Context, selected any, run string) ([]replycontext.Record, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.replyPostgresOwner.ReadReplyContextRecordsForTest(ctx, run)
	case *SQLiteRuntimeStore:
		return store.replySQLiteOwner.ReadReplyContextRecordsForTest(ctx, run)
	default:
		return nil, fmt.Errorf("reply context observation requires the original native owner")
	}
}

func ReadReplyContextRequestIDsForTest(ctx context.Context, selected any, run string) ([]string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.replyPostgresOwner.ReadReplyContextRequestIDsForTest(ctx, run)
	case *SQLiteRuntimeStore:
		return store.replySQLiteOwner.ReadReplyContextRequestIDsForTest(ctx, run)
	default:
		return nil, fmt.Errorf("reply context observation requires the original native owner")
	}
}

func ReadFirstReplyContextStorageForTest(ctx context.Context, selected any, run string) (FirstReplyContextStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return FirstReplyContextStorage{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.replyPostgresOwner.ReadFirstReplyContextStorageForTest(ctx, run)
	case *SQLiteRuntimeStore:
		return store.replySQLiteOwner.ReadFirstReplyContextStorageForTest(ctx, run)
	default:
		return FirstReplyContextStorage{}, fmt.Errorf("reply context observation requires the original native owner")
	}
}

func ReadReplyContextStorageCountsForTest(ctx context.Context, selected any, run string) (ReplyContextStorageCounts, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ReplyContextStorageCounts{}, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.replyPostgresOwner.ReadReplyContextStorageCountsForTest(ctx, run)
	case *SQLiteRuntimeStore:
		return store.replySQLiteOwner.ReadReplyContextStorageCountsForTest(ctx, run)
	default:
		return ReplyContextStorageCounts{}, fmt.Errorf("reply context observation requires the original native owner")
	}
}
