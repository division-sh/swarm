package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
)

type ReplyContextStorageCounts = replycontextstore.ReplyContextStorageCounts
type FirstReplyContextStorage = replycontextstore.FirstReplyContextStorage

func ReadReplyContextRecordsForTest(ctx context.Context, selected any, run string) ([]replycontext.Record, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []replycontext.Record
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = replycontextstore.ReadReplyContextRecords(ctx, tx, run, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadReplyContextRequestIDsForTest(ctx context.Context, selected any, run string) ([]string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = replycontextstore.ReadReplyContextRequestIDs(ctx, tx, run)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadFirstReplyContextStorageForTest(ctx context.Context, selected any, run string) (FirstReplyContextStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return FirstReplyContextStorage{}, err
	}
	var out FirstReplyContextStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = replycontextstore.ReadFirstReplyContextStorage(ctx, tx, run)
		return err
	})
	if err != nil {
		return FirstReplyContextStorage{}, err
	}
	return out, nil
}

func ReadReplyContextStorageCountsForTest(ctx context.Context, selected any, run string) (ReplyContextStorageCounts, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ReplyContextStorageCounts{}, err
	}
	var out ReplyContextStorageCounts
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = replycontextstore.ReadReplyContextStorageCounts(ctx, tx, run)
		return err
	})
	if err != nil {
		return ReplyContextStorageCounts{}, err
	}
	return out, nil
}
