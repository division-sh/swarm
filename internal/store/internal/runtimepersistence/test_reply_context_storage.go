package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type ReplyContextStorageCounts = replycontextstore.ReplyContextStorageCounts
type FirstReplyContextStorage = replycontextstore.FirstReplyContextStorage

// Preserve the original Story evidence and reply-context fact on the exact
// selected coordinator, not a reconstructed backend or candidate owner.
func CorruptOpenReplyOriginForTest(ctx context.Context, selected any, runID, contextID, origin string) error {
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return err
	}
	apply := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, replycontextstore.CorruptOpenReplyOriginForTest(ctx, attempt, dialect == authoractivityfixture.DialectPostgres, runID, contextID, origin)
	}
	if owner, ok := selected.(*PostgresStore); ok {
		return mutationprotocol.RunPostgres(ctx, owner.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, apply).Err()
	}
	owner := selected.(*SQLiteRuntimeStore)
	return mutationprotocol.RunSQLite(ctx, owner.backend, "canonical event fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, apply).Err()
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
