package replycontextstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type completionAwaitOwner interface {
	PrepareCompletionAwaitTx(context.Context, *mutationprotocol.Attempt, effects.Attempt) error
	BindCompletionAwaitTx(context.Context, *mutationprotocol.Attempt, replycontext.Record, effects.CompletionAwaitPublication, bool) (replycontext.AwaitBinding, error)
}

func (s *ReplyPostgresOwner) PrepareCompletionAwaitTx(ctx context.Context, mutation *mutationprotocol.Attempt, attempt effects.Attempt) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	return s.completionAwait.PrepareCompletionAwaitTx(ctx, mutation, attempt)
}

func (s *ReplySQLiteOwner) PrepareCompletionAwaitTx(ctx context.Context, mutation *mutationprotocol.Attempt, attempt effects.Attempt) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	return s.completionAwait.PrepareCompletionAwaitTx(ctx, mutation, attempt)
}

func (s *ReplyPostgresOwner) BindCompletionAwait(owner completionAwaitOwner) error {
	if owner == nil || s.completionAwait != nil {
		return fmt.Errorf("reply await requires one native completion owner")
	}
	s.completionAwait = owner
	return nil
}

func (s *ReplySQLiteOwner) BindCompletionAwait(owner completionAwaitOwner) error {
	if owner == nil || s.completionAwait != nil {
		return fmt.Errorf("reply await requires one native completion owner")
	}
	s.completionAwait = owner
	return nil
}

func (s *ReplyPostgresOwner) CompletionAwaitWithinTransaction(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command effects.CompletionAwaitPublication, existing bool) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	binding, err := s.completionAwait.BindCompletionAwaitTx(ctx, mutation, record, command, existing)
	if err != nil {
		return err
	}
	record.Await = &binding
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if existing {
			loaded, err := loadPostgresReplyContext(ctx, tx, record.ID, true)
			return requireCompletionAwaitIdentity(record, loaded, err)
		}
		return createPostgresReplyContext(ctx, tx, mutation, record)
	})
}

func (s *ReplySQLiteOwner) CompletionAwaitWithinTransaction(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command effects.CompletionAwaitPublication, existing bool) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	binding, err := s.completionAwait.BindCompletionAwaitTx(ctx, mutation, record, command, existing)
	if err != nil {
		return err
	}
	record.Await = &binding
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if existing {
			loaded, err := loadSQLiteReplyContext(ctx, tx, record.ID)
			return requireCompletionAwaitIdentity(record, loaded, err)
		}
		return createSQLiteReplyContextTx(ctx, tx, mutation, record)
	})
}

func requireCompletionAwaitIdentity(expected, loaded replycontext.Record, err error) error {
	if err != nil {
		return err
	}
	if !loaded.SameIdentity(expected) {
		return fmt.Errorf("duplicate await publication changed its immutable reply binding")
	}
	return nil
}
