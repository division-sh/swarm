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
	BindCompletionAwaitTx(context.Context, *mutationprotocol.Attempt, replycontext.Record, effects.CompletionAwaitPublication) (replycontext.AwaitBinding, error)
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

func (s *ReplyPostgresOwner) CreateAwaitWithinTransaction(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command effects.CompletionAwaitPublication) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	binding, err := s.completionAwait.BindCompletionAwaitTx(ctx, mutation, record, command)
	if err != nil {
		return err
	}
	record.Await = &binding
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return createPostgresReplyContext(ctx, tx, mutation, record)
	})
}

func (s *ReplySQLiteOwner) CreateAwaitWithinTransaction(ctx context.Context, mutation *mutationprotocol.Attempt, record replycontext.Record, command effects.CompletionAwaitPublication) error {
	if s.completionAwait == nil {
		return fmt.Errorf("reply await completion owner is not bound")
	}
	if err := requireFreshReplyContext(record); err != nil {
		return err
	}
	binding, err := s.completionAwait.BindCompletionAwaitTx(ctx, mutation, record, command)
	if err != nil {
		return err
	}
	record.Await = &binding
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return createSQLiteReplyContextTx(ctx, tx, mutation, record)
	})
}
