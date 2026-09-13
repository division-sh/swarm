package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type decisionCardRequestLease struct {
	request  apiidempotency.Request
	mutation pipeline.DecisionCardMutation
	runID    string
	source   correlation.SourceArtifactFact
	postgres *PipelinePostgresOwner
	sqlite   *PipelineSQLiteOwner
	pgLease  *storeapiidempotency.PostgresRequestLease
	sqLease  *storeapiidempotency.SQLiteRequestLease
	closed   bool
	used     bool
}

type decisionCardRequestSource interface {
	GetDecisionCard(context.Context, string) (decisioncard.Card, error)
	RequirePresentRunSource(context.Context, string) (correlation.SourceArtifactFact, error)
}

func admitDecisionCardRequest(ctx context.Context, owner decisionCardRequestSource, req apiidempotency.Request) (string, correlation.SourceArtifactFact, error) {
	if err := req.Actor.ValidateMethod(req.Method); err != nil {
		return "", correlation.SourceArtifactFact{}, err
	}
	if !apiidempotency.IsHumanMailboxMethod(req.Method) || req.Method == "mailbox.acknowledge" || req.ResourceID == "" {
		return "", correlation.SourceArtifactFact{}, fmt.Errorf("typed card request identity is required")
	}
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.Validate() != nil {
		return "", fact, fmt.Errorf("admitted card source fact is required")
	}
	card, err := owner.GetDecisionCard(ctx, req.ResourceID)
	if err != nil {
		return "", fact, err
	}
	actual, err := owner.RequirePresentRunSource(ctx, card.RunID)
	if err != nil {
		return "", fact, err
	}
	if !fact.Matches(actual) || card.BundleHash != fact.BundleHash() {
		return "", fact, fmt.Errorf("card source differs from admitted runtime source")
	}
	return card.RunID, fact, nil
}

func (s *PipelinePostgresOwner) AcquireDecisionCardMutation(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) (pipeline.DecisionCardMutationLease, error) {
	if err := mutation.ValidateRequest(req); err != nil {
		return nil, err
	}
	runID, source, err := admitDecisionCardRequest(ctx, s, req)
	if err != nil {
		return nil, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, req)
	if err != nil {
		return nil, err
	}
	if _, _, err := admitDecisionCardRequest(ctx, s, req); err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	return &decisionCardRequestLease{request: req, mutation: mutation, runID: runID, source: source, postgres: s, pgLease: lease}, nil
}

func (s *PipelineSQLiteOwner) AcquireDecisionCardMutation(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) (pipeline.DecisionCardMutationLease, error) {
	if err := mutation.ValidateRequest(req); err != nil {
		return nil, err
	}
	runID, source, err := admitDecisionCardRequest(ctx, s, req)
	if err != nil {
		return nil, err
	}
	lease, err := storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, req)
	if err != nil {
		return nil, err
	}
	if _, _, err := admitDecisionCardRequest(ctx, s, req); err != nil {
		lease.Release()
		return nil, err
	}
	return &decisionCardRequestLease{request: req, mutation: mutation, runID: runID, source: source, sqlite: s, sqLease: lease}, nil
}

func (l *decisionCardRequestLease) Replay() (apiidempotency.Completion, bool) {
	if l == nil || l.closed {
		return apiidempotency.Completion{}, false
	}
	if l.pgLease != nil {
		return l.pgLease.Replay()
	}
	return l.sqLease.Replay()
}

func (l *decisionCardRequestLease) Release(ctx context.Context) error {
	if l == nil || l.closed {
		return nil
	}
	l.closed = true
	if l.pgLease != nil {
		return l.pgLease.Release(ctx)
	}
	l.sqLease.Release()
	return nil
}

func (l *decisionCardRequestLease) Commit(ctx context.Context, command pipeline.DecisionCardMutationCommand) (pipeline.CommittedDecisionCardMutation, error) {
	if l == nil || l.closed || l.used {
		return pipeline.CommittedDecisionCardMutation{}, fmt.Errorf("card request lease is not current")
	}
	if _, replay := l.Replay(); replay {
		return pipeline.CommittedDecisionCardMutation{}, fmt.Errorf("completed card request cannot mutate again")
	}
	if err := command.Mutation.ValidateRequest(l.request); err != nil {
		return pipeline.CommittedDecisionCardMutation{}, err
	}
	if !command.Mutation.SameRequest(l.mutation) {
		return pipeline.CommittedDecisionCardMutation{}, fmt.Errorf("prepared card command differs from its acquired request")
	}
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || !source.Matches(l.source) {
		return pipeline.CommittedDecisionCardMutation{}, fmt.Errorf("card request source changed before commit")
	}
	l.used = true
	effects := newRevisionEffects()
	if s := l.postgres; s != nil {
		return commitDecisionCardOperation(ctx, s, s.DecisionPostgresOwner, true, effects, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				current, err := s.RequireActiveSourceTx(txctx, tx, l.runID)
				if err != nil {
					return err
				}
				if !current.Matches(l.source) {
					return fmt.Errorf("card source changed at commit")
				}
				return fn(txctx, tx, story)
			})
		}, command, func(ctx context.Context, tx *sql.Tx, completion apiidempotency.Completion) error {
			return storeapiidempotency.StorePostgresCompletionTx(ctx, l.pgLease, tx, completion)
		})
	}
	s := l.sqlite
	return commitDecisionCardOperation(ctx, s, s.DecisionSQLiteOwner, false, effects, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite decision-card operation", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			current, err := s.RequireActiveSourceTx(txctx, tx, l.runID)
			if err != nil {
				return err
			}
			if !current.Matches(l.source) {
				return fmt.Errorf("card source changed at commit")
			}
			return fn(txctx, tx, story)
		})
	}, command, func(ctx context.Context, tx *sql.Tx, completion apiidempotency.Completion) error {
		return storeapiidempotency.StoreSQLiteCompletionTx(ctx, l.sqLease, tx, completion)
	})
}
