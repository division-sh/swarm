package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storechanneldelivery "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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

func requireChannelCardMutationTx(ctx context.Context, tx *sql.Tx, req apiidempotency.Request, mutation pipeline.DecisionCardMutation, postgres, lock bool) error {
	if err := storedecision.RequireNormalCardControlTx(ctx, tx, req.ResourceID, runfork.SelectedControl(req.Method), postgres); err != nil {
		return err
	}
	if err := requireChannelCardActionTx(ctx, tx, req, mutation, postgres, lock); err != nil {
		return err
	}
	if err := requireChannelCardTextTx(ctx, tx, req, mutation, postgres, lock); err != nil {
		return err
	}
	return requireChannelCardSkipTx(ctx, tx, req, mutation, postgres, lock)
}

func settleChannelCardMutationTx(ctx context.Context, tx *sql.Tx, mutation pipeline.DecisionCardMutation, postgres bool) error {
	if err := settleChannelCardActionTx(ctx, tx, mutation, postgres); err != nil {
		return err
	}
	if err := settleChannelCardTextTx(ctx, tx, mutation, postgres); err != nil {
		return err
	}
	return settleChannelCardSkipTx(ctx, tx, mutation, postgres)
}

func requireChannelCardActionTx(ctx context.Context, tx *sql.Tx, req apiidempotency.Request, mutation pipeline.DecisionCardMutation, postgres, lock bool) error {
	fact, present := mutation.ChannelAction()
	if !present {
		return nil
	}
	if _, err := storechanneldelivery.RequireActionIntentTx(ctx, tx, fact, postgres, lock); err != nil {
		return err
	}
	demand := channeldelivery.CardActionDemand{CardID: req.ResourceID, PrincipalID: req.Actor.ID, Method: req.Method}
	switch mutation.Kind() {
	case pipeline.DecisionCardMutationDecide:
		decision, _ := mutation.Decision()
		demand.Verdict, demand.ReceiptOperationID, demand.RenderHash = decision.Verdict, decision.DeliveryReceiptID, decision.DeliveryRenderHash
	case pipeline.DecisionCardMutationBeginInput:
		begin, _, _ := mutation.InputBegin()
		demand.Verdict, demand.ReceiptOperationID = begin.Verdict, begin.DeliveryReceiptID
	case pipeline.DecisionCardMutationCancelInput:
		cancel, _ := mutation.InputCancellation()
		demand.DraftID = cancel.InputDraftID
	default:
		return fmt.Errorf("channel action does not authorize this card method")
	}
	return storechanneldelivery.RequireCardActionTx(ctx, tx, fact.ActionFact, demand, postgres, lock)
}

func settleChannelCardActionTx(ctx context.Context, tx *sql.Tx, mutation pipeline.DecisionCardMutation, postgres bool) error {
	fact, present := mutation.ChannelAction()
	if !present {
		return nil
	}
	var disposition channeldelivery.ActionDisposition
	switch mutation.Kind() {
	case pipeline.DecisionCardMutationDecide:
		disposition = channeldelivery.ActionApplied
	case pipeline.DecisionCardMutationBeginInput:
		disposition = channeldelivery.ActionInputStarted
	case pipeline.DecisionCardMutationCancelInput:
		disposition = channeldelivery.ActionApplied
	default:
		return fmt.Errorf("channel action does not authorize this card method")
	}
	if err := storechanneldelivery.SettleAppliedActionIntentTx(ctx, tx, fact, disposition, postgres); err != nil {
		return err
	}
	if mutation.Kind() == pipeline.DecisionCardMutationBeginInput {
		begin, _, _ := mutation.InputBegin()
		return storechanneldelivery.PlanInputPromptForActionTx(ctx, tx, fact, begin.CardID, "", postgres)
	}
	return nil
}

func requireChannelCardTextTx(ctx context.Context, tx *sql.Tx, req apiidempotency.Request, mutation pipeline.DecisionCardMutation, postgres, lock bool) error {
	text, present := mutation.ChannelText()
	if !present {
		return nil
	}
	decision, ok := mutation.Decision()
	if !ok || decision.InputDraftID == "" || decision.CardID != req.ResourceID || decision.PrincipalID != req.Actor.ID {
		return fmt.Errorf("channel text requires an exact draft-backed decision")
	}
	choice, chosen := mutation.ChannelChoice()
	var selected channeldelivery.InputDraftCandidate
	var resolved channeldelivery.ResolvedText
	var err error
	if chosen {
		var retained channeldelivery.PendingText
		selected, retained, resolved, err = storechanneldelivery.RequireChosenInputDraftTx(ctx, tx, choice, decision.Now, lock, postgres)
		if err == nil && retained.Fact != text {
			return fmt.Errorf("channel choice does not bind the exact retained text")
		}
	} else {
		selected, resolved, err = storechanneldelivery.RequireCurrentInputDraftTx(ctx, tx, text, decision.Now,
			decision.InputDraftID, lock, postgres)
	}
	if err != nil {
		return err
	}
	if resolved.PrincipalID != req.Actor.ID {
		return fmt.Errorf("channel text principal is no longer current")
	}
	if selected.CardID != decision.CardID ||
		selected.Verdict != decision.Verdict || selected.ReceiptOperationID != decision.DeliveryReceiptID {
		return fmt.Errorf("channel text draft is no longer exact current authority")
	}
	progress, err := storedecision.PreviewInputDraftTextTx(ctx, tx, selected.DraftID, resolved.PrincipalID, text.Text, decision.Now, postgres)
	if err != nil {
		return err
	}
	if !progress.Complete || !progress.Fields.Equal(decision.Fields) {
		return fmt.Errorf("channel text does not complete the exact current draft fields")
	}
	return nil
}

func settleChannelCardTextTx(ctx context.Context, tx *sql.Tx, mutation pipeline.DecisionCardMutation, postgres bool) error {
	text, present := mutation.ChannelText()
	if !present {
		return nil
	}
	if choice, chosen := mutation.ChannelChoice(); chosen {
		return storechanneldelivery.SettleAppliedActionIntentTx(ctx, tx, choice, channeldelivery.ActionApplied, postgres)
	}
	return storechanneldelivery.SettleTextIntentTx(ctx, tx, text, "input_complete", postgres)
}

func requireChannelCardSkipTx(ctx context.Context, tx *sql.Tx, req apiidempotency.Request,
	mutation pipeline.DecisionCardMutation, postgres, lock bool) error {
	skip, present := mutation.ChannelSkip()
	if !present {
		return nil
	}
	decision, ok := mutation.Decision()
	if !ok || decision.InputDraftID == "" || decision.CardID != req.ResourceID || decision.PrincipalID != req.Actor.ID {
		return fmt.Errorf("channel skip requires an exact draft-backed decision")
	}
	resolved, progress, draft, err := storechanneldelivery.RequireCurrentSkipActionTx(ctx, tx, skip, decision.Now, lock, postgres)
	if err != nil {
		return err
	}
	if !progress.Complete || !progress.Fields.Equal(decision.Fields) ||
		draft.InputDraftID != decision.InputDraftID || draft.CardID != decision.CardID ||
		draft.Verdict != decision.Verdict || draft.DeliveryReceiptID != decision.DeliveryReceiptID ||
		resolved.PrincipalID != decision.PrincipalID {
		return fmt.Errorf("channel skip does not complete the exact current draft")
	}
	return nil
}

func settleChannelCardSkipTx(ctx context.Context, tx *sql.Tx, mutation pipeline.DecisionCardMutation, postgres bool) error {
	skip, present := mutation.ChannelSkip()
	if !present {
		return nil
	}
	return storechanneldelivery.SettleAppliedActionIntentTx(ctx, tx, skip, channeldelivery.ActionApplied, postgres)
}

func (s *PipelinePostgresOwner) requireChannelCardAction(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) error {
	return s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return requireChannelCardMutationTx(txctx, tx, req, mutation, true, false)
	})
}

func (s *PipelineSQLiteOwner) requireChannelCardAction(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) error {
	return s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return requireChannelCardMutationTx(txctx, tx, req, mutation, false, false)
	})
}

func (s *PipelinePostgresOwner) AcquireDecisionCardMutation(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) (pipeline.DecisionCardMutationLease, error) {
	if err := mutation.ValidateRequest(req); err != nil {
		return nil, err
	}
	runID, source, err := admitDecisionCardRequest(ctx, s, req)
	if err != nil {
		return nil, err
	}
	if err := s.requireChannelCardAction(ctx, req, mutation); err != nil {
		return nil, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, req)
	if err != nil {
		return nil, err
	}
	if _, _, err := admitDecisionCardRequest(ctx, s, req); err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	if err := s.requireChannelCardAction(ctx, req, mutation); err != nil {
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
	if err := s.requireChannelCardAction(ctx, req, mutation); err != nil {
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
	if err := s.requireChannelCardAction(ctx, req, mutation); err != nil {
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

func (l *decisionCardRequestLease) Commit(ctx context.Context, command pipeline.DecisionCardMutationCommand) (result pipeline.CommittedDecisionCardMutation, resultErr error) {
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
	defer func() {
		// A proven rolled-back CAS loss can reevaluate the same acquired request.
		// Acknowledged or uncertain outcomes never permit another mutation.
		if !result.Acknowledged && failures.IsStateContention(resultErr) {
			l.used = false
		}
	}()
	if s := l.postgres; s != nil {
		return commitDecisionCardOperation(ctx, s, s.DecisionPostgresOwner, true, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (pipeline.CommittedDecisionCardMutation, error)) mutationprotocol.Result[pipeline.CommittedDecisionCardMutation] {
			if err := s.requireCurrentSchema(); err != nil {
				return mutationprotocol.Reject[pipeline.CommittedDecisionCardMutation](err)
			}
			return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (pipeline.CommittedDecisionCardMutation, error) {
				err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					current, err := s.RequireActiveSourceTx(txctx, tx, l.runID)
					if err != nil {
						return err
					}
					if !current.Matches(l.source) {
						return fmt.Errorf("card source changed at commit")
					}
					return requireChannelCardMutationTx(txctx, tx, l.request, l.mutation, true, true)
				})
				if err != nil {
					return pipeline.CommittedDecisionCardMutation{}, err
				}
				return write(txctx, attempt)
			})
		}, command, func(ctx context.Context, tx *sql.Tx, completion apiidempotency.Completion) error {
			if err := settleChannelCardMutationTx(ctx, tx, l.mutation, true); err != nil {
				return err
			}
			return storeapiidempotency.StorePostgresCompletionTx(ctx, l.pgLease, tx, completion)
		})
	}
	s := l.sqlite
	return commitDecisionCardOperation(ctx, s, s.DecisionSQLiteOwner, false, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (pipeline.CommittedDecisionCardMutation, error)) mutationprotocol.Result[pipeline.CommittedDecisionCardMutation] {
		if err := s.requireCurrentSchema(); err != nil {
			return mutationprotocol.Reject[pipeline.CommittedDecisionCardMutation](err)
		}
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite decision-card operation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (pipeline.CommittedDecisionCardMutation, error) {
			err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				current, err := s.RequireActiveSourceTx(txctx, tx, l.runID)
				if err != nil {
					return err
				}
				if !current.Matches(l.source) {
					return fmt.Errorf("card source changed at commit")
				}
				return requireChannelCardMutationTx(txctx, tx, l.request, l.mutation, false, true)
			})
			if err != nil {
				return pipeline.CommittedDecisionCardMutation{}, err
			}
			return write(txctx, attempt)
		})
	}, command, func(ctx context.Context, tx *sql.Tx, completion apiidempotency.Completion) error {
		if err := settleChannelCardMutationTx(ctx, tx, l.mutation, false); err != nil {
			return err
		}
		return storeapiidempotency.StoreSQLiteCompletionTx(ctx, l.sqLease, tx, completion)
	})
}
