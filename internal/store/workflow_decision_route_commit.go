package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

func commitProposedEffectRoute(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	command runtimepipeline.ProposedEffectRouteCommand,
) (runtimepipeline.CommittedProposedEffectRoute, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	plan, ok := command.Publication.(runtimebus.EnginePublicationPlan)
	if !ok {
		return runtimepipeline.CommittedProposedEffectRoute{}, fmt.Errorf("proposed-effect route publication has unexpected type %T", command.Publication)
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	defer handoff.rollback()

	var result runtimepipeline.CommittedProposedEffectRoute
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committed, err := commitPublicationTx(txctx, tx, story, store, postgres, plan.PublicationCommand(), publicationCommitOptions{})
		if err != nil {
			return fmt.Errorf("commit proposed-effect route publication: %w", err)
		}
		continuation, changed, err := completeProposedEffectRoute(txctx, tx, command.CardID, command.RouteEventID, command.OccurredAt, postgres)
		if err != nil {
			return err
		}
		if changed {
			candidate, err := requestCandidate(txctx, tx, continuation.RunID)
			if err != nil {
				return err
			}
			if err := prepare(handoff, candidate); err != nil {
				return err
			}
		}
		evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
		if err != nil {
			return err
		}
		result.Publication = evidence
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	if err := handoff.commit(); err != nil {
		return runtimepipeline.CommittedProposedEffectRoute{}, err
	}
	return result, nil
}

func (s *PostgresStore) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, true, s.runPrivateAuthorActivityMutation, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.prepare(&s.runLifecycleSinks, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, command)
}

func (s *SQLiteRuntimeStore) CommitProposedEffectRoute(ctx context.Context, command runtimepipeline.ProposedEffectRouteCommand) (runtimepipeline.CommittedProposedEffectRoute, error) {
	return commitProposedEffectRoute(ctx, s, false,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite proposed-effect route", fn)
		}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.prepare(&s.runLifecycleSinks, result)
		}, func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, command)
}

var _ runtimepipeline.WorkflowDecisionRouteOwner = (*PostgresStore)(nil)
var _ runtimepipeline.WorkflowDecisionRouteOwner = (*SQLiteRuntimeStore)(nil)
