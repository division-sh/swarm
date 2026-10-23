package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func validateGroupedSingletonSettlementTx(ctx context.Context, tx *sql.Tx, state *pipelineClaimState) error {
	g := state.operationMu.group
	if g == nil {
		return nil
	}
	if g.closed || !g.committed {
		return errors.New("grouped singleton settlement lacks committed attempt authority")
	}
	member, err := g.member(state.claim)
	if err != nil {
		return err
	}
	if err := g.admitSettlementTx(ctx, tx); err != nil {
		return err
	}
	return g.validateCommittedMemberTx(ctx, tx, member)
}

// The enclosing attempt owns the acknowledged disposition and revision tail.
func settlePipelineMemberTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, now time.Time,
	claim pipelineobligation.Claim, disposition pipelineobligation.Disposition,
	candidates completionCandidateWriter,
) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return writePipelineDispositionTx(ctx, tx, attempt, claim.EventID(), claim.Purpose(), disposition, postgres, now)
	}); err != nil {
		return err
	}
	if !disposition.Successful() {
		return nil
	}
	if postgres {
		return postgresDeliveryAdapter.CommitPipelineHandoff(ctx, attempt, claim.EventID())
	} else {
		return sqliteDeliveryAdapter.CommitPipelineHandoff(ctx, attempt, claim.EventID())
	}
}
