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

// The enclosing attempt owns candidate representation and revision finalization.
func settlePipelineMemberTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, now time.Time,
	claim pipelineobligation.Claim, disposition pipelineobligation.Disposition,
	candidates mutationprotocol.CandidateWriter,
) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	var runID string
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := writePipelineDispositionTx(ctx, tx, attempt, claim.EventID(), claim.Purpose(), disposition, postgres, now); err != nil {
			return err
		}
		var err error
		runID, err = eventRunIDForCompletionCandidateTx(ctx, tx, claim.EventID(), postgres)
		return err
	}); err != nil {
		return err
	}
	if runID != "" {
		if _, err := attempt.RequestCompletion(ctx, candidates, runID, nil); err != nil {
			return err
		}
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
