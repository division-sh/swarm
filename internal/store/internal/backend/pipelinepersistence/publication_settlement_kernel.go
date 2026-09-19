package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
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

// The enclosing named operation owns claim admission, transaction and the one
// revision finalizer. Singleton and segment settlement share all domain writes.
func settlePipelineMemberTx(ctx context.Context, tx *sql.Tx, postgres bool, now time.Time,
	claim pipelineobligation.Claim, disposition pipelineobligation.Disposition,
	effects *revisionEffects, candidates CompletionCandidateRequester, handoff *runhandoff.CandidateHandoff,
) error {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	if err := writePipelineDispositionTx(ctx, tx, effects, claim.EventID(), claim.Purpose(), disposition, postgres, now); err != nil {
		return err
	}
	runID, err := eventRunIDForCompletionCandidateTx(ctx, tx, claim.EventID(), postgres)
	if err != nil {
		return err
	}
	if runID != "" {
		if _, err := candidates.RequestCompletionCandidateTx(ctx, tx, runID, nil, handoff); err != nil {
			return err
		}
	}
	if !disposition.Successful() {
		return nil
	}
	if postgres {
		err = postgresDeliveryAdapter.CommitPipelineHandoff(ctx, tx, effects, claim.EventID())
	} else {
		err = sqliteDeliveryAdapter.CommitPipelineHandoff(ctx, tx, effects, claim.EventID())
	}
	if err != nil {
		return err
	}
	return nil
}
