package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	privaterunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type selectedHandoffEffectOwner interface {
	RequireCurrentExternalEffectAuthorityTx(context.Context, *sql.Tx, effects.Authority) error
	RequireCompletionAuthorityNoLiveAttemptsTx(context.Context, *sql.Tx, effects.Authority) error
}

// The activation attempt owns all four writes: fence/close, live selection,
// immutable ACK and transfer. None of them requires fictional run terminality.
func commitSelectedLiveHandoffTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, evidence runForkSelectedContractActivationEvidence,
	selectLive func(context.Context, *mutationprotocol.Attempt, string, time.Time) (string, error),
	effectOwner selectedHandoffEffectOwner, deliveries *storedelivery.Adapter, candidates mutationprotocol.CandidateWriter, sqlite bool,
) (string, error) {
	grant, present := startupownership.GenerationGrantEvidenceFromContext(ctx)
	if !present {
		return "", errors.New("live handoff requires an admitted ordinary generation")
	}
	if err := generationauthority.RequireAdmittedOrdinaryGrantTx(ctx, tx, grant, evidence.lineage.ForkBundleHash, sqlite); err != nil {
		return "", err
	}
	authority, err := selectedContractActivationAuthority(ctx, evidence.lineage.ForkRunID)
	if err != nil {
		return "", err
	}
	if err := effectOwner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
		return "", err
	}
	if err := effectOwner.RequireCompletionAuthorityNoLiveAttemptsTx(ctx, tx, authority); err != nil {
		return "", err
	}
	if err := agentpersistence.RequireSelectedSelfReleaseTx(ctx, tx, evidence.lineage.ForkRunID, authority.ID); err != nil {
		return "", err
	}
	previous, found, err := privaterunlifecycle.LiveRunForTargetTx(ctx, tx, evidence.lineage.ForkRunID, !sqlite)
	if err != nil {
		return "", err
	}
	if found {
		if err := agentpersistence.RequireRunActorsParkedTx(ctx, tx, previous.RunID); err != nil {
			return "", err
		}
		if err := storedelivery.RequireRunContinuationsParkedTx(ctx, tx, previous.RunID); err != nil {
			return "", err
		}
		if err := effectpersistence.RequireRunPauseSettledTx(ctx, tx, previous.RunID); err != nil {
			return "", err
		}
	}
	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions
		SET state='closed',lease_expires_at=NULL,fence_generation=fence_generation+1,terminal_at=$6,updated_at=$6
		WHERE execution_id=$1 AND fork_run_id=$2 AND state='running' AND fence_generation=$3
		AND generation=$4 AND execution_owner=$5 AND lease_expires_at>$6 AND failure IS NULL`,
		authority.ID, evidence.lineage.ForkRunID, authority.FenceGeneration, authority.SelectedFork.Generation, authority.ExecutionOwner, now)
	if err := requireExactlyOneMutation(result, err, "close exact selected executor for live handoff"); err != nil {
		return "", err
	}
	if _, err := selectLive(ctx, attempt, evidence.lineage.ForkRunID, now); err != nil {
		return "", err
	}
	var sourceStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, evidence.lineage.SourceRunID).Scan(&sourceStatus); err != nil {
		return "", err
	}
	evidence.lineage.ForkStatus = runfork.RunForkLiveStatus
	if err := completeSelectedForkOperationAtActivation(ctx, tx, evidence.request, evidence.lineage, sourceStatus, sourceStatus == runfork.RunForkSourceFrozenStatus, !sqlite); err != nil {
		return "", err
	}
	fact, err := correlation.DecodeSourceArtifactFact(grant.BundleHash)
	if err != nil {
		return "", err
	}
	successor, err := deliverylifecycle.NewNormalExecutionAuthority(fact, grant.GrantID, grant.RuntimeGeneration)
	if err != nil {
		return "", err
	}
	if err := deliveries.TransferSelectedLiveAuthority(ctx, attempt, authority, successor); err != nil {
		return "", err
	}
	unfinished, err := pipelinepersistence.UnacknowledgedRunPipelineCountTx(ctx, tx, evidence.lineage.ForkRunID)
	if err != nil {
		return "", err
	}
	if unfinished != 0 {
		return "", errors.New("live handoff awaits canonical pipeline acknowledgement")
	}
	if err := startupownership.WithLentOrdinaryMutationAuthority(ctx, evidence.request.OrdinaryGenerationGrant, func(ordinary context.Context) error {
		_, err := attempt.RequestCompletion(ordinary, candidates, evidence.lineage.ForkRunID, nil)
		return err
	}); err != nil {
		return "", err
	}
	return sourceStatus, nil
}
