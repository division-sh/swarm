package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type sourceSetUpgrade struct {
	attempt *mutationprotocol.Attempt
	owner   interface {
		CommitBundleUpgradeTx(context.Context, *mutationprotocol.Attempt, *runbundle.UpgradePlan) error
	}
}

func (u sourceSetUpgrade) commit(ctx context.Context, plan *runbundle.UpgradePlan) error {
	if u.attempt == nil || u.owner == nil {
		return errors.New("bundle upgrade requires the retained native mutation and pipeline owner")
	}
	return u.owner.CommitBundleUpgradeTx(ctx, u.attempt, plan)
}

func sourceSetHasBundle(plan agenttopology.SourceSetPlan, hash string) bool {
	return len(plan.Sources) == 1 && plan.Sources[0].BundleHash == hash
}

func applySourceSetUpgrade(ctx context.Context, req agenttopology.SourceSetCommitRequest, upgrades []sourceSetUpgrade) error {
	if req.BundleUpgrade == nil {
		return nil
	}
	if len(upgrades) != 1 || !sourceSetHasBundle(req.Plan, req.BundleUpgrade.BundleHash) {
		return errors.New("bundle upgrade requires its exact native source transition")
	}
	return upgrades[0].commit(ctx, req.BundleUpgrade)
}

func (s *postgresSession) commitBundleUpgrade(ctx context.Context, req agenttopology.SourceSetCommitRequest) (agenttopology.SourceSetCommitResult, error) {
	commit := mutationprotocol.RunRetainedPostgres(ctx, s.lease.Session(), mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (result agenttopology.SourceSetCommitResult, err error) {
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			result, err = commitSourceSetTx(ctx, tx, req, false, sourceSetUpgrade{attempt: attempt, owner: s.owner.fanOutPipeline})
			return err
		})
		return result, err
	})
	value, _ := commit.Value()
	return value, commit.Err()
}

func (s *sqliteSession) commitBundleUpgrade(ctx context.Context, req agenttopology.SourceSetCommitRequest) (agenttopology.SourceSetCommitResult, error) {
	commit := mutationprotocol.RunSQLite(ctx, s.owner.backend, "upgrade admitted run bundles", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.owner.lifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (result agenttopology.SourceSetCommitResult, err error) {
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			result, err = commitSourceSetTx(ctx, tx, req, true, sourceSetUpgrade{attempt: attempt, owner: s.owner.fanOutPipeline})
			return err
		})
		return result, err
	})
	value, _ := commit.Value()
	return value, commit.Err()
}
