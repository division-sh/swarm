package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunbundle "github.com/division-sh/swarm/internal/store/internal/runbundle"
)

type upgradeSnapshot struct {
	tx     *sql.Tx
	owner  bundleUpgradeReadOwner
	sqlite bool
}

func (s upgradeSnapshot) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]runbundle.Availability, error) {
	return storerunbundle.ListActiveNonStandingTx(ctx, s.tx, s.sqlite)
}

func (s upgradeSnapshot) ReadBundleUpgradeState(ctx context.Context, runID string, source semanticview.Source) (runbundle.UpgradeState, error) {
	return readBundleUpgradeStateTx(ctx, s.tx, s.owner, s.sqlite, runID, source)
}

type bundleUpgradeMutationOwner interface {
	bundleUpgradeReadOwner
	ReviseSourceTx(context.Context, *mutationprotocol.Attempt, runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
}

func (s *PipelinePostgresOwner) CommitBundleUpgradeTx(ctx context.Context, attempt *mutationprotocol.Attempt, plan *runbundle.UpgradePlan) error {
	return commitBundleUpgrade(ctx, attempt, s, false, plan)
}

func (s *PipelineSQLiteOwner) CommitBundleUpgradeTx(ctx context.Context, attempt *mutationprotocol.Attempt, plan *runbundle.UpgradePlan) error {
	return commitBundleUpgrade(ctx, attempt, s, true, plan)
}

func commitBundleUpgrade(ctx context.Context, attempt *mutationprotocol.Attempt, owner bundleUpgradeMutationOwner, sqlite bool, plan *runbundle.UpgradePlan) error {
	if attempt == nil || plan == nil {
		return errors.New("bundle upgrade requires its native attempt and exact plan")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		candidate := plan.Sources[plan.BundleHash]
		fresh, err := runbundle.AdmitPinnedSources(ctx, upgradeSnapshot{tx: tx, owner: owner, sqlite: sqlite}, candidate, func(_ context.Context, hash string) (semanticview.Source, error) {
			retained := plan.Sources[hash]
			if retained == nil {
				return nil, fmt.Errorf("bundle upgrade retained source %s changed before admission", hash)
			}
			return retained, nil
		})
		if err != nil {
			return err
		}
		if fresh.BundleHash != plan.BundleHash {
			return errors.New("bundle upgrade candidate identity changed before admission")
		}
		source, err := correlation.NewSourceArtifactFact(fresh.BundleHash)
		if err != nil {
			return err
		}
		for _, runID := range slices.Sorted(maps.Keys(fresh.Runs)) {
			if err := cancelRemovedUpgradeTimers(ctx, tx, attempt, sqlite, runID, candidate); err != nil {
				return err
			}
			if _, err := owner.ReviseSourceTx(ctx, attempt, runlifecycle.SourceRevisionRequest{RunID: runID, Source: source}); err != nil {
				return err
			}
		}
		return nil
	})
}

func cancelRemovedUpgradeTimers(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, sqlite bool, runID string, candidate semanticview.Source) error {
	timers, err := listWorkflowTimerActivations(ctx, tx, sqlite, runID, "", true)
	if err != nil || len(timers) == 0 {
		return err
	}
	instances, err := listWorkflowInstances(ctx, tx, sqlite, runID)
	if err != nil {
		return err
	}
	for _, timer := range timers {
		var header *runtimepipeline.WorkflowInstance
		for i := range instances {
			if instances[i].StorageRef == timer.Route.InstancePath && instances[i].EntityID == timer.EntityID {
				header = &instances[i]
				break
			}
		}
		if header == nil {
			return fmt.Errorf("bundle upgrade timer %s lacks its exact instance", timer.Ref.ActivationID)
		}
		selected, err := runtimepipeline.SelectInheritedWorkflowTimer(candidate, timer, *header)
		if err != nil {
			return err
		}
		if selected == nil {
			if _, err := cancelWorkflowEngineTimerActivation(ctx, tx, !sqlite, attempt, timer, runtimepipeline.WorkflowTimerCancelCauseRuleRemoved, time.Now().UTC()); err != nil {
				return err
			}
		}
	}
	return nil
}
