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
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
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
	if _, err := mutationprotocol.RunRowLock(ctx, s.tx, runID, true); err != nil {
		return runbundle.UpgradeState{}, err
	}
	return readBundleUpgradeStateTx(ctx, s.tx, s.owner, s.sqlite, runID, source)
}

type bundleUpgradeMutationOwner interface {
	bundleUpgradeReadOwner
	ReviseSourceTx(context.Context, *mutationprotocol.Attempt, runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
	genericScheduleTxOwner() GenericScheduleTxOwner
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
		fresh, err := runbundle.AdmitPinnedSources(ctx, upgradeSnapshot{tx: tx, owner: owner, sqlite: sqlite}, candidate, func(_ context.Context, reference sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
			retained := plan.Sources[reference.BundleHash]
			if retained == nil {
				return nil, fmt.Errorf("bundle upgrade retained source %s changed before admission", reference.BundleHash)
			}
			bundle, ok := semanticview.Bundle(retained)
			if !ok || bundle == nil {
				return nil, errors.New("bundle upgrade retained source lacks its compiled root")
			}
			rooted, err := bundle.RootedSource()
			if err != nil || rooted.Admission().Reference() != reference {
				return nil, errors.Join(errors.New("bundle upgrade retained source root changed before admission"), err)
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
		candidateBundle, ok := semanticview.Bundle(candidate)
		if !ok || candidateBundle == nil {
			return errors.New("bundle upgrade candidate lacks its compiled root")
		}
		rooted, err := candidateBundle.RootedSource()
		if err != nil {
			return err
		}
		if rooted.Admission().BundleHash() != source.BundleHash() {
			return errors.New("bundle upgrade candidate root differs from its revalidated source")
		}
		for _, runID := range slices.Sorted(maps.Keys(fresh.Runs)) {
			if err := cancelRemovedUpgradeTimers(ctx, tx, attempt, sqlite, runID, fresh.Sources[fresh.Runs[runID]], candidate); err != nil {
				return err
			}
			if err := cancelRemovedUpgradeClocks(ctx, tx, attempt, owner.genericScheduleTxOwner(), sqlite, runID, candidate); err != nil {
				return err
			}
			if _, err := owner.ReviseSourceTx(ctx, attempt, runlifecycle.SourceRevisionRequest{RunID: runID, Source: source, RootAdmission: rooted.Admission()}); err != nil {
				return err
			}
		}
		return nil
	})
}

func cancelRemovedUpgradeClocks(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, owner GenericScheduleTxOwner, sqlite bool, runID string, candidate semanticview.Source) error {
	clocks, err := storegenericschedule.ReadRunClocks(ctx, tx, !sqlite, runID, true)
	if err != nil {
		return err
	}
	declarations := semanticview.ClockSchedules(candidate)
	for _, clock := range clocks {
		if clock.Status != genericschedule.StatusActive || slices.ContainsFunc(declarations, func(rule semanticview.ClockSchedule) bool {
			return rule.FlowID == clock.FlowID && rule.Name == clock.Name
		}) {
			continue
		}
		if owner == nil {
			return errors.New("bundle upgrade requires the generic schedule transaction owner")
		}
		activation, found, err := owner.LoadActivationTx(ctx, attempt, clock.ActivationID)
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("bundle upgrade clock %s disappeared", clock.ActivationID))
		}
		if _, err := owner.CancelAdmissionTx(ctx, attempt, activation.Command, "rule_removed", time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func cancelRemovedUpgradeTimers(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, sqlite bool, runID string, current, candidate semanticview.Source) error {
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
		selected, err := runtimepipeline.SelectWorkflowTimerSuccessor(current, candidate, timer, *header)
		if err != nil {
			return err
		}
		if selected == nil {
			if _, err := cancelWorkflowEngineTimerActivation(ctx, tx, !sqlite, attempt, timer, runtimepipeline.WorkflowTimerCancelCauseRuleRemoved, time.Now().UTC()); err != nil {
				return err
			}
		} else if selected.Ref != timer.Ref {
			if _, err := cancelWorkflowEngineTimerActivation(ctx, tx, !sqlite, attempt, timer, "", time.Time{}); err != nil {
				return err
			}
			if _, err := insertWorkflowEngineTimerActivation(ctx, tx, !sqlite, attempt, *selected); err != nil {
				return err
			}
		}
	}
	return nil
}
