package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) RequireMaterializedRunFork(ctx context.Context, forkRunID string, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkMaterializerAccess(); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("begin exact fork materialization snapshot: %w", err)
	}
	defer tx.Rollback()
	return requireMaterializedRunForkSnapshot(ctx, tx, true, forkRunID, req, s.durableData,
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, false)
		})
}

func (s *RunForkSQLiteOwner) RequireMaterializedRunFork(ctx context.Context, forkRunID string, req runfork.RunForkMaterializeRequest) (materialization runfork.RunForkMaterialization, err error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("sqlite store is required")
	}
	if err := s.requireRunForkMaterializerAccess(); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	err = s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		materialization, err = requireMaterializedRunForkSnapshot(txctx, tx, false, forkRunID, req, s.durableData,
			func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
				return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
			})
		return err
	})
	return materialization, err
}

func requireMaterializedRunForkSnapshot(ctx context.Context, tx *sql.Tx, postgres bool, forkRunID string, req runfork.RunForkMaterializeRequest, data *storedurabledata.Owner, loadSnapshot runForkLifecycleSnapshotLoader) (runfork.RunForkMaterialization, error) {
	id, err := uuid.Parse(forkRunID)
	if err != nil || id == uuid.Nil || id.String() != forkRunID {
		return runfork.RunForkMaterialization{}, fmt.Errorf("required fork materialization needs a canonical child UUID")
	}
	snapshot, err := loadSnapshot(ctx, tx, forkRunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("load required fork materialization: %w", err)
	}
	origin := snapshot.Origin
	if origin.Kind() != runtimerunlifecycle.OriginForkMaterialization || origin.SourceRunID() != strings.TrimSpace(req.SourceRunID) {
		return runfork.RunForkMaterialization{}, fmt.Errorf("fork materialization %s conflicts with persisted lifecycle state", forkRunID)
	}
	if origin.ForkPointKind() != runfork.RunForkPointEvent {
		return runfork.RunForkMaterialization{}, fmt.Errorf("deployment revision materialization requires selected fork operation authority")
	}
	selector := strings.TrimSpace(req.At)
	if selector != "" && selector != origin.SourceEventID() {
		return runfork.RunForkMaterialization{}, fmt.Errorf("required fork materialization disagrees with event selector")
	}
	validate, resolve := runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	if postgres {
		validate, resolve = runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	}
	// The retained child selects its original cut. An omitted selector never
	// resolves today's latest source event or discovers a same-cut sibling.
	plan, err := planRunForkSnapshot(ctx, tx, runfork.RunForkPlanRequest{SourceRunID: origin.SourceRunID(), At: origin.SourceEventID()}, validate, resolve)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if plan.ForkPoint.Kind != origin.ForkPointKind() || plan.ForkPoint.Revision != origin.ForkRevision() || plan.ForkPoint.EventID != origin.SourceEventID() {
		return runfork.RunForkMaterialization{}, fmt.Errorf("required fork materialization disagrees with fixed source revision")
	}
	plan.ForkPoint.Input = selector
	if !plan.ExecutionReady {
		return runfork.RunForkMaterialization{}, fmt.Errorf("fork materialization requires execution-ready plan; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}
	loadSource := sourceadmission.LoadSQLiteTx
	if postgres {
		loadSource = sourceadmission.LoadPostgresTx
	}
	source := runForkSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
		return loadSource(ctx, tx, runID, true, true)
	})
	identity, err := resolveRunForkBundleInsertIdentity(ctx, source, plan.SourceRunID, req.SourceArtifactFact)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	var selection *runfork.RunForkContractSelection
	if req.ContractSelection != nil {
		normalized, err := normalizeRunForkSelectedContractSelection(*req.ContractSelection)
		if err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		selection = &normalized
	}
	if err := requireOriginalFanOutCarriage(ctx, source, plan, req.OriginalLoopCarriage); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	refs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	var profile scenarioexecution.Profile
	var profiled bool
	if postgres {
		profile, profiled, err = admitRunForkScenarioProfile(ctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
	} else {
		profile, profiled, err = admitSQLiteRunForkScenarioProfile(ctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
	}
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	materialization, found, err := loadExactRunForkMaterialization(ctx, loadSnapshot, tx, postgres, forkRunID, plan, identity, selection)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if !found {
		return runfork.RunForkMaterialization{}, &runtimerunlifecycle.RunNotFoundError{RunID: forkRunID}
	}
	if postgres {
		err = requireExactRunForkScenarioProfile(ctx, tx, forkRunID, profile, profiled)
	} else {
		err = requireExactSQLiteRunForkScenarioProfile(ctx, tx, forkRunID, profile, profiled)
	}
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	pins, err := storedurabledata.MaterializeForkPinsTx(data, ctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if err := requireExactMaterializedRunForkFanOut(ctx, tx, postgres, forkRunID, plan, refs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), data, pins); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	materialization.DataPins = pins
	materialization.MaterializedFanOutCount = len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + len(pins)
	return materialization, nil
}
