package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

// runForkSelectedContractMaterializationPort is deliberately operation-specific.
// It exposes persistence mechanics while the materialization lifecycle executes
// once in materializeRunForkForSelectedContractExecution.
type runForkSelectedContractMaterializationPort struct {
	postgres            bool
	requireCurrent      func() error
	runMutation         func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error
	lockSourceStatus    func(context.Context, *sql.Tx, string) (string, error)
	plan                func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	deliveries          *storedelivery.Adapter
	loadSource          func(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error)
	activeForkSource    func(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error)
	admitProfile        func(context.Context, *sql.Tx, string, scenarioexecution.EffectiveSourceIdentity, runtimecorrelation.SourceArtifactFact) (scenarioexecution.Profile, bool, error)
	loadSnapshot        runForkLifecycleSnapshotLoader
	requireProfile      func(context.Context, *sql.Tx, string, scenarioexecution.Profile, bool) error
	durableData         *storedurabledata.Owner
	insertRun           func(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, string, string, string, int, time.Time, runtimecorrelation.SourceArtifactFact) error
	ensureProfile       func(context.Context, *sql.Tx, string, scenarioexecution.Profile, time.Time) error
	materializeEntity   func(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, activeRunSourceOwnerFunc, *runforkrevision.Effects, string, runfork.RunForkPlan, runfork.RunForkEntityState, runForkEntityMetadata, time.Time) error
	materializeBarriers runForkFanOutBarrierOwner
	now                 func() time.Time
}

func materializeRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionMaterializeRequest, port runForkSelectedContractMaterializationPort) (materialization runfork.RunForkMaterialization, err error) {
	if port.requireCurrent == nil || port.runMutation == nil || port.lockSourceStatus == nil || port.plan == nil ||
		port.deliveries == nil || port.loadSource == nil || port.activeForkSource == nil || port.admitProfile == nil || port.loadSnapshot == nil ||
		port.requireProfile == nil || port.durableData == nil || port.insertRun == nil || port.ensureProfile == nil ||
		port.materializeEntity == nil || port.materializeBarriers == nil || port.now == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("selected-contract fork materialization operations are incomplete")
	}
	if err := port.requireCurrent(); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}

	err = port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects) error {
		sourceRunID := strings.TrimSpace(req.SourceRunID)
		sourceStatus, err := port.lockSourceStatus(txctx, tx, sourceRunID)
		if err != nil {
			return err
		}
		if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
			state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
			if parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("selected-contract fork source state %s is unsupported", state)
		}
		plan, err := port.plan(txctx, tx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: strings.TrimSpace(req.At)})
		if err != nil {
			return err
		}
		replayAdmission := runfork.RunForkSelectedContractReplayResumeAdmission(plan)
		forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
		routeRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(plan, forkRunID, selection, req.FrontierAdmission, req.RouteTopology, req.RecipientPlanning)
		if err != nil {
			return err
		}
		if routeResolved {
			replayAdmission = runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(replayAdmission)
		}
		if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil); len(blockers) > 0 {
			materialization = runfork.RunForkMaterialization{
				SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, ExecutionReady: false,
				ReplayResumeAdmission: replayAdmission, UnsupportedBlockers: blockers, DeliveryResumeBlocked: true,
			}
			return fmt.Errorf("selected-contract fork execution materialization blocked: %s", runForkBlockerCodes(blockers))
		}
		if err := ensureRunForkActivationNoForkReplayState(txctx, tx, port.deliveries, forkRunID); err != nil {
			return err
		}
		source := runForkSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return port.loadSource(ctx, tx, runID)
		})
		identity, err := resolveRunForkBundleInsertIdentity(txctx, source, plan.SourceRunID, req.SourceArtifactFact)
		if err != nil {
			return fmt.Errorf("resolve selected-contract fork bundle identity: %w", err)
		}
		fanOutPlanRefs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
		if err != nil {
			return err
		}
		scenarioProfile, sourceProfiled, err := port.admitProfile(txctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
		if err != nil {
			return err
		}
		existing, found, err := loadExactRunForkMaterialization(txctx, port.loadSnapshot, tx, forkRunID, plan, identity, &selection)
		if err != nil {
			return err
		}
		if found {
			if err := requireExactMaterializedRunForkFanOut(txctx, tx, port.postgres, forkRunID, plan, fanOutPlanRefs); err != nil {
				return err
			}
			if err := port.requireProfile(txctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
				return err
			}
			pins, err := storedurabledata.MaterializeForkPinsTx(port.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
			if err != nil {
				return err
			}
			existing.DataPins = pins
			existing.MaterializedFanOutCount = len(plan.FanOutObligations)
			if routeResolved {
				if err := validateRunForkSelectedContractRouteRecoveryAtActivation(txctx, tx, routeRecovery); err != nil {
					return err
				}
			} else {
				var count int
				if err := tx.QueryRowContext(txctx, `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1`, forkRunID).Scan(&count); err != nil {
					return fmt.Errorf("count existing selected-contract route recovery: %w", err)
				}
				if count != 0 {
					return fmt.Errorf("fork materialization %s has unexpected selected-contract route recovery", forkRunID)
				}
			}
			existing.ExecutionReady = false
			existing.ReplayResumeAdmission = replayAdmission
			existing.UnsupportedBlockers = runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil)
			materialization = existing
			return nil
		}

		metadata, err := loadRunForkEntityMetadata(plan)
		if err != nil {
			return err
		}
		workflowStates, err := selectedContractWorkflowStates(plan, forkRunID, selection, req.RecipientPlanning, req.WorkflowStates)
		if err != nil {
			return err
		}
		now := port.now().UTC()
		txctx = runtimecorrelation.WithSourceArtifactFact(txctx, identity.SourceArtifactFact)
		forkScope, err := runtimeauthoractivity.BundleScopeForTarget(txctx, identity.SourceArtifactFact.BundleHash())
		if err != nil {
			return fmt.Errorf("resolve selected-contract fork author activity scope: %w", err)
		}
		txctx = runtimeauthoractivity.WithScope(txctx, forkScope)
		if err := port.insertRun(txctx, tx, story, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now, identity.SourceArtifactFact); err != nil {
			return fmt.Errorf("insert selected-contract fork run: %w", err)
		}
		pins, err := storedurabledata.MaterializeForkPinsTx(port.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
		if err != nil {
			return err
		}
		if sourceProfiled {
			if err := port.ensureProfile(txctx, tx, forkRunID, scenarioProfile, now); err != nil {
				return fmt.Errorf("inherit selected-contract fork scenario execution profile: %w", err)
			}
		}
		forkCtx := runtimecorrelation.WithRunID(txctx, forkRunID)
		forkMutationSource := activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return port.activeForkSource(ctx, tx, runID)
		})
		for _, entity := range plan.Entities {
			if err := port.materializeEntity(forkCtx, tx, story, forkMutationSource, effects, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
				return err
			}
		}
		materializedFanOutCount, err := materializeRunForkFanOutObligations(txctx, tx, port.postgres, effects, port.materializeBarriers, forkRunID, plan, fanOutPlanRefs, now)
		if err != nil {
			return err
		}
		for _, state := range workflowStates {
			if err := materializeSelectedContractWorkflowState(txctx, tx, state, now); err != nil {
				return err
			}
		}
		binding, err := insertRunForkSelectedContractBinding(txctx, tx, runfork.RunForkSelectedContractBindingRequest{
			ForkRunID: forkRunID, SourceRunID: plan.SourceRunID, ForkEventID: plan.ForkPoint.EventID, ContractSelection: selection,
		}, now)
		if err != nil {
			return err
		}
		if routeResolved {
			if err := insertRunForkSelectedContractRouteRecovery(txctx, tx, routeRecovery); err != nil {
				return err
			}
		}
		materialization = runfork.RunForkMaterialization{
			SourceRunID: plan.SourceRunID, ForkRunID: forkRunID, ForkRunStatus: runfork.RunForkMaterializedStatus,
			ForkPoint: plan.ForkPoint, MaterializedEntityCount: len(plan.Entities), MaterializedFanOutCount: materializedFanOutCount, ExecutionReady: false,
			ReplayResumeAdmission: replayAdmission, SelectedContractBinding: &binding,
			UnsupportedBlockers:   runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil),
			DeliveryResumeBlocked: true, SourceRunStatusUnchanged: true, DataPins: pins,
		}
		return nil
	})
	return materialization, err
}

func postgresRunForkSelectedContractMaterializationPort(s *RunForkPostgresOwner) runForkSelectedContractMaterializationPort {
	return runForkSelectedContractMaterializationPort{
		postgres:       true,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				return fmt.Errorf("begin selected-contract fork materialization: %w", err)
			}
			committed := false
			defer func() {
				if !committed {
					_ = tx.Rollback()
				}
			}()
			story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
			if err != nil {
				return err
			}
			effects := runforkrevision.NewEffects()
			if err := operation(ctx, tx, story, effects); err != nil {
				return err
			}
			if err := commitRunForkAuthorActivityTransaction(ctx, tx, story, effects); err != nil {
				return fmt.Errorf("commit selected-contract fork materialization: %w", err)
			}
			committed = true
			return nil
		},
		lockSourceStatus: func(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
			var status string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid FOR UPDATE`, runID).Scan(&status); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return "", &runtimerunlifecycle.RunNotFoundError{RunID: runID}
				}
				return "", fmt.Errorf("load selected-contract fork materialization source: %w", err)
			}
			return status, nil
		},
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint)
		},
		deliveries: postgresDeliveryAdapter,
		loadSource: func(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecyclePostgresOwner.RequirePresentSourceTx(ctx, tx, runID)
		},
		activeForkSource: func(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
		},
		admitProfile: admitRunForkScenarioProfile,
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		requireProfile: requireExactRunForkScenarioProfile,
		durableData:    s.durableData,
		insertRun:      s.InsertRunForkRunTx,
		ensureProfile:  scenarioexecutionpersistence.EnsurePostgres,
		materializeEntity: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, source activeRunSourceOwnerFunc, effects *runforkrevision.Effects, forkRunID string, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, metadata runForkEntityMetadata, now time.Time) error {
			return materializeRunForkEntityState(ctx, s.DecisionPostgresOwner, s.MaterializeRunForkProposedEffectCardsTx, privatemutationlog.InsertEntityStateDiffWithStory, tx, story, source, effects, forkRunID, plan, entity, metadata, now)
		},
		materializeBarriers: s.PipelinePostgresOwner,
		now:                 func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractMaterializationPort(s *RunForkSQLiteOwner) runForkSelectedContractMaterializationPort {
	return runForkSelectedContractMaterializationPort{
		postgres:       false,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			return s.runRuntimeMutation(ctx, "sqlite selected-contract fork materialization", func(txctx context.Context, tx *sql.Tx) error {
				story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
				if err != nil {
					return err
				}
				effects := runforkrevision.NewEffects()
				if err := operation(txctx, tx, story, effects); err != nil {
					return err
				}
				if err := story.Finalize(txctx); err != nil {
					return err
				}
				_, err = runforkrevision.FinalizeSQLite(txctx, tx, effects)
				return err
			})
		},
		lockSourceStatus: func(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
			var status string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, runID).Scan(&status); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return "", &runtimerunlifecycle.RunNotFoundError{RunID: runID}
				}
				return "", fmt.Errorf("load selected-contract fork materialization source: %w", err)
			}
			return status, nil
		},
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		},
		deliveries: sqliteDeliveryAdapter,
		loadSource: func(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecycleSQLiteOwner.RequirePresentSourceTx(ctx, tx, runID)
		},
		activeForkSource: func(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
		},
		admitProfile: admitSQLiteRunForkScenarioProfile,
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		requireProfile: requireExactSQLiteRunForkScenarioProfile,
		durableData:    s.durableData,
		insertRun:      s.InsertRunForkRunTx,
		ensureProfile:  scenarioexecutionpersistence.EnsureSQLite,
		materializeEntity: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, source activeRunSourceOwnerFunc, effects *runforkrevision.Effects, forkRunID string, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, metadata runForkEntityMetadata, now time.Time) error {
			return materializeRunForkEntityState(ctx, s.DecisionSQLiteOwner, s.MaterializeRunForkProposedEffectCardsTx, privatemutationlog.InsertSQLiteEntityStateDiffWithStory, tx, story, source, effects, forkRunID, plan, entity, metadata, now)
		},
		materializeBarriers: s.PipelineSQLiteOwner,
		now:                 s.now,
	}
}
