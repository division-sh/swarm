package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

type runForkEntityMetadata struct {
	FlowInstance string
	EntityType   string
	Slug         string
	Name         string
}

type ActiveRunSourceOwnerFunc func(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)
type activeRunSourceOwnerFunc = ActiveRunSourceOwnerFunc
type runForkSourceOwnerFunc func(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)

type runForkLifecycleSnapshotLoader func(context.Context, *sql.Tx, string) (storerunlifecycle.Snapshot, error)
type runForkEntityStateDiffWriter func(context.Context, *sql.Tx, privatemutationlog.ActiveRunSourceOwner, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, string, runtimemutationlog.EntityStateProjection, runtimemutationlog.EntityStateProjection, runtimemutationlog.Writer) error

func (fn activeRunSourceOwnerFunc) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return fn(ctx, runID)
}

func (fn runForkSourceOwnerFunc) LoadRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return fn(ctx, runID)
}

func (s *RunForkPostgresOwner) requireRunForkMaterializerAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkSQLiteOwner) requireRunForkMaterializerAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkPostgresOwner) MaterializeRunFork(ctx context.Context, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkMaterializerAccess(); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	var selection *runfork.RunForkContractSelection
	if req.ContractSelection != nil {
		normalized, err := normalizeRunForkSelectedContractSelection(*req.ContractSelection)
		if err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		selection = &normalized
		if err := s.requireRunForkSelectedContractBindingAccess(); err != nil {
			return runfork.RunForkMaterialization{}, err
		}
	}
	plan, err := s.PlanRunFork(ctx, runfork.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if !plan.ExecutionReady {
		return runfork.RunForkMaterialization{
			SourceRunID:           plan.SourceRunID,
			ForkPoint:             plan.ForkPoint,
			ExecutionReady:        false,
			ReplayResumeAdmission: plan.ReplayResumeAdmission,
			UnsupportedBlockers:   plan.UnsupportedBlockers,
			DeliveryResumeBlocked: true,
		}, fmt.Errorf("fork materialization requires execution-ready plan; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}

	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("begin fork materialization: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if err := requirePostgresRunActive(ctx, tx, plan.SourceRunID); err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("admit fork materialization source: %w", err)
	}

	identity, err := resolveRunForkBundleInsertIdentity(ctx, runForkSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
		return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
	}), plan.SourceRunID, req.SourceArtifactFact)
	if err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("resolve fork bundle identity: %w", err)
	}
	fanOutPlanRefs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	scenarioProfile, sourceProfiled, err := admitRunForkScenarioProfile(ctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	existing, found, err := loadExactRunForkMaterialization(
		ctx, func(ctx context.Context, tx *sql.Tx, runID string) (storerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		}, tx, forkRunID, plan, identity, selection,
	)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if found {
		if err := requireExactMaterializedRunForkFanOut(ctx, tx, true, forkRunID, plan, fanOutPlanRefs); err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		if err := requireExactRunForkScenarioProfile(ctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, ctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
		if err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		existing.DataPins = pins
		existing.MaterializedFanOutCount = len(plan.FanOutObligations)
		return existing, nil
	}
	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	effects := privaterunforkrevision.NewEffects()
	now := time.Now().UTC()
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, identity.SourceArtifactFact)
	forkScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, identity.SourceArtifactFact.BundleHash())
	if err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("resolve fork author activity scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, forkScope)
	if err := s.InsertRunForkRunTx(ctx, tx, story, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now, identity.SourceArtifactFact); err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("insert fork run: %w", err)
	}
	pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, ctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if sourceProfiled {
		if err := scenarioexecutionpersistence.EnsurePostgres(ctx, tx, forkRunID, scenarioProfile, now); err != nil {
			return runfork.RunForkMaterialization{}, fmt.Errorf("inherit fork scenario execution profile: %w", err)
		}
	}

	forkCtx := runtimecorrelation.WithRunID(ctx, forkRunID)
	for _, entity := range plan.Entities {
		if err := materializeRunForkEntityState(forkCtx, s.DecisionPostgresOwner, s.MaterializeRunForkProposedEffectCardsTx, privatemutationlog.InsertEntityStateDiffWithStory, tx, story, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
		}), effects, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
			return runfork.RunForkMaterialization{}, err
		}
	}
	materializedFanOutCount, err := materializeRunForkFanOutObligations(ctx, tx, true, effects, s.PipelinePostgresOwner, forkRunID, plan, fanOutPlanRefs, now)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	var selectedContractBinding *runfork.RunForkSelectedContractBinding
	if selection != nil {
		binding, err := insertRunForkSelectedContractBinding(ctx, tx, runfork.RunForkSelectedContractBindingRequest{
			ForkRunID:         forkRunID,
			SourceRunID:       plan.SourceRunID,
			ForkEventID:       plan.ForkPoint.EventID,
			ContractSelection: *selection,
		}, now)
		if err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		selectedContractBinding = &binding
	}
	if err := commitRunForkAuthorActivityTransaction(ctx, tx, story, effects); err != nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("commit fork materialization: %w", err)
	}
	committed = true
	return runfork.RunForkMaterialization{
		SourceRunID:              plan.SourceRunID,
		ForkRunID:                forkRunID,
		ForkRunStatus:            runfork.RunForkMaterializedStatus,
		ForkPoint:                plan.ForkPoint,
		MaterializedEntityCount:  len(plan.Entities),
		MaterializedFanOutCount:  materializedFanOutCount,
		ExecutionReady:           true,
		ReplayResumeAdmission:    plan.ReplayResumeAdmission,
		SelectedContractBinding:  selectedContractBinding,
		DeliveryResumeBlocked:    true,
		SourceRunStatusUnchanged: true,
		DataPins:                 pins,
	}, nil
}

func (s *RunForkSQLiteOwner) MaterializeRunFork(ctx context.Context, req runfork.RunForkMaterializeRequest) (materialization runfork.RunForkMaterialization, err error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("sqlite store is required")
	}
	if err := s.requireRunForkMaterializerAccess(); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	var selection *runfork.RunForkContractSelection
	if req.ContractSelection != nil {
		normalized, err := normalizeRunForkSelectedContractSelection(*req.ContractSelection)
		if err != nil {
			return runfork.RunForkMaterialization{}, err
		}
		selection = &normalized
		if err := s.requireRunForkSelectedContractBindingAccess(); err != nil {
			return runfork.RunForkMaterialization{}, err
		}
	}
	plan, err := s.PlanRunFork(ctx, runfork.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if !plan.ExecutionReady {
		return runfork.RunForkMaterialization{
			SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, ExecutionReady: false,
			ReplayResumeAdmission: plan.ReplayResumeAdmission, UnsupportedBlockers: plan.UnsupportedBlockers,
			DeliveryResumeBlocked: true,
		}, fmt.Errorf("fork materialization requires execution-ready plan; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}

	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
	err = s.runRuntimeMutation(ctx, "sqlite run fork materialization", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := requireSQLiteRunActive(txctx, tx, plan.SourceRunID); err != nil {
			return fmt.Errorf("admit fork materialization source: %w", err)
		}
		source := activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
			return s.RunLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
		})
		identity, err := resolveRunForkBundleInsertIdentity(txctx, runForkSourceOwnerFunc(source), plan.SourceRunID, req.SourceArtifactFact)
		if err != nil {
			return fmt.Errorf("resolve fork bundle identity: %w", err)
		}
		fanOutPlanRefs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
		if err != nil {
			return err
		}
		scenarioProfile, sourceProfiled, err := admitSQLiteRunForkScenarioProfile(txctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
		if err != nil {
			return err
		}
		existing, found, err := loadExactRunForkMaterialization(
			txctx,
			func(ctx context.Context, tx *sql.Tx, runID string) (storerunlifecycle.Snapshot, error) {
				return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
			},
			tx, forkRunID, plan, identity, selection,
		)
		if err != nil {
			return err
		}
		if found {
			if err := requireExactMaterializedRunForkFanOut(txctx, tx, false, forkRunID, plan, fanOutPlanRefs); err != nil {
				return err
			}
			if err := requireExactSQLiteRunForkScenarioProfile(txctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
				return err
			}
			pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
			if err != nil {
				return err
			}
			existing.DataPins = pins
			existing.MaterializedFanOutCount = len(plan.FanOutObligations)
			materialization = existing
			return nil
		}
		metadata, err := loadRunForkEntityMetadata(plan)
		if err != nil {
			return err
		}
		effects := privaterunforkrevision.NewEffects()
		now := s.now()
		txctx = runtimecorrelation.WithSourceArtifactFact(txctx, identity.SourceArtifactFact)
		forkScope, err := runtimeauthoractivity.BundleScopeForTarget(txctx, identity.SourceArtifactFact.BundleHash())
		if err != nil {
			return fmt.Errorf("resolve fork author activity scope: %w", err)
		}
		txctx = runtimeauthoractivity.WithScope(txctx, forkScope)
		if err := s.InsertRunForkRunTx(txctx, tx, story, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now, identity.SourceArtifactFact); err != nil {
			return fmt.Errorf("insert fork run: %w", err)
		}
		pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
		if err != nil {
			return err
		}
		if sourceProfiled {
			if err := scenarioexecutionpersistence.EnsureSQLite(txctx, tx, forkRunID, scenarioProfile, now); err != nil {
				return fmt.Errorf("inherit fork scenario execution profile: %w", err)
			}
		}
		forkCtx := runtimecorrelation.WithRunID(txctx, forkRunID)
		for _, entity := range plan.Entities {
			if err := materializeRunForkEntityState(forkCtx, s.DecisionSQLiteOwner, s.MaterializeRunForkProposedEffectCardsTx, privatemutationlog.InsertSQLiteEntityStateDiffWithStory, tx, story, source, effects, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
				return err
			}
		}
		materializedFanOutCount, err := materializeRunForkFanOutObligations(txctx, tx, false, effects, s.PipelineSQLiteOwner, forkRunID, plan, fanOutPlanRefs, now)
		if err != nil {
			return err
		}
		var selectedContractBinding *runfork.RunForkSelectedContractBinding
		if selection != nil {
			binding, err := insertRunForkSelectedContractBinding(txctx, tx, runfork.RunForkSelectedContractBindingRequest{
				ForkRunID: forkRunID, SourceRunID: plan.SourceRunID, ForkEventID: plan.ForkPoint.EventID,
				ContractSelection: *selection,
			}, now)
			if err != nil {
				return err
			}
			selectedContractBinding = &binding
		}
		if err := story.Finalize(txctx); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
			return err
		}
		materialization = runfork.RunForkMaterialization{
			SourceRunID: plan.SourceRunID, ForkRunID: forkRunID, ForkRunStatus: runfork.RunForkMaterializedStatus,
			ForkPoint: plan.ForkPoint, MaterializedEntityCount: len(plan.Entities), MaterializedFanOutCount: materializedFanOutCount, ExecutionReady: true,
			ReplayResumeAdmission: plan.ReplayResumeAdmission, SelectedContractBinding: selectedContractBinding,
			DeliveryResumeBlocked: true, SourceRunStatusUnchanged: true, DataPins: pins,
		}
		return nil
	})
	return materialization, err
}

func loadExactRunForkMaterialization(
	ctx context.Context,
	loadSnapshot runForkLifecycleSnapshotLoader,
	tx *sql.Tx,
	forkRunID string,
	plan runfork.RunForkPlan,
	identity runForkBundleInsertIdentity,
	selection *runfork.RunForkContractSelection,
) (runfork.RunForkMaterialization, bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT CAST(run_id AS TEXT)
		FROM runs
		WHERE run_id = $1
		   OR (forked_from_run_id = $2 AND forked_from_event_id = $3)
		ORDER BY started_at ASC
		LIMIT 2
	`, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("check existing fork materialization: %w", err)
	}
	var matches []string
	for rows.Next() {
		var existing string
		if err := rows.Scan(&existing); err != nil {
			_ = rows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf("scan existing fork materialization: %w", err)
		}
		matches = append(matches, existing)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("read existing fork materialization: %w", err)
	}
	_ = rows.Close()
	if len(matches) == 0 {
		return runfork.RunForkMaterialization{}, false, nil
	}
	if len(matches) != 1 || matches[0] != forkRunID {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization identity conflict for source run %s at event %s: matches=%v",
			plan.SourceRunID, plan.ForkPoint.EventID, matches,
		)
	}

	if loadSnapshot == nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("fork lifecycle snapshot loader is required")
	}
	snapshot, err := loadSnapshot(ctx, tx, forkRunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("load existing fork lifecycle: %w", err)
	}
	wantOrigin, err := storerunlifecycle.ForkMaterializationRunOrigin(plan.SourceRunID, plan.ForkPoint.EventID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, err
	}
	bundleHash := identity.SourceArtifactFact.BundleHash()
	if snapshot.State != storerunlifecycle.StatePaused ||
		!snapshot.Origin.Equal(wantOrigin) ||
		snapshot.BundleHash != bundleHash ||
		snapshot.EventCount != 0 ||
		snapshot.EntityCount != len(plan.Entities) {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization %s conflicts with persisted lifecycle state",
			forkRunID,
		)
	}

	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, err
	}
	expectedEntities := make(map[string]string, len(plan.Entities))
	for _, entity := range plan.Entities {
		sourceEntityID := strings.TrimSpace(entity.EntityID)
		identity, err := projectRunForkEntityIdentity(plan.SourceRunID, forkRunID, sourceEntityID, metadata[sourceEntityID].FlowInstance)
		if err != nil {
			return runfork.RunForkMaterialization{}, false, err
		}
		if _, duplicate := expectedEntities[identity.EntityID]; duplicate {
			return runfork.RunForkMaterialization{}, false, fmt.Errorf("fork materialization projects duplicate entity %s", identity.EntityID)
		}
		expectedEntities[identity.EntityID] = identity.FlowInstance
	}
	entityRows, err := tx.QueryContext(ctx, `
		SELECT CAST(entity_id AS TEXT), flow_instance
		FROM entity_state
		WHERE run_id = $1
	`, forkRunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("load existing fork entities: %w", err)
	}
	for entityRows.Next() {
		var entityID, flowInstance string
		if err := entityRows.Scan(&entityID, &flowInstance); err != nil {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf("scan existing fork entity: %w", err)
		}
		expectedFlowInstance, ok := expectedEntities[entityID]
		if !ok {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf(
				"fork materialization %s has unexpected entity %s",
				forkRunID, entityID,
			)
		}
		if flowInstance != expectedFlowInstance {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf(
				"fork materialization %s entity %s has flow_instance %q; want %q",
				forkRunID, entityID, flowInstance, expectedFlowInstance,
			)
		}
		delete(expectedEntities, entityID)
	}
	if err := entityRows.Err(); err != nil {
		_ = entityRows.Close()
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("read existing fork entities: %w", err)
	}
	_ = entityRows.Close()
	if len(expectedEntities) != 0 {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization %s is missing expected entities",
			forkRunID,
		)
	}

	var binding *runfork.RunForkSelectedContractBinding
	persistedBinding, bindingErr := loadRunForkSelectedContractBinding(ctx, tx, forkRunID)
	switch {
	case bindingErr == sql.ErrNoRows && selection == nil:
	case bindingErr == sql.ErrNoRows:
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization %s is missing its selected contract binding",
			forkRunID,
		)
	case bindingErr != nil:
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("load existing selected contract binding: %w", bindingErr)
	case selection == nil:
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization %s has an unexpected selected contract binding",
			forkRunID,
		)
	default:
		normalizedSelection, normalizeErr := normalizeRunForkSelectedContractSelection(*selection)
		if normalizeErr != nil {
			return runfork.RunForkMaterialization{}, false, normalizeErr
		}
		if persistedBinding.SourceRunID != plan.SourceRunID ||
			persistedBinding.ForkEventID != plan.ForkPoint.EventID ||
			persistedBinding.ContractSelection != normalizedSelection {
			return runfork.RunForkMaterialization{}, false, fmt.Errorf(
				"fork materialization %s selected contract binding conflicts with the replay",
				forkRunID,
			)
		}
		binding = &persistedBinding
	}

	return runfork.RunForkMaterialization{
		SourceRunID:              plan.SourceRunID,
		ForkRunID:                forkRunID,
		ForkRunStatus:            runfork.RunForkMaterializedStatus,
		ForkPoint:                plan.ForkPoint,
		MaterializedEntityCount:  len(plan.Entities),
		ExecutionReady:           true,
		ReplayResumeAdmission:    plan.ReplayResumeAdmission,
		SelectedContractBinding:  binding,
		DeliveryResumeBlocked:    true,
		SourceRunStatusUnchanged: true,
	}, true, nil
}

type runForkBundleInsertIdentity struct {
	SourceArtifactFact runtimecorrelation.SourceArtifactFact
}

func resolveRunForkBundleInsertIdentity(ctx context.Context, source runForkSourceOwnerFunc, sourceRunID string, requestedFact runtimecorrelation.SourceArtifactFact) (runForkBundleInsertIdentity, error) {
	if err := ctx.Err(); err != nil {
		return runForkBundleInsertIdentity{}, err
	}
	if requestedFact.BundleHash() != "" {
		if err := requestedFact.Validate(); err != nil {
			return runForkBundleInsertIdentity{}, err
		}
		return runForkBundleInsertIdentity{SourceArtifactFact: requestedFact}, nil
	}

	if source == nil {
		return runForkBundleInsertIdentity{}, fmt.Errorf("run source owner is required")
	}
	fact, err := source.LoadRunSource(ctx, sourceRunID)
	if err != nil {
		return runForkBundleInsertIdentity{}, fmt.Errorf("load source run bundle identity: %w", err)
	}
	return runForkBundleInsertIdentity{SourceArtifactFact: fact}, nil
}

func loadRunForkEntityMetadata(plan runfork.RunForkPlan) (map[string]runForkEntityMetadata, error) {
	out := make(map[string]runForkEntityMetadata, len(plan.Entities))
	for _, entity := range plan.Entities {
		entityID := strings.TrimSpace(entity.EntityID)
		if entityID == "" {
			return nil, fmt.Errorf("fork entity_id is required")
		}
		if entity.MaterializationMetadata == nil {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization cannot prove source-at-T flow_instance/entity_type metadata for entity %s", entityID))
		}
		metadataOwner := strings.TrimSpace(entity.MaterializationMetadata.Owner)
		if metadataOwner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization metadata for entity %s must be owned by %s", entityID, runfork.RunForkMaterializedEntitySnapshotMetadataOwner))
		}
		meta := runForkEntityMetadata{
			FlowInstance: strings.TrimSpace(entity.MaterializationMetadata.FlowInstance),
			EntityType:   strings.TrimSpace(entity.MaterializationMetadata.EntityType),
			Slug:         strings.TrimSpace(entity.MaterializationMetadata.Slug),
			Name:         strings.TrimSpace(entity.MaterializationMetadata.Name),
		}
		if meta.FlowInstance == "" || meta.EntityType == "" {
			return nil, fmt.Errorf("source entity_state metadata for entity %s must include flow_instance and entity_type", entityID)
		}
		out[entityID] = meta
	}
	return out, nil
}

type runForkEntityIdentity struct {
	EntityID     string
	FlowInstance string
}

type runForkEntityProjection struct {
	Source runForkEntityIdentity
	Fork   runForkEntityIdentity
}

type RunForkEntityProjection = runForkEntityProjection

func projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance string) (runForkEntityProjection, error) {
	source := runForkEntityIdentity{
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
	}
	fork, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, source.EntityID, source.FlowInstance)
	if err != nil {
		return runForkEntityProjection{}, err
	}
	return runForkEntityProjection{Source: source, Fork: fork}, nil
}

func ProjectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance string) (RunForkEntityProjection, error) {
	return projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance)
}

func projectRunForkEntityIdentity(sourceRunID, forkRunID, entityID, flowInstance string) (runForkEntityIdentity, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkRunID = strings.TrimSpace(forkRunID)
	entityID = strings.TrimSpace(entityID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if sourceRunID == "" || forkRunID == "" || entityID == "" || flowInstance == "" {
		return runForkEntityIdentity{}, fmt.Errorf("fork entity identity requires source run, fork run, entity, and flow instance")
	}
	if entityID != sourceRunID {
		if flowInstance == sourceRunID {
			return runForkEntityIdentity{}, fmt.Errorf("fork source entity %s uses root flow_instance %s without canonical root entity identity", entityID, sourceRunID)
		}
		return runForkEntityIdentity{EntityID: entityID, FlowInstance: flowInstance}, nil
	}
	if flowInstance != sourceRunID {
		return runForkEntityIdentity{}, fmt.Errorf("fork root entity %s has flow_instance %q; want source run identity", entityID, flowInstance)
	}
	return runForkEntityIdentity{EntityID: forkRunID, FlowInstance: forkRunID}, nil
}

func materializeRunForkEntityState(ctx context.Context, decisions runForkDecisionMaterializer, materializeProposed runForkProposedEffectMaterializer, insertDiff runForkEntityStateDiffWriter, tx *sql.Tx, story runtimeauthoractivity.Mutation, runLifecycle privatemutationlog.ActiveRunSourceOwner, effects *privaterunforkrevision.Effects, forkRunID string, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, meta runForkEntityMetadata, now time.Time) error {
	projection, err := projectRunForkEntityOwnership(plan.SourceRunID, forkRunID, entity.EntityID, meta.FlowInstance)
	if err != nil {
		return err
	}
	entityID := projection.Fork.EntityID
	meta.FlowInstance = projection.Fork.FlowInstance
	currentState := strings.TrimSpace(entity.CurrentState)
	if currentState == "" {
		return fmt.Errorf("reconstructed current_state is required for entity %s", entityID)
	}
	if entity.EnteredStateAt == nil || entity.EnteredStateAt.IsZero() {
		return fmt.Errorf("reconstructed entered_state_at is required for entity %s", entityID)
	}
	fieldsJSON, err := jsonMapArg(entity.Fields)
	if err != nil {
		return fmt.Errorf("encode fork fields for entity %s: %w", entityID, err)
	}
	bookkeepingJSON, err := jsonMapArg(entity.Bookkeeping)
	if err != nil {
		return fmt.Errorf("encode fork bookkeeping for entity %s: %w", entityID, err)
	}
	gatesJSON, err := jsonMapArg(entity.Gates)
	if err != nil {
		return fmt.Errorf("encode fork gates for entity %s: %w", entityID, err)
	}
	forkAccumulator, err := forkAttemptGenerationState(entity.Accumulator, forkRunID, entityID)
	if err != nil {
		return fmt.Errorf("fork loop state for entity %s: %w", entityID, err)
	}
	forkAccumulator, gateBindings, err := forkGateActivationState(forkAccumulator, forkRunID, meta.FlowInstance, entityID)
	if err != nil {
		return fmt.Errorf("fork gate state for entity %s: %w", entityID, err)
	}
	accJSON, err := jsonMapArg(forkAccumulator)
	if err != nil {
		return fmt.Errorf("encode fork accumulator for entity %s: %w", entityID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
			$7, $8, $9, $10, $11, 1,
			$12, $13, $13
		)
	`, forkRunID, entityID, meta.FlowInstance, meta.EntityType, meta.Slug, meta.Name,
		currentState, gatesJSON, fieldsJSON, bookkeepingJSON, accJSON, entity.EnteredStateAt, now); err != nil {
		return fmt.Errorf("insert fork entity_state %s: %w", entityID, err)
	}
	if err := effects.Add(forkRunID, privaterunforkrevision.FamilyEntityMetadata); err != nil {
		return err
	}
	if err := materializeRunForkDecisionCards(ctx, decisions, tx, story, forkRunID, projection, gateBindings, now); err != nil {
		return err
	}
	if materializeProposed == nil {
		return fmt.Errorf("fork proposed-effect materialization owner is required")
	}
	if err := materializeProposed(ctx, tx, story, plan.SourceRunID, forkRunID, projection, plan.ForkPoint, now); err != nil {
		return err
	}
	if insertDiff == nil {
		return fmt.Errorf("fork entity mutation owner is required")
	}
	return insertDiff(ctx, tx, runLifecycle, story, effects, entityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
		CurrentState: currentState,
		Fields:       entity.Fields,
		Bookkeeping:  entity.Bookkeeping,
		Gates:        entity.Gates,
		Accumulator:  forkAccumulator,
	}, runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "run_fork_materializer",
		HandlerStep: "materialize_snapshot",
	})
}

func deterministicRunForkMaterializationID(sourceRunID, forkEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:run-fork-materialization:"+strings.TrimSpace(sourceRunID)+":"+strings.TrimSpace(forkEventID))).String()
}

func DeterministicRunForkMaterializationID(sourceRunID, forkEventID string) string {
	return deterministicRunForkMaterializationID(sourceRunID, forkEventID)
}

func jsonMapArg(values map[string]any) (string, error) {
	if values == nil {
		values = map[string]any{}
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func runForkBlockerCodes(blockers []runfork.RunForkUnsupportedBlocker) string {
	if len(blockers) == 0 {
		return "none"
	}
	codes := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if code := strings.TrimSpace(blocker.Code); code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "unnamed"
	}
	return strings.Join(codes, ", ")
}
