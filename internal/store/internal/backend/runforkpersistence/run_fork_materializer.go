package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

type runForkEntityMetadata struct {
	FlowInstance   string
	EntityType     string
	Slug           string
	Name           string
	PreparedHeader *runtimepipeline.WorkflowEngineStateRecord
}

type ActiveRunSourceOwnerFunc func(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)
type activeRunSourceOwnerFunc = ActiveRunSourceOwnerFunc
type runForkSourceOwnerFunc func(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)

type runForkLifecycleSnapshotLoader func(context.Context, *sql.Tx, string) (storerunlifecycle.Snapshot, error)

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
		AtStart:     req.AtStart,
	})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if plan.ForkPoint.Kind != runfork.RunForkPointEvent {
		return runfork.RunForkMaterialization{}, fmt.Errorf("deployment revision materialization requires selected fork operation authority")
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
	var materialization runfork.RunForkMaterialization
	result := mutationprotocol.RunPostgresWithOptions(ctx, s.backend, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if err := requirePostgresRunActive(ctx, tx, plan.SourceRunID); err != nil {
				return fmt.Errorf("admit fork materialization source: %w", err)
			}

			identity, err := resolveRunForkBundleInsertIdentity(ctx, runForkSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
				return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), plan.SourceRunID, req.SourceArtifactFact)
			if err != nil {
				return fmt.Errorf("resolve fork bundle identity: %w", err)
			}
			target, err := contracts.SourceExecutionIdentity(identity.SourceArtifactFact.BundleHash())
			if err != nil {
				return err
			}
			if err := requireOriginalFanOutCarriage(ctx, runForkSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
				return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), plan, req.OriginalLoopCarriage); err != nil {
				return err
			}
			fanOutPlanRefs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
			if err != nil {
				return err
			}
			scenarioProfile, sourceProfiled, err := admitRunForkScenarioProfile(ctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
			if err != nil {
				return err
			}
			existing, found, err := loadExactRunForkMaterialization(
				ctx, func(ctx context.Context, tx *sql.Tx, runID string) (storerunlifecycle.Snapshot, error) {
					return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
				}, tx, true, forkRunID, plan, identity, selection,
			)
			if err != nil {
				return err
			}
			if found {
				if err := requireExactRunForkScenarioProfile(ctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
					return err
				}
				pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, ctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
				if err != nil {
					return err
				}
				if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
					return err
				}
				if err := requireExactMaterializedRunForkFanOut(ctx, tx, true, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), s.durableData, pins); err != nil {
					return err
				}
				existing.DataPins = pins
				existing.MaterializedFanOutCount = len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + len(pins)
				materialization = existing
				return nil
			}
			metadata, err := loadRunForkEntityMetadata(plan)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, identity.SourceArtifactFact)
			forkScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, identity.SourceArtifactFact.BundleHash())
			if err != nil {
				return fmt.Errorf("resolve fork author activity scope: %w", err)
			}
			ctx = runtimeauthoractivity.WithScope(ctx, forkScope)
			if err := s.InsertRunForkRunTx(ctx, attempt, forkRunID, plan.SourceRunID, plan.ForkPoint, now, identity.SourceArtifactFact); err != nil {
				return fmt.Errorf("insert fork run: %w", err)
			}
			pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, ctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
			if err != nil {
				return err
			}
			if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
				return err
			}
			if sourceProfiled {
				if err := scenarioexecutionpersistence.EnsurePostgres(ctx, tx, forkRunID, scenarioProfile, now); err != nil {
					return fmt.Errorf("inherit fork scenario execution profile: %w", err)
				}
			}

			forkCtx := runtimecorrelation.WithRunID(ctx, forkRunID)
			if err := attempt.BeginInitialRunProjection(forkCtx, forkRunID); err != nil {
				return err
			}
			for _, entity := range plan.Entities {
				if err := materializeRunForkEntityState(forkCtx, s.DecisionPostgresOwner, s.MaterializeRunForkProposedEffectCardsTx, true, tx, attempt, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
					return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
				}), forkRunID, target, plan, entity, metadata[entity.EntityID], now); err != nil {
					return err
				}
			}
			materializedFanOutCount, err := materializeRunForkFanOutObligations(ctx, tx, true, attempt, s.PipelinePostgresOwner, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), s.durableData, pins, now)
			if err != nil {
				return err
			}
			if err := attempt.EndInitialRunProjection(forkCtx, forkRunID); err != nil {
				return err
			}
			var selectedContractBinding *runfork.RunForkSelectedContractBinding
			if selection != nil {
				binding, err := insertRunForkSelectedContractBinding(ctx, tx, runfork.RunForkSelectedContractBindingRequest{
					ForkRunID:         forkRunID,
					SourceRunID:       plan.SourceRunID,
					ForkPoint:         plan.ForkPoint,
					ContractSelection: *selection,
				}, now)
				if err != nil {
					return err
				}
				selectedContractBinding = &binding
			}
			materialization = runfork.RunForkMaterialization{
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
			}
			return nil
		})
		return struct{}{}, err
	})
	if !result.Acknowledged() {
		return runfork.RunForkMaterialization{}, result.Err()
	}
	return materialization, result.Err()
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
		AtStart:     req.AtStart,
	})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if plan.ForkPoint.Kind != runfork.RunForkPointEvent {
		return runfork.RunForkMaterialization{}, fmt.Errorf("deployment revision materialization requires selected fork operation authority")
	}
	if !plan.ExecutionReady {
		return runfork.RunForkMaterialization{
			SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, ExecutionReady: false,
			ReplayResumeAdmission: plan.ReplayResumeAdmission, UnsupportedBlockers: plan.UnsupportedBlockers,
			DeliveryResumeBlocked: true,
		}, fmt.Errorf("fork materialization requires execution-ready plan; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}

	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite run fork materialization", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
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
			target, err := contracts.SourceExecutionIdentity(identity.SourceArtifactFact.BundleHash())
			if err != nil {
				return err
			}
			if err := requireOriginalFanOutCarriage(txctx, runForkSourceOwnerFunc(source), plan, req.OriginalLoopCarriage); err != nil {
				return err
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
				tx, false, forkRunID, plan, identity, selection,
			)
			if err != nil {
				return err
			}
			if found {
				if err := requireExactSQLiteRunForkScenarioProfile(txctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
					return err
				}
				pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
				if err != nil {
					return err
				}
				if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
					return err
				}
				if err := requireExactMaterializedRunForkFanOut(txctx, tx, false, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), s.durableData, pins); err != nil {
					return err
				}
				existing.DataPins = pins
				existing.MaterializedFanOutCount = len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + len(pins)
				materialization = existing
				return nil
			}
			metadata, err := loadRunForkEntityMetadata(plan)
			if err != nil {
				return err
			}
			now := s.now()
			txctx = runtimecorrelation.WithSourceArtifactFact(txctx, identity.SourceArtifactFact)
			forkScope, err := runtimeauthoractivity.BundleScopeForTarget(txctx, identity.SourceArtifactFact.BundleHash())
			if err != nil {
				return fmt.Errorf("resolve fork author activity scope: %w", err)
			}
			txctx = runtimeauthoractivity.WithScope(txctx, forkScope)
			if err := s.InsertRunForkRunTx(txctx, attempt, forkRunID, plan.SourceRunID, plan.ForkPoint, now, identity.SourceArtifactFact); err != nil {
				return fmt.Errorf("insert fork run: %w", err)
			}
			pins, err := storedurabledata.MaterializeForkPinsTx(s.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
			if err != nil {
				return err
			}
			if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
				return err
			}
			if sourceProfiled {
				if err := scenarioexecutionpersistence.EnsureSQLite(txctx, tx, forkRunID, scenarioProfile, now); err != nil {
					return fmt.Errorf("inherit fork scenario execution profile: %w", err)
				}
			}
			forkCtx := runtimecorrelation.WithRunID(txctx, forkRunID)
			if err := attempt.BeginInitialRunProjection(forkCtx, forkRunID); err != nil {
				return err
			}
			for _, entity := range plan.Entities {
				if err := materializeRunForkEntityState(forkCtx, s.DecisionSQLiteOwner, s.MaterializeRunForkProposedEffectCardsTx, false, tx, attempt, source, forkRunID, target, plan, entity, metadata[entity.EntityID], now); err != nil {
					return err
				}
			}
			materializedFanOutCount, err := materializeRunForkFanOutObligations(txctx, tx, false, attempt, s.PipelineSQLiteOwner, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), s.durableData, pins, now)
			if err != nil {
				return err
			}
			if err := attempt.EndInitialRunProjection(forkCtx, forkRunID); err != nil {
				return err
			}
			var selectedContractBinding *runfork.RunForkSelectedContractBinding
			if selection != nil {
				binding, err := insertRunForkSelectedContractBinding(txctx, tx, runfork.RunForkSelectedContractBindingRequest{
					ForkRunID: forkRunID, SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint,
					ContractSelection: *selection,
				}, now)
				if err != nil {
					return err
				}
				selectedContractBinding = &binding
			}
			materialization = runfork.RunForkMaterialization{
				SourceRunID: plan.SourceRunID, ForkRunID: forkRunID, ForkRunStatus: runfork.RunForkMaterializedStatus,
				ForkPoint: plan.ForkPoint, MaterializedEntityCount: len(plan.Entities), MaterializedFanOutCount: materializedFanOutCount, ExecutionReady: true,
				ReplayResumeAdmission: plan.ReplayResumeAdmission, SelectedContractBinding: selectedContractBinding,
				DeliveryResumeBlocked: true, SourceRunStatusUnchanged: true, DataPins: pins,
			}
			return nil
		})
		return struct{}{}, err
	})
	if !result.Acknowledged() {
		return runfork.RunForkMaterialization{}, result.Err()
	}
	return materialization, result.Err()
}

func loadExactRunForkMaterialization(
	ctx context.Context,
	loadSnapshot runForkLifecycleSnapshotLoader,
	tx *sql.Tx,
	postgres bool,
	forkRunID string,
	plan runfork.RunForkPlan,
	identity runForkBundleInsertIdentity,
	selection *runfork.RunForkContractSelection,
) (runfork.RunForkMaterialization, bool, error) {
	query := `
		SELECT CAST(run_id AS TEXT)
		FROM runs
		WHERE run_id = $1
	`
	args := []any{forkRunID}
	if plan.ForkPoint.Kind == runfork.RunForkPointEvent {
		query += ` OR (forked_from_run_id = $2 AND forked_from_point_kind = 'event'
			AND forked_from_revision = $3 AND forked_from_event_id = $4)`
		args = append(args, plan.SourceRunID, plan.ForkPoint.Revision, plan.ForkPoint.EventID)
	}
	query += ` ORDER BY started_at ASC LIMIT 2`
	rows, err := tx.QueryContext(ctx, query, args...)
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
			"fork materialization identity conflict for source run %s at %s revision %d: matches=%v",
			plan.SourceRunID, plan.ForkPoint.Kind, plan.ForkPoint.Revision, matches,
		)
	}

	if loadSnapshot == nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("fork lifecycle snapshot loader is required")
	}
	snapshot, err := loadSnapshot(ctx, tx, forkRunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("load existing fork lifecycle: %w", err)
	}
	wantOrigin, err := storerunlifecycle.ForkMaterializationRunOrigin(plan.SourceRunID, plan.ForkPoint.Kind, plan.ForkPoint.Revision, plan.ForkPoint.EventID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, err
	}
	bundleHash := identity.SourceArtifactFact.BundleHash()
	fieldRows := 0
	for _, entity := range plan.Entities {
		if entity.MaterializationMetadata != nil && entity.MaterializationMetadata.EntityType != "" {
			fieldRows++
		}
	}
	if snapshot.State != storerunlifecycle.StatePaused ||
		!snapshot.Origin.Equal(wantOrigin) ||
		snapshot.BundleHash != bundleHash ||
		snapshot.EventCount != 0 ||
		snapshot.EntityCount != fieldRows {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf(
			"fork materialization %s conflicts with persisted lifecycle state",
			forkRunID,
		)
	}

	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, err
	}
	type historicalOwner struct{ path, entityType string }
	expectedEntities := make(map[string]historicalOwner, 2*len(plan.Entities))
	projectedEntities := make(map[string]struct{}, len(plan.Entities))
	for _, entity := range plan.Entities {
		sourceEntityID := strings.TrimSpace(entity.EntityID)
		identity, err := projectRunForkEntityIdentity(plan.SourceRunID, forkRunID, sourceEntityID, metadata[sourceEntityID].FlowInstance)
		if err != nil {
			return runfork.RunForkMaterialization{}, false, err
		}
		if _, duplicate := projectedEntities[identity.EntityID]; duplicate {
			return runfork.RunForkMaterialization{}, false, fmt.Errorf("fork materialization projects duplicate entity %s", identity.EntityID)
		}
		projectedEntities[identity.EntityID] = struct{}{}
		meta := metadata[sourceEntityID]
		owner := historicalOwner{identity.FlowInstance, meta.EntityType}
		if meta.EntityType != "" {
			expectedEntities["fields/"+identity.EntityID] = owner
		}
		if entity.MaterializationMetadata.Source == runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			expectedEntities["header/"+identity.EntityID] = owner
		}
	}
	// One historical instance can own a header and optional fields. Imports own
	// fields only; exact replay must preserve that distinction, not count fields
	// as a proxy for construction or repair a missing header.
	entityRows, err := tx.QueryContext(ctx, `
		SELECT 'header', CAST(entity_id AS TEXT), instance_path, entity_type
		FROM flow_instances
		WHERE run_id = $1
		UNION ALL
		SELECT 'fields', CAST(entity_id AS TEXT), flow_instance, entity_type FROM entity_state
		WHERE run_id = $1
	`, forkRunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, false, fmt.Errorf("load existing fork entities: %w", err)
	}
	for entityRows.Next() {
		var kind, entityID, flowInstance string
		var entityType sql.NullString
		if err := entityRows.Scan(&kind, &entityID, &flowInstance, &entityType); err != nil {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf("scan existing fork entity: %w", err)
		}
		key := kind + "/" + entityID
		expected, ok := expectedEntities[key]
		if !ok {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf(
				"fork materialization %s has unexpected entity %s",
				forkRunID, key,
			)
		}
		if flowInstance != expected.path || entityType.String != expected.entityType || entityType.Valid != (expected.entityType != "") {
			_ = entityRows.Close()
			return runfork.RunForkMaterialization{}, false, fmt.Errorf(
				"fork materialization %s entity %s has flow_instance/type %q/%q; want %q/%q",
				forkRunID, key, flowInstance, entityType.String, expected.path, expected.entityType,
			)
		}
		delete(expectedEntities, key)
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
	if selection == nil {
		for _, entity := range plan.Entities {
			projected, err := projectRunForkEntityIdentity(plan.SourceRunID, forkRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
			if err != nil {
				return runfork.RunForkMaterialization{}, false, err
			}
			if entity.MaterializationMetadata.Source == runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState {
				meta := metadata[entity.EntityID]
				meta.FlowInstance = projected.FlowInstance
				fields, err := projectRunForkHistoricalFields(plan.SourceRunID, forkRunID, projected.EntityID, snapshot.BundleHash, entity, meta, snapshot.StartedAt)
				if err != nil {
					return runfork.RunForkMaterialization{}, false, err
				}
				owner, err := runtimeflowidentity.NewRunScopedFlowInstance(forkRunID, runtimeflowidentity.RouteForInstancePath(projected.FlowInstance))
				if err != nil {
					return runfork.RunForkMaterialization{}, false, err
				}
				if err := pipelinepersistence.RequireHistoricalImportedWorkflowState(ctx, tx, postgres, owner, fields.Record); err != nil {
					return runfork.RunForkMaterialization{}, false, err
				}
				continue
			}
			header, err := projectRunForkHistoricalHeader(plan.SourceRunID, forkRunID, projected.EntityID, projected.FlowInstance, snapshot.BundleHash, entity)
			if err != nil {
				return runfork.RunForkMaterialization{}, false, err
			}
			if err := pipelinepersistence.RequireSelectedHistoricalWorkflowHeader(ctx, tx, postgres, header); err != nil {
				return runfork.RunForkMaterialization{}, false, err
			}
			receipt, err := projectRunForkConstructionReceipt(plan, forkRunID, entity, header.CreatedAt)
			if err != nil {
				return runfork.RunForkMaterialization{}, false, err
			}
			if err := pipelinepersistence.RequireSelectedHistoricalWorkflowReceipt(ctx, tx, receipt); err != nil {
				return runfork.RunForkMaterialization{}, false, err
			}
		}
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
			persistedBinding.ForkPoint.Kind != plan.ForkPoint.Kind ||
			persistedBinding.ForkPoint.Revision != plan.ForkPoint.Revision ||
			persistedBinding.ForkPoint.EventID != plan.ForkPoint.EventID ||
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
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, "fork entity_id is required")
		}
		if _, duplicate := out[entityID]; duplicate {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("duplicate fork entity owner %q", entityID))
		}
		if entity.MaterializationMetadata == nil {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization cannot prove source-at-T flow_instance/entity_type metadata for entity %s", entityID))
		}
		metadataOwner := strings.TrimSpace(entity.MaterializationMetadata.Owner)
		if metadataOwner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization metadata for entity %s must be owned by %s", entityID, runfork.RunForkMaterializedEntitySnapshotMetadataOwner))
		}
		if entity.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState && entity.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization metadata for entity %s requires fixed-revision entity state metadata", entityID))
		}
		meta := runForkEntityMetadata{
			FlowInstance: strings.TrimSpace(entity.MaterializationMetadata.FlowInstance),
			EntityType:   strings.TrimSpace(entity.MaterializationMetadata.EntityType),
			Slug:         strings.TrimSpace(entity.MaterializationMetadata.Slug),
			Name:         strings.TrimSpace(entity.MaterializationMetadata.Name),
		}
		if meta.FlowInstance == "" || (meta.EntityType == "" && (entity.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance || len(entity.Fields) != 0)) {
			return nil, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("source entity_state metadata for entity %s must include flow_instance and entity_type", entityID))
		}
		out[entityID] = meta
	}
	return out, nil
}

func projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance string) (runfork.EntityProjection, error) {
	return runfork.ProjectEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance)
}

func ProjectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance string) (runfork.EntityProjection, error) {
	return projectRunForkEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance)
}

func projectRunForkEntityIdentity(sourceRunID, forkRunID, entityID, flowInstance string) (runfork.EntityIdentity, error) {
	projection, err := runfork.ProjectEntityOwnership(sourceRunID, forkRunID, entityID, flowInstance)
	return projection.Fork, err
}

func materializeRunForkEntityState(ctx context.Context, decisions runForkDecisionMaterializer, materializeProposed runForkProposedEffectMaterializer, postgres bool, tx *sql.Tx, attempt *mutationprotocol.Attempt, runLifecycle privatemutationlog.ActiveRunSourceOwner, forkRunID string, target contracts.BundleIdentity, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, meta runForkEntityMetadata, now time.Time) error {
	projection, err := projectRunForkEntityOwnership(plan.SourceRunID, forkRunID, entity.EntityID, meta.FlowInstance)
	if err != nil {
		return err
	}
	entityID := projection.Fork.EntityID
	meta.FlowInstance = projection.Fork.FlowInstance
	fields, err := projectRunForkHistoricalFields(plan.SourceRunID, forkRunID, entityID, target.BundleHash, entity, meta, now)
	if err != nil {
		return err
	}
	record := fields.Record
	if meta.EntityType != "" {
		if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
			$7, $8, $9, $10, $11, 1,
			$12, $13, $14
		)
	`, forkRunID, entityID, meta.FlowInstance, meta.EntityType, meta.Slug, meta.Name,
			record.CurrentState, string(record.Gates), string(record.Fields), string(record.Bookkeeping), string(record.Accumulator), record.EnteredStageAt, record.CreatedAt, record.UpdatedAt); err != nil {
			return fmt.Errorf("insert fork entity_state %s: %w", entityID, err)
		}
	}
	if entity.MaterializationMetadata.Source == runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
		var header runtimepipeline.WorkflowEngineStateRecord
		if meta.PreparedHeader != nil {
			header = *meta.PreparedHeader
		} else {
			header, err = projectRunForkHistoricalHeader(plan.SourceRunID, forkRunID, entityID, meta.FlowInstance, target.BundleHash, entity)
			if err != nil {
				return err
			}
		}
		if header.Identity.RunID != forkRunID || header.Identity.Route.InstancePath != meta.FlowInstance || header.EntityID != entityID || header.EntityType != meta.EntityType {
			return fmt.Errorf("historical header projection crossed entity ownership")
		}
		receipt, err := projectRunForkConstructionReceipt(plan, forkRunID, entity, header.CreatedAt)
		if err != nil {
			return err
		}
		if err := pipelinepersistence.CommitSelectedHistoricalWorkflowHeader(ctx, tx, postgres, header, receipt); err != nil {
			return err
		}
	}
	if err := attempt.AddFact(forkRunID, privaterunforkrevision.FamilyEntityMetadata, entityID); err != nil {
		return err
	}
	if err := materializeRunForkDecisionCards(ctx, decisions, attempt, forkRunID, target, projection, fields.GateBindings, now); err != nil {
		return err
	}
	if materializeProposed == nil {
		return fmt.Errorf("fork proposed-effect materialization owner is required")
	}
	if err := materializeProposed(ctx, attempt, plan.SourceRunID, forkRunID, target, projection, plan.ForkPoint, fields.Correspondence, now); err != nil {
		return err
	}
	writer := runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "run_fork_materializer",
		HandlerStep: "materialize_snapshot",
	}
	if postgres {
		return privatemutationlog.InsertEntityStateDiff(ctx, attempt, runLifecycle, entityID, runtimemutationlog.EntityStateProjection{}, fields.Mutation, writer)
	}
	return privatemutationlog.InsertSQLiteEntityStateDiff(ctx, attempt, runLifecycle, entityID, runtimemutationlog.EntityStateProjection{}, fields.Mutation, writer, now)
}

type runForkHistoricalFields struct {
	Record         runtimepipeline.WorkflowEntityStatePersistenceRecord
	Mutation       runtimemutationlog.EntityStateProjection
	Correspondence *loopruntime.ForkCorrespondence
	GateBindings   []runForkGateActivationBinding
}

// Writer and exact reuse share the same historical projection. Its loop/gate
// rebinding changes branch identity, never grants construction or execution.
func projectRunForkHistoricalFields(sourceRunID, forkRunID, entityID, targetBundleHash string, entity runfork.RunForkEntityState, meta runForkEntityMetadata, now time.Time) (runForkHistoricalFields, error) {
	var projected runForkHistoricalFields
	if strings.TrimSpace(entity.CurrentState) == "" || entity.EnteredStateAt == nil || entity.EnteredStateAt.IsZero() {
		return projected, fmt.Errorf("reconstructed state and entry time are required for entity %s", entityID)
	}
	projection, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
	if err != nil {
		return projected, err
	}
	if projection.Fork.EntityID != entityID || projection.Fork.FlowInstance != meta.FlowInstance {
		return projected, fmt.Errorf("historical fields disagree with exact fork ownership")
	}
	bookkeeping, accumulator, correspondence, err := projectRunForkEntityExecutionState(entity, sourceRunID, forkRunID, projection)
	if err != nil {
		return projected, fmt.Errorf("fork loop state for entity %s: %w", entityID, err)
	}
	accumulator, bindings, err := forkGateActivationState(accumulator, forkRunID, meta.FlowInstance, entityID, targetBundleHash)
	if err != nil {
		return projected, fmt.Errorf("fork gate state for entity %s: %w", entityID, err)
	}
	createdAt := storerunlifecycle.CanonicalTimestamp(now)
	updatedAt := createdAt
	if meta.PreparedHeader != nil {
		createdAt, updatedAt = meta.PreparedHeader.CreatedAt, meta.PreparedHeader.UpdatedAt
	} else if entity.MaterializationMetadata.Source == runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
		createdAt, updatedAt = entity.MaterializationMetadata.CreatedAt, entity.MaterializationMetadata.UpdatedAt
	}
	projected.Record = runtimepipeline.WorkflowEntityStatePersistenceRecord{
		EntityID: entityID, FlowInstance: meta.FlowInstance, EntityType: meta.EntityType, Slug: meta.Slug, Name: meta.Name,
		CurrentState: strings.TrimSpace(entity.CurrentState), Revision: 1, EnteredStageAt: *entity.EnteredStateAt,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	for _, field := range []struct {
		value map[string]any
		raw   *json.RawMessage
	}{{entity.Fields, &projected.Record.Fields}, {entity.Gates, &projected.Record.Gates}, {bookkeeping, &projected.Record.Bookkeeping}, {accumulator, &projected.Record.Accumulator}} {
		encoded, err := jsonMapArg(field.value)
		if err != nil {
			return projected, fmt.Errorf("encode historical fork state for entity %s: %w", entityID, err)
		}
		*field.raw = json.RawMessage(encoded)
	}
	projected.Mutation = runtimemutationlog.EntityStateProjection{
		CurrentState: projected.Record.CurrentState, Fields: entity.Fields, Bookkeeping: bookkeeping, Gates: entity.Gates, Accumulator: accumulator,
	}
	projected.Correspondence, projected.GateBindings = correspondence, bindings
	return projected, nil
}

func projectRunForkHistoricalHeader(sourceRunID, forkRunID, entityID, path, targetBundleHash string, entity runfork.RunForkEntityState) (runtimepipeline.WorkflowEngineStateRecord, error) {
	metadata := entity.MaterializationMetadata
	receipt, err := runtimepipeline.DecodeStoredFlowConstructionReceipt(metadata.InitialMaterialization,
		sourceRunID, entity.EntityID, metadata.FlowInstance, metadata.FlowTemplate)
	if err != nil {
		return runtimepipeline.WorkflowEngineStateRecord{}, err
	}
	recorded, err := runtimepipeline.DecodeWorkflowInstanceRecordedHeader(receipt.Identity.Route(), metadata.FlowConfig)
	if err != nil {
		return runtimepipeline.WorkflowEngineStateRecord{}, err
	}
	if recorded.ParentRoute() != receipt.Identity.ParentRoute {
		return runtimepipeline.WorkflowEngineStateRecord{}, fmt.Errorf("historical header contradicts captured construction ancestry")
	}
	identity, err := runfork.ProjectConstructionIdentity(sourceRunID, forkRunID, receipt.Identity)
	if err != nil {
		return runtimepipeline.WorkflowEngineStateRecord{}, err
	}
	if identity.EntityID != entityID || identity.InstancePath != path {
		return runtimepipeline.WorkflowEngineStateRecord{}, fmt.Errorf("historical header contradicts projected construction identity")
	}
	config, err := recorded.Project(identity.Route(), identity.ParentRoute)
	if err != nil {
		return runtimepipeline.WorkflowEngineStateRecord{}, err
	}
	header, err := selectedContractHistoricalHeader(selectedContractWorkflowState{
		SourceRunID: sourceRunID, RunID: forkRunID, EntityID: entityID, EntityType: metadata.EntityType, WorkflowName: metadata.FlowTemplate,
		Mode: metadata.Mode, WorkflowVersion: recorded.WorkflowVersion(), Route: path, History: entity,
	}, config, targetBundleHash, metadata.CreatedAt)
	if err != nil {
		return runtimepipeline.WorkflowEngineStateRecord{}, err
	}
	header.Status, header.CreatedAt, header.UpdatedAt, header.TerminatedAt = metadata.Status, metadata.CreatedAt, metadata.UpdatedAt, metadata.TerminatedAt
	return header, header.Validate()
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
