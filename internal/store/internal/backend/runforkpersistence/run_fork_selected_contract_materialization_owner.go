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
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

func selectedRunForkMaterializationID(sourceRunID string, point runfork.RunForkPoint, operation *runfork.ForkOperationRequest) (string, error) {
	if err := point.Validate(); err != nil {
		return "", err
	}
	if point.Kind == runfork.RunForkPointEvent {
		return deterministicRunForkMaterializationID(sourceRunID, point.EventID), nil
	}
	if operation == nil {
		return "", fmt.Errorf("deployment revision fork requires a durable invocation")
	}
	id, err := uuid.Parse(operation.OperationID)
	if err != nil || id == uuid.Nil || id.String() != operation.OperationID {
		return "", fmt.Errorf("deployment revision fork requires a canonical operation ID")
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("swarm:run-fork-deployment-materialization:%s:%d:%s",
		sourceRunID, point.Revision, operation.OperationID))).String(), nil
}

// runForkSelectedContractMaterializationPort is deliberately operation-specific.
// It exposes persistence mechanics while the materialization lifecycle executes
// once in materializeRunForkForSelectedContractExecution.
type runForkSelectedContractMaterializationPort struct {
	postgres            bool
	requireCurrent      func() error
	runMutation         func(context.Context, func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error)
	lockSourceStatus    func(context.Context, *sql.Tx, string) (string, error)
	plan                func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	deliveries          *storedelivery.Adapter
	loadSource          func(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error)
	activeForkSource    func(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error)
	admitProfile        func(context.Context, *sql.Tx, string, scenarioexecution.EffectiveSourceIdentity, runtimecorrelation.SourceArtifactFact) (scenarioexecution.Profile, bool, error)
	loadSnapshot        runForkLifecycleSnapshotLoader
	requireProfile      func(context.Context, *sql.Tx, string, scenarioexecution.Profile, bool) error
	durableData         *storedurabledata.Owner
	insertRun           func(context.Context, *mutationprotocol.Attempt, string, string, runfork.RunForkPoint, int, time.Time, runtimecorrelation.SourceArtifactFact) error
	ensureProfile       func(context.Context, *sql.Tx, string, scenarioexecution.Profile, time.Time) error
	materializeEntity   func(context.Context, *sql.Tx, *mutationprotocol.Attempt, activeRunSourceOwnerFunc, string, runfork.RunForkPlan, runfork.RunForkEntityState, runForkEntityMetadata, time.Time) error
	materializeBarriers runForkFanOutBarrierOwner
	now                 func() time.Time
}

func materializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest, port runForkSelectedContractMaterializationPort) (materialization runfork.RunForkMaterialization, err error) {
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
	var refusal *runfork.RunForkMaterialization

	committed, err := port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) error {
		materialization = runfork.RunForkMaterialization{}
		refusal = nil
		if err := proveSelectedPreparationForMutationTx(txctx, tx, req.Preparation, !port.postgres); err != nil {
			return err
		}
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
		planRequest := runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: strings.TrimSpace(req.At)}
		if req.ForkOperation != nil {
			planRequest.ResolvedPoint = req.ForkOperation.ResolvedPoint
		}
		plan, err := port.plan(txctx, tx, planRequest)
		if err != nil {
			return err
		}
		fingerprint, err := runfork.SelectedPreparationPlanFingerprint(plan, req.FrontierAdmission, req.RecipientPlanning, req.Preparation.DeclarationPlanFingerprint)
		if err != nil {
			return err
		}
		if req.Preparation.SourceRunID != plan.SourceRunID || req.Preparation.ForkPoint != plan.ForkPoint ||
			req.Preparation.ForkEventID != plan.ForkPoint.EventID ||
			req.Preparation.Coordinates.BundleHash != req.SourceArtifactFact.BundleHash() || fingerprint != req.Preparation.Coordinates.AdmittedPlanFingerprint {
			return fmt.Errorf("selected preparation differs from transaction's fixed admitted plan")
		}
		sourceFingerprint, err := runfork.SelectedPreparationSourceFingerprint(req.EffectiveSourceIdentity)
		if err != nil || sourceFingerprint != req.Preparation.Coordinates.SourceFingerprint {
			return fmt.Errorf("selected preparation differs from transaction's effective source")
		}
		replayAdmission := runfork.RunForkSelectedContractReplayResumeAdmission(plan)
		forkRunID, err := selectedRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint, req.ForkOperation)
		if err != nil {
			return err
		}
		routeRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(plan, forkRunID, selection, req.FrontierAdmission, req.RouteTopology, req.RecipientPlanning)
		if err != nil {
			return err
		}
		if routeResolved {
			replayAdmission = runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(replayAdmission)
		}
		if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, replayAdmission, nil); len(blockers) > 0 {
			blocked := runfork.RunForkMaterialization{
				SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, ExecutionReady: false,
				ReplayResumeAdmission: replayAdmission, UnsupportedBlockers: blockers, DeliveryResumeBlocked: true,
			}
			refusal = &blocked
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
		req.ForkOperation, err = bindResolvedForkOperation(req.ForkOperation, plan.ForkPoint)
		if err != nil {
			return err
		}
		if err := requireForkOperationMaterializationRequest(req.ForkOperation, plan.SourceRunID, plan.ForkPoint, identity.SourceArtifactFact.BundleHash(), selection, req.DataPinOverrides); err != nil {
			return err
		}
		if err := requireOriginalFanOutCarriage(txctx, source, plan, req.OriginalLoopCarriage); err != nil {
			return err
		}
		fanOutPlanRefs, err := resolveRunForkFanOutPlanRefs(plan, identity.SourceArtifactFact.BundleHash(), req.FanOutPlanRefs)
		if err != nil {
			return err
		}
		scenarioProfile, sourceProfiled, err := port.admitProfile(txctx, tx, plan.SourceRunID, req.EffectiveSourceIdentity, identity.SourceArtifactFact)
		if err != nil {
			return err
		}
		sourceModes, err := selectedContractWorkflowSourceModes(txctx, tx, port.postgres, plan.SourceRunID, req.FrontierAdmission)
		if err != nil {
			return err
		}
		if err := req.Readiness.ValidateAgainst(runforkreadiness.Binding{
			Plan: plan, ContractSelection: selection, SourceArtifactFact: identity.SourceArtifactFact,
			EffectiveSourceIdentity: req.EffectiveSourceIdentity,
			FrontierAdmission:       req.FrontierAdmission, RecipientPlanning: req.RecipientPlanning, SourceModes: sourceModes,
		}); err != nil {
			return err
		}
		if err := req.Readiness.ValidatePreparation(req.Preparation); err != nil {
			return err
		}
		workflowStates, err := selectedContractAdmittedWorkflowStates(plan, forkRunID, req.Readiness)
		if err != nil {
			return err
		}
		existing, found, err := loadExactRunForkMaterialization(txctx, port.loadSnapshot, tx, forkRunID, plan, identity, &selection)
		if err != nil {
			return err
		}
		if found {
			if req.ForkOperation != nil {
				binding, err := loadRunForkSelectedContractBinding(txctx, tx, forkRunID)
				if err != nil {
					return err
				}
				if _, _, err := bindForkOperationTx(txctx, tx, *req.ForkOperation, forkRunID, binding.BindingID, port.postgres); err != nil {
					return err
				}
			}
			if err := port.requireProfile(txctx, tx, forkRunID, scenarioProfile, sourceProfiled); err != nil {
				return err
			}
			existing.AgentTopologies = nil
			for _, state := range workflowStates {
				topologies, err := requireSelectedContractWorkflowState(txctx, tx, port.postgres, identity.SourceArtifactFact, state)
				if err != nil {
					return err
				}
				existing.AgentTopologies = append(existing.AgentTopologies, topologies...)
			}
			pins, err := storedurabledata.MaterializeForkPinsTx(port.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, true, time.Time{})
			if err != nil {
				return err
			}
			if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
				return err
			}
			if err := requireExactMaterializedRunForkFanOut(txctx, tx, port.postgres, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), port.durableData, pins); err != nil {
				return err
			}
			existing.DataPins = pins
			existing.MaterializedFanOutCount = len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + len(pins)
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
		now := port.now().UTC()
		txctx = runtimecorrelation.WithSourceArtifactFact(txctx, identity.SourceArtifactFact)
		forkScope, err := runtimeauthoractivity.BundleScopeForTarget(txctx, identity.SourceArtifactFact.BundleHash())
		if err != nil {
			return fmt.Errorf("resolve selected-contract fork author activity scope: %w", err)
		}
		txctx = runtimeauthoractivity.WithScope(txctx, forkScope)
		if err := port.insertRun(txctx, attempt, forkRunID, plan.SourceRunID, plan.ForkPoint, len(plan.Entities), now, identity.SourceArtifactFact); err != nil {
			return fmt.Errorf("insert selected-contract fork run: %w", err)
		}
		pins, err := storedurabledata.MaterializeForkPinsTx(port.durableData, txctx, tx, plan.SourceRunID, forkRunID, identity.SourceArtifactFact.BundleHash(), req.DataPinOverrides, false, now)
		if err != nil {
			return err
		}
		if err := requireForkResourceSourcePinAgreement(plan, pins); err != nil {
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
			if err := port.materializeEntity(forkCtx, tx, attempt, forkMutationSource, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
				return err
			}
		}
		materializedFanOutCount, err := materializeRunForkFanOutObligations(txctx, tx, port.postgres, attempt, port.materializeBarriers, forkRunID, plan, fanOutPlanRefs, req.OriginalLoopCarriage, identity.SourceArtifactFact.BundleHash(), port.durableData, pins, now)
		if err != nil {
			return err
		}
		for _, state := range workflowStates {
			topologies, err := materializeSelectedContractWorkflowState(txctx, tx, port.postgres, identity.SourceArtifactFact, state, now)
			if err != nil {
				return err
			}
			materialization.AgentTopologies = append(materialization.AgentTopologies, topologies...)
		}
		binding, err := insertRunForkSelectedContractBinding(txctx, tx, runfork.RunForkSelectedContractBindingRequest{
			ForkRunID: forkRunID, SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, ContractSelection: selection,
		}, now)
		if err != nil {
			return err
		}
		if req.ForkOperation != nil {
			if _, replay, err := bindForkOperationTx(txctx, tx, *req.ForkOperation, forkRunID, binding.BindingID, port.postgres); err != nil {
				return err
			} else if replay {
				return fmt.Errorf("new fork materialization encountered an already-bound operation")
			}
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
			AgentTopologies: append([]runfork.RunForkSelectedContractAgentTopology(nil), materialization.AgentTopologies...),
		}
		return nil
	})
	if !committed {
		if refusal != nil {
			return *refusal, err
		}
		return runfork.RunForkMaterialization{}, err
	}
	if err != nil {
		return materialization, &selectedForkMaterializationPostCommitError{value: materialization, cause: err}
	}
	return materialization, err
}

func selectedContractWorkflowSourceModes(ctx context.Context, tx *sql.Tx, postgres bool, sourceRunID string, frontier runfork.RunForkContractFrontierAdmission) (map[string]executionmode.Mode, error) {
	_, ids, _, bindingErr := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	if bindingErr != nil {
		return nil, bindingErr
	}
	modes := make(map[string]executionmode.Mode, len(ids))
	if len(ids) == 0 {
		return modes, nil
	}
	var records []eventrecord.Record
	var err error
	if postgres {
		records, err = eventrecordpostgres.LoadMany(ctx, tx, ids)
	} else {
		records, err = eventrecordsqlite.LoadMany(ctx, tx, ids)
	}
	if err != nil {
		return nil, fmt.Errorf("load selected-contract workflow source associations: %w", err)
	}
	for _, record := range records {
		admitted, err := record.Decode()
		if err != nil {
			return nil, fmt.Errorf("decode selected-contract workflow source association: %w", err)
		}
		event := admitted.Event()
		if event.RunID() != sourceRunID {
			return nil, fmt.Errorf("selected-contract workflow source event %s belongs to another run", event.ID())
		}
		if _, duplicate := modes[event.ID()]; duplicate {
			return nil, fmt.Errorf("selected-contract workflow source event %s has duplicate records", event.ID())
		}
		modes[event.ID()] = event.ExecutionMode()
	}
	if len(modes) != len(ids) {
		return nil, fmt.Errorf("selected-contract workflow source associations require every persisted event")
	}
	return modes, nil
}

func postgresRunForkSelectedContractMaterializationPort(s *RunForkPostgresOwner) runForkSelectedContractMaterializationPort {
	return runForkSelectedContractMaterializationPort{
		postgres:       true,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
			result := mutationprotocol.RunPostgresWithOptions(ctx, s.backend, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					return operation(txctx, tx, attempt)
				})
			})
			return result.Acknowledged(), result.Err()
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
		materializeEntity: func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, source activeRunSourceOwnerFunc, forkRunID string, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, metadata runForkEntityMetadata, now time.Time) error {
			return materializeRunForkEntityState(ctx, s.DecisionPostgresOwner, s.MaterializeRunForkProposedEffectCardsTx, true, tx, attempt, source, forkRunID, plan, entity, metadata, now)
		},
		materializeBarriers: s.PipelinePostgresOwner,
		now:                 func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractMaterializationPort(s *RunForkSQLiteOwner) runForkSelectedContractMaterializationPort {
	return runForkSelectedContractMaterializationPort{
		postgres:       false,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
			if err := s.requireCurrentSchema(); err != nil {
				return false, err
			}
			result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected-contract fork materialization", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					return operation(txctx, tx, attempt)
				})
			})
			return result.Acknowledged(), result.Err()
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
		materializeEntity: func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, source activeRunSourceOwnerFunc, forkRunID string, plan runfork.RunForkPlan, entity runfork.RunForkEntityState, metadata runForkEntityMetadata, now time.Time) error {
			return materializeRunForkEntityState(ctx, s.DecisionSQLiteOwner, s.MaterializeRunForkProposedEffectCardsTx, false, tx, attempt, source, forkRunID, plan, entity, metadata, now)
		},
		materializeBarriers: s.PipelineSQLiteOwner,
		now:                 s.now,
	}
}
