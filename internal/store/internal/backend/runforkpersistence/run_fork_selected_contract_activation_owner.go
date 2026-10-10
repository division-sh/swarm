package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// runForkSelectedContractActivationPort contains only dialect-specific work for
// the selected-contract activation operation. Lifecycle meaning is owned by
// activateRunForkForSelectedContractExecution below.
type runForkSelectedContractActivationPort struct {
	postgres         bool
	requireCurrent   func() error
	runMutation      func(context.Context, func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error)
	loadLineage      func(context.Context, *sql.Tx, string) (runForkActivationLineage, error)
	lockFrontier     func(context.Context, *sql.Tx, *runForkActivationLineage) error
	plan             func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	loadSnapshot     runForkLifecycleSnapshotLoader
	workflowTimers   runForkWorkflowTimerMaterializationOwner
	arrivalSchedules runForkArrivalJoinMaterializationOwner
	deliveries       *storedelivery.Adapter
	attachment       func(context.Context, *sql.Tx, runForkSelectedContractActivationEvidence) error
	transition       func(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.ActiveTransitionRequest) error
	diverge          func(context.Context, *sql.Tx, runfork.RunForkSelectedContractBranchDivergence) error
	freeze           func(context.Context, *sql.Tx, *mutationprotocol.Attempt, runForkActivationLineage, time.Time, bool) error
	now              func() time.Time
}

type runForkSelectedContractActivationEvidence struct {
	lineage runForkActivationLineage
	binding runfork.RunForkSelectedContractBinding
	plan    runfork.RunForkPlan
	request runfork.RunForkSelectedContractExecutionActivateRequest
}

func activateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest, port runForkSelectedContractActivationPort) (result runfork.RunForkActivation, err error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if port.requireCurrent == nil || port.runMutation == nil || port.loadLineage == nil || port.lockFrontier == nil ||
		port.plan == nil || port.deliveries == nil || port.attachment == nil || port.transition == nil || port.diverge == nil ||
		port.freeze == nil || port.now == nil || port.loadSnapshot == nil || port.workflowTimers == nil || port.arrivalSchedules == nil {
		return runfork.RunForkActivation{}, fmt.Errorf("selected-contract fork activation operations are incomplete")
	}
	if err := port.requireCurrent(); err != nil {
		return runfork.RunForkActivation{}, err
	}
	var divergence *runfork.RunForkSelectedContractBranchDivergence
	committed, err := port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt) error {
		divergence = nil
		lineage, err := port.loadLineage(txctx, tx, forkRunID)
		if err != nil {
			return err
		}
		if err := port.lockFrontier(txctx, tx, &lineage); err != nil {
			return err
		}
		result = runfork.RunForkActivation{
			SourceRunID:             lineage.SourceRunID,
			ForkRunID:               lineage.ForkRunID,
			ForkRunStatus:           lineage.ForkStatus,
			SourceRunStatus:         lineage.SourceRunStatus,
			ForkPoint:               lineage.ForkPoint,
			ReplayResumeBlocked:     true,
			MaterializedEntityCount: len(lineage.EntityIDs),
		}
		if lineage.ForkStatus != runfork.RunForkMaterializedStatus {
			result.RepeatedActivationFailed = lineage.ForkStatus == runfork.RunForkActivatedStatus
			return fmt.Errorf("selected-contract fork activation requires materialized fork status %q; got %q", runfork.RunForkMaterializedStatus, lineage.ForkStatus)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(lineage.SourceRunStatus) {
			return fmt.Errorf("selected-contract fork activation requires supported branch source status; got %q", lineage.SourceRunStatus)
		}
		if err := requireSelectedForkMaterializedWorkTx(txctx, tx, lineage.ForkRunID, len(lineage.EntityIDs)); err != nil {
			return err
		}
		binding, err := loadRunForkSelectedContractBinding(txctx, tx, lineage.ForkRunID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("selected-contract fork activation requires selected contract binding")
			}
			return fmt.Errorf("load selected contract binding: %w", err)
		}
		result.SelectedContractBinding = &binding
		if binding.SourceRunID != lineage.SourceRunID || binding.ForkPoint != lineage.ForkPoint {
			return fmt.Errorf("selected-contract fork activation binding differs from child origin")
		}

		planRequest := runfork.RunForkPlanRequest{SourceRunID: lineage.SourceRunID}
		if lineage.ForkPoint.Kind == runfork.RunForkPointEvent {
			planRequest.At = lineage.ForkPoint.EventID
		} else {
			planRequest.ResolvedPoint = &lineage.ForkPoint
			planRequest.AtStart = lineage.ForkPoint.Kind == runfork.RunForkPointRunStart
		}
		plan, err := port.plan(txctx, tx, planRequest)
		if err != nil {
			return err
		}
		if plan.ForkPoint.Kind != lineage.ForkPoint.Kind || plan.ForkPoint.Revision != lineage.ForkPoint.Revision ||
			plan.ForkPoint.EventID != lineage.ForkPoint.EventID {
			return fmt.Errorf("selected-contract fork activation plan differs from fixed child origin")
		}
		lineage.ForkPoint = plan.ForkPoint
		result.ForkPoint = plan.ForkPoint
		result.ReplayResumeAdmission = runfork.RunForkSelectedContractReplayResumeAdmission(plan)
		expectedRouteRecovery, routeResolved, err := prepareRunForkSelectedContractRouteResolution(
			plan, lineage.ForkRunID, binding.ContractSelection,
			req.FrontierAdmission, req.RouteTopology, req.RecipientPlanning,
		)
		if err != nil {
			return err
		}
		if routeResolved {
			if err := validateRunForkSelectedContractRouteRecoveryAtActivation(txctx, tx, expectedRouteRecovery); err != nil {
				return err
			}
			result.ReplayResumeAdmission = runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(result.ReplayResumeAdmission)
		}
		snapshot, err := port.loadSnapshot(txctx, tx, lineage.ForkRunID)
		if err != nil {
			return err
		}
		result.ReplayResumeAdmission, err = requireMaterializedRunForkTimerHistory(runtimecorrelation.WithRunID(txctx, lineage.ForkRunID), attempt,
			plan, lineage.ForkRunID, req.InheritedWorkflowTimers, port.workflowTimers, port.arrivalSchedules, runtimerunlifecycle.CanonicalTimestamp(snapshot.StartedAt), result.ReplayResumeAdmission)
		if err != nil {
			return err
		}
		if blockers := runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, result.ReplayResumeAdmission, req.AllowedSourceEventIDs); len(blockers) > 0 {
			result.UnsupportedBlockers = blockers
			return fmt.Errorf("selected-contract fork activation blocked: %s", runForkBlockerCodes(blockers))
		}

		sourceAdvancedFacts, err := collectRunForkSelectedContractSourceAdvancedFacts(txctx, tx, lineage)
		if err != nil {
			return err
		}
		conversationAdvancedFacts := runForkSelectedContractConversationAdvancedFacts(sourceAdvancedFacts)
		result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(result.ReplayResumeAdmission, conversationAdvancedFacts)
		if err := ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(txctx, tx, port.deliveries, lineage); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		sourceAdvancedFacts = append(sourceAdvancedFacts, runfork.ActiveSourceDeliveryConversationCouplingFacts(result.ReplayResumeAdmission)...)
		sourceAdvancedFacts = uniqueNonEmptyStrings(sourceAdvancedFacts)
		result.SourceAdvancedAfterFork = len(sourceAdvancedFacts) > 0
		if err := port.attachment(txctx, tx, runForkSelectedContractActivationEvidence{lineage, binding, plan, req}); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}

		now := port.now().UTC()
		if len(sourceAdvancedFacts) > 0 {
			if err := port.transition(txctx, attempt, runtimerunlifecycle.ActiveTransitionRequest{RunID: lineage.ForkRunID, State: runtimerunlifecycle.StateRunning}); err != nil {
				return fmt.Errorf("activate selected-contract branch fork run lifecycle: %w", err)
			}
			value := runfork.RunForkSelectedContractBranchDivergence{
				Owner:                          runfork.RunForkSelectedContractBranchDivergenceOwner,
				ForkRunID:                      lineage.ForkRunID,
				SourceRunID:                    lineage.SourceRunID,
				ForkPoint:                      lineage.ForkPoint,
				ForkEventID:                    lineage.ForkPoint.EventID,
				Policy:                         runfork.RunForkSelectedContractSourceAdvancedBranchPolicy,
				SourceRunStatusAtActivation:    lineage.SourceRunStatus,
				SourceRunStatusAfterActivation: lineage.SourceRunStatus,
				SourceFrozen:                   false,
				SourceAdvancedFacts:            sourceAdvancedFacts,
				CreatedAt:                      now,
			}
			value, err = normalizeSelectedForkBranchDivergence(value)
			if err != nil {
				return err
			}
			if err := port.diverge(txctx, tx, value); err != nil {
				return err
			}
			if err := recordRunForkActivationAuthorActivity(txctx, attempt, lineage, now); err != nil {
				return err
			}
			divergence = &value
			if err := completeSelectedForkOperationAtActivation(txctx, tx, req, lineage, value.SourceRunStatusAfterActivation, false, port.postgres); err != nil {
				return err
			}
			return nil
		}
		if err := port.freeze(txctx, tx, attempt, lineage, now, req.AllowSourceFreeze); err != nil {
			return err
		}
		return completeSelectedForkOperationAtActivation(txctx, tx, req, lineage, runfork.RunForkSourceFrozenStatus, true, port.postgres)
	})
	if !committed {
		return result, err
	}
	result.ForkRunStatus = runfork.RunForkActivatedStatus
	result.Activated = true
	if divergence != nil {
		result.SourceRunStatus = divergence.SourceRunStatusAfterActivation
		result.SourceFrozen = false
		result.BranchDivergence = divergence
	} else {
		result.SourceRunStatus = runfork.RunForkSourceFrozenStatus
		result.SourceFrozen = true
	}
	return result, err
}

type selectedContractCurrentAuthorityOwner interface {
	RequireCurrentExternalEffectAuthorityTx(context.Context, *sql.Tx, runtimeeffects.Authority) error
}

// A running execution is an attached, fenced executor, not proof that its owed
// business work has already completed. The retained reader owns the immutable
// declaration/preparation decoding; the preparation owner proves currentness.
func requireSelectedContractPreparedAttachmentTx(ctx context.Context, tx *sql.Tx, snapshot runtimerunlifecycle.Snapshot, evidence runForkSelectedContractActivationEvidence, sqlite bool, owner selectedContractCurrentAuthorityOwner) error {
	authority, err := selectedContractActivationAuthority(ctx, evidence.lineage.ForkRunID)
	if err != nil {
		return err
	}
	record, err := loadSelectedRecoveryRecordTx(ctx, tx, snapshot, runfork.SelectedForkRecoveryEntry{
		Binding: evidence.binding, BundleHash: evidence.lineage.ForkBundleHash,
	}, sqlite, true)
	if err != nil {
		return err
	}
	if err := validateSelectedContractPreparedAttachment(record, authority, evidence); err != nil {
		return err
	}
	if err := validateSelectedContractStagedConstruction(evidence); err != nil {
		return err
	}
	if err := owner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
		return err
	}
	return proveSelectedPreparationForMutationTx(ctx, tx, record.preparation.SelectedForkPreparation, sqlite)
}

func selectedContractActivationAuthority(ctx context.Context, forkRunID string) (runtimeeffects.Authority, error) {
	if err := ctx.Err(); err != nil {
		return runtimeeffects.Authority{}, err
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthoritySelectedContractFork || authority.SelectedFork.ForkRunID != forkRunID {
		return runtimeeffects.Authority{}, fmt.Errorf("selected-contract activation requires exact attached execution authority")
	}
	return authority, nil
}

func validateSelectedContractPreparedAttachment(record selectedRecoveryRecord, authority runtimeeffects.Authority, evidence runForkSelectedContractActivationEvidence) error {
	if !record.hasExecution || record.state != "running" || record.failure != nil || record.ExecutionID != authority.SelectedFork.ExecutionID {
		return fmt.Errorf("selected-contract activation requires the current running prepared executor")
	}
	preparation := record.preparation
	if preparation.SourceRunID != evidence.lineage.SourceRunID || preparation.ForkRunID != evidence.lineage.ForkRunID ||
		!sameSelectedForkPointIdentity(preparation.ForkPoint, evidence.plan.ForkPoint) || preparation.ForkEventID != evidence.plan.ForkPoint.EventID ||
		preparation.Coordinates.BundleHash != evidence.lineage.ForkBundleHash {
		return fmt.Errorf("selected-contract attachment differs from fixed child lineage")
	}
	_, ids, _, err := runfork.RunForkContractFrontierEvidenceBinding(evidence.request.FrontierAdmission)
	if err != nil {
		return err
	}
	if !equalTrimmedStrings(ids, evidence.request.AllowedSourceEventIDs) {
		return fmt.Errorf("selected-contract activation inputs differ from prepared frontier")
	}
	return nil
}

func validateSelectedContractStagedConstruction(evidence runForkSelectedContractActivationEvidence) error {
	metadata, err := loadRunForkEntityMetadata(evidence.plan)
	if err != nil {
		return err
	}
	expected := make([]string, 0, len(metadata))
	for entityID, meta := range metadata {
		projection, err := projectRunForkEntityOwnership(evidence.lineage.SourceRunID, evidence.lineage.ForkRunID, entityID, meta.FlowInstance)
		if err != nil {
			return err
		}
		expected = append(expected, projection.Fork.EntityID)
	}
	// Construction is an exact owner inventory, not map or header iteration order.
	actual := slices.Clone(evidence.lineage.EntityIDs)
	slices.Sort(expected)
	slices.Sort(actual)
	if !slices.Equal(expected, actual) {
		return fmt.Errorf("selected-contract staged construction differs from exact fixed-cut child inventory")
	}
	return nil
}

func completeSelectedForkOperationAtActivation(ctx context.Context, tx *sql.Tx, req runfork.RunForkSelectedContractExecutionActivateRequest, lineage runForkActivationLineage, sourceStatus string, frozen, postgres bool) error {
	if req.ForkOperation == nil {
		return nil
	}
	operation, err := resolvedForkOperationForActivationTx(ctx, tx, *req.ForkOperation, lineage, postgres)
	if err != nil {
		return err
	}
	executed, err := selectedForkDurableExecutedEventCountTx(ctx, tx, lineage.ForkRunID)
	if err != nil {
		return err
	}
	pins := append([]durabledata.Pin(nil), req.DataPins...)
	for i := range pins {
		pins[i].RunState = runfork.RunForkActivatedStatus
	}
	return completeForkOperationTx(ctx, tx, operation, runfork.ForkOperationResult{
		SourceRunID: lineage.SourceRunID, SourceRunStatus: sourceStatus, SourceFrozen: frozen,
		ForkRunID: lineage.ForkRunID, ForkEventID: lineage.ForkEventID, ForkPoint: *operation.ResolvedPoint,
		ForkRunStatus: runfork.RunForkActivatedStatus, BundleHash: req.ForkOperation.TargetBundleHash,
		ExecutedEventCount: executed, DataPins: pins,
	}, postgres)
}

func selectedDeploymentFeedPresentTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var present bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment')`, runID).Scan(&present)
	return present, err
}

func requireSelectedForkMaterializedWorkTx(ctx context.Context, tx *sql.Tx, runID string, constructedCount int) error {
	if constructedCount > 0 {
		return nil
	}
	present, err := selectedDeploymentFeedPresentTx(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("check selected fork deployment work: %w", err)
	}
	if !present {
		return fmt.Errorf("selected-contract fork activation requires constructed instance or deployment work")
	}
	return nil
}

func selectedForkDurableExecutedEventCountTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var total, distinct int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT event_id) FROM (
		SELECT fork_event_id AS event_id FROM run_fork_selected_contract_executions WHERE fork_run_id=$1
		UNION ALL
		SELECT o.event_id FROM fan_out_outcomes o
		JOIN fan_out_intents i ON i.run_id=o.run_id AND i.deployment_feed_id=o.deployment_feed_id
		WHERE o.run_id=$1 AND i.origin_kind='deployment' AND o.outcome_kind='committed' AND o.event_id IS NOT NULL
	) executed`, runID).Scan(&total, &distinct)
	if err != nil {
		return 0, fmt.Errorf("count durable selected-fork executions: %w", err)
	}
	if total != distinct {
		return 0, fmt.Errorf("selected-fork execution evidence assigns one event to multiple origins")
	}
	return total, nil
}

func postgresRunForkSelectedContractActivationPort(s *RunForkPostgresOwner) runForkSelectedContractActivationPort {
	return runForkSelectedContractActivationPort{
		postgres:       true,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
			result := mutationprotocol.RunPostgresWithOptions(ctx, s.backend, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					return operation(txctx, tx, attempt)
				})
			})
			return result.Acknowledged(), result.Err()
		},
		loadLineage: func(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
			return loadRunForkActivationLineage(ctx, s.RunLifecyclePostgresOwner, tx, forkRunID)
		},
		lockFrontier: lockRunForkSourceRevisionFrontier,
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint)
		},
		deliveries: postgresDeliveryAdapter,
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		workflowTimers: s.PipelinePostgresOwner, arrivalSchedules: s.PipelinePostgresOwner,
		attachment: func(ctx context.Context, tx *sql.Tx, evidence runForkSelectedContractActivationEvidence) error {
			snapshot, err := s.LoadSnapshotTx(ctx, tx, evidence.lineage.ForkRunID, true)
			if err != nil {
				return err
			}
			if err := requireSelectedContractPreparedAttachmentTx(ctx, tx, snapshot, evidence, false, s.EffectPostgresOwner); err != nil {
				return err
			}
			return requireExactMaterializedRunForkDeploymentFeeds(ctx, tx, true, evidence.lineage.ForkRunID, evidence.lineage.ForkBundleHash, s.durableData, evidence.plan, evidence.request.DataPins)
		},
		transition: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.ActiveTransitionRequest) error {
			_, err := s.RunLifecyclePostgresOwner.TransitionActiveTx(ctx, attempt, req)
			return err
		},
		diverge: insertRunForkSelectedContractBranchDivergence,
		freeze:  s.applyRunForkSourceFreeze,
		now:     func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractActivationPort(s *RunForkSQLiteOwner) runForkSelectedContractActivationPort {
	return runForkSelectedContractActivationPort{
		postgres:       false,
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error) {
			if err := s.requireCurrentSchema(); err != nil {
				return false, err
			}
			result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected-contract fork activation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					return operation(txctx, tx, attempt)
				})
			})
			return result.Acknowledged(), result.Err()
		},
		loadLineage: func(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
			return loadSQLiteRunForkActivationLineage(ctx, s.RunLifecycleSQLiteOwner, tx, forkRunID)
		},
		lockFrontier: lockSQLiteRunForkSourceRevisionFrontier,
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		},
		deliveries: sqliteDeliveryAdapter,
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		workflowTimers: s.PipelineSQLiteOwner, arrivalSchedules: s.PipelineSQLiteOwner,
		attachment: func(ctx context.Context, tx *sql.Tx, evidence runForkSelectedContractActivationEvidence) error {
			snapshot, err := s.LoadSnapshotTx(ctx, tx, evidence.lineage.ForkRunID)
			if err != nil {
				return err
			}
			if err := requireSelectedContractPreparedAttachmentTx(ctx, tx, snapshot, evidence, true, s.EffectSQLiteOwner); err != nil {
				return err
			}
			return requireExactMaterializedRunForkDeploymentFeeds(ctx, tx, false, evidence.lineage.ForkRunID, evidence.lineage.ForkBundleHash, s.durableData, evidence.plan, evidence.request.DataPins)
		},
		transition: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.ActiveTransitionRequest) error {
			_, err := s.RunLifecycleSQLiteOwner.TransitionActiveTx(ctx, attempt, req)
			return err
		},
		diverge: insertSQLiteRunForkSelectedContractBranchDivergence,
		freeze:  s.applyRunForkSourceFreeze,
		now:     s.now,
	}
}
