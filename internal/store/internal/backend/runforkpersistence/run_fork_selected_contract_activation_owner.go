package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// runForkSelectedContractActivationPort contains only dialect-specific work for
// the selected-contract activation operation. Lifecycle meaning is owned by
// activateRunForkForSelectedContractExecution below.
type runForkSelectedContractActivationPort struct {
	postgres       bool
	requireCurrent func() error
	runMutation    func(context.Context, func(context.Context, *sql.Tx, *mutationprotocol.Attempt) error) (bool, error)
	loadLineage    func(context.Context, *sql.Tx, string) (runForkActivationLineage, error)
	lockFrontier   func(context.Context, *sql.Tx, *runForkActivationLineage) error
	plan           func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	deliveries     *storedelivery.Adapter
	ensureState    func(context.Context, *sql.Tx, string, []string, semanticview.Source) error
	transition     func(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.ActiveTransitionRequest) error
	diverge        func(context.Context, *sql.Tx, runfork.RunForkSelectedContractBranchDivergence) error
	freeze         func(context.Context, *sql.Tx, *mutationprotocol.Attempt, runForkActivationLineage, time.Time, bool) error
	now            func() time.Time
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
		port.plan == nil || port.deliveries == nil || port.ensureState == nil || port.transition == nil || port.diverge == nil ||
		port.freeze == nil || port.now == nil {
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
		if err := ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(txctx, tx, lineage.SourceRunID, lineage.ForkEventRevision); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		sourceAdvancedFacts = append(sourceAdvancedFacts, runfork.ActiveSourceDeliveryConversationCouplingFacts(result.ReplayResumeAdmission)...)
		sourceAdvancedFacts = uniqueNonEmptyStrings(sourceAdvancedFacts)
		result.SourceAdvancedAfterFork = len(sourceAdvancedFacts) > 0
		if err := requireSelectedDeploymentDrainedTx(txctx, tx, lineage.ForkRunID); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}
		if err := port.ensureState(txctx, tx, lineage.ForkRunID, req.AllowedSourceEventIDs, req.ExecutionSource); err != nil {
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
				ForkEventID:                    lineage.ForkEventID,
				Policy:                         runfork.RunForkSelectedContractSourceAdvancedBranchPolicy,
				SourceRunStatusAtActivation:    lineage.SourceRunStatus,
				SourceRunStatusAfterActivation: lineage.SourceRunStatus,
				SourceFrozen:                   false,
				SourceAdvancedFacts:            sourceAdvancedFacts,
				CreatedAt:                      now,
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

func requireSelectedForkMaterializedWorkTx(ctx context.Context, tx *sql.Tx, runID string, entityCount int) error {
	if entityCount > 0 {
		return nil
	}
	present, err := selectedDeploymentFeedPresentTx(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("check selected fork deployment work: %w", err)
	}
	if !present {
		return fmt.Errorf("selected-contract fork activation requires materialized entity or deployment work")
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
		deliveries:  postgresDeliveryAdapter,
		ensureState: s.ensureRunForkSelectedContractExecutionForkState,
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
		deliveries:  sqliteDeliveryAdapter,
		ensureState: s.ensureSQLiteRunForkSelectedContractExecutionForkState,
		transition: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.ActiveTransitionRequest) error {
			_, err := s.RunLifecycleSQLiteOwner.TransitionActiveTx(ctx, attempt, req)
			return err
		},
		diverge: insertSQLiteRunForkSelectedContractBranchDivergence,
		freeze:  s.applyRunForkSourceFreeze,
		now:     s.now,
	}
}
