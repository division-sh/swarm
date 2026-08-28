package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// runForkSelectedContractActivationPort contains only dialect-specific work for
// the selected-contract activation operation. Lifecycle meaning is owned by
// activateRunForkForSelectedContractExecution below.
type runForkSelectedContractActivationPort struct {
	requireCurrent func() error
	runMutation    func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error
	loadLineage    func(context.Context, *sql.Tx, string) (runForkActivationLineage, error)
	lockFrontier   func(context.Context, *sql.Tx, *runForkActivationLineage) error
	plan           func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	deliveries     *storedelivery.Adapter
	ensureState    func(context.Context, *sql.Tx, string, []string) error
	transition     func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runLifecycleCandidateHandoffReservation, runtimerunlifecycle.ActiveTransitionRequest) error
	diverge        func(context.Context, *sql.Tx, runfork.RunForkSelectedContractBranchDivergence) error
	freeze         func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects, runForkActivationLineage, time.Time, bool, *runLifecycleCandidateHandoffReservation) error
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
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	defer handoff.Rollback()

	var divergence *runfork.RunForkSelectedContractBranchDivergence
	err = port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects) error {
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
			ForkPoint:               runfork.RunForkPoint{Input: lineage.ForkEventID, EventID: lineage.ForkEventID, EventName: lineage.ForkEventName, Timestamp: lineage.ForkEventTime.UTC(), Revision: lineage.ForkEventRevision},
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
		if len(lineage.EntityIDs) == 0 {
			return fmt.Errorf("selected-contract fork activation requires materialized fork entity_state rows")
		}
		binding, err := loadRunForkSelectedContractBinding(txctx, tx, lineage.ForkRunID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("selected-contract fork activation requires selected contract binding")
			}
			return fmt.Errorf("load selected contract binding: %w", err)
		}
		result.SelectedContractBinding = &binding

		plan, err := port.plan(txctx, tx, runfork.RunForkPlanRequest{SourceRunID: lineage.SourceRunID, At: lineage.ForkEventID})
		if err != nil {
			return err
		}
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
		if err := port.ensureState(txctx, tx, lineage.ForkRunID, req.AllowedSourceEventIDs); err != nil {
			return addRunForkActivationBlocker(&result, err)
		}

		now := port.now().UTC()
		if len(sourceAdvancedFacts) > 0 {
			if err := port.transition(txctx, tx, story, handoff, runtimerunlifecycle.ActiveTransitionRequest{RunID: lineage.ForkRunID, State: runtimerunlifecycle.StateRunning}); err != nil {
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
			if err := recordRunForkActivationAuthorActivity(txctx, story, lineage, now); err != nil {
				return err
			}
			divergence = &value
			return nil
		}
		return port.freeze(txctx, tx, story, effects, lineage, now, req.ConfirmSourceFreeze, handoff)
	})
	if err != nil {
		return result, err
	}
	if err := handoff.Commit(); err != nil {
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
	return result, nil
}

func postgresRunForkSelectedContractActivationPort(s *RunForkPostgresOwner) runForkSelectedContractActivationPort {
	return runForkSelectedContractActivationPort{
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				return fmt.Errorf("begin selected-contract fork activation: %w", err)
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
				return fmt.Errorf("commit selected-contract fork activation: %w", err)
			}
			committed = true
			return nil
		},
		loadLineage: func(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
			return loadRunForkActivationLineage(ctx, s.RunLifecyclePostgresOwner, tx, forkRunID)
		},
		lockFrontier: lockRunForkSourceRevisionFrontier,
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint)
		},
		deliveries:  postgresDeliveryAdapter,
		ensureState: ensureRunForkSelectedContractExecutionForkState,
		transition: func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, handoff *runLifecycleCandidateHandoffReservation, req runtimerunlifecycle.ActiveTransitionRequest) error {
			_, err := s.RunLifecyclePostgresOwner.TransitionActiveTx(ctx, tx, story, handoff, req)
			return err
		},
		diverge: insertRunForkSelectedContractBranchDivergence,
		freeze:  s.applyRunForkSourceFreeze,
		now:     func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractActivationPort(s *RunForkSQLiteOwner) runForkSelectedContractActivationPort {
	return runForkSelectedContractActivationPort{
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			return s.runRuntimeMutation(ctx, "sqlite selected-contract fork activation", func(txctx context.Context, tx *sql.Tx) error {
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
		loadLineage: func(ctx context.Context, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
			return loadSQLiteRunForkActivationLineage(ctx, s.RunLifecycleSQLiteOwner, tx, forkRunID)
		},
		lockFrontier: lockSQLiteRunForkSourceRevisionFrontier,
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		},
		deliveries:  sqliteDeliveryAdapter,
		ensureState: ensureSQLiteRunForkSelectedContractExecutionForkState,
		transition: func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, handoff *runLifecycleCandidateHandoffReservation, req runtimerunlifecycle.ActiveTransitionRequest) error {
			_, err := s.RunLifecycleSQLiteOwner.TransitionActiveTx(ctx, tx, story, handoff, req)
			return err
		},
		diverge: insertSQLiteRunForkSelectedContractBranchDivergence,
		freeze:  s.applyRunForkSourceFreeze,
		now:     s.now,
	}
}
