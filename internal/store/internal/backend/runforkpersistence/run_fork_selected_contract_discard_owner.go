package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type runForkSelectedContractDiscardPort struct {
	requireCurrent func() error
	runMutation    func(context.Context, func(context.Context, *mutationprotocol.Attempt) error) error
	loadSnapshot   runForkLifecycleSnapshotLoader
	guard          func(context.Context, *sql.Tx, string) error
	terminalize    func(context.Context, *mutationprotocol.Attempt, string, string) error
	markTerminal   func(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.TerminalRequest) error
	deleteEvents   func(context.Context, *sql.Tx, string) error
	deleteRun      func(context.Context, *mutationprotocol.Attempt, string) error
	now            func() time.Time
}

func discardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string, port runForkSelectedContractDiscardPort) error {
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if port.requireCurrent == nil || port.runMutation == nil || port.loadSnapshot == nil || port.guard == nil ||
		port.terminalize == nil || port.markTerminal == nil || port.deleteEvents == nil || port.deleteRun == nil ||
		port.now == nil {
		return fmt.Errorf("selected-contract fork discard operations are incomplete")
	}
	if err := port.requireCurrent(); err != nil {
		return err
	}
	// Retention is decided under the transaction lock; whole-parent deletion adds no facts.
	return port.runMutation(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			snapshot, err := port.loadSnapshot(txctx, tx, forkRunID)
			if err != nil {
				if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
					retained, classifyErr := attempt.SelectForkDiscardRetention(txctx, forkRunID)
					if classifyErr != nil {
						return classifyErr
					}
					if retained {
						return errors.New("absent selected fork has retained execution evidence")
					}
					return attempt.BeginDestructiveCleanup(txctx)
				}
				return err
			}
			if snapshot.State != runtimerunlifecycle.StatePaused {
				return fmt.Errorf("selected-contract fork discard requires materialized fork state %q; got %q", runtimerunlifecycle.StatePaused, snapshot.State)
			}
			if err := port.guard(txctx, tx, forkRunID); err != nil {
				return fmt.Errorf("discard selected-contract fork with dependent lineage: %w", err)
			}
			preserveCompletionEvidence, err := attempt.SelectForkDiscardRetention(txctx, forkRunID)
			if err != nil {
				return err
			}
			if err := port.terminalize(txctx, attempt, forkRunID, "fork_discarded"); err != nil {
				return fmt.Errorf("terminalize selected-contract fork deliveries before discard: %w", err)
			}
			if preserveCompletionEvidence {
				if err := port.markTerminal(txctx, attempt, runtimerunlifecycle.TerminalRequest{
					RunID: forkRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: port.now().UTC(),
				}); err != nil {
					return fmt.Errorf("retain selected-contract completion run tombstone: %w", err)
				}
			}
			if err := attempt.BeginDestructiveCleanup(txctx); err != nil {
				return fmt.Errorf("finalize selected-contract fork terminalization activity: %w", err)
			}
			if err := deleteSelectedContractForkState(txctx, tx, forkRunID, preserveCompletionEvidence); err != nil {
				return err
			}
			if err := port.deleteEvents(txctx, tx, forkRunID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `DELETE FROM entity_state WHERE run_id = $1`, forkRunID); err != nil {
				return fmt.Errorf("delete selected-contract fork entity state: %w", err)
			}
			if !preserveCompletionEvidence {
				if _, err := tx.ExecContext(txctx, `DELETE FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1`, forkRunID); err != nil {
					return fmt.Errorf("delete selected-contract fork binding: %w", err)
				}
				return port.deleteRun(txctx, attempt, forkRunID)
			}
			for _, family := range []runforkrevision.Family{
				runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations,
				runforkrevision.FamilyEntityMetadata, runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes, runforkrevision.FamilyEventReceipts,
				runforkrevision.FamilyDeadLetters, runforkrevision.FamilyTimers,
				runforkrevision.FamilyAgentSessions, runforkrevision.FamilyFanOutObligations,
			} {
				if err := attempt.AddWholeFamily(forkRunID, family); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func deleteSelectedContractForkState(ctx context.Context, tx *sql.Tx, forkRunID string, preserveCompletionEvidence bool) error {
	statements := []struct {
		label string
		query string
	}{
		{"lifecycle diagnostic projections", `DELETE FROM agent_lifecycle_diagnostic_outbox WHERE run_id = $1`},
		{"fan-out barriers", `DELETE FROM fan_out_obligation_barriers WHERE run_id = $1`},
		{"fan-out outcomes", `DELETE FROM fan_out_outcomes WHERE run_id = $1`},
		{"fan-out intents", `DELETE FROM fan_out_intents WHERE run_id = $1`},
		{"dead letters", `DELETE FROM dead_letters WHERE original_event_id IN (SELECT event_id FROM events WHERE run_id = $1)`},
		{"replay lineage", `DELETE FROM run_fork_delivery_event_replays WHERE fork_run_id = $1`},
		{"handler rule selections", `DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1 OR event_id IN (SELECT event_id FROM events WHERE run_id = $1))`},
		{"delivery attempts", `DELETE FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1 OR event_id IN (SELECT event_id FROM events WHERE run_id = $1))`},
		{"deliveries", `DELETE FROM event_deliveries WHERE run_id = $1 OR event_id IN (SELECT event_id FROM events WHERE run_id = $1)`},
		{"sessions", `DELETE FROM agent_sessions WHERE run_id = $1`},
		{"branch divergence", `DELETE FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id = $1`},
		{"route recovery", `DELETE FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1`},
		{"execution lineage", `DELETE FROM run_fork_selected_contract_executions WHERE fork_run_id = $1`},
		{"receipts", `DELETE FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id = $1)`},
		{"committed replay scopes", `DELETE FROM committed_replay_scopes WHERE event_id IN (SELECT event_id FROM events WHERE run_id = $1)`},
		{"timers", `DELETE FROM timers WHERE run_id = $1`},
		{"activity evidence", `DELETE FROM activity_attempts WHERE run_id = $1`},
		{"mutations", `DELETE FROM entity_mutations WHERE run_id = $1`},
	}
	if !preserveCompletionEvidence {
		statements = append([]struct{ label, query string }{
			{"turns", `DELETE FROM agent_turns WHERE run_id = $1`},
			{"conversation audits", `DELETE FROM agent_conversation_audits WHERE run_id = $1`},
			{"author activity", `DELETE FROM author_activity_occurrences WHERE run_id = $1`},
		}, statements...)
		statements = append(statements, struct{ label, query string }{
			"agents", `DELETE FROM agents WHERE run_id = $1`,
		})
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, forkRunID); err != nil {
			return fmt.Errorf("delete selected-contract fork %s: %w", statement.label, err)
		}
	}
	return nil
}

func postgresRunForkSelectedContractDiscardPort(s *RunForkPostgresOwner) runForkSelectedContractDiscardPort {
	return runForkSelectedContractDiscardPort{
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunPostgresWithOptions(ctx, s.backend, &sql.TxOptions{Isolation: sql.LevelSerializable}, mutationprotocol.Story, mutationprotocol.RetainedForkCleanup, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, operation(txctx, attempt)
			})
			return result.Err()
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		guard: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return storeadmin.GuardSourceForkDependencies(ctx, tx, []string{runID})
		},
		terminalize: func(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, attempt, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecyclePostgresOwner.MarkTerminalTx(ctx, attempt, req)
			return err
		},
		deleteEvents: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return eventrecordpostgres.DeleteSelectedForkRunEvents(ctx, tx, runID)
		},
		deleteRun: s.RunLifecyclePostgresOwner.DeleteMaterializedForkRunTx,
		now:       func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractDiscardPort(s *RunForkSQLiteOwner) runForkSelectedContractDiscardPort {
	return runForkSelectedContractDiscardPort{
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected-contract fork discard", mutationprotocol.Story, mutationprotocol.RetainedForkCleanup, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, operation(txctx, attempt)
			})
			return result.Err()
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		guard: guardSQLiteSelectedContractForkDependencies,
		terminalize: func(ctx context.Context, attempt *mutationprotocol.Attempt, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, attempt, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecycleSQLiteOwner.MarkTerminalTx(ctx, attempt, req)
			return err
		},
		deleteEvents: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return eventrecordsqlite.DeleteSelectedForkRunEvents(ctx, tx, runID)
		},
		deleteRun: s.RunLifecycleSQLiteOwner.DeleteMaterializedForkRunTx,
		now:       s.now,
	}
}
