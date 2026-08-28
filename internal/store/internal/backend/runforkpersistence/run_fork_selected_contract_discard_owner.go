package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type runForkSelectedContractDiscardPort struct {
	requireCurrent func() error
	runMutation    func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error
	loadSnapshot   runForkLifecycleSnapshotLoader
	guard          func(context.Context, *sql.Tx, string) error
	terminalize    func(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *runforkrevision.Effects, string, string) error
	markTerminal   func(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *runforkrevision.Effects, runtimerunlifecycle.TerminalRequest) error
	deleteEvents   func(context.Context, *sql.Tx, string) error
	deleteRun      func(context.Context, *sql.Tx, string) error
	finalize       func(context.Context, *sql.Tx, *runforkrevision.Effects) error
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
		port.finalize == nil || port.now == nil {
		return fmt.Errorf("selected-contract fork discard operations are incomplete")
	}
	if err := port.requireCurrent(); err != nil {
		return err
	}
	return port.runMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects) error {
		snapshot, err := port.loadSnapshot(txctx, tx, forkRunID)
		if err != nil {
			if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
				return nil
			}
			return err
		}
		if snapshot.State != runtimerunlifecycle.StatePaused {
			return fmt.Errorf("selected-contract fork discard requires materialized fork state %q; got %q", runtimerunlifecycle.StatePaused, snapshot.State)
		}
		if err := port.guard(txctx, tx, forkRunID); err != nil {
			return fmt.Errorf("discard selected-contract fork with dependent lineage: %w", err)
		}
		var preserveCompletionEvidence bool
		if err := tx.QueryRowContext(txctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id = $1)`, forkRunID).Scan(&preserveCompletionEvidence); err != nil {
			return fmt.Errorf("check selected-contract completion evidence preservation: %w", err)
		}
		if err := port.terminalize(txctx, tx, story, effects, forkRunID, "fork_discarded"); err != nil {
			return fmt.Errorf("terminalize selected-contract fork deliveries before discard: %w", err)
		}
		if preserveCompletionEvidence {
			if err := port.markTerminal(txctx, tx, story, effects, runtimerunlifecycle.TerminalRequest{
				RunID: forkRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: port.now().UTC(),
			}); err != nil {
				return fmt.Errorf("retain selected-contract completion run tombstone: %w", err)
			}
		}
		if err := story.Finalize(txctx); err != nil {
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
			return port.deleteRun(txctx, tx, forkRunID)
		}
		if err := effects.Add(forkRunID,
			runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations,
			runforkrevision.FamilyEntityMetadata, runforkrevision.FamilyEventDeliveries,
			runforkrevision.FamilyCommittedReplayScopes, runforkrevision.FamilyEventReceipts,
			runforkrevision.FamilyDeadLetters, runforkrevision.FamilyTimers,
			runforkrevision.FamilyAgentSessions, runforkrevision.FamilyFanOutObligations,
		); err != nil {
			return err
		}
		return port.finalize(txctx, tx, effects)
	})
}

func deleteSelectedContractForkState(ctx context.Context, tx *sql.Tx, forkRunID string, preserveCompletionEvidence bool) error {
	statements := []struct {
		label string
		query string
	}{
		{"fan-out outcomes", `DELETE FROM fan_out_outcomes WHERE run_id = $1`},
		{"fan-out intents", `DELETE FROM fan_out_intents WHERE run_id = $1`},
		{"dead letters", `DELETE FROM dead_letters WHERE original_event_id IN (SELECT event_id FROM events WHERE run_id = $1)`},
		{"replay lineage", `DELETE FROM run_fork_delivery_event_replays WHERE fork_run_id = $1`},
		{"handler rule selections", `DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1 OR event_id IN (SELECT event_id FROM events WHERE run_id = $1))`},
		{"delivery outcomes", `DELETE FROM event_delivery_outcomes WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1 OR event_id IN (SELECT event_id FROM events WHERE run_id = $1))`},
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
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				return fmt.Errorf("begin selected-contract fork discard: %w", err)
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
			if err := operation(ctx, tx, story, runforkrevision.NewEffects()); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit selected-contract fork discard: %w", err)
			}
			committed = true
			return nil
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		guard: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return storeadmin.GuardSourceForkDependencies(ctx, tx, []string{runID})
		},
		terminalize: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, tx, story, effects, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecyclePostgresOwner.MarkTerminalTx(ctx, tx, story, effects, req)
			return err
		},
		deleteEvents: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return eventrecordpostgres.DeleteSelectedForkRunEvents(ctx, tx, runID)
		},
		deleteRun: s.RunLifecyclePostgresOwner.DeleteMaterializedForkRunTx,
		finalize: func(ctx context.Context, tx *sql.Tx, effects *runforkrevision.Effects) error {
			_, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
			return err
		},
		now: func() time.Time { return time.Now().UTC() },
	}
}

func sqliteRunForkSelectedContractDiscardPort(s *RunForkSQLiteOwner) runForkSelectedContractDiscardPort {
	return runForkSelectedContractDiscardPort{
		requireCurrent: s.requireRunForkSelectedContractExecutionAccess,
		runMutation: func(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) error {
			return s.runRuntimeMutation(ctx, "sqlite selected-contract fork discard", func(txctx context.Context, tx *sql.Tx) error {
				story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
				if err != nil {
					return err
				}
				return operation(txctx, tx, story, runforkrevision.NewEffects())
			})
		},
		loadSnapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		guard: guardSQLiteSelectedContractForkDependencies,
		terminalize: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, runID, reason string) error {
			_, err := s.TerminalizeRunDeliveriesTx(ctx, tx, story, effects, runID, reason)
			return err
		},
		markTerminal: func(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *runforkrevision.Effects, req runtimerunlifecycle.TerminalRequest) error {
			_, _, err := s.RunLifecycleSQLiteOwner.MarkTerminalTx(ctx, tx, story, effects, req)
			return err
		},
		deleteEvents: func(ctx context.Context, tx *sql.Tx, runID string) error {
			return eventrecordsqlite.DeleteSelectedForkRunEvents(ctx, tx, runID)
		},
		deleteRun: s.RunLifecycleSQLiteOwner.DeleteMaterializedForkRunTx,
		finalize: func(ctx context.Context, tx *sql.Tx, effects *runforkrevision.Effects) error {
			_, err := runforkrevision.FinalizeSQLite(ctx, tx, effects)
			return err
		},
		now: s.now,
	}
}
