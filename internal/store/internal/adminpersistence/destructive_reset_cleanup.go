package adminpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/lib/pq"
)

func (s *DestructiveResetPostgresOwner) ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	if s == nil || s.backend == nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("postgres store is required")
	}
	txOptions := (*sql.TxOptions)(nil)
	if req.Result.DryRun {
		txOptions = &sql.TxOptions{ReadOnly: true}
	}
	tx, err := s.backend.BeginTx(ctx, txOptions)
	if err != nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("begin destructive reset cleanup tx: %w", err)
	}
	out, err := applyDestructiveResetCleanupTx(ctx, tx, req, false)
	if err != nil {
		_ = tx.Rollback()
		return destructivereset.CleanupResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("commit destructive reset cleanup tx: %w", err)
	}
	return out, nil
}

// ApplyDestructiveResetCleanupInRetainedTransaction lets the startup owner
// compose cleanup with its topology-head update without exporting a
// transaction-bearing method on a semantic owner.
func ApplyDestructiveResetCleanupInRetainedTransaction(s *DestructiveResetPostgresOwner, ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	if s == nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("postgres destructive reset owner is required")
	}
	return applyDestructiveResetCleanupTx(ctx, tx, req, false)
}

func ApplyDestructiveResetSQLiteCleanupInRetainedTransaction(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	return applyDestructiveResetCleanupTx(ctx, tx, req, true)
}

// applyDestructiveResetCleanupTx is the retained-session operation used when
// cleanup and the complete topology plan must share one commit.
func applyDestructiveResetCleanupTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) (destructivereset.CleanupResult, error) {
	if tx == nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("destructive reset retained transaction is required")
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := validateDestructiveResetCleanupCatalog(); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	runIDs, err := validateDestructiveResetCleanupRequest(req, now)
	if err != nil {
		return destructivereset.CleanupResult{}, err
	}
	out := destructivereset.CleanupResult{
		OperationName: strings.TrimSpace(req.Result.OperationName), DryRun: req.Result.DryRun,
		IncludeSourceArtifacts: req.Result.IncludeSourceArtifacts, AppliedAt: now,
	}
	if out.OperationName == "" {
		out.OperationName = destructivereset.DefaultOperationName
	}
	if req.Result.DryRun {
		out.RunIDs = runIDs
		out.Tables, err = destructiveResetCleanupTableResults(ctx, tx, runIDs, req.Result.IncludeSourceArtifacts, sqlite)
		return out, err
	}

	if err := lockDestructiveResetCleanupRuns(ctx, tx, runIDs, sqlite); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := guardSourceForkDependencies(ctx, tx, runIDs, sqlite); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := guardDestructiveResetDirectiveAuthority(ctx, tx, runIDs, now, sqlite); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := guardDestructiveResetProviderAuthority(ctx, tx, runIDs, sqlite); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if req.Result.IncludeSourceArtifacts {
		if err := prepareDestructiveResetSourceArtifactDelete(ctx, tx, runIDs, sqlite); err != nil {
			return destructivereset.CleanupResult{}, err
		}
	}
	if err := destructiveResetCleanupSeverPreservedReferences(ctx, tx, runIDs, sqlite); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	out.RunIDs = runIDs
	rows, err := destructiveResetCleanupTableResults(ctx, tx, runIDs, req.Result.IncludeSourceArtifacts, sqlite)
	if err != nil {
		return destructivereset.CleanupResult{}, err
	}
	for i := range rows {
		if rows[i].TableKind == destructivereset.CleanupTableKindGenerated {
			continue
		}
		deleted, err := destructiveResetCleanupDeleteTable(ctx, tx, rows[i].Table, runIDs, req.Result.IncludeSourceArtifacts, sqlite)
		if err != nil {
			return destructivereset.CleanupResult{}, err
		}
		rows[i].DeletedRows = deleted
	}
	out.Tables = rows
	return out, nil
}

func validateDestructiveResetCleanupRequest(req destructivereset.CleanupRequest, requestedAt time.Time) ([]string, error) {
	if strings.TrimSpace(req.ActorTokenID) == "" {
		return nil, fmt.Errorf("%w: actor token id is required", destructivereset.ErrInvalidRequest)
	}
	if req.Result.PlannedAt.IsZero() {
		return nil, fmt.Errorf("%w: destructive reset plan result is required", destructivereset.ErrInvalidRequest)
	}
	runIDs, err := destructiveResetCleanupRunIDsFromPlan(req.Result.Plan)
	if err != nil {
		return nil, err
	}
	if req.Result.Plan.IncludeSourceArtifacts != req.Result.IncludeSourceArtifacts {
		return nil, fmt.Errorf("%w: destructive reset include_source_artifacts result and plan mismatch", destructivereset.ErrInvalidRequest)
	}
	if req.Result.DryRun {
		return runIDs, nil
	}
	if req.Quiescence.AppliedAt.IsZero() {
		return nil, fmt.Errorf("%w: destructive reset quiescence result is required", destructivereset.ErrInvalidRequest)
	}
	if req.Quiescence.DryRun {
		return nil, fmt.Errorf("%w: destructive reset cleanup requires applied quiescence", destructivereset.ErrInvalidRequest)
	}
	if normalizeDestructiveResetOperationName(req.Quiescence.OperationName) != normalizeDestructiveResetOperationName(req.Result.OperationName) {
		return nil, fmt.Errorf("%w: destructive reset quiescence operation does not match plan result", destructivereset.ErrInvalidRequest)
	}
	if req.Quiescence.AppliedAt.UTC().Before(req.Result.PlannedAt.UTC()) {
		return nil, fmt.Errorf("%w: destructive reset quiescence predates plan result", destructivereset.ErrInvalidRequest)
	}
	if !requestedAt.IsZero() && requestedAt.UTC().Before(req.Quiescence.AppliedAt.UTC()) {
		return nil, fmt.Errorf("%w: destructive reset cleanup request predates quiescence", destructivereset.ErrInvalidRequest)
	}
	return runIDs, nil
}

func ValidateDestructiveResetCleanupRequest(req destructivereset.CleanupRequest, requestedAt time.Time) ([]string, error) {
	return validateDestructiveResetCleanupRequest(req, requestedAt)
}

func normalizeDestructiveResetOperationName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return destructivereset.DefaultOperationName
	}
	return name
}

func validateDestructiveResetCleanupCatalog() error {
	seen := map[string]struct{}{}
	for _, entry := range destructivereset.DefaultPlatformCleanupCatalog() {
		table := strings.TrimSpace(entry.Table)
		if table == "" {
			return fmt.Errorf("destructive reset cleanup catalog contains empty table")
		}
		if _, ok := seen[table]; ok {
			return fmt.Errorf("destructive reset cleanup catalog duplicates table %s", table)
		}
		seen[table] = struct{}{}
		if strings.TrimSpace(entry.Classification) == "" {
			return fmt.Errorf("destructive reset cleanup catalog table %s has empty classification", table)
		}
		if strings.TrimSpace(entry.PredicateOwner) == "" {
			return fmt.Errorf("destructive reset cleanup catalog table %s has empty predicate owner", table)
		}
	}
	return nil
}

func destructiveResetCleanupRunIDsFromPlan(plan destructivereset.Plan) ([]string, error) {
	if !plan.CleanupRunSetKnown {
		return nil, fmt.Errorf("%w: destructive reset cleanup run set is required", destructivereset.ErrInvalidRequest)
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(plan.CleanupRuns))
	for _, run := range plan.CleanupRuns {
		runID := nullUUIDString(run.RunID)
		if runID == "" {
			return nil, fmt.Errorf("%w: destructive reset cleanup run_id is required", destructivereset.ErrInvalidRequest)
		}
		if _, ok := seen[runID]; ok {
			continue
		}
		seen[runID] = struct{}{}
		out = append(out, runID)
	}
	return out, nil
}

func lockDestructiveResetCleanupRuns(ctx context.Context, tx *sql.Tx, runIDs []string, sqlite bool) error {
	if sqlite {
		// The retained SQLite write transaction excludes concurrent writers.
		return nil
	}
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if len(runIDs) == 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT CAST(run_id AS TEXT)
		FROM runs
		WHERE run_id IN %[1]s
		ORDER BY CAST(run_id AS TEXT)
		FOR UPDATE
	`, runSet), runArg)
	if err != nil {
		return fmt.Errorf("lock destructive reset cleanup run set: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return fmt.Errorf("scan destructive reset cleanup run id: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lock destructive reset cleanup run ids: %w", err)
	}
	return nil
}

func GuardSourceForkDependencies(ctx context.Context, tx *sql.Tx, runIDs []string) error {
	return guardSourceForkDependencies(ctx, tx, runIDs, false)
}

func guardSourceForkDependencies(ctx context.Context, tx *sql.Tx, runIDs []string, sqlite bool) error {
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if len(runIDs) == 0 {
		return nil
	}
	var forkRunID, sourceRunID string
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT CAST(fork.run_id AS TEXT), CAST(fork.forked_from_run_id AS TEXT)
		FROM runs fork
		WHERE fork.forked_from_run_id IN %[1]s
		  AND NOT (fork.run_id IN %[1]s)
		ORDER BY fork.run_id
		LIMIT 1
	`, runSet), runArg).Scan(&forkRunID, &sourceRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect destructive reset fork dependencies: %w", err)
	}
	return fmt.Errorf("%w: cannot delete source run %s while dependent fork %s remains outside the cleanup set", destructivereset.ErrInvalidRequest, sourceRunID, forkRunID)
}

func guardDestructiveResetDirectiveAuthority(ctx context.Context, tx *sql.Tx, runIDs []string, now time.Time, sqlite bool) error {
	expired := "expires_at <= $2"
	if sqlite {
		expired = "julianday(expires_at) <= julianday($2)"
	}
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if len(runIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM agent_directive_operations
		WHERE resolved_run_id IN %[1]s
		  AND state IN ('succeeded', 'failed')
		  AND %[2]s
	`, runSet, expired), runArg, now.UTC()); err != nil {
		return fmt.Errorf("expire terminal directive authority before destructive reset: %w", err)
	}
	var operationID, state string
	var expiresAt sql.NullString
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT CAST(operation_id AS TEXT), state, CAST(expires_at AS TEXT)
		FROM agent_directive_operations
		WHERE resolved_run_id IN %[1]s
		ORDER BY created_at, operation_id
		LIMIT 1
	`, runSet), runArg).Scan(&operationID, &state, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect directive authority before destructive reset: %w", err)
	}
	detail := fmt.Sprintf("operation_id=%s state=%s", operationID, state)
	if expiresAt.Valid {
		detail += " expires_at=" + expiresAt.String
	}
	return fmt.Errorf("%w: runtime.nuke cannot delete retained agent directive authority (%s)", destructivereset.ErrInvalidRequest, detail)
}

func guardDestructiveResetProviderAuthority(ctx context.Context, tx *sql.Tx, runIDs []string, sqlite bool) error {
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if len(runIDs) == 0 {
		return nil
	}
	var authorityKind, authorityID, state string
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT authority_kind,authority_id,state
		FROM (
			SELECT 'attempt' AS authority_kind,CAST(attempt_id AS TEXT) AS authority_id,state,authorized_at AS ordered_at
			FROM runtime_external_effect_attempts
			WHERE origin_run_id IN %[1]s
			  AND state IN ('authorized','launched','response_observed')
			UNION ALL
			SELECT 'drain' AS authority_kind,CAST(drain_id AS TEXT) AS authority_id,state,captured_at AS ordered_at
			FROM runtime_provider_attempt_drains
			WHERE origin_run_id IN %[1]s AND state='pending'
		) retained
		ORDER BY ordered_at,authority_kind,authority_id
		LIMIT 1
	`, runSet), runArg).Scan(&authorityKind, &authorityID, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect provider authority before destructive reset: %w", err)
	}
	return fmt.Errorf(
		"%w: runtime.nuke cannot delete a run with nonterminal provider authority (kind=%s id=%s state=%s)",
		destructivereset.ErrInvalidRequest, authorityKind, authorityID, state,
	)
}

func prepareDestructiveResetSourceArtifactDelete(ctx context.Context, tx *sql.Tx, runIDs []string, sqlite bool) error {
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if err := lockDestructiveResetRunCreationTx(ctx, tx, sqlite); err != nil {
		return fmt.Errorf("lock runtime.nuke source artifact cleanup: %w", err)
	}
	var outOfPlan int
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM runs
		WHERE NULLIF(bundle_hash, '') IS NOT NULL
		  AND NOT (run_id IN %[1]s)
	`, runSet), runArg).Scan(&outOfPlan); err != nil {
		return fmt.Errorf("validate runtime.nuke source artifact cleanup run snapshot: %w", err)
	}
	if outOfPlan > 0 {
		return fmt.Errorf("%w: runtime.nuke include_source_artifacts cannot delete admitted source with runs outside the cleanup plan", destructivereset.ErrInvalidRequest)
	}
	return nil
}

func lockDestructiveResetRunCreationTx(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	if sqlite {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE runs IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("lock destructive reset run creation: %w", err)
	}
	return nil
}

func destructiveResetCleanupSeverPreservedReferences(ctx context.Context, tx *sql.Tx, runIDs []string, sqlite bool) error {
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	if len(runIDs) == 0 {
		return nil
	}
	statements := []struct {
		name  string
		query string
	}{
		{
			name: "managed_agent_capability_surfaces.run_id",
			query: `
				UPDATE managed_agent_capability_surfaces
				SET run_id = NULL
				WHERE run_id IN %[1]s
			`,
		},
		{
			name: "agent_sessions.successor_session_id",
			query: `
				UPDATE agent_sessions AS preserved
				SET successor_session_id = NULL
				WHERE preserved.successor_session_id IS NOT NULL
				  AND (preserved.run_id IS NULL OR NOT (preserved.run_id IN %[1]s))
				  AND EXISTS (
					SELECT 1
					FROM agent_sessions cleanup
					WHERE cleanup.session_id = preserved.successor_session_id
					  AND cleanup.run_id IN %[1]s
				  )
			`,
		},
		{
			name: "runtime_ingress_state.transition_event_id",
			query: `
				UPDATE runtime_ingress_state AS preserved
				SET transition_event_id = NULL
				WHERE preserved.transition_event_id IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM events cleanup_event
					WHERE cleanup_event.event_id = preserved.transition_event_id
					  AND cleanup_event.run_id IN %[1]s
				  )
			`,
		},
		{
			name: "entity_mutations.caused_by_event",
			query: `
				UPDATE entity_mutations AS preserved
				SET caused_by_event = NULL
				WHERE NOT (preserved.run_id IN %[1]s)
				  AND preserved.caused_by_event IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM events cleanup_event
					WHERE cleanup_event.event_id = preserved.caused_by_event
					  AND cleanup_event.run_id IN %[1]s
				  )
			`,
		},
		{
			name: "timers.source_timer_id",
			query: `
				WITH cleanup_timers AS (
					SELECT cleanup.timer_id
					FROM timers cleanup
					WHERE cleanup.run_id IN %[1]s
					   OR cleanup.forked_from_run_id IN %[1]s
					   OR EXISTS (
							SELECT 1
							FROM events cleanup_event
							WHERE cleanup_event.event_id = cleanup.forked_from_event_id
							  AND cleanup_event.run_id IN %[1]s
					   )
				)
				UPDATE timers AS preserved
				SET source_timer_id = NULL
				WHERE preserved.source_timer_id IN (SELECT timer_id FROM cleanup_timers)
				  AND NOT EXISTS (
					SELECT 1
					FROM cleanup_timers cleanup
					WHERE cleanup.timer_id = preserved.timer_id
				  )
			`,
		},
		{
			name: "mailbox.reply_context_id",
			query: `
				UPDATE mailbox AS preserved
				SET reply_context_id = NULL
				WHERE preserved.reply_context_id IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM reply_contexts cleanup
					WHERE cleanup.reply_context_id = preserved.reply_context_id
					  AND cleanup.run_id IN %[1]s
				  )
			`,
		},
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(stmt.query, runSet), runArg); err != nil {
			return fmt.Errorf("sever destructive reset preserved reference %s: %w", stmt.name, err)
		}
	}
	return nil
}

func destructiveResetCleanupTableResults(ctx context.Context, tx *sql.Tx, runIDs []string, includeSourceArtifacts bool, sqlite bool) ([]destructivereset.CleanupTableResult, error) {
	catalog := destructivereset.CleanupCatalogForPolicy(destructivereset.CleanupPolicy{IncludeSourceArtifacts: includeSourceArtifacts})
	out := make([]destructivereset.CleanupTableResult, 0, len(catalog))
	for _, entry := range catalog {
		result := destructivereset.CleanupTableResult{
			Table:            entry.Table,
			TableKind:        entry.TableKind,
			Classification:   entry.Classification,
			PredicateOwner:   entry.PredicateOwner,
			DeleteOrderGroup: entry.DeleteOrderGroup,
		}
		if entry.TableKind == destructivereset.CleanupTableKindGenerated {
			out = append(out, result)
			continue
		}
		count, err := destructiveResetCleanupCountTable(ctx, tx, entry, runIDs, includeSourceArtifacts, sqlite)
		if err != nil {
			return nil, err
		}
		switch entry.Classification {
		case destructivereset.CleanupPreserve, destructivereset.CleanupSplitPreserve, destructivereset.CleanupRetainDirectiveAuthority:
			result.PreservedRows = count
		default:
			result.MatchedRows = count
		}
		out = append(out, result)
	}
	return out, nil
}

func destructiveResetCleanupCountTable(ctx context.Context, tx *sql.Tx, entry destructivereset.CleanupCatalogEntry, runIDs []string, includeSourceArtifacts bool, sqlite bool) (int64, error) {
	statements, err := destructiveResetCleanupStatementsForTable(entry.Table, runIDs, includeSourceArtifacts, sqlite)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, statements.count, statements.args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count destructive reset cleanup table %s: %w", entry.Table, err)
	}
	return count, nil
}

func destructiveResetCleanupDeleteTable(ctx context.Context, tx *sql.Tx, table string, runIDs []string, includeSourceArtifacts bool, sqlite bool) (int64, error) {
	statements, err := destructiveResetCleanupStatementsForTable(table, runIDs, includeSourceArtifacts, sqlite)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(statements.delete) == "" {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, statements.delete, statements.args...)
	if err != nil {
		return 0, fmt.Errorf("delete destructive reset cleanup table %s: %w", table, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read destructive reset cleanup affected rows for %s: %w", table, err)
	}
	return rows, nil
}

type destructiveResetCleanupStatements struct {
	count  string
	delete string
	args   []any
}

func destructiveResetCleanupStatementsForTable(table string, runIDs []string, includeSourceArtifacts bool, sqlite bool) (destructiveResetCleanupStatements, error) {
	table = strings.TrimSpace(table)
	if destructiveResetCleanupPreservesTable(table, includeSourceArtifacts) {
		return destructiveResetCleanupStatements{count: fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(table))}, nil
	}
	if table == "source_artifacts" && includeSourceArtifacts {
		return destructiveResetCleanupStatements{count: "SELECT COUNT(*) FROM source_artifacts", delete: "DELETE FROM source_artifacts"}, nil
	}
	switch table {
	case "connected_channel_activations", "channel_onboarding_operations", "standing_service_journal", "standing_service_generations", "standing_services":
		return destructiveResetCleanupStatements{count: fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(table)), delete: fmt.Sprintf("DELETE FROM %s", quoteIdent(table))}, nil
	}
	if len(runIDs) == 0 {
		return destructiveResetCleanupStatements{count: "SELECT 0"}, nil
	}
	runSet, runArg := destructiveResetRunSet(runIDs, sqlite)
	var predicate string
	switch table {
	case "inbound_publication_events":
		predicate = "EXISTS (SELECT 1 FROM inbound_publications p WHERE p.publication_id = target.publication_id AND p.resolved_run_id IN %[1]s)"
	case "inbound_publications", "agent_directive_operations":
		predicate = "target.resolved_run_id IN %[1]s"
	case "event_receipts":
		predicate = "EXISTS (SELECT 1 FROM events e WHERE e.event_id = target.event_id AND e.run_id IN %[1]s)"
	case "event_delivery_handler_rule_selections", "event_delivery_attempts", "event_delivery_outcomes":
		predicate = "EXISTS (SELECT 1 FROM event_deliveries d LEFT JOIN events e ON e.event_id = d.event_id WHERE d.delivery_id = target.delivery_id AND (d.run_id IN %[1]s OR e.run_id IN %[1]s))"
	case "dead_letters":
		predicate = "EXISTS (SELECT 1 FROM events e WHERE e.event_id = target.original_event_id AND e.run_id IN %[1]s)"
	case "event_deliveries", "committed_replay_scopes":
		predicate = "target.run_id IN %[1]s OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = target.event_id AND e.run_id IN %[1]s)"
	case "fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents", "resource_version_pins", "author_activity_occurrences", "run_fork_fact_revisions", "run_fork_revisions", "run_fork_revision_heads", "activity_attempts", "agent_turns", "agent_conversation_audits", "agent_sessions", "agents", "flow_instances", "routing_rules", "decision_card_route_obligations", "decision_card_changes", "decision_card_input_drafts", "proposed_effect_continuations", "human_task_continuations", "decision_cards", "entity_mutations", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "run_control_state", "reply_contexts", "run_scenario_execution_profiles", "events", "runs":
		predicate = "target.run_id IN %[1]s"
	case "conversation_forks":
		predicate = "target.source_run_id IN %[1]s"
	case "conversation_fork_snapshots", "conversation_fork_turns":
		predicate = "EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id = target.fork_id AND f.source_run_id IN %[1]s)"
	case "conversation_fork_turn_completions":
		predicate = "EXISTS (SELECT 1 FROM conversation_fork_turns t JOIN conversation_forks f ON f.fork_id = t.fork_id WHERE t.fork_turn_id = target.fork_turn_id AND f.source_run_id IN %[1]s)"
	case "run_fork_delivery_event_replays":
		predicate = "target.fork_run_id IN %[1]s OR target.source_run_id IN %[1]s OR EXISTS (SELECT 1 FROM events e WHERE e.event_id IN (target.source_event_id, target.fork_event_id) AND e.run_id IN %[1]s) OR EXISTS (SELECT 1 FROM event_deliveries d LEFT JOIN events e ON e.event_id = d.event_id WHERE d.delivery_id IN (target.source_delivery_id, target.fork_delivery_id) AND (d.run_id IN %[1]s OR e.run_id IN %[1]s))"
	case "run_fork_selected_contract_executions":
		predicate = "target.fork_run_id IN %[1]s OR target.source_run_id IN %[1]s OR EXISTS (SELECT 1 FROM events e WHERE e.event_id IN (target.source_event_id, target.fork_event_id) AND e.run_id IN %[1]s)"
	case "run_fork_selected_contract_branch_divergences", "run_fork_selected_contract_route_recoveries", "run_fork_selected_contract_bindings":
		predicate = "target.fork_run_id IN %[1]s OR target.source_run_id IN %[1]s OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = target.fork_event_id AND e.run_id IN %[1]s)"
	case "timers":
		predicate = "target.run_id IN %[1]s OR target.forked_from_run_id IN %[1]s OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = target.forked_from_event_id AND e.run_id IN %[1]s)"
	default:
		return destructiveResetCleanupStatements{}, fmt.Errorf("destructive reset cleanup table %s is not implemented", table)
	}
	from := quoteIdent(table) + " AS target WHERE " + fmt.Sprintf(predicate, runSet)
	out := destructiveResetCleanupStatements{count: "SELECT COUNT(*) FROM " + from, args: []any{runArg}}
	if table != "agent_directive_operations" {
		out.delete = "DELETE FROM " + from
	}
	return out, nil
}

// Only run-set binding differs. The closed cleanup catalog and predicates above
// are shared; backend syntax cannot choose a different destructive scope.
func destructiveResetRunSet(runIDs []string, sqlite bool) (string, any) {
	if sqlite {
		raw, _ := json.Marshal(runIDs)
		return "(SELECT value FROM json_each($1))", string(raw)
	}
	return "(SELECT unnest($1::uuid[]))", pq.Array(runIDs)
}

func destructiveResetCleanupPreservesTable(table string, includeSourceArtifacts bool) bool {
	entry, ok := destructivereset.CleanupCatalogByTableForPolicy(destructivereset.CleanupPolicy{IncludeSourceArtifacts: includeSourceArtifacts})[strings.TrimSpace(table)]
	if !ok {
		return false
	}
	switch entry.Classification {
	case destructivereset.CleanupPreserve, destructivereset.CleanupSplitPreserve:
		return true
	default:
		return false
	}
}
