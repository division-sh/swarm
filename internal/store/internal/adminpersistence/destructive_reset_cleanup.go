package adminpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/lib/pq"
)

func (s *DestructiveResetPostgresOwner) ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	if s == nil || s.backend == nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("postgres store is required")
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
		OperationName:  strings.TrimSpace(req.Result.OperationName),
		DryRun:         req.Result.DryRun,
		IncludeBundles: req.Result.IncludeBundles,
		AppliedAt:      now,
	}
	if out.OperationName == "" {
		out.OperationName = destructivereset.DefaultOperationName
	}
	txOptions := (*sql.TxOptions)(nil)
	if req.Result.DryRun {
		txOptions = &sql.TxOptions{ReadOnly: true}
	}
	tx, err := s.backend.BeginTx(ctx, txOptions)
	if err != nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("begin destructive reset cleanup tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if req.Result.DryRun {
		out.RunIDs = runIDs
		rows, err := destructiveResetCleanupTableResults(ctx, tx, runIDs, req.Result.IncludeBundles)
		if err != nil {
			return destructivereset.CleanupResult{}, err
		}
		out.Tables = rows
		if err := tx.Commit(); err != nil {
			return destructivereset.CleanupResult{}, fmt.Errorf("commit destructive reset cleanup dry-run tx: %w", err)
		}
		return out, nil
	}

	if err := lockDestructiveResetCleanupRuns(ctx, tx, runIDs); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := GuardSourceForkDependencies(ctx, tx, runIDs); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := guardDestructiveResetDirectiveAuthority(ctx, tx, runIDs, now); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if req.Result.IncludeBundles {
		if err := prepareDestructiveResetBundleCatalogDelete(ctx, tx, runIDs); err != nil {
			return destructivereset.CleanupResult{}, err
		}
	}
	if err := destructiveResetCleanupSeverPreservedReferences(ctx, tx, runIDs); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	out.RunIDs = runIDs
	rows, err := destructiveResetCleanupTableResults(ctx, tx, runIDs, req.Result.IncludeBundles)
	if err != nil {
		return destructivereset.CleanupResult{}, err
	}
	for i := range rows {
		if rows[i].TableKind == destructivereset.CleanupTableKindGenerated {
			continue
		}
		deleted, err := destructiveResetCleanupDeleteTable(ctx, tx, rows[i].Table, runIDs, req.Result.IncludeBundles)
		if err != nil {
			return destructivereset.CleanupResult{}, err
		}
		rows[i].DeletedRows = deleted
	}
	out.Tables = rows
	if err := tx.Commit(); err != nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("commit destructive reset cleanup tx: %w", err)
	}
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
	if req.Result.Plan.IncludeBundles != req.Result.IncludeBundles {
		return nil, fmt.Errorf("%w: destructive reset include_bundles result and plan mismatch", destructivereset.ErrInvalidRequest)
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

func lockDestructiveResetCleanupRuns(ctx context.Context, tx *sql.Tx, runIDs []string) error {
	if len(runIDs) == 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text
		FROM runs
		WHERE run_id = ANY($1::uuid[])
		ORDER BY run_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
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
	if len(runIDs) == 0 {
		return nil
	}
	var forkRunID, sourceRunID string
	err := tx.QueryRowContext(ctx, `
		SELECT fork.run_id::text, fork.forked_from_run_id::text
		FROM runs fork
		WHERE fork.forked_from_run_id = ANY($1::uuid[])
		  AND NOT (fork.run_id = ANY($1::uuid[]))
		ORDER BY fork.run_id
		LIMIT 1
	`, pq.Array(runIDs)).Scan(&forkRunID, &sourceRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect destructive reset fork dependencies: %w", err)
	}
	return fmt.Errorf("%w: cannot delete source run %s while dependent fork %s remains outside the cleanup set", destructivereset.ErrInvalidRequest, sourceRunID, forkRunID)
}

func guardDestructiveResetDirectiveAuthority(ctx context.Context, tx *sql.Tx, runIDs []string, now time.Time) error {
	if len(runIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM agent_directive_operations
		WHERE resolved_run_id = ANY($1::uuid[])
		  AND state IN ('succeeded', 'failed')
		  AND expires_at <= $2
	`, pq.Array(runIDs), now.UTC()); err != nil {
		return fmt.Errorf("expire terminal directive authority before destructive reset: %w", err)
	}
	var operationID, state string
	var expiresAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT operation_id::text, state, expires_at
		FROM agent_directive_operations
		WHERE resolved_run_id = ANY($1::uuid[])
		ORDER BY created_at, operation_id
		LIMIT 1
	`, pq.Array(runIDs)).Scan(&operationID, &state, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect directive authority before destructive reset: %w", err)
	}
	detail := fmt.Sprintf("operation_id=%s state=%s", operationID, state)
	if expiresAt.Valid {
		detail += " expires_at=" + expiresAt.Time.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Errorf("%w: runtime.nuke cannot delete retained agent directive authority (%s)", destructivereset.ErrInvalidRequest, detail)
}

func prepareDestructiveResetBundleCatalogDelete(ctx context.Context, tx *sql.Tx, runIDs []string) error {
	if err := lockBundleDeleteRunCreationTx(ctx, tx); err != nil {
		return fmt.Errorf("lock runtime.nuke bundle catalog cleanup: %w", err)
	}
	var outOfPlan int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE bundle_source = $2
		  AND NULLIF(bundle_hash, '') IS NOT NULL
		  AND NOT (run_id = ANY($1::uuid[]))
	`, pq.Array(runIDs), runtimerunlifecycle.BundleSourcePersisted).Scan(&outOfPlan); err != nil {
		return fmt.Errorf("validate runtime.nuke bundle catalog cleanup run snapshot: %w", err)
	}
	if outOfPlan > 0 {
		return fmt.Errorf("%w: runtime.nuke include_bundles cannot delete bundle catalog with persisted bundle-source runs outside the cleanup plan", destructivereset.ErrInvalidRequest)
	}
	return nil
}

func destructiveResetCleanupSeverPreservedReferences(ctx context.Context, tx *sql.Tx, runIDs []string) error {
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
				WHERE run_id = ANY($1::uuid[])
			`,
		},
		{
			name: "agent_sessions.successor_session_id",
			query: `
				UPDATE agent_sessions preserved
				SET successor_session_id = NULL
				WHERE preserved.successor_session_id IS NOT NULL
				  AND (preserved.run_id IS NULL OR NOT (preserved.run_id = ANY($1::uuid[])))
				  AND EXISTS (
					SELECT 1
					FROM agent_sessions cleanup
					WHERE cleanup.session_id = preserved.successor_session_id
					  AND cleanup.run_id = ANY($1::uuid[])
				  )
			`,
		},
		{
			name: "runtime_ingress_state.transition_event_id",
			query: `
				UPDATE runtime_ingress_state preserved
				SET transition_event_id = NULL
				WHERE preserved.transition_event_id IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM events cleanup_event
					WHERE cleanup_event.event_id = preserved.transition_event_id
					  AND cleanup_event.run_id = ANY($1::uuid[])
				  )
			`,
		},
		{
			name: "entity_mutations.caused_by_event",
			query: `
				UPDATE entity_mutations preserved
				SET caused_by_event = NULL
				WHERE NOT (preserved.run_id = ANY($1::uuid[]))
				  AND preserved.caused_by_event IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM events cleanup_event
					WHERE cleanup_event.event_id = preserved.caused_by_event
					  AND cleanup_event.run_id = ANY($1::uuid[])
				  )
			`,
		},
		{
			name: "timers.source_timer_id",
			query: `
				WITH cleanup_timers AS (
					SELECT cleanup.timer_id
					FROM timers cleanup
					WHERE cleanup.run_id = ANY($1::uuid[])
					   OR cleanup.forked_from_run_id = ANY($1::uuid[])
					   OR EXISTS (
							SELECT 1
							FROM events cleanup_event
							WHERE cleanup_event.event_id = cleanup.forked_from_event_id
							  AND cleanup_event.run_id = ANY($1::uuid[])
					   )
				)
				UPDATE timers preserved
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
				UPDATE mailbox preserved
				SET reply_context_id = NULL
				WHERE preserved.reply_context_id IS NOT NULL
				  AND EXISTS (
					SELECT 1
					FROM reply_contexts cleanup
					WHERE cleanup.reply_context_id = preserved.reply_context_id
					  AND cleanup.run_id = ANY($1::uuid[])
				  )
			`,
		},
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt.query, pq.Array(runIDs)); err != nil {
			return fmt.Errorf("sever destructive reset preserved reference %s: %w", stmt.name, err)
		}
	}
	return nil
}

func destructiveResetCleanupTableResults(ctx context.Context, tx *sql.Tx, runIDs []string, includeBundles bool) ([]destructivereset.CleanupTableResult, error) {
	catalog := destructivereset.CleanupCatalogForPolicy(destructivereset.CleanupPolicy{IncludeBundles: includeBundles})
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
		count, err := destructiveResetCleanupCountTable(ctx, tx, entry, runIDs, includeBundles)
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

func destructiveResetCleanupCountTable(ctx context.Context, tx *sql.Tx, entry destructivereset.CleanupCatalogEntry, runIDs []string, includeBundles bool) (int64, error) {
	statements, err := destructiveResetCleanupStatementsForTable(entry.Table, runIDs, includeBundles)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, statements.count, statements.args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count destructive reset cleanup table %s: %w", entry.Table, err)
	}
	return count, nil
}

func destructiveResetCleanupDeleteTable(ctx context.Context, tx *sql.Tx, table string, runIDs []string, includeBundles bool) (int64, error) {
	statements, err := destructiveResetCleanupStatementsForTable(table, runIDs, includeBundles)
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

func destructiveResetCleanupStatementsForTable(table string, runIDs []string, includeBundles bool) (destructiveResetCleanupStatements, error) {
	table = strings.TrimSpace(table)
	if destructiveResetCleanupPreservesTable(table, includeBundles) {
		return destructiveResetCleanupStatements{count: fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table))}, nil
	}
	if table == "bundles" && includeBundles {
		return destructiveResetCleanupStatements{count: `SELECT COUNT(*) FROM bundles`, delete: `DELETE FROM bundles`}, nil
	}
	switch table {
	case "standing_service_journal", "standing_service_generations", "standing_services":
		return destructiveResetCleanupStatements{
			count:  fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table)),
			delete: fmt.Sprintf(`DELETE FROM %s`, quoteIdent(table)),
		}, nil
	}
	if len(runIDs) == 0 {
		return destructiveResetCleanupStatements{count: `SELECT 0`}, nil
	}
	args := []any{pq.Array(runIDs)}
	statements := destructiveResetCleanupStatements{args: args}
	switch table {
	case "inbound_publication_events":
		statements.count = `SELECT COUNT(*) FROM inbound_publication_events c WHERE EXISTS (SELECT 1 FROM inbound_publications p WHERE p.publication_id = c.publication_id AND p.resolved_run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM inbound_publication_events c USING inbound_publications p WHERE c.publication_id = p.publication_id AND p.resolved_run_id = ANY($1::uuid[])`
	case "inbound_publications":
		statements.count = `SELECT COUNT(*) FROM inbound_publications WHERE resolved_run_id = ANY($1::uuid[])`
		statements.delete = `DELETE FROM inbound_publications WHERE resolved_run_id = ANY($1::uuid[])`
	case "event_receipts":
		statements.count = `SELECT COUNT(*) FROM event_receipts r WHERE EXISTS (SELECT 1 FROM events e WHERE e.event_id = r.event_id AND e.run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM event_receipts r USING events e WHERE r.event_id = e.event_id AND e.run_id = ANY($1::uuid[])`
	case "event_delivery_attempts", "event_delivery_outcomes":
		statements.count = fmt.Sprintf(`SELECT COUNT(*) FROM %s child WHERE EXISTS (SELECT 1 FROM event_deliveries d LEFT JOIN events e ON e.event_id = d.event_id WHERE d.delivery_id = child.delivery_id AND (d.run_id = ANY($1::uuid[]) OR e.run_id = ANY($1::uuid[])))`, quoteIdent(table))
		statements.delete = fmt.Sprintf(`DELETE FROM %s child WHERE EXISTS (SELECT 1 FROM event_deliveries d LEFT JOIN events e ON e.event_id = d.event_id WHERE d.delivery_id = child.delivery_id AND (d.run_id = ANY($1::uuid[]) OR e.run_id = ANY($1::uuid[])))`, quoteIdent(table))
	case "dead_letters":
		statements.count = `SELECT COUNT(*) FROM dead_letters d WHERE EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.original_event_id AND e.run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM dead_letters d USING events e WHERE d.original_event_id = e.event_id AND e.run_id = ANY($1::uuid[])`
	case "event_deliveries":
		statements.count = `SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.event_id AND e.run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM event_deliveries d WHERE d.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.event_id AND e.run_id = ANY($1::uuid[]))`
	case "committed_replay_scopes":
		statements.count = `SELECT COUNT(*) FROM committed_replay_scopes s WHERE s.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = s.event_id AND e.run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM committed_replay_scopes s WHERE s.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = s.event_id AND e.run_id = ANY($1::uuid[]))`
	case "author_activity_occurrences", "run_fork_fact_revisions", "run_fork_revisions", "run_fork_revision_heads", "activity_attempts", "agent_turns", "agent_conversation_audits", "agent_sessions", "decision_card_route_obligations", "decision_card_changes", "decision_card_input_drafts", "proposed_effect_continuations", "human_task_continuations", "decision_cards", "entity_mutations", "entity_state", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness", "run_control_state", "reply_contexts", "run_scenario_execution_profiles", "events", "runs":
		statements.count = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE run_id = ANY($1::uuid[])`, quoteIdent(table))
		statements.delete = fmt.Sprintf(`DELETE FROM %s WHERE run_id = ANY($1::uuid[])`, quoteIdent(table))
	case "agent_directive_operations":
		statements.count = `SELECT COUNT(*) FROM agent_directive_operations WHERE resolved_run_id = ANY($1::uuid[])`
	case "conversation_forks":
		statements.count = `SELECT COUNT(*) FROM conversation_forks WHERE source_run_id = ANY($1::uuid[])`
		statements.delete = `DELETE FROM conversation_forks WHERE source_run_id = ANY($1::uuid[])`
	case "conversation_fork_snapshots":
		statements.count = `SELECT COUNT(*) FROM conversation_fork_snapshots s WHERE EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id = s.fork_id AND f.source_run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM conversation_fork_snapshots s USING conversation_forks f WHERE s.fork_id = f.fork_id AND f.source_run_id = ANY($1::uuid[])`
	case "conversation_fork_turns":
		statements.count = `SELECT COUNT(*) FROM conversation_fork_turns t WHERE EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id = t.fork_id AND f.source_run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM conversation_fork_turns t USING conversation_forks f WHERE t.fork_id = f.fork_id AND f.source_run_id = ANY($1::uuid[])`
	case "conversation_fork_turn_completions":
		statements.count = `SELECT COUNT(*) FROM conversation_fork_turn_completions c WHERE EXISTS (SELECT 1 FROM conversation_fork_turns t JOIN conversation_forks f ON f.fork_id = t.fork_id WHERE t.fork_turn_id = c.fork_turn_id AND f.source_run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM conversation_fork_turn_completions c USING conversation_fork_turns t, conversation_forks f WHERE c.fork_turn_id = t.fork_turn_id AND t.fork_id = f.fork_id AND f.source_run_id = ANY($1::uuid[])`
	case "run_fork_delivery_event_replays":
		statements.count = `
				SELECT COUNT(*)
				FROM run_fork_delivery_event_replays r
				WHERE r.fork_run_id = ANY($1::uuid[])
				   OR r.source_run_id = ANY($1::uuid[])
				   OR EXISTS (
						SELECT 1
						FROM events e
						WHERE e.event_id IN (r.source_event_id, r.fork_event_id)
						  AND e.run_id = ANY($1::uuid[])
				   )
				   OR EXISTS (
						SELECT 1
						FROM event_deliveries d
						LEFT JOIN events e ON e.event_id = d.event_id
						WHERE d.delivery_id IN (r.source_delivery_id, r.fork_delivery_id)
						  AND (d.run_id = ANY($1::uuid[]) OR e.run_id = ANY($1::uuid[]))
				   )
			`
		statements.delete = `
			DELETE FROM run_fork_delivery_event_replays r
			WHERE r.fork_run_id = ANY($1::uuid[])
			   OR r.source_run_id = ANY($1::uuid[])
			   OR EXISTS (
					SELECT 1
					FROM events e
					WHERE e.event_id IN (r.source_event_id, r.fork_event_id)
					  AND e.run_id = ANY($1::uuid[])
			   )
			   OR EXISTS (
					SELECT 1
					FROM event_deliveries d
					LEFT JOIN events e ON e.event_id = d.event_id
					WHERE d.delivery_id IN (r.source_delivery_id, r.fork_delivery_id)
					  AND (d.run_id = ANY($1::uuid[]) OR e.run_id = ANY($1::uuid[]))
			   )
		`
	case "run_fork_selected_contract_executions":
		statements.count = `
				SELECT COUNT(*)
				FROM run_fork_selected_contract_executions r
				WHERE r.fork_run_id = ANY($1::uuid[])
				   OR r.source_run_id = ANY($1::uuid[])
				   OR EXISTS (
						SELECT 1
						FROM events e
						WHERE e.event_id IN (r.source_event_id, r.fork_event_id)
						  AND e.run_id = ANY($1::uuid[])
				   )
			`
		statements.delete = `
			DELETE FROM run_fork_selected_contract_executions r
			WHERE r.fork_run_id = ANY($1::uuid[])
			   OR r.source_run_id = ANY($1::uuid[])
			   OR EXISTS (
					SELECT 1
					FROM events e
					WHERE e.event_id IN (r.source_event_id, r.fork_event_id)
					  AND e.run_id = ANY($1::uuid[])
			   )
		`
	case "run_fork_selected_contract_branch_divergences", "run_fork_selected_contract_route_recoveries", "run_fork_selected_contract_bindings":
		statements.count = fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s r
				WHERE r.fork_run_id = ANY($1::uuid[])
				   OR r.source_run_id = ANY($1::uuid[])
				   OR EXISTS (
						SELECT 1
						FROM events e
						WHERE e.event_id = r.fork_event_id
						  AND e.run_id = ANY($1::uuid[])
				   )
			`, quoteIdent(table))
		statements.delete = fmt.Sprintf(`
			DELETE FROM %s r
			WHERE r.fork_run_id = ANY($1::uuid[])
			   OR r.source_run_id = ANY($1::uuid[])
			   OR EXISTS (
					SELECT 1
					FROM events e
					WHERE e.event_id = r.fork_event_id
					  AND e.run_id = ANY($1::uuid[])
			   )
		`, quoteIdent(table))
	case "timers":
		statements.count = `SELECT COUNT(*) FROM timers t WHERE t.run_id = ANY($1::uuid[]) OR t.forked_from_run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = t.forked_from_event_id AND e.run_id = ANY($1::uuid[]))`
		statements.delete = `DELETE FROM timers t WHERE t.run_id = ANY($1::uuid[]) OR t.forked_from_run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = t.forked_from_event_id AND e.run_id = ANY($1::uuid[]))`
	default:
		return destructiveResetCleanupStatements{}, fmt.Errorf("destructive reset cleanup table %s is not implemented", table)
	}
	return statements, nil
}

func destructiveResetCleanupPreservesTable(table string, includeBundles bool) bool {
	entry, ok := destructivereset.CleanupCatalogByTableForPolicy(destructivereset.CleanupPolicy{IncludeBundles: includeBundles})[strings.TrimSpace(table)]
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
