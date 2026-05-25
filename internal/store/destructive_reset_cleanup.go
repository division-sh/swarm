package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"swarm/internal/runtime/destructivereset"
)

type destructiveResetCleanupExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	if s == nil || s.DB == nil {
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
		OperationName: strings.TrimSpace(req.Result.OperationName),
		DryRun:        req.Result.DryRun,
		AppliedAt:     now,
	}
	if out.OperationName == "" {
		out.OperationName = destructivereset.DefaultOperationName
	}
	if req.Result.DryRun {
		out.RunIDs = runIDs
		rows, err := destructiveResetCleanupTableResults(ctx, s.DB, runIDs)
		if err != nil {
			return destructivereset.CleanupResult{}, err
		}
		out.Tables = rows
		return out, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return destructivereset.CleanupResult{}, fmt.Errorf("begin destructive reset cleanup tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockDestructiveResetCleanupRuns(ctx, tx, runIDs); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	if err := destructiveResetCleanupSeverPreservedReferences(ctx, tx, runIDs); err != nil {
		return destructivereset.CleanupResult{}, err
	}
	out.RunIDs = runIDs
	rows, err := destructiveResetCleanupTableResults(ctx, tx, runIDs)
	if err != nil {
		return destructivereset.CleanupResult{}, err
	}
	for i := range rows {
		if rows[i].TableKind == destructivereset.CleanupTableKindGenerated {
			continue
		}
		deleted, err := destructiveResetCleanupDeleteTable(ctx, tx, rows[i].Table, runIDs)
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

func lockDestructiveResetCleanupRuns(ctx context.Context, exec destructiveResetCleanupExecutor, runIDs []string) error {
	if len(runIDs) == 0 {
		return nil
	}
	rows, err := exec.QueryContext(ctx, `
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

func destructiveResetCleanupSeverPreservedReferences(ctx context.Context, exec destructiveResetCleanupExecutor, runIDs []string) error {
	if len(runIDs) == 0 {
		return nil
	}
	statements := []struct {
		name  string
		query string
	}{
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
			name: "runs.fork_lineage",
			query: `
				UPDATE runs preserved
				SET forked_from_run_id = NULL,
				    forked_from_event_id = NULL
				WHERE NOT (preserved.run_id = ANY($1::uuid[]))
				  AND (
					preserved.forked_from_run_id = ANY($1::uuid[])
					OR EXISTS (
						SELECT 1
						FROM events cleanup_event
						WHERE cleanup_event.event_id = preserved.forked_from_event_id
						  AND cleanup_event.run_id = ANY($1::uuid[])
					)
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
	}
	for _, stmt := range statements {
		if _, err := exec.ExecContext(ctx, stmt.query, pq.Array(runIDs)); err != nil {
			return fmt.Errorf("sever destructive reset preserved reference %s: %w", stmt.name, err)
		}
	}
	return nil
}

func destructiveResetCleanupTableResults(ctx context.Context, exec destructiveResetCleanupExecutor, runIDs []string) ([]destructivereset.CleanupTableResult, error) {
	catalog := destructivereset.DefaultCleanupCatalog()
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
		count, err := destructiveResetCleanupCountTable(ctx, exec, entry, runIDs)
		if err != nil {
			return nil, err
		}
		switch entry.Classification {
		case destructivereset.CleanupPreserve, destructivereset.CleanupSplitPreserve:
			result.PreservedRows = count
		default:
			result.MatchedRows = count
		}
		out = append(out, result)
	}
	return out, nil
}

func destructiveResetCleanupCountTable(ctx context.Context, exec destructiveResetCleanupExecutor, entry destructivereset.CleanupCatalogEntry, runIDs []string) (int64, error) {
	query, args, err := destructiveResetCleanupQuery(entry.Table, "count", runIDs)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := exec.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count destructive reset cleanup table %s: %w", entry.Table, err)
	}
	return count, nil
}

func destructiveResetCleanupDeleteTable(ctx context.Context, exec destructiveResetCleanupExecutor, table string, runIDs []string) (int64, error) {
	query, args, err := destructiveResetCleanupQuery(table, "delete", runIDs)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(query) == "" {
		return 0, nil
	}
	res, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete destructive reset cleanup table %s: %w", table, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read destructive reset cleanup affected rows for %s: %w", table, err)
	}
	return rows, nil
}

func destructiveResetCleanupQuery(table, mode string, runIDs []string) (string, []any, error) {
	table = strings.TrimSpace(table)
	mode = strings.TrimSpace(mode)
	if mode != "count" && mode != "delete" {
		return "", nil, fmt.Errorf("unsupported destructive reset cleanup query mode %q", mode)
	}
	if len(runIDs) == 0 && mode == "delete" {
		return "", nil, nil
	}
	if destructiveResetCleanupPreservesTable(table) {
		if mode == "delete" {
			return "", nil, nil
		}
		return fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table)), nil, nil
	}
	if len(runIDs) == 0 {
		return `SELECT 0`, nil, nil
	}
	args := []any{pq.Array(runIDs)}
	switch table {
	case "event_receipts":
		if mode == "count" {
			return `SELECT COUNT(*) FROM event_receipts r WHERE EXISTS (SELECT 1 FROM events e WHERE e.event_id = r.event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM event_receipts r USING events e WHERE r.event_id = e.event_id AND e.run_id = ANY($1::uuid[])`, args, nil
	case "dead_letters":
		if mode == "count" {
			return `SELECT COUNT(*) FROM dead_letters d WHERE EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.original_event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM dead_letters d USING events e WHERE d.original_event_id = e.event_id AND e.run_id = ANY($1::uuid[])`, args, nil
	case "event_deliveries":
		if mode == "count" {
			return `SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM event_deliveries d WHERE d.run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = d.event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
	case "agent_turns", "agent_conversation_audits", "agent_sessions", "entity_mutations", "entity_state", "run_control_state", "events", "runs":
		if mode == "count" {
			return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE run_id = ANY($1::uuid[])`, quoteIdent(table)), args, nil
		}
		return fmt.Sprintf(`DELETE FROM %s WHERE run_id = ANY($1::uuid[])`, quoteIdent(table)), args, nil
	case "conversation_forks":
		if mode == "count" {
			return `SELECT COUNT(*) FROM conversation_forks WHERE source_run_id = ANY($1::uuid[])`, args, nil
		}
		return `DELETE FROM conversation_forks WHERE source_run_id = ANY($1::uuid[])`, args, nil
	case "conversation_fork_snapshots":
		if mode == "count" {
			return `SELECT COUNT(*) FROM conversation_fork_snapshots s WHERE EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id = s.fork_id AND f.source_run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM conversation_fork_snapshots s USING conversation_forks f WHERE s.fork_id = f.fork_id AND f.source_run_id = ANY($1::uuid[])`, args, nil
	case "conversation_fork_turns":
		if mode == "count" {
			return `SELECT COUNT(*) FROM conversation_fork_turns t WHERE EXISTS (SELECT 1 FROM conversation_forks f WHERE f.fork_id = t.fork_id AND f.source_run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM conversation_fork_turns t USING conversation_forks f WHERE t.fork_id = f.fork_id AND f.source_run_id = ANY($1::uuid[])`, args, nil
	case "run_fork_delivery_event_replays":
		if mode == "count" {
			return `
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
			`, args, nil
		}
		return `
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
		`, args, nil
	case "run_fork_selected_contract_executions":
		if mode == "count" {
			return `
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
			`, args, nil
		}
		return `
			DELETE FROM run_fork_selected_contract_executions r
			WHERE r.fork_run_id = ANY($1::uuid[])
			   OR r.source_run_id = ANY($1::uuid[])
			   OR EXISTS (
					SELECT 1
					FROM events e
					WHERE e.event_id IN (r.source_event_id, r.fork_event_id)
					  AND e.run_id = ANY($1::uuid[])
			   )
		`, args, nil
	case "run_fork_selected_contract_branch_divergences", "run_fork_selected_contract_route_recoveries", "run_fork_selected_contract_bindings":
		if mode == "count" {
			return fmt.Sprintf(`
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
			`, quoteIdent(table)), args, nil
		}
		return fmt.Sprintf(`
			DELETE FROM %s r
			WHERE r.fork_run_id = ANY($1::uuid[])
			   OR r.source_run_id = ANY($1::uuid[])
			   OR EXISTS (
					SELECT 1
					FROM events e
					WHERE e.event_id = r.fork_event_id
					  AND e.run_id = ANY($1::uuid[])
			   )
		`, quoteIdent(table)), args, nil
	case "timers":
		if mode == "count" {
			return `SELECT COUNT(*) FROM timers t WHERE t.run_id = ANY($1::uuid[]) OR t.forked_from_run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = t.forked_from_event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
		}
		return `DELETE FROM timers t WHERE t.run_id = ANY($1::uuid[]) OR t.forked_from_run_id = ANY($1::uuid[]) OR EXISTS (SELECT 1 FROM events e WHERE e.event_id = t.forked_from_event_id AND e.run_id = ANY($1::uuid[]))`, args, nil
	default:
		return "", nil, fmt.Errorf("destructive reset cleanup table %s is not implemented", table)
	}
}

func destructiveResetCleanupPreservesTable(table string) bool {
	entry, ok := destructivereset.CleanupCatalogByTable()[strings.TrimSpace(table)]
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
