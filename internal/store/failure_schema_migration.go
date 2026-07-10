package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
)

type failureMigrationCount struct {
	table  string
	legacy string
	class  runtimefailures.Class
	detail string
	count  int
}

func canonicalFailureJSON(class runtimefailures.Class, detail, component, operation string, attributes map[string]any) (string, error) {
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(class, detail, component, operation, attributes))
	if !ok {
		return "", fmt.Errorf("construct canonical failure %s/%s", class, detail)
	}
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func legacyFailure(reason, table, rowID string, attributes map[string]any) (runtimefailures.Envelope, bool, error) {
	reason = strings.TrimSpace(reason)
	if attributes == nil {
		attributes = map[string]any{}
	}
	attributes["legacy_table"] = table
	attributes["legacy_row_id"] = rowID
	attributes["legacy_reason"] = reason
	var class runtimefailures.Class
	var detail string
	switch reason {
	case "handler_error", "handler_failure":
		class, detail = runtimefailures.ClassInternalFailure, "legacy_handler_failure"
	case "retry_exhausted":
		class, detail = runtimefailures.ClassRetryExhausted, "legacy_retry_exhausted"
	case "chain_depth_exceeded":
		class, detail = runtimefailures.ClassChainDepthExceeded, "legacy_chain_depth_exceeded"
	case "target_resolution_failed":
		class, detail = runtimefailures.ClassTargetUnreachable, "legacy_target_resolution_failed"
	case "target_ambiguous", "route_plan_target_ambiguous", "route_plan_instance_conflict":
		class, detail = runtimefailures.ClassTargetAmbiguous, reason
	case "target_required_missing", "target_invalid_syntax", "target_unreachable_no_subscriber", "target_not_subscribed", "target_unreachable_terminated", "parent_route_incomplete", "target_unknown_flow", "target_sender_no_inbound_runtime", "target_sender_empty_source_runtime", "producer_target_common_path_forbidden", "producer_broadcast_common_path_forbidden", "route_plan_address_value_missing", "route_plan_target_unsupported", "route_plan_target_unresolved", "route_plan_instance_key_adapter_invalid", "route_plan_instance_resolution_invalid", "route_plan_lifecycle_unavailable", "parent_route_lookup_failed", "route_plan_preflight_failed":
		class, detail = runtimefailures.ClassTargetUnreachable, reason
	default:
		return runtimefailures.Envelope{}, false, nil
	}
	raw, err := canonicalFailureJSON(class, detail, "failure-migration", "normalize_legacy_row", attributes)
	if err != nil {
		return runtimefailures.Envelope{}, false, err
	}
	envelope, err := runtimefailures.UnmarshalEnvelope([]byte(raw))
	return envelope, err == nil, err
}

func legacyControlDeliveryReason(reason string) bool {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return false
	}
	for _, allowed := range activeRunQuiescenceTerminalReasonCodes() {
		if reason == allowed {
			return true
		}
	}
	switch reason {
	case "run_cancelled", "run_paused", "replay_suppressed", "delivery_abandoned":
		return true
	default:
		return false
	}
}

func reportFailureMigration(counts map[string]*failureMigrationCount) {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := counts[key]
		if item == nil || item.count == 0 {
			continue
		}
		log.Printf("normalized %d legacy runtime failures table=%s legacy=%s class=%s detail=%s", item.count, item.table, item.legacy, item.class, item.detail)
	}
}

func addFailureMigrationCount(counts map[string]*failureMigrationCount, table, legacy string, failure runtimefailures.Envelope) {
	key := table + "|" + legacy + "|" + string(failure.Class) + "|" + failure.Detail.Code
	item := counts[key]
	if item == nil {
		item = &failureMigrationCount{table: table, legacy: legacy, class: failure.Class, detail: failure.Detail.Code}
		counts[key] = item
	}
	item.count++
}

func postgresTableColumns(ctx context.Context, tx *sql.Tx, table string) (map[string]bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
	`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		out[column] = true
	}
	return out, rows.Err()
}

func ensurePostgresCanonicalFailureSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin canonical failure migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	counts := map[string]*failureMigrationCount{}
	for _, table := range []string{"event_deliveries", "event_receipts", "dead_letters", "activity_attempts", "agent_turns"} {
		columns, err := postgresTableColumns(ctx, tx, table)
		if err != nil {
			return fmt.Errorf("inspect %s failure columns: %w", table, err)
		}
		if len(columns) == 0 {
			continue
		}
		if !columns["failure"] {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN failure JSONB`); err != nil {
				return fmt.Errorf("add %s.failure: %w", table, err)
			}
		}
		switch table {
		case "event_deliveries":
			if err := migratePostgresDeliveries(ctx, tx, counts); err != nil {
				return fmt.Errorf("migrate event_deliveries failures: %w", err)
			}
			if columns["last_error"] {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE event_deliveries DROP COLUMN last_error`); err != nil {
					return fmt.Errorf("drop event_deliveries.last_error: %w", err)
				}
			}
		case "event_receipts":
			if err := migratePostgresReceipts(ctx, tx, counts); err != nil {
				return fmt.Errorf("migrate event_receipts failures: %w", err)
			}
		case "dead_letters":
			if err := migratePostgresDeadLetters(ctx, tx, columns, counts); err != nil {
				return fmt.Errorf("migrate dead_letters failures: %w", err)
			}
		case "activity_attempts":
			if err := migratePostgresActivityAttempts(ctx, tx, columns, counts); err != nil {
				return fmt.Errorf("migrate activity_attempts failures: %w", err)
			}
		case "agent_turns":
			if err := migratePostgresAgentTurns(ctx, tx, columns); err != nil {
				return fmt.Errorf("migrate agent_turns failures: %w", err)
			}
		}
	}
	if err := migratePostgresRunFailures(ctx, tx); err != nil {
		return fmt.Errorf("migrate runs terminal evidence: %w", err)
	}
	if err := migratePostgresFailureEventPayloads(ctx, tx, counts); err != nil {
		return fmt.Errorf("migrate failure-bearing event payloads: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit canonical failure migration: %w", err)
	}
	reportFailureMigration(counts)
	return nil
}

func migratePostgresDeliveries(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id::text, status, COALESCE(reason_code, '') FROM event_deliveries WHERE failure IS NULL AND status IN ('failed', 'dead_letter') FOR UPDATE`)
	if err != nil {
		return err
	}
	type row struct{ id, status, reason string }
	var pending []row
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.id, &item.status, &item.reason); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	_ = rows.Close()
	var unknown []string
	for _, item := range pending {
		if item.status == "dead_letter" && legacyControlDeliveryReason(item.reason) {
			continue
		}
		failure, ok, err := legacyFailure(item.reason, "event_deliveries", item.id, nil)
		if err != nil {
			return err
		}
		if !ok {
			unknown = append(unknown, item.id+":"+item.reason)
			continue
		}
		raw, _ := runtimefailures.MarshalEnvelope(failure)
		if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET failure = $2::jsonb WHERE delivery_id = $1::uuid`, item.id, raw); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "event_deliveries", item.reason, failure)
	}
	if len(unknown) > 0 {
		return fmt.Errorf("event_deliveries canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
	}
	return nil
}

func migratePostgresReceipts(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_receipts r
		SET failure = d.failure
		FROM event_deliveries d
		WHERE r.failure IS NULL
		  AND r.event_id = d.event_id
		  AND r.subscriber_type = d.subscriber_type
		  AND r.subscriber_id = d.subscriber_id
		  AND d.failure IS NOT NULL
	`); err != nil {
		return fmt.Errorf("migrate receipt failures from deliveries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE event_receipts SET side_effects = COALESCE(side_effects, '{}'::jsonb) - 'error' - 'failure_type'`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT receipt_id::text, COALESCE(reason_code, '') FROM event_receipts WHERE outcome = 'dead_letter' AND failure IS NULL`)
	if err != nil {
		return err
	}
	type receiptRow struct{ id, reason string }
	var pending []receiptRow
	for rows.Next() {
		var item receiptRow
		if err := rows.Scan(&item.id, &item.reason); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	_ = rows.Close()
	var unknown []string
	for _, item := range pending {
		failure, ok, err := legacyFailure(item.reason, "event_receipts", item.id, nil)
		if err != nil {
			return err
		}
		if !ok {
			unknown = append(unknown, item.id+":"+item.reason)
			continue
		}
		raw, _ := runtimefailures.MarshalEnvelope(failure)
		if _, err := tx.ExecContext(ctx, `UPDATE event_receipts SET failure = $2::jsonb WHERE receipt_id = $1::uuid`, item.id, raw); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "event_receipts", item.reason, failure)
	}
	if len(unknown) > 0 {
		return fmt.Errorf("event_receipts canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
	}
	return nil
}

func migratePostgresDeadLetters(ctx context.Context, tx *sql.Tx, columns map[string]bool, counts map[string]*failureMigrationCount) error {
	if columns["failure_type"] {
		query := `SELECT dead_letter_id::text, failure_type, COALESCE(target_failure_reason, ''), COALESCE(target_context, '{}'::jsonb) FROM dead_letters WHERE failure IS NULL FOR UPDATE`
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		type deadLetterRow struct {
			id, failureType, targetReason string
			targetContext                 []byte
		}
		var pending []deadLetterRow
		for rows.Next() {
			var item deadLetterRow
			if err := rows.Scan(&item.id, &item.failureType, &item.targetReason, &item.targetContext); err != nil {
				_ = rows.Close()
				return err
			}
			pending = append(pending, item)
		}
		_ = rows.Close()
		var unknown []string
		for _, item := range pending {
			reason := item.failureType
			attributes := map[string]any{}
			if item.failureType == "target_resolution_failed" {
				if item.targetReason == "" {
					unknown = append(unknown, item.id+":missing_target_failure_reason")
					continue
				}
				reason = item.targetReason
				var contextValue any
				if len(item.targetContext) > 0 && json.Unmarshal(item.targetContext, &contextValue) == nil {
					attributes["target_context"] = contextValue
				}
			}
			failure, ok, err := legacyFailure(reason, "dead_letters", item.id, attributes)
			if err != nil {
				return err
			}
			if !ok {
				unknown = append(unknown, item.id+":"+item.failureType+":"+item.targetReason)
				continue
			}
			raw, _ := runtimefailures.MarshalEnvelope(failure)
			if _, err := tx.ExecContext(ctx, `UPDATE dead_letters SET failure = $2::jsonb WHERE dead_letter_id = $1::uuid`, item.id, raw); err != nil {
				return err
			}
			addFailureMigrationCount(counts, "dead_letters", item.failureType+":"+item.targetReason, failure)
		}
		if len(unknown) > 0 {
			return fmt.Errorf("dead_letters canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
		}
	}
	var missing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE failure IS NULL`).Scan(&missing); err != nil {
		return err
	}
	if missing > 0 {
		return fmt.Errorf("dead_letters canonical failure migration blocked by %d rows without failure", missing)
	}
	if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS idx_dead_letters_type`); err != nil {
		return err
	}
	for _, column := range []string{"failure_type", "target_failure_reason", "target_context", "error_message"} {
		if columns[column] {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE dead_letters DROP COLUMN `+column+` CASCADE`); err != nil {
				return fmt.Errorf("drop dead_letters.%s: %w", column, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE dead_letters ALTER COLUMN failure SET NOT NULL`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_dead_letters_class ON dead_letters ((failure->>'class'))`)
	return err
}

func migratePostgresActivityAttempts(ctx context.Context, tx *sql.Tx, columns map[string]bool, counts map[string]*failureMigrationCount) error {
	rows, err := tx.QueryContext(ctx, `SELECT request_event_id::text, status FROM activity_attempts WHERE status IN ('failed', 'uncertain') AND failure IS NULL FOR UPDATE`)
	if err != nil {
		return err
	}
	type activityRow struct{ id, status string }
	var pending []activityRow
	for rows.Next() {
		var item activityRow
		if err := rows.Scan(&item.id, &item.status); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	_ = rows.Close()
	for _, item := range pending {
		class, detail := runtimefailures.ClassConnectorFailure, "legacy_activity_failed"
		if item.status == "uncertain" {
			class, detail = runtimefailures.ClassOutcomeUncertain, "legacy_activity_outcome_uncertain"
		}
		raw, err := canonicalFailureJSON(class, detail, "failure-migration", "normalize_activity_attempt", map[string]any{"request_event_id": item.id, "legacy_status": item.status})
		if err != nil {
			return err
		}
		failure, _ := runtimefailures.UnmarshalEnvelope([]byte(raw))
		if _, err := tx.ExecContext(ctx, `UPDATE activity_attempts SET failure = $2::jsonb WHERE request_event_id = $1::uuid`, item.id, raw); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "activity_attempts", item.status, failure)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE activity_attempts
		SET result_payload = (COALESCE(result_payload, '{}'::jsonb) - 'error' - 'failure') || jsonb_build_object('failure', failure)
		WHERE status IN ('failed', 'uncertain') AND failure IS NOT NULL AND result_payload IS NOT NULL
	`); err != nil {
		return fmt.Errorf("migrate activity result payload failures: %w", err)
	}
	eventColumns, err := postgresTableColumns(ctx, tx, "events")
	if err != nil {
		return err
	}
	if eventColumns["event_id"] && eventColumns["payload"] {
		if _, err := tx.ExecContext(ctx, `
			UPDATE events e
			SET payload = (COALESCE(e.payload, '{}'::jsonb) - 'error' - 'failure') || jsonb_build_object('failure', a.failure)
			FROM activity_attempts a
			WHERE a.result_event_id = e.event_id AND a.status IN ('failed', 'uncertain') AND a.failure IS NOT NULL
		`); err != nil {
			return fmt.Errorf("migrate activity result event failures: %w", err)
		}
	}
	if columns["error"] {
		if _, err := tx.ExecContext(ctx, `
			DO $$
			DECLARE item RECORD;
			BEGIN
				FOR item IN SELECT conname FROM pg_constraint WHERE conrelid = 'activity_attempts'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%error%' LOOP
					EXECUTE format('ALTER TABLE activity_attempts DROP CONSTRAINT %I', item.conname);
				END LOOP;
			END $$;
		`); err != nil {
			return fmt.Errorf("drop activity_attempts error constraints: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `ALTER TABLE activity_attempts DROP COLUMN error`); err != nil {
			return fmt.Errorf("drop activity_attempts.error: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE activity_attempts DROP CONSTRAINT IF EXISTS activity_attempts_failure_state_check`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		ALTER TABLE activity_attempts ADD CONSTRAINT activity_attempts_failure_state_check CHECK (
			(status = 'started' AND result_event_id IS NULL AND result_event_type IS NULL AND result_payload IS NULL AND failure IS NULL AND completed_at IS NULL)
			OR (status = 'succeeded' AND result_event_id IS NOT NULL AND result_event_type IS NOT NULL AND result_payload IS NOT NULL AND failure IS NULL AND completed_at IS NOT NULL)
			OR (status IN ('failed', 'uncertain') AND result_event_id IS NOT NULL AND result_event_type IS NOT NULL AND result_payload IS NOT NULL AND failure IS NOT NULL AND completed_at IS NOT NULL)
		)
	`); err != nil {
		return err
	}
	return nil
}

func migratePostgresAgentTurns(ctx context.Context, tx *sql.Tx, columns map[string]bool) error {
	if !columns["error"] {
		return nil
	}
	var legacyCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE NULLIF(BTRIM(error), '') IS NOT NULL`).Scan(&legacyCount); err != nil {
		return err
	}
	if legacyCount > 0 {
		return fmt.Errorf("canonical failure migration blocked by %d ambiguous legacy error rows", legacyCount)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns DROP COLUMN error`); err != nil {
		return fmt.Errorf("drop agent_turns.error: %w", err)
	}
	return nil
}

var failureBearingPlatformEventTypes = []string{
	"platform.agent_failed",
	"platform.agent_panic",
	"platform.auth_required",
	"platform.boot",
	"platform.dead_letter",
	"platform.event_quarantined",
	"platform.paused",
	"platform.recovery_failed",
	"platform.runtime_log",
}

type failureEventMigrationRow struct {
	id        string
	eventType string
	payload   []byte
}

func migratePostgresFailureEventPayloads(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	columns, err := postgresTableColumns(ctx, tx, "events")
	if err != nil {
		return err
	}
	if !columns["event_id"] || !columns["event_name"] || !columns["payload"] {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT event_id::text, event_name, payload::text
		FROM events
		WHERE event_name IN ('platform.agent_failed','platform.agent_panic','platform.auth_required','platform.boot','platform.dead_letter','platform.event_quarantined','platform.paused','platform.recovery_failed','platform.runtime_log')
		FOR UPDATE
	`)
	if err != nil {
		return err
	}
	pending, err := collectFailureEventMigrationRows(rows)
	if err != nil {
		return err
	}
	for _, row := range pending {
		payload, changed, failure, err := normalizeFailureEventPayload(row)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal migrated event %s payload: %w", row.id, err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE events SET payload = $2::jsonb WHERE event_id = $1::uuid`, row.id, string(raw)); err != nil {
			return err
		}
		if failure != nil {
			addFailureMigrationCount(counts, "events", row.eventType, *failure)
		}
	}
	return nil
}

func migrateSQLiteFailureEventPayloads(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	columns, err := sqliteColumnsTx(ctx, tx, "events")
	if err != nil {
		return err
	}
	if !columns["event_id"] || !columns["event_name"] || !columns["payload"] {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, event_name, payload
		FROM events
		WHERE event_name IN ('platform.agent_failed','platform.agent_panic','platform.auth_required','platform.boot','platform.dead_letter','platform.event_quarantined','platform.paused','platform.recovery_failed','platform.runtime_log')
	`)
	if err != nil {
		return err
	}
	pending, err := collectFailureEventMigrationRows(rows)
	if err != nil {
		return err
	}
	for _, row := range pending {
		payload, changed, failure, err := normalizeFailureEventPayload(row)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal migrated sqlite event %s payload: %w", row.id, err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE events SET payload = ? WHERE event_id = ?`, string(raw), row.id); err != nil {
			return err
		}
		if failure != nil {
			addFailureMigrationCount(counts, "events", row.eventType, *failure)
		}
	}
	return nil
}

func collectFailureEventMigrationRows(rows *sql.Rows) ([]failureEventMigrationRow, error) {
	defer rows.Close()
	var out []failureEventMigrationRow
	for rows.Next() {
		var row failureEventMigrationRow
		if err := rows.Scan(&row.id, &row.eventType, &row.payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func normalizeFailureEventPayload(row failureEventMigrationRow) (map[string]any, bool, *runtimefailures.Envelope, error) {
	var payload map[string]any
	if err := json.Unmarshal(row.payload, &payload); err != nil {
		return nil, false, nil, fmt.Errorf("failure event %s (%s) has invalid payload JSON: %w", row.id, row.eventType, err)
	}
	if payload == nil {
		return nil, false, nil, fmt.Errorf("failure event %s (%s) has null payload", row.id, row.eventType)
	}
	switch row.eventType {
	case "platform.agent_panic":
		return migrateLegacyEventFailure(row, payload, "failure", []string{"error"}, runtimefailures.ClassInternalFailure, "legacy_agent_panic", nil)
	case "platform.agent_failed":
		return migrateLegacyEventFailure(row, payload, "failure", []string{"error"}, runtimefailures.ClassInternalFailure, "legacy_agent_failed", nil)
	case "platform.recovery_failed":
		return migrateLegacyEventFailure(row, payload, "failure", []string{"error"}, runtimefailures.ClassDependencyUnavailable, "legacy_startup_recovery_failed", nil)
	case "platform.event_quarantined":
		if existing, ok, err := failureEnvelopeFromPayload(payload, "last_failure"); err != nil {
			return nil, false, nil, failureEventMigrationError(row, err)
		} else if ok {
			if hasPayloadKey(payload, "sample_error", "quarantine_reason") {
				return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("contains canonical last_failure and legacy quarantine fields"))
			}
			return payload, false, &existing, nil
		}
		if !hasPayloadKey(payload, "sample_error", "quarantine_reason") {
			return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("missing last_failure and stable legacy quarantine fields"))
		}
		failure, value, err := migrationFailureValue(runtimefailures.ClassInternalFailure, "legacy_agent_panic", row)
		if err != nil {
			return nil, false, nil, err
		}
		delete(payload, "sample_error")
		delete(payload, "quarantine_reason")
		payload["reason_code"] = "repeated_agent_panic"
		payload["last_failure"] = value
		return payload, true, &failure, nil
	case "platform.auth_required":
		if existing, ok, err := failureEnvelopeFromPayload(payload, "failure"); err != nil {
			return nil, false, nil, failureEventMigrationError(row, err)
		} else if ok {
			if _, legacy := payload["reason"]; legacy {
				return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("contains canonical failure and legacy reason"))
			}
			return payload, false, &existing, nil
		}
		reason, _ := payload["reason"].(string)
		if strings.TrimSpace(reason) != "claude_auth_required" {
			return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("unknown legacy auth reason %q", reason))
		}
		failure, value, err := migrationFailureValueWithAttributes(runtimefailures.ClassAuthenticationNeeded, "legacy_authentication_required", row, map[string]any{"auth_kind": "provider"})
		if err != nil {
			return nil, false, nil, err
		}
		delete(payload, "reason")
		payload["failure"] = value
		return payload, true, &failure, nil
	case "platform.dead_letter":
		return migrateLegacyDeadLetterEvent(row, payload)
	case "platform.paused":
		return migrateLegacyPausedEvent(row, payload)
	case "platform.boot":
		return migrateLegacyBootEvent(row, payload)
	case "platform.runtime_log":
		details, _ := payload["details"].(map[string]any)
		if details == nil {
			return payload, false, nil, nil
		}
		if _, legacy := details["error"]; !legacy {
			if existing, ok, err := failureEnvelopeFromPayload(details, "failure"); err != nil {
				return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("details.failure: %w", err))
			} else if ok {
				return payload, false, &existing, nil
			}
			return payload, false, nil, nil
		}
		delete(details, "error")
		payload["details"] = details
		return payload, true, nil, nil
	default:
		return payload, false, nil, nil
	}
}

func migrateLegacyEventFailure(row failureEventMigrationRow, payload map[string]any, key string, legacyKeys []string, class runtimefailures.Class, code string, attributes map[string]any) (map[string]any, bool, *runtimefailures.Envelope, error) {
	if existing, ok, err := failureEnvelopeFromPayload(payload, key); err != nil {
		return nil, false, nil, failureEventMigrationError(row, err)
	} else if ok {
		if hasPayloadKey(payload, legacyKeys...) {
			return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("contains canonical %s and legacy failure fields", key))
		}
		return payload, false, &existing, nil
	}
	if !hasPayloadKey(payload, legacyKeys...) {
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("missing %s and stable legacy failure fields", key))
	}
	failure, value, err := migrationFailureValueWithAttributes(class, code, row, attributes)
	if err != nil {
		return nil, false, nil, err
	}
	for _, legacyKey := range legacyKeys {
		delete(payload, legacyKey)
	}
	payload[key] = value
	return payload, true, &failure, nil
}

func migrateLegacyDeadLetterEvent(row failureEventMigrationRow, payload map[string]any) (map[string]any, bool, *runtimefailures.Envelope, error) {
	legacyKeys := []string{"failure_type", "target_failure_reason", "target_context", "error_message"}
	if existing, ok, err := failureEnvelopeFromPayload(payload, "failure"); err != nil {
		return nil, false, nil, failureEventMigrationError(row, err)
	} else if ok {
		if hasPayloadKey(payload, legacyKeys...) {
			return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("contains canonical failure and legacy dead-letter fields"))
		}
		return payload, false, &existing, nil
	}
	failureType, _ := payload["failure_type"].(string)
	failureType = strings.TrimSpace(failureType)
	if failureType == "" {
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("missing legacy failure_type"))
	}
	reason := failureType
	attributes := map[string]any{"legacy_event_id": row.id}
	if failureType == "target_resolution_failed" {
		if targetReason, _ := payload["target_failure_reason"].(string); strings.TrimSpace(targetReason) != "" {
			reason = strings.TrimSpace(targetReason)
		}
		if contextValue, ok := payload["target_context"]; ok {
			attributes["target_context"] = contextValue
		}
	}
	failure, ok, err := legacyFailure(reason, "events", row.id, attributes)
	if err != nil {
		return nil, false, nil, err
	}
	if !ok {
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("unknown legacy dead-letter failure %q", reason))
	}
	value, err := runtimefailures.EnvelopeValue(failure)
	if err != nil {
		return nil, false, nil, err
	}
	for _, key := range legacyKeys {
		delete(payload, key)
	}
	payload["failure"] = value
	return payload, true, &failure, nil
}

func migrateLegacyPausedEvent(row failureEventMigrationRow, payload map[string]any) (map[string]any, bool, *runtimefailures.Envelope, error) {
	if existing, ok, err := failureEnvelopeFromPayload(payload, "last_failure"); err != nil {
		return nil, false, nil, failureEventMigrationError(row, err)
	} else if ok {
		return payload, false, &existing, nil
	}
	reason, _ := payload["reason"].(string)
	switch strings.TrimSpace(reason) {
	case "claude_auth_required":
		failure, value, err := migrationFailureValueWithAttributes(runtimefailures.ClassAuthenticationNeeded, "legacy_authentication_required", row, map[string]any{"auth_kind": "provider"})
		if err != nil {
			return nil, false, nil, err
		}
		payload["reason"] = "authentication_intervention_required"
		payload["last_failure"] = value
		return payload, true, &failure, nil
	case "claude_credit_exhausted":
		failure, value, err := migrationFailureValue(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", row)
		if err != nil {
			return nil, false, nil, err
		}
		payload["reason"] = "provider_credit_intervention_required"
		payload["last_failure"] = value
		return payload, true, &failure, nil
	default:
		return payload, false, nil, nil
	}
}

func migrateLegacyBootEvent(row failureEventMigrationRow, payload map[string]any) (map[string]any, bool, *runtimefailures.Envelope, error) {
	decision, ok := payload["recovery_decision"].(map[string]any)
	if !ok {
		return payload, false, nil, nil
	}
	existing, hasFailure, err := failureEnvelopeFromPayload(decision, "failure")
	if err != nil {
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("recovery_decision: %w", err))
	}
	errorText, hasLegacy := decision["error_text"]
	if !hasLegacy {
		if hasFailure {
			return payload, false, &existing, nil
		}
		return payload, false, nil, nil
	}
	if hasFailure && strings.TrimSpace(fmt.Sprint(errorText)) != "" {
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("recovery_decision contains canonical failure and legacy error_text"))
	}
	delete(decision, "error_text")
	if strings.TrimSpace(fmt.Sprint(errorText)) == "" {
		payload["recovery_decision"] = decision
		return payload, true, nil, nil
	}
	reason, _ := decision["reason_code"].(string)
	class, code := runtimefailures.ClassDependencyUnavailable, "legacy_startup_recovery_failed"
	switch strings.TrimSpace(reason) {
	case "startup_recovery_inspection_failed":
		code = "startup_recovery_inspection_failed"
	case "schedule_restore_failed":
		code = "schedule_restore_failed"
	case "startup_recovery_failed":
		code = "startup_manager_recovery_failed"
	case "recovery_disabled_with_persisted_work":
		class, code = runtimefailures.ClassSchemaInvalid, "startup_recovery_disabled_with_work"
	default:
		return nil, false, nil, failureEventMigrationError(row, fmt.Errorf("unknown recovery reason %q for non-empty error_text", reason))
	}
	failure, value, err := migrationFailureValue(class, code, row)
	if err != nil {
		return nil, false, nil, err
	}
	decision["failure"] = value
	payload["recovery_decision"] = decision
	return payload, true, &failure, nil
}

func failureEnvelopeFromPayload(payload map[string]any, key string) (runtimefailures.Envelope, bool, error) {
	value, ok := payload[key]
	if !ok || value == nil {
		return runtimefailures.Envelope{}, false, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return runtimefailures.Envelope{}, false, err
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		return runtimefailures.Envelope{}, false, err
	}
	return failure, true, nil
}

func migrationFailureValue(class runtimefailures.Class, code string, row failureEventMigrationRow) (runtimefailures.Envelope, map[string]any, error) {
	return migrationFailureValueWithAttributes(class, code, row, nil)
}

func migrationFailureValueWithAttributes(class runtimefailures.Class, code string, row failureEventMigrationRow, attributes map[string]any) (runtimefailures.Envelope, map[string]any, error) {
	if attributes == nil {
		attributes = map[string]any{}
	}
	attributes["legacy_event_id"] = row.id
	attributes["legacy_event_type"] = row.eventType
	failure := runtimefailures.Normalize(runtimefailures.New(class, code, "failure-migration", "normalize_event_payload", attributes), "failure-migration", "normalize_event_payload")
	if failure.Class == runtimefailures.ClassInternalFailure && failure.Detail.Code == "invalid_failure_construction" {
		return runtimefailures.Envelope{}, nil, fmt.Errorf("construct migration failure for %s/%s", class, code)
	}
	value, err := runtimefailures.EnvelopeValue(failure)
	return failure, value, err
}

func hasPayloadKey(payload map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := payload[key]; ok {
			return true
		}
	}
	return false
}

func failureEventMigrationError(row failureEventMigrationRow, err error) error {
	return fmt.Errorf("failure event %s (%s) migration blocked: %w", row.id, row.eventType, err)
}

type legacyRunTerminalEvidence struct {
	runID         string
	status        string
	errorSummary  string
	failureRaw    []byte
	ended         bool
	controlStatus string
	controlReason string
}

func canonicalRunFailureEvidence(row legacyRunTerminalEvidence) (*runtimefailures.Envelope, error) {
	row.runID = strings.TrimSpace(row.runID)
	row.status = strings.ToLower(strings.TrimSpace(row.status))
	row.errorSummary = strings.TrimSpace(row.errorSummary)
	row.controlStatus = strings.ToLower(strings.TrimSpace(row.controlStatus))
	row.controlReason = strings.TrimSpace(row.controlReason)

	var failure *runtimefailures.Envelope
	trimmedFailure := bytes.TrimSpace(row.failureRaw)
	if len(trimmedFailure) > 0 && !bytes.Equal(trimmedFailure, []byte("null")) {
		decoded, err := runtimefailures.UnmarshalEnvelope(trimmedFailure)
		if err != nil {
			return nil, fmt.Errorf("invalid canonical failure: %w", err)
		}
		failure = &decoded
	}

	terminal := row.status == "completed" || row.status == "failed" || row.status == "cancelled" || row.status == "forked"
	if terminal != row.ended {
		return nil, fmt.Errorf("status %s has incompatible ended_at presence", row.status)
	}

	switch row.status {
	case "failed":
		if row.errorSummary != "" {
			legacy, err := runtimefailures.UnmarshalEnvelope([]byte(row.errorSummary))
			if err != nil {
				return nil, fmt.Errorf("failed prose is ambiguous and is not a platform.failure/v1 envelope")
			}
			if failure != nil && !failureEnvelopesEqual(*failure, legacy) {
				return nil, fmt.Errorf("failed legacy and canonical envelopes conflict")
			}
			failure = &legacy
		}
		if failure == nil {
			return nil, fmt.Errorf("failed status requires canonical failure evidence")
		}
	case "cancelled":
		if failure != nil {
			return nil, fmt.Errorf("cancelled status forbids failure")
		}
		if row.controlStatus != "stopped" || row.controlReason == "" {
			return nil, fmt.Errorf("cancelled status requires stopped run_control_state.reason")
		}
		if row.errorSummary != "" {
			if !preservationTerminalReason(row.errorSummary) {
				return nil, fmt.Errorf("cancelled legacy error_summary %q is not a closed preservation reason", row.errorSummary)
			}
			if row.controlReason != row.errorSummary {
				return nil, fmt.Errorf("cancelled legacy reason conflicts with run_control_state.reason")
			}
		}
	case "running", "paused", "completed", "forked":
		if failure != nil {
			return nil, fmt.Errorf("status %s forbids failure", row.status)
		}
		if row.errorSummary != "" {
			return nil, fmt.Errorf("status %s forbids legacy error_summary", row.status)
		}
	default:
		return nil, fmt.Errorf("unsupported run status %q", row.status)
	}
	return runtimefailures.CloneEnvelope(failure), nil
}

func failureEnvelopesEqual(left, right runtimefailures.Envelope) bool {
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func preservationTerminalReason(reason string) bool {
	reason = strings.TrimSpace(reason)
	for _, candidate := range preservationcleanup.TerminalReasonCodes() {
		if reason == candidate {
			return true
		}
	}
	return false
}

func migratePostgresRunFailures(ctx context.Context, tx *sql.Tx) error {
	columns, err := postgresTableColumns(ctx, tx, "runs")
	if err != nil || len(columns) == 0 {
		return err
	}
	if !columns["status"] {
		return nil
	}
	if !columns["failure"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN failure JSONB`); err != nil {
			return fmt.Errorf("add runs.failure: %w", err)
		}
		columns["failure"] = true
	}
	if !columns["ended_at"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN ended_at TIMESTAMPTZ`); err != nil {
			return fmt.Errorf("add runs.ended_at: %w", err)
		}
		columns["ended_at"] = true
	}
	errorExpr := "''"
	if columns["error_summary"] {
		errorExpr = "COALESCE(error_summary, '')"
	}
	rows, err := tx.QueryContext(ctx, `SELECT run_id::text, COALESCE(status, ''), `+errorExpr+`, failure, ended_at FROM runs ORDER BY run_id::text FOR UPDATE`)
	if err != nil {
		return err
	}
	type rowValue struct {
		runID, status, summary string
		failure                []byte
		ended                  sql.NullTime
	}
	var pending []rowValue
	for rows.Next() {
		var item rowValue
		if err := rows.Scan(&item.runID, &item.status, &item.summary, &item.failure, &item.ended); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	controlColumns, err := postgresTableColumns(ctx, tx, "run_control_state")
	if err != nil {
		return err
	}
	var blocked []string
	for _, item := range pending {
		evidence := legacyRunTerminalEvidence{runID: item.runID, status: item.status, errorSummary: item.summary, failureRaw: item.failure, ended: item.ended.Valid}
		if len(controlColumns) > 0 {
			_ = tx.QueryRowContext(ctx, `SELECT COALESCE(control_status, ''), COALESCE(reason, '') FROM run_control_state WHERE run_id = $1::uuid`, item.runID).Scan(&evidence.controlStatus, &evidence.controlReason)
		}
		failure, err := canonicalRunFailureEvidence(evidence)
		if err != nil {
			blocked = append(blocked, item.runID+"["+strings.TrimSpace(item.status)+"]:"+err.Error())
			continue
		}
		var raw any
		if failure != nil {
			encoded, _ := runtimefailures.MarshalEnvelope(*failure)
			raw = encoded
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET failure = $2::jsonb WHERE run_id = $1::uuid`, item.runID, raw); err != nil {
			return err
		}
	}
	if len(blocked) > 0 {
		return fmt.Errorf("runs canonical terminal evidence migration blocked by %d rows: %s", len(blocked), strings.Join(blocked, ", "))
	}
	if columns["error_summary"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs DROP COLUMN error_summary`); err != nil {
			return fmt.Errorf("drop runs.error_summary: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_failure_status_check;
		ALTER TABLE runs ADD CONSTRAINT runs_failure_status_check CHECK ((status = 'failed') = (failure IS NOT NULL));
		ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_terminal_time_check;
		ALTER TABLE runs ADD CONSTRAINT runs_terminal_time_check CHECK ((status IN ('running', 'paused')) = (ended_at IS NULL));
	`); err != nil {
		return fmt.Errorf("install runs terminal evidence constraints: %w", err)
	}
	return nil
}

func migrateSQLiteRunFailures(ctx context.Context, tx *sql.Tx) error {
	columns, err := sqliteColumnsTx(ctx, tx, "runs")
	if err != nil || len(columns) == 0 {
		return err
	}
	if !columns["status"] {
		return nil
	}
	if !columns["failure"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN failure TEXT`); err != nil {
			return fmt.Errorf("add sqlite runs.failure: %w", err)
		}
		columns["failure"] = true
	}
	if !columns["ended_at"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN ended_at TIMESTAMP`); err != nil {
			return fmt.Errorf("add sqlite runs.ended_at: %w", err)
		}
		columns["ended_at"] = true
	}
	errorExpr := "''"
	if columns["error_summary"] {
		errorExpr = "COALESCE(error_summary, '')"
	}
	rows, err := tx.QueryContext(ctx, `SELECT run_id, COALESCE(status, ''), `+errorExpr+`, failure, ended_at FROM runs ORDER BY run_id`)
	if err != nil {
		return err
	}
	type rowValue struct {
		runID, status, summary string
		failure                sql.NullString
		ended                  any
	}
	var pending []rowValue
	for rows.Next() {
		var item rowValue
		if err := rows.Scan(&item.runID, &item.status, &item.summary, &item.failure, &item.ended); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	controlColumns, err := sqliteColumnsTx(ctx, tx, "run_control_state")
	if err != nil {
		return err
	}
	var blocked []string
	for _, item := range pending {
		_, ended, endedErr := sqliteTimeValue(item.ended)
		if endedErr != nil {
			blocked = append(blocked, item.runID+"["+strings.TrimSpace(item.status)+"]:invalid ended_at")
			continue
		}
		evidence := legacyRunTerminalEvidence{runID: item.runID, status: item.status, errorSummary: item.summary, ended: ended}
		if item.failure.Valid {
			evidence.failureRaw = []byte(item.failure.String)
		}
		if len(controlColumns) > 0 {
			_ = tx.QueryRowContext(ctx, `SELECT COALESCE(control_status, ''), COALESCE(reason, '') FROM run_control_state WHERE run_id = ?`, item.runID).Scan(&evidence.controlStatus, &evidence.controlReason)
		}
		failure, err := canonicalRunFailureEvidence(evidence)
		if err != nil {
			blocked = append(blocked, item.runID+"["+strings.TrimSpace(item.status)+"]:"+err.Error())
			continue
		}
		var raw any
		if failure != nil {
			encoded, _ := runtimefailures.MarshalEnvelope(*failure)
			raw = string(encoded)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET failure = ? WHERE run_id = ?`, raw, item.runID); err != nil {
			return err
		}
	}
	if len(blocked) > 0 {
		return fmt.Errorf("sqlite runs canonical terminal evidence migration blocked by %d rows: %s", len(blocked), strings.Join(blocked, ", "))
	}
	if columns["error_summary"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE runs DROP COLUMN error_summary`); err != nil {
			return fmt.Errorf("drop sqlite runs.error_summary: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DROP TRIGGER IF EXISTS runs_terminal_evidence_insert;
		DROP TRIGGER IF EXISTS runs_terminal_evidence_update;
		CREATE TRIGGER runs_terminal_evidence_insert BEFORE INSERT ON runs
		WHEN ((NEW.status = 'failed') != (NEW.failure IS NOT NULL))
		  OR ((NEW.status IN ('running', 'paused')) != (NEW.ended_at IS NULL))
		BEGIN SELECT RAISE(ABORT, 'runs terminal evidence invariant violated'); END;
		CREATE TRIGGER runs_terminal_evidence_update BEFORE UPDATE OF status, failure, ended_at ON runs
		WHEN ((NEW.status = 'failed') != (NEW.failure IS NOT NULL))
		  OR ((NEW.status IN ('running', 'paused')) != (NEW.ended_at IS NULL))
		BEGIN SELECT RAISE(ABORT, 'runs terminal evidence invariant violated'); END;
	`); err != nil {
		return fmt.Errorf("install sqlite runs terminal evidence triggers: %w", err)
	}
	return nil
}

func sqliteColumnsTx(ctx context.Context, tx *sql.Tx, table string) (map[string]bool, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

func ensureSQLiteCanonicalFailureSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`) }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite canonical failure migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	counts := map[string]*failureMigrationCount{}
	for _, table := range []string{"event_deliveries", "event_receipts", "dead_letters", "activity_attempts", "agent_turns"} {
		columns, err := sqliteColumnsTx(ctx, tx, table)
		if err != nil {
			return fmt.Errorf("inspect sqlite %s failure columns: %w", table, err)
		}
		if len(columns) == 0 {
			continue
		}
		if !columns["failure"] {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN failure TEXT`); err != nil {
				return fmt.Errorf("add sqlite %s.failure: %w", table, err)
			}
		}
		switch table {
		case "event_deliveries":
			if err := migrateSQLiteDeliveries(ctx, tx, counts); err != nil {
				return err
			}
			if columns["last_error"] {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE event_deliveries DROP COLUMN last_error`); err != nil {
					return fmt.Errorf("drop sqlite event_deliveries.last_error: %w", err)
				}
			}
		case "event_receipts":
			if err := migrateSQLiteReceipts(ctx, tx, counts); err != nil {
				return err
			}
		case "dead_letters":
			if err := migrateSQLiteDeadLetters(ctx, tx, columns, counts); err != nil {
				return err
			}
		case "activity_attempts":
			if err := migrateSQLiteActivityAttempts(ctx, tx, columns, counts); err != nil {
				return err
			}
		case "agent_turns":
			if err := migrateSQLiteAgentTurns(ctx, tx, columns); err != nil {
				return err
			}
		}
	}
	if err := migrateSQLiteRunFailures(ctx, tx); err != nil {
		return fmt.Errorf("migrate sqlite runs terminal evidence: %w", err)
	}
	if err := migrateSQLiteFailureEventPayloads(ctx, tx, counts); err != nil {
		return fmt.Errorf("migrate sqlite failure-bearing event payloads: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite canonical failure migration: %w", err)
	}
	reportFailureMigration(counts)
	return nil
}

func migrateSQLiteDeliveries(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE failure IS NULL AND status IN ('failed', 'dead_letter')`)
	if err != nil {
		return err
	}
	type item struct{ id, status, reason string }
	var pending []item
	for rows.Next() {
		var row item
		if err := rows.Scan(&row.id, &row.status, &row.reason); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, row)
	}
	_ = rows.Close()
	var unknown []string
	for _, row := range pending {
		if row.status == "dead_letter" && legacyControlDeliveryReason(row.reason) {
			continue
		}
		failure, ok, err := legacyFailure(row.reason, "event_deliveries", row.id, nil)
		if err != nil {
			return err
		}
		if !ok {
			unknown = append(unknown, row.id+":"+row.reason)
			continue
		}
		raw, _ := runtimefailures.MarshalEnvelope(failure)
		if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET failure = ? WHERE delivery_id = ?`, string(raw), row.id); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "event_deliveries", row.reason, failure)
	}
	if len(unknown) > 0 {
		return fmt.Errorf("sqlite event_deliveries canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
	}
	return nil
}

func migrateSQLiteReceipts(ctx context.Context, tx *sql.Tx, counts map[string]*failureMigrationCount) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_receipts
		SET failure = (
			SELECT d.failure FROM event_deliveries d
			WHERE d.event_id = event_receipts.event_id
			  AND d.subscriber_type = event_receipts.subscriber_type
			  AND d.subscriber_id = event_receipts.subscriber_id
			  AND d.failure IS NOT NULL
			LIMIT 1
		)
		WHERE failure IS NULL
	`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE event_receipts SET side_effects = json_remove(COALESCE(side_effects, '{}'), '$.error', '$.failure_type') WHERE json_valid(COALESCE(side_effects, '{}'))`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT receipt_id, COALESCE(reason_code, '') FROM event_receipts WHERE outcome = 'dead_letter' AND failure IS NULL`)
	if err != nil {
		return err
	}
	var unknown []string
	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			_ = rows.Close()
			return err
		}
		failure, ok, err := legacyFailure(reason, "event_receipts", id, nil)
		if err != nil {
			return err
		}
		if !ok {
			unknown = append(unknown, id+":"+reason)
			continue
		}
		raw, _ := runtimefailures.MarshalEnvelope(failure)
		if _, err := tx.ExecContext(ctx, `UPDATE event_receipts SET failure = ? WHERE receipt_id = ?`, string(raw), id); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "event_receipts", reason, failure)
	}
	_ = rows.Close()
	if len(unknown) > 0 {
		return fmt.Errorf("sqlite event_receipts canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
	}
	return nil
}

func migrateSQLiteDeadLetters(ctx context.Context, tx *sql.Tx, columns map[string]bool, counts map[string]*failureMigrationCount) error {
	if columns["failure_type"] {
		rows, err := tx.QueryContext(ctx, `SELECT dead_letter_id, failure_type, COALESCE(target_failure_reason, ''), COALESCE(target_context, '{}') FROM dead_letters WHERE failure IS NULL`)
		if err != nil {
			return err
		}
		var unknown []string
		for rows.Next() {
			var id, failureType, targetReason, targetContext string
			if err := rows.Scan(&id, &failureType, &targetReason, &targetContext); err != nil {
				_ = rows.Close()
				return err
			}
			reason := failureType
			attributes := map[string]any{}
			if failureType == "target_resolution_failed" {
				if targetReason == "" {
					unknown = append(unknown, id+":missing_target_failure_reason")
					continue
				}
				reason = targetReason
				var contextValue any
				if json.Unmarshal([]byte(targetContext), &contextValue) == nil {
					attributes["target_context"] = contextValue
				}
			}
			failure, ok, err := legacyFailure(reason, "dead_letters", id, attributes)
			if err != nil {
				return err
			}
			if !ok {
				unknown = append(unknown, id+":"+failureType+":"+targetReason)
				continue
			}
			raw, _ := runtimefailures.MarshalEnvelope(failure)
			if _, err := tx.ExecContext(ctx, `UPDATE dead_letters SET failure = ? WHERE dead_letter_id = ?`, string(raw), id); err != nil {
				return err
			}
			addFailureMigrationCount(counts, "dead_letters", failureType+":"+targetReason, failure)
		}
		_ = rows.Close()
		if len(unknown) > 0 {
			return fmt.Errorf("sqlite dead_letters canonical failure migration blocked by %d unknown or ambiguous rows: %s", len(unknown), strings.Join(unknown, ", "))
		}
	}
	var missing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE failure IS NULL`).Scan(&missing); err != nil {
		return err
	}
	if missing > 0 {
		return fmt.Errorf("sqlite dead_letters canonical failure migration blocked by %d rows without failure", missing)
	}
	if !columns["failure_type"] {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE dead_letters__failure_new (
			dead_letter_id TEXT PRIMARY KEY,
			original_event_id TEXT REFERENCES events(event_id),
			original_event TEXT NOT NULL,
			original_payload TEXT NOT NULL DEFAULT '{}',
			entity_id TEXT,
			flow_instance TEXT NOT NULL,
			failure TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			chain_depth INTEGER NOT NULL DEFAULT 0,
			handler_node TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		INSERT INTO dead_letters__failure_new (dead_letter_id, original_event_id, original_event, original_payload, entity_id, flow_instance, failure, retry_count, chain_depth, handler_node, created_at)
		SELECT dead_letter_id, original_event_id, original_event, original_payload, entity_id, flow_instance, failure, retry_count, chain_depth, handler_node, created_at FROM dead_letters;
		DROP TABLE dead_letters;
		ALTER TABLE dead_letters__failure_new RENAME TO dead_letters;
		CREATE INDEX idx_dead_letters_flow ON dead_letters (flow_instance, created_at);
		CREATE INDEX idx_dead_letters_class ON dead_letters (json_extract(failure, '$.class'));
	`); err != nil {
		return fmt.Errorf("rebuild sqlite dead_letters canonical failure schema: %w", err)
	}
	return nil
}

func migrateSQLiteActivityAttempts(ctx context.Context, tx *sql.Tx, columns map[string]bool, counts map[string]*failureMigrationCount) error {
	rows, err := tx.QueryContext(ctx, `SELECT request_event_id, status FROM activity_attempts WHERE status IN ('failed', 'uncertain') AND failure IS NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			_ = rows.Close()
			return err
		}
		class, detail := runtimefailures.ClassConnectorFailure, "legacy_activity_failed"
		if status == "uncertain" {
			class, detail = runtimefailures.ClassOutcomeUncertain, "legacy_activity_outcome_uncertain"
		}
		raw, err := canonicalFailureJSON(class, detail, "failure-migration", "normalize_activity_attempt", map[string]any{"request_event_id": id, "legacy_status": status})
		if err != nil {
			return err
		}
		failure, _ := runtimefailures.UnmarshalEnvelope([]byte(raw))
		if _, err := tx.ExecContext(ctx, `UPDATE activity_attempts SET failure = ? WHERE request_event_id = ?`, raw, id); err != nil {
			return err
		}
		addFailureMigrationCount(counts, "activity_attempts", status, failure)
	}
	_ = rows.Close()
	if _, err := tx.ExecContext(ctx, `
		UPDATE activity_attempts
		SET result_payload = json_set(json_remove(COALESCE(result_payload, '{}'), '$.error', '$.failure'), '$.failure', json(failure))
		WHERE status IN ('failed', 'uncertain') AND failure IS NOT NULL AND result_payload IS NOT NULL
	`); err != nil {
		return fmt.Errorf("migrate sqlite activity result payload failures: %w", err)
	}
	eventColumns, err := sqliteColumnsTx(ctx, tx, "events")
	if err != nil {
		return err
	}
	if eventColumns["event_id"] && eventColumns["payload"] {
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET payload = json_set(
				json_remove(COALESCE(payload, '{}'), '$.error', '$.failure'),
				'$.failure',
				json((SELECT failure FROM activity_attempts WHERE result_event_id = events.event_id))
			)
			WHERE event_id IN (
				SELECT result_event_id FROM activity_attempts
				WHERE status IN ('failed', 'uncertain') AND failure IS NOT NULL AND result_event_id IS NOT NULL
			)
		`); err != nil {
			return fmt.Errorf("migrate sqlite activity result event failures: %w", err)
		}
	}
	if !columns["error"] {
		return nil
	}
	replyContextSelect := "NULL"
	if columns["reply_context_id"] {
		replyContextSelect = "reply_context_id"
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE activity_attempts__failure_new (
			request_event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL REFERENCES runs(run_id),
			source_event_id TEXT,
			parent_event_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			node_id TEXT NOT NULL,
			handler_event_key TEXT NOT NULL,
			activity_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			effect_class TEXT NOT NULL CHECK (effect_class = 'non_idempotent_write'),
			attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt = 1),
			status TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed', 'uncertain')),
			success_event TEXT NOT NULL,
			failure_event TEXT NOT NULL,
			result_event_id TEXT,
			result_event_type TEXT,
			result_payload TEXT,
			failure TEXT,
			input_hash TEXT NOT NULL,
			reply_context_id TEXT REFERENCES reply_contexts(reply_context_id),
			started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CHECK ((status = 'started' AND result_event_id IS NULL AND result_event_type IS NULL AND result_payload IS NULL AND failure IS NULL AND completed_at IS NULL)
			 OR (status = 'succeeded' AND result_event_id IS NOT NULL AND result_event_type IS NOT NULL AND result_payload IS NOT NULL AND failure IS NULL AND completed_at IS NOT NULL)
			 OR (status IN ('failed', 'uncertain') AND result_event_id IS NOT NULL AND result_event_type IS NOT NULL AND result_payload IS NOT NULL AND failure IS NOT NULL AND completed_at IS NOT NULL))
		);
		INSERT INTO activity_attempts__failure_new (request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance, node_id, handler_event_key, activity_id, tool, effect_class, attempt, status, success_event, failure_event, result_event_id, result_event_type, result_payload, failure, input_hash, reply_context_id, started_at, completed_at, updated_at)
		SELECT request_event_id, run_id, source_event_id, parent_event_id, entity_id, flow_instance, node_id, handler_event_key, activity_id, tool, effect_class, attempt, status, success_event, failure_event, result_event_id, result_event_type, result_payload, failure, input_hash, %s, started_at, completed_at, updated_at FROM activity_attempts;
		DROP TABLE activity_attempts;
		ALTER TABLE activity_attempts__failure_new RENAME TO activity_attempts;
		CREATE INDEX idx_activity_attempts_run ON activity_attempts (run_id, started_at);
		CREATE INDEX idx_activity_attempts_activity ON activity_attempts (run_id, activity_id, tool);
		CREATE INDEX idx_activity_attempts_result_event ON activity_attempts (result_event_id) WHERE result_event_id IS NOT NULL;
	`, replyContextSelect)); err != nil {
		return fmt.Errorf("rebuild sqlite activity_attempts canonical failure schema: %w", err)
	}
	return nil
}

func migrateSQLiteAgentTurns(ctx context.Context, tx *sql.Tx, columns map[string]bool) error {
	if !columns["error"] {
		return nil
	}
	var legacyCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE NULLIF(TRIM(error), '') IS NOT NULL`).Scan(&legacyCount); err != nil {
		return err
	}
	if legacyCount > 0 {
		return fmt.Errorf("sqlite agent_turns canonical failure migration blocked by %d ambiguous legacy error rows", legacyCount)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_turns DROP COLUMN error`); err != nil {
		return fmt.Errorf("drop sqlite agent_turns.error: %w", err)
	}
	return nil
}
