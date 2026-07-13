package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
)

const (
	legacyExternalEffectOperations  = "agent_external_effect_operations"
	legacyExternalEffectAttempts    = "agent_external_effect_attempts"
	runtimeExternalEffectOperations = "runtime_external_effect_operations"
	runtimeExternalEffectAttempts   = "runtime_external_effect_attempts"
)

func migratePostgresCompletionAuthoritySchema(ctx context.Context, tx *sql.Tx, plans []SchemaTableDDL) error {
	tables, err := postgresPublicTables(ctx, tx)
	if err != nil {
		return err
	}
	legacyOperations := tablePresent(tables, legacyExternalEffectOperations)
	legacyAttempts := tablePresent(tables, legacyExternalEffectAttempts)
	neutralOperations := tablePresent(tables, runtimeExternalEffectOperations)
	neutralAttempts := tablePresent(tables, runtimeExternalEffectAttempts)
	if !legacyOperations && !legacyAttempts {
		return nil
	}
	if !legacyOperations || !legacyAttempts || neutralOperations || neutralAttempts {
		return fmt.Errorf("completion authority schema migration requires exactly both legacy effect tables and no neutral effect tables")
	}

	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_external_effect_operations RENAME TO runtime_external_effect_operations`); err != nil {
		return fmt.Errorf("rename legacy effect operations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE agent_external_effect_attempts RENAME TO runtime_external_effect_attempts`); err != nil {
		return fmt.Errorf("rename legacy effect attempts: %w", err)
	}
	if err := executeRequiredPostgresPlans(ctx, tx, plans,
		"run_fork_selected_contract_runtime_executions",
	); err != nil {
		return err
	}
	if err := migratePostgresConversationForkTurns(ctx, tx); err != nil {
		return err
	}
	if err := migratePostgresRuntimeEffectTables(ctx, tx); err != nil {
		return err
	}
	if err := migratePostgresCompletionAgentTurns(ctx, tx); err != nil {
		return err
	}
	if err := migratePostgresSpendLedger(ctx, tx); err != nil {
		return err
	}
	if err := executeRequiredPostgresPlans(ctx, tx, plans,
		"conversation_fork_turn_completions",
		"budget_admission_scopes",
		"runtime_effect_budget_reservations",
	); err != nil {
		return err
	}
	return nil
}

func migratePostgresConversationForkTurns(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE conversation_fork_turns ADD COLUMN request_occurrence_id UUID`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN request_hash TEXT`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN idempotency_key TEXT`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN state TEXT`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN execution_owner TEXT`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN lease_expires_at TIMESTAMPTZ`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN fence_generation BIGINT NOT NULL DEFAULT 1`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN failure JSONB`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN evidence JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN updated_at TIMESTAMPTZ`,
		`ALTER TABLE conversation_fork_turns ADD COLUMN terminal_at TIMESTAMPTZ`,
		`UPDATE conversation_fork_turns SET request_occurrence_id=fork_turn_id, request_hash=encode(digest(message, 'sha256'), 'hex'), state='succeeded', updated_at=created_at, terminal_at=created_at`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN request_occurrence_id SET NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN request_hash SET NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN state SET NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN updated_at SET NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN updated_at SET DEFAULT NOW()`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN assistant_message DROP NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN request_payload DROP NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN response_payload DROP NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN sandbox_policy DROP NOT NULL`,
		`ALTER TABLE conversation_fork_turns ALTER COLUMN snapshot_owner DROP NOT NULL`,
		`ALTER TABLE conversation_fork_turns DROP CONSTRAINT IF EXISTS conversation_fork_turns_snapshot_owner_check`,
		`ALTER TABLE conversation_fork_turns ADD CHECK (snapshot_owner IS NULL OR snapshot_owner = 'conversation.fork_chat.snapshot.v1')`,
		`ALTER TABLE conversation_fork_turns ADD CHECK (state IN ('prepared', 'executing', 'succeeded', 'failed', 'outcome_uncertain', 'abandoned'))`,
		`ALTER TABLE conversation_fork_turns ADD CHECK (fence_generation > 0)`,
		`ALTER TABLE conversation_fork_turns ADD CHECK ((state IN ('prepared', 'executing') AND NULLIF(TRIM(COALESCE(execution_owner, '')), '') IS NOT NULL AND lease_expires_at IS NOT NULL AND assistant_message IS NULL AND response_payload IS NULL AND terminal_at IS NULL) OR (state = 'succeeded' AND NULLIF(TRIM(COALESCE(assistant_message, '')), '') IS NOT NULL AND request_payload IS NOT NULL AND response_payload IS NOT NULL AND sandbox_policy IS NOT NULL AND snapshot_owner IS NOT NULL AND failure IS NULL AND terminal_at IS NOT NULL) OR (state IN ('failed', 'outcome_uncertain', 'abandoned') AND failure IS NOT NULL AND terminal_at IS NOT NULL))`,
		`ALTER TABLE conversation_fork_turns ADD UNIQUE (actor_token_id, request_occurrence_id)`,
		`CREATE UNIQUE INDEX idx_conversation_fork_turns_idempotency ON conversation_fork_turns (actor_token_id, idempotency_key) WHERE idempotency_key IS NOT NULL`,
		`CREATE INDEX idx_conversation_fork_turns_recovery ON conversation_fork_turns (state, lease_expires_at, updated_at)`,
	}
	return execPostgresMigrationStatements(ctx, tx, "conversation_fork_turns", statements)
}

func migratePostgresRuntimeEffectTables(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`DROP INDEX IF EXISTS idx_agent_external_effect_operations_agent`,
		`DROP INDEX IF EXISTS idx_agent_external_effect_operations_recovery`,
		`DROP INDEX IF EXISTS idx_agent_external_effect_attempts_recovery`,
		`ALTER TABLE runtime_external_effect_operations ALTER COLUMN agent_id DROP NOT NULL`,
		`ALTER TABLE runtime_external_effect_operations ALTER COLUMN runtime_epoch DROP NOT NULL`,
		`ALTER TABLE runtime_external_effect_operations ALTER COLUMN generation DROP NOT NULL`,
		`ALTER TABLE runtime_external_effect_operations ADD COLUMN authority_kind TEXT`,
		`ALTER TABLE runtime_external_effect_operations ADD COLUMN authority_id TEXT`,
		`ALTER TABLE runtime_external_effect_operations ADD COLUMN selected_execution_id UUID REFERENCES run_fork_selected_contract_runtime_executions(execution_id)`,
		`ALTER TABLE runtime_external_effect_operations ADD COLUMN fork_turn_id UUID REFERENCES conversation_fork_turns(fork_turn_id)`,
		`ALTER TABLE runtime_external_effect_operations ADD COLUMN authority_evidence JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`UPDATE runtime_external_effect_operations SET authority_kind='normal_agent', authority_id=agent_id`,
		`ALTER TABLE runtime_external_effect_operations ALTER COLUMN authority_kind SET NOT NULL`,
		`ALTER TABLE runtime_external_effect_operations ALTER COLUMN authority_id SET NOT NULL`,
		`ALTER TABLE runtime_external_effect_operations ADD CHECK (authority_kind IN ('normal_agent', 'selected_contract_fork', 'conversation_fork_chat'))`,
		`ALTER TABLE runtime_external_effect_operations ADD CHECK ((authority_kind = 'normal_agent' AND agent_id IS NOT NULL AND runtime_epoch > 0 AND generation > 0 AND selected_execution_id IS NULL AND fork_turn_id IS NULL) OR (authority_kind = 'selected_contract_fork' AND agent_id IS NULL AND runtime_epoch IS NULL AND generation > 0 AND selected_execution_id IS NOT NULL AND fork_turn_id IS NULL) OR (authority_kind = 'conversation_fork_chat' AND agent_id IS NULL AND runtime_epoch IS NULL AND generation > 0 AND selected_execution_id IS NULL AND fork_turn_id IS NOT NULL))`,
		`ALTER TABLE runtime_external_effect_operations ADD UNIQUE (authority_kind, authority_id, operation_id)`,
		`CREATE INDEX idx_runtime_external_effect_operations_agent ON runtime_external_effect_operations (agent_id, created_at) WHERE agent_id IS NOT NULL`,
		`CREATE INDEX idx_runtime_external_effect_operations_authority ON runtime_external_effect_operations (authority_kind, authority_id, created_at)`,
		`CREATE INDEX idx_runtime_external_effect_operations_recovery ON runtime_external_effect_operations (state, updated_at)`,
		`ALTER TABLE runtime_external_effect_attempts ALTER COLUMN runtime_epoch DROP NOT NULL`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN execution_owner TEXT`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN lease_expires_at TIMESTAMPTZ`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN fence_generation BIGINT`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN usage_target_kind TEXT`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN usage_target_id UUID`,
		`ALTER TABLE runtime_external_effect_attempts ADD COLUMN target_ordinal INTEGER`,
		`UPDATE runtime_external_effect_attempts a SET execution_owner='migration:' || o.authority_id, lease_expires_at=a.updated_at, fence_generation=1 FROM runtime_external_effect_operations o WHERE o.operation_id=a.operation_id`,
		`ALTER TABLE runtime_external_effect_attempts ALTER COLUMN execution_owner SET NOT NULL`,
		`ALTER TABLE runtime_external_effect_attempts ALTER COLUMN lease_expires_at SET NOT NULL`,
		`ALTER TABLE runtime_external_effect_attempts ALTER COLUMN fence_generation SET NOT NULL`,
		`ALTER TABLE runtime_external_effect_attempts ADD FOREIGN KEY (operation_id) REFERENCES runtime_external_effect_operations(operation_id)`,
		`ALTER TABLE runtime_external_effect_attempts ADD CHECK (generation > 0)`,
		`ALTER TABLE runtime_external_effect_attempts ADD CHECK (fence_generation > 0)`,
		`ALTER TABLE runtime_external_effect_attempts ADD CHECK (usage_target_kind IN ('agent_turn', 'conversation_fork_turn_completion'))`,
		`ALTER TABLE runtime_external_effect_attempts ADD CHECK (target_ordinal IS NULL OR target_ordinal > 0)`,
		`ALTER TABLE runtime_external_effect_attempts ADD CHECK ((usage_target_kind IS NULL AND usage_target_id IS NULL AND target_ordinal IS NULL) OR (usage_target_kind = 'agent_turn' AND usage_target_id IS NOT NULL AND target_ordinal IS NULL) OR (usage_target_kind = 'conversation_fork_turn_completion' AND usage_target_id IS NOT NULL AND target_ordinal > 0))`,
		`CREATE INDEX idx_runtime_external_effect_attempts_recovery ON runtime_external_effect_attempts (state, lease_expires_at, updated_at)`,
		`CREATE INDEX idx_runtime_external_effect_attempts_target ON runtime_external_effect_attempts (usage_target_kind, usage_target_id) WHERE usage_target_id IS NOT NULL`,
	}
	return execPostgresMigrationStatements(ctx, tx, "runtime effect tables", statements)
}

func migratePostgresCompletionAgentTurns(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE agent_turns ADD COLUMN completion_attempt_id UUID UNIQUE REFERENCES runtime_external_effect_attempts(attempt_id)`,
		`ALTER TABLE agent_turns ADD COLUMN resolved_model TEXT`,
		`ALTER TABLE agent_turns ADD COLUMN usage_exactness TEXT`,
		`ALTER TABLE agent_turns ADD COLUMN input_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN output_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN cache_read_input_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN cache_creation_input_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN cache_creation_5m_input_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN cache_creation_1h_input_tokens BIGINT`,
		`ALTER TABLE agent_turns ADD COLUMN provider_reported_cost_usd NUMERIC(18,9)`,
		`ALTER TABLE agent_turns ADD CHECK (usage_exactness IS NULL OR usage_exactness IN ('exact', 'estimated', 'unavailable'))`,
		`ALTER TABLE agent_turns ADD CHECK (usage_exactness IS NULL OR (usage_exactness IN ('exact', 'estimated') AND input_tokens >= 0 AND output_tokens >= 0) OR (usage_exactness = 'unavailable' AND input_tokens IS NULL AND output_tokens IS NULL))`,
		`ALTER TABLE agent_turns ADD CHECK ((input_tokens IS NULL) = (output_tokens IS NULL))`,
		`ALTER TABLE agent_turns ADD CHECK (cache_read_input_tokens IS NULL OR cache_read_input_tokens >= 0)`,
		`ALTER TABLE agent_turns ADD CHECK (cache_creation_input_tokens IS NULL OR cache_creation_input_tokens >= 0)`,
		`ALTER TABLE agent_turns ADD CHECK (cache_creation_5m_input_tokens IS NULL OR cache_creation_5m_input_tokens >= 0)`,
		`ALTER TABLE agent_turns ADD CHECK (cache_creation_1h_input_tokens IS NULL OR cache_creation_1h_input_tokens >= 0)`,
		`ALTER TABLE agent_turns ADD CHECK ((cache_creation_5m_input_tokens IS NULL AND cache_creation_1h_input_tokens IS NULL) OR (cache_creation_input_tokens IS NOT NULL AND COALESCE(cache_creation_5m_input_tokens,0) + COALESCE(cache_creation_1h_input_tokens,0) <= cache_creation_input_tokens))`,
		`ALTER TABLE agent_turns ADD CHECK (input_tokens IS NULL OR COALESCE(cache_read_input_tokens,0) + COALESCE(cache_creation_input_tokens,0) <= input_tokens)`,
		`ALTER TABLE agent_turns ADD CHECK (provider_reported_cost_usd IS NULL OR provider_reported_cost_usd >= 0)`,
		`CREATE INDEX idx_agent_turns_completion_attempt ON agent_turns (completion_attempt_id) WHERE completion_attempt_id IS NOT NULL`,
	}
	return execPostgresMigrationStatements(ctx, tx, "agent_turns", statements)
}

func migratePostgresSpendLedger(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`ALTER TABLE spend_ledger ADD COLUMN external_effect_attempt_id UUID UNIQUE REFERENCES runtime_external_effect_attempts(attempt_id)`,
		`ALTER TABLE spend_ledger ADD COLUMN accounting_basis TEXT`,
		`ALTER TABLE spend_ledger ADD CHECK (accounting_basis IS NULL OR accounting_basis = 'accounting_unavailable_exhaustion')`,
		`ALTER TABLE spend_ledger ADD CHECK (external_effect_attempt_id IS NOT NULL OR accounting_basis IS NULL)`,
		`CREATE INDEX idx_spend_effect_attempt ON spend_ledger (external_effect_attempt_id) WHERE external_effect_attempt_id IS NOT NULL`,
	}
	return execPostgresMigrationStatements(ctx, tx, "spend_ledger", statements)
}

func migrateSQLiteCompletionAuthoritySchema(ctx context.Context, conn *sql.Conn, plans []SchemaTableDDL) error {
	tables, err := sqliteUserTables(ctx, conn)
	if err != nil {
		return err
	}
	legacyOperations := tablePresent(tables, legacyExternalEffectOperations)
	legacyAttempts := tablePresent(tables, legacyExternalEffectAttempts)
	neutralOperations := tablePresent(tables, runtimeExternalEffectOperations)
	neutralAttempts := tablePresent(tables, runtimeExternalEffectAttempts)
	if !legacyOperations && !legacyAttempts {
		return nil
	}
	if !legacyOperations || !legacyAttempts || neutralOperations || neutralAttempts {
		return fmt.Errorf("completion authority sqlite migration requires exactly both legacy effect tables and no neutral effect tables")
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("defer sqlite completion migration foreign keys: %w", err)
	}
	if err := sqliteRenameLegacyCompletionTables(ctx, conn); err != nil {
		return err
	}
	if err := executeRequiredSQLitePlans(ctx, conn, plans,
		"run_fork_selected_contract_runtime_executions",
		"conversation_fork_turns",
		"runtime_external_effect_operations",
		"runtime_external_effect_attempts",
		"agent_turns",
		"conversation_fork_turn_completions",
		"spend_ledger",
		"budget_admission_scopes",
		"runtime_effect_budget_reservations",
	); err != nil {
		return err
	}
	if err := sqliteCopyLegacyCompletionRows(ctx, conn); err != nil {
		return err
	}
	for _, table := range []string{
		"legacy_2043_spend_ledger",
		"legacy_2043_agent_turns",
		"legacy_2043_runtime_external_effect_attempts",
		"legacy_2043_runtime_external_effect_operations",
		"legacy_2043_conversation_fork_turns",
	} {
		if _, err := conn.ExecContext(ctx, `DROP TABLE `+quoteIdent(table)); err != nil {
			return fmt.Errorf("drop sqlite completion migration table %s: %w", table, err)
		}
	}
	return nil
}

func sqliteRenameLegacyCompletionTables(ctx context.Context, conn *sql.Conn) error {
	statements := []string{
		`ALTER TABLE conversation_fork_turns RENAME TO legacy_2043_conversation_fork_turns`,
		`ALTER TABLE agent_external_effect_operations RENAME TO legacy_2043_runtime_external_effect_operations`,
		`ALTER TABLE agent_external_effect_attempts RENAME TO legacy_2043_runtime_external_effect_attempts`,
		`ALTER TABLE agent_turns RENAME TO legacy_2043_agent_turns`,
		`ALTER TABLE spend_ledger RENAME TO legacy_2043_spend_ledger`,
		`DROP INDEX IF EXISTS idx_conversation_fork_turns_fork`,
		`DROP INDEX IF EXISTS idx_agent_external_effect_operations_agent`,
		`DROP INDEX IF EXISTS idx_agent_external_effect_operations_recovery`,
		`DROP INDEX IF EXISTS idx_agent_external_effect_attempts_recovery`,
		`DROP INDEX IF EXISTS idx_agent_turns_run`,
		`DROP INDEX IF EXISTS idx_agent_turns_session`,
		`DROP INDEX IF EXISTS idx_agent_turns_agent`,
		`DROP INDEX IF EXISTS idx_agent_turns_event`,
		`DROP INDEX IF EXISTS idx_spend_flow`,
		`DROP INDEX IF EXISTS idx_spend_agent`,
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("prepare sqlite completion migration: %w", err)
		}
	}
	return nil
}

func sqliteCopyLegacyCompletionRows(ctx context.Context, conn *sql.Conn) error {
	statements := []string{
		`INSERT INTO conversation_fork_turns (fork_turn_id, fork_id, turn_index, actor_token_id, request_occurrence_id, request_hash, message, state, fence_generation, assistant_message, request_payload, response_payload, tool_calls, sandbox_policy, snapshot_owner, evidence, created_at, updated_at, terminal_at) SELECT fork_turn_id, fork_id, turn_index, actor_token_id, fork_turn_id, lower(hex(message)), message, 'succeeded', 1, assistant_message, request_payload, response_payload, tool_calls, sandbox_policy, snapshot_owner, '{}', created_at, created_at, created_at FROM legacy_2043_conversation_fork_turns`,
		`INSERT INTO runtime_external_effect_operations (operation_id, effect_kind, effect_class, authority_kind, authority_id, agent_id, runtime_epoch, generation, authority_evidence, lineage, request_fingerprint, state, created_at, updated_at, completed_at) SELECT operation_id, effect_kind, effect_class, 'normal_agent', agent_id, agent_id, runtime_epoch, generation, '{}', lineage, request_fingerprint, state, created_at, updated_at, completed_at FROM legacy_2043_runtime_external_effect_operations`,
		`INSERT INTO runtime_external_effect_attempts (attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch, generation, execution_owner, lease_expires_at, fence_generation, state, evidence, failure, authorized_at, launched_at, response_observed_at, completed_at, updated_at) SELECT a.attempt_id, a.operation_id, a.attempt_ordinal, a.adapter, a.transport, a.runtime_epoch, a.generation, 'migration:' || o.agent_id, a.updated_at, 1, a.state, a.evidence, a.failure, a.authorized_at, a.launched_at, a.response_observed_at, a.completed_at, a.updated_at FROM legacy_2043_runtime_external_effect_attempts a JOIN legacy_2043_runtime_external_effect_operations o ON o.operation_id=a.operation_id`,
		`INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id, trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls, emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible, request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure, created_at) SELECT turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id, trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls, emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible, request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure, created_at FROM legacy_2043_agent_turns`,
		`INSERT INTO spend_ledger (ledger_id, entity_id, flow_instance, agent_id, model, model_alias, backend_profile, provider, transport, resolved_model, input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at) SELECT ledger_id, entity_id, flow_instance, agent_id, model, model_alias, backend_profile, provider, transport, resolved_model, input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at FROM legacy_2043_spend_ledger`,
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("copy sqlite completion migration rows: %w", err)
		}
	}
	return migrateSQLiteForkChatRequestHashes(ctx, conn)
}

func migrateSQLiteForkChatRequestHashes(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT fork_turn_id,message FROM conversation_fork_turns`)
	if err != nil {
		return fmt.Errorf("list sqlite forkchat request hashes: %w", err)
	}
	type update struct {
		id   string
		hash string
	}
	var updates []update
	for rows.Next() {
		var id, message string
		if err := rows.Scan(&id, &message); err != nil {
			rows.Close()
			return fmt.Errorf("scan sqlite forkchat request hash: %w", err)
		}
		digest := sha256.Sum256([]byte(message))
		updates = append(updates, update{id: id, hash: fmt.Sprintf("%x", digest[:])})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read sqlite forkchat request hashes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite forkchat request hashes: %w", err)
	}
	for _, item := range updates {
		if _, err := conn.ExecContext(ctx, `UPDATE conversation_fork_turns SET request_hash=? WHERE fork_turn_id=?`, item.hash, item.id); err != nil {
			return fmt.Errorf("migrate sqlite forkchat request hash: %w", err)
		}
	}
	return nil
}

func executeRequiredPostgresPlans(ctx context.Context, tx *sql.Tx, plans []SchemaTableDDL, names ...string) error {
	for _, name := range names {
		plan, ok := schemaPlanByName(plans, name)
		if !ok {
			return fmt.Errorf("completion schema migration requires platform plan %s", name)
		}
		if err := executePostgresPlans(ctx, tx, []SchemaTableDDL{plan}); err != nil {
			return err
		}
	}
	return nil
}

func executeRequiredSQLitePlans(ctx context.Context, conn *sql.Conn, plans []SchemaTableDDL, names ...string) error {
	for _, name := range names {
		plan, ok := schemaPlanByName(plans, name)
		if !ok {
			return fmt.Errorf("completion sqlite migration requires platform plan %s", name)
		}
		if err := executeSQLitePlans(ctx, conn, []SchemaTableDDL{plan}); err != nil {
			return err
		}
	}
	return nil
}

func schemaPlanByName(plans []SchemaTableDDL, name string) (SchemaTableDDL, bool) {
	for _, plan := range plans {
		if strings.TrimSpace(plan.TableName) == strings.TrimSpace(name) {
			return plan, true
		}
	}
	return SchemaTableDDL{}, false
}

func tablePresent(tables map[string]struct{}, name string) bool {
	_, ok := tables[name]
	return ok
}

func execPostgresMigrationStatements(ctx context.Context, tx *sql.Tx, subject string, statements []string) error {
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate postgres completion authority %s: %w", subject, err)
		}
	}
	return nil
}
