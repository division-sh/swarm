package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

var _ runtimeeffects.Store = (*PostgresStore)(nil)
var _ runtimeeffects.Store = (*SQLiteRuntimeStore)(nil)
var _ runtimeeffects.CompletionHeartbeatStore = (*PostgresStore)(nil)
var _ runtimeeffects.CompletionHeartbeatStore = (*SQLiteRuntimeStore)(nil)
var _ runtimeeffects.RecoveryStore = (*PostgresStore)(nil)
var _ runtimeeffects.RecoveryStore = (*SQLiteRuntimeStore)(nil)

const postgresExternalEffectActiveOwnerPredicate = `(o.authority_kind = 'conversation_fork_chat'
	OR COALESCE(NULLIF(o.lineage->>'run_id', ''), NULLIF(o.authority_evidence #>> '{usage_target,run_id}', '')) IS NULL
	OR EXISTS (
	SELECT 1 FROM runs run
	WHERE run.run_id = COALESCE(NULLIF(o.lineage->>'run_id', ''), NULLIF(o.authority_evidence #>> '{usage_target,run_id}', ''))::uuid
	  AND run.status IN ('running', 'paused')
))`

const sqliteExternalEffectActiveOwnerPredicate = `(o.authority_kind = 'conversation_fork_chat'
	OR COALESCE(NULLIF(json_extract(o.lineage, '$.run_id'), ''), NULLIF(json_extract(o.authority_evidence, '$.usage_target.run_id'), '')) IS NULL
	OR EXISTS (
	SELECT 1 FROM runs run
	WHERE run.run_id = COALESCE(NULLIF(json_extract(o.lineage, '$.run_id'), ''), NULLIF(json_extract(o.authority_evidence, '$.usage_target.run_id'), ''))
	  AND run.status IN ('running', 'paused')
))`

func (s *PostgresStore) ReconcileExternalEffectAttempts(ctx context.Context, now time.Time) (runtimeeffects.RecoverySummary, error) {
	var summary runtimeeffects.RecoverySummary
	err := s.runAuthorActivityMutation(ctx, "postgres reconcile external effect attempts", func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := loadExternalEffectRecoveryCandidates(txctx, tx, true)
		if err != nil {
			return err
		}
		summary, err = reconcileExternalEffectAttemptsPostgres(txctx, tx, now.UTC())
		if err != nil {
			return err
		}
		return recordRecoveredExternalEffectStories(txctx, tx, candidates, now.UTC(), true)
	})
	return summary, err
}

func (s *SQLiteRuntimeStore) ReconcileExternalEffectAttempts(ctx context.Context, now time.Time) (runtimeeffects.RecoverySummary, error) {
	var summary runtimeeffects.RecoverySummary
	err := s.runAuthorActivityMutation(ctx, "sqlite reconcile external effect attempts", func(txctx context.Context, tx *sql.Tx) error {
		candidates, err := loadExternalEffectRecoveryCandidates(txctx, tx, false)
		if err != nil {
			return err
		}
		summary, err = reconcileExternalEffectAttemptsSQLiteTx(txctx, tx, now.UTC())
		if err != nil {
			return err
		}
		return recordRecoveredExternalEffectStories(txctx, tx, candidates, now.UTC(), false)
	})
	return summary, err
}

func (s *PostgresStore) IsExternalEffectAuthorityCurrent(ctx context.Context, authority runtimeeffects.Authority) (bool, error) {
	return externalEffectAuthorityCurrentPostgres(ctx, s.DB, authority)
}

func (s *SQLiteRuntimeStore) IsExternalEffectAuthorityCurrent(ctx context.Context, authority runtimeeffects.Authority) (bool, error) {
	return externalEffectAuthorityCurrentSQLite(ctx, s.DB, authority)
}

func (s *PostgresStore) AuthorizeExternalAttempt(ctx context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	var err error
	req.Lineage, err = bindExternalEffectRunLineage(ctx, authority, req.Lineage)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("authorize external attempt begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireExternalEffectAuthorityPostgres(ctx, tx, authority, true); err != nil {
		return runtimeeffects.Attempt{}, err
	}
	authority.LeaseExpiresAt, err = externalEffectAttemptLeasePostgres(ctx, tx, authority)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	reservations, err := prepareCompletionBudgetReservationsPostgres(ctx, tx, authority, req.Now.UTC())
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if existing, found, err := loadExistingExternalAttemptPostgres(ctx, tx, req.OperationID); err != nil {
		return runtimeeffects.Attempt{}, err
	} else if found {
		attempt, retry, err := authorizeClaudePrelaunchRetryPostgres(ctx, tx, authority, req, existing)
		if err != nil {
			return runtimeeffects.Attempt{}, err
		}
		if !retry {
			return runtimeeffects.Attempt{}, externalEffectReplayRefusal(authority, req, existing)
		}
		if err := insertCompletionBudgetReservationsPostgres(ctx, tx, attempt.AttemptID, reservations, req.Now.UTC()); err != nil {
			return runtimeeffects.Attempt{}, err
		}
		if err := tx.Commit(); err != nil {
			return runtimeeffects.Attempt{}, fmt.Errorf("authorize external retry commit: %w", err)
		}
		return attempt, nil
	}
	attempt, err := insertExternalAttemptPostgres(ctx, tx, authority, req)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if err := insertCompletionBudgetReservationsPostgres(ctx, tx, attempt.AttemptID, reservations, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("authorize external attempt commit: %w", err)
	}
	return attempt, nil
}

func (s *SQLiteRuntimeStore) AuthorizeExternalAttempt(ctx context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	var err error
	req.Lineage, err = bindExternalEffectRunLineage(ctx, authority, req.Lineage)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	var attempt runtimeeffects.Attempt
	err = s.runRuntimeMutation(ctx, "sqlite authorize external attempt", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, authority, true); err != nil {
			return err
		}
		var err error
		authority.LeaseExpiresAt, err = externalEffectAttemptLeaseSQLite(txctx, tx, authority)
		if err != nil {
			return err
		}
		reservations, err := prepareCompletionBudgetReservationsSQLite(txctx, tx, authority, req.Now.UTC())
		if err != nil {
			return err
		}
		if existing, found, err := loadExistingExternalAttemptSQLite(txctx, tx, req.OperationID); err != nil {
			return err
		} else if found {
			var retry bool
			attempt, retry, err = authorizeClaudePrelaunchRetrySQLite(txctx, tx, authority, req, existing)
			if err != nil {
				return err
			}
			if retry {
				return insertCompletionBudgetReservationsSQLite(txctx, tx, attempt.AttemptID, reservations, req.Now.UTC())
			}
			return externalEffectReplayRefusal(authority, req, existing)
		}
		attempt, err = insertExternalAttemptSQLiteTx(txctx, tx, authority, req)
		if err != nil {
			return err
		}
		return insertCompletionBudgetReservationsSQLite(txctx, tx, attempt.AttemptID, reservations, req.Now.UTC())
	})
	return attempt, err
}

func bindExternalEffectRunLineage(ctx context.Context, authority runtimeeffects.Authority, lineage map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(lineage)+1)
	for key, value := range lineage {
		out[key] = value
	}
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat {
		return out, nil
	}
	runID := strings.TrimSpace(authority.SelectedFork.ForkRunID)
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		runID = strings.TrimSpace(authority.Target.RunID)
		if runID == "" {
			var ok bool
			var err error
			runID, ok, err = runtimecurrentstate.RunIDFromContext(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				return out, nil
			}
		}
	}
	if runID == "" {
		return out, nil
	}
	if existing := strings.TrimSpace(out["run_id"]); existing != "" && existing != runID {
		return nil, fmt.Errorf("external effect lineage run_id conflicts with authority run_id")
	}
	out["run_id"] = runID
	return out, nil
}

type existingExternalAttempt struct {
	authorityKind  string
	authorityID    string
	operationMode  string
	attemptMode    string
	kind           string
	class          string
	agentID        string
	epoch          int64
	generation     uint64
	fingerprint    string
	operationState string
	attemptID      string
	adapter        string
	transport      string
	attemptState   string
	attemptOrdinal int
	launched       bool
	failureJSON    string
}

type externalEffectStorySource struct {
	AttemptID     string
	Kind          string
	Class         string
	Adapter       string
	Transport     string
	AuthorityKind string
	AuthorityID   string
	AgentID       string
	ExecutionMode string
	Ordinal       int
}

func externalEffectStorySourceFromAttempt(attempt runtimeeffects.Attempt) externalEffectStorySource {
	return externalEffectStorySource{
		AttemptID: attempt.AttemptID, Kind: string(attempt.Kind), Class: string(attempt.Class),
		Adapter: attempt.Adapter, Transport: attempt.Transport, AuthorityKind: string(attempt.Authority.Kind),
		AuthorityID: attempt.Authority.ID, AgentID: attempt.Authority.Normal.AgentID,
		ExecutionMode: string(attempt.Authority.ExecutionMode), Ordinal: attempt.Ordinal,
	}
}

func loadExternalEffectStorySource(ctx context.Context, tx *sql.Tx, attemptID string, postgres bool) (externalEffectStorySource, error) {
	query := `
		SELECT CAST(a.attempt_id AS TEXT), o.effect_kind, o.effect_class, a.adapter, a.transport,
		       o.authority_kind, o.authority_id, COALESCE(o.agent_id, ''), o.execution_mode, a.attempt_ordinal
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE a.attempt_id = ?
	`
	if postgres {
		query = `
			SELECT a.attempt_id::text, o.effect_kind, o.effect_class, a.adapter, a.transport,
			       o.authority_kind, o.authority_id, COALESCE(o.agent_id, ''), o.execution_mode, a.attempt_ordinal
			FROM runtime_external_effect_attempts a
			JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
			WHERE a.attempt_id = $1::uuid
		`
	}
	var source externalEffectStorySource
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(attemptID)).Scan(
		&source.AttemptID, &source.Kind, &source.Class, &source.Adapter, &source.Transport,
		&source.AuthorityKind, &source.AuthorityID, &source.AgentID, &source.ExecutionMode, &source.Ordinal,
	); err != nil {
		return externalEffectStorySource{}, fmt.Errorf("load external effect author activity source: %w", err)
	}
	return source, nil
}

type externalEffectStoryDisposition struct {
	Launch bool
}

var externalEffectStoryDispositions = map[string]externalEffectStoryDisposition{
	"provider_turn/anthropic_api":                       {Launch: true},
	"provider_turn/openai_compatible":                   {Launch: true},
	"provider_turn/openai_responses":                    {Launch: true},
	"provider_turn/claude_cli":                          {Launch: true},
	"provider_turn/mock_python":                         {Launch: true},
	"http_tool_target/authored_http_tool":               {Launch: true},
	"managed_credential_request/managed_credential":     {},
	"native_web_search_http/native_web_search":          {Launch: true},
	"mcp_http_request/mcp_tools_call_http":              {Launch: true},
	"mcp_stdio_request/mcp_tools_call_stdio":            {Launch: true},
	"native_command/native_bash":                        {Launch: true},
	"native_command/native_read_file":                   {Launch: true},
	"native_file_write/native_write_file":               {Launch: true},
	"tool_result_relay/tool_result_relay":               {},
	"claude_tool_result_relay/claude_tool_result_relay": {},
}

func externalEffectStoryDispositionFor(kind, adapter string) (externalEffectStoryDisposition, error) {
	key := strings.TrimSpace(kind) + "/" + strings.TrimSpace(adapter)
	disposition, ok := externalEffectStoryDispositions[key]
	if !ok {
		return externalEffectStoryDisposition{}, fmt.Errorf("external effect registration %q has no author activity disposition", key)
	}
	return disposition, nil
}

func recordExternalEffectStory(ctx context.Context, source externalEffectStorySource, state runtimeeffects.State, failure *runtimefailures.Envelope, occurredAt time.Time) error {
	disposition, err := externalEffectStoryDispositionFor(source.Kind, source.Adapter)
	if err != nil {
		return err
	}
	if state == runtimeeffects.StateLaunched {
		if !disposition.Launch {
			return nil
		}
	}
	if state != runtimeeffects.StateLaunched && state != runtimeeffects.StateTerminalFailure && state != runtimeeffects.StateOutcomeUncertain {
		return nil
	}
	transition := string(state)
	attempt := source.Ordinal
	identity := source.AttemptID + ":" + transition
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindEffectLifecycle, Transition: transition,
		SourceOwner: "runtime_external_effect_attempts", SourceIdentity: identity, DedupKey: "effect:" + identity,
		OccurredAt: occurredAt.UTC(), AgentID: source.AgentID,
		Projection: runtimeauthoractivity.Projection{
			EffectClass: source.Class, Attempt: &attempt, Adapter: source.Adapter, Transport: source.Transport,
			AuthorityKind: source.AuthorityKind, AuthorityID: source.AuthorityID, ExecutionMode: source.ExecutionMode,
		},
		Failure: failure,
	})
}

func recordSettledExternalEffectStory(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement, postgres bool) error {
	if settlement.State != runtimeeffects.StateTerminalFailure && settlement.State != runtimeeffects.StateOutcomeUncertain {
		return nil
	}
	source, err := loadExternalEffectStorySource(ctx, tx, settlement.AttemptID, postgres)
	if err != nil {
		return err
	}
	state, failure, occurredAt, err := loadExternalEffectStorySettlement(ctx, tx, settlement.AttemptID, settlement.OperationID, postgres)
	if err != nil {
		return err
	}
	return recordExternalEffectStory(ctx, source, state, failure, occurredAt)
}

func loadExternalEffectStorySettlement(ctx context.Context, tx *sql.Tx, attemptID, operationID string, postgres bool) (runtimeeffects.State, *runtimefailures.Envelope, time.Time, error) {
	query := `SELECT state, COALESCE(failure, 'null'), completed_at FROM runtime_external_effect_attempts WHERE attempt_id = ? AND operation_id = ?`
	if postgres {
		query = `SELECT state, COALESCE(failure, 'null'::jsonb), completed_at FROM runtime_external_effect_attempts WHERE attempt_id = $1::uuid AND operation_id = $2::uuid`
	}
	var state string
	var failureRaw []byte
	var completedAtRaw any
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(attemptID), strings.TrimSpace(operationID)).Scan(&state, &failureRaw, &completedAtRaw); err != nil {
		return "", nil, time.Time{}, fmt.Errorf("load settled external effect author activity source: %w", err)
	}
	completedAt, ok, err := sqliteTimeValue(completedAtRaw)
	if err != nil || !ok {
		return "", nil, time.Time{}, fmt.Errorf("load settled external effect completion time: %w", firstNonNilError(err, fmt.Errorf("completed_at is required")))
	}
	var failure *runtimefailures.Envelope
	if raw := strings.TrimSpace(string(failureRaw)); raw != "" && raw != "null" {
		decoded, err := runtimefailures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return "", nil, time.Time{}, fmt.Errorf("decode settled external effect failure: %w", err)
		}
		failure = &decoded
	}
	return runtimeeffects.State(strings.TrimSpace(state)), failure, completedAt.UTC(), nil
}

func loadExternalEffectRecoveryCandidates(ctx context.Context, tx *sql.Tx, postgres bool) ([]string, error) {
	query := `SELECT CAST(a.attempt_id AS TEXT) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state IN ('authorized','launched','response_observed') AND (o.authority_kind='conversation_fork_chat' OR EXISTS (SELECT 1 FROM runs run WHERE run.run_id=COALESCE(NULLIF(json_extract(o.lineage, '$.run_id'), ''), NULLIF(json_extract(o.authority_evidence, '$.usage_target.run_id'), '')) AND run.status IN ('running','paused'))) ORDER BY a.attempt_id`
	if postgres {
		query = `SELECT a.attempt_id::text FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state IN ('authorized','launched','response_observed') AND (o.authority_kind='conversation_fork_chat' OR EXISTS (SELECT 1 FROM runs run WHERE run.run_id=COALESCE(NULLIF(o.lineage->>'run_id',''), NULLIF(o.authority_evidence #>> '{usage_target,run_id}',''))::uuid AND run.status IN ('running','paused'))) ORDER BY a.attempt_id`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []string
	for rows.Next() {
		var attemptID string
		if err := rows.Scan(&attemptID); err != nil {
			return nil, err
		}
		candidates = append(candidates, attemptID)
	}
	return candidates, rows.Err()
}

func recordRecoveredExternalEffectStories(ctx context.Context, tx *sql.Tx, candidates []string, occurredAt time.Time, postgres bool) error {
	query := `SELECT state, failure FROM runtime_external_effect_attempts WHERE attempt_id = ?`
	if postgres {
		query = `SELECT state, failure FROM runtime_external_effect_attempts WHERE attempt_id = $1::uuid`
	}
	for _, attemptID := range candidates {
		var state string
		var failureRaw []byte
		if err := tx.QueryRowContext(ctx, query, attemptID).Scan(&state, &failureRaw); err != nil {
			return err
		}
		if state != string(runtimeeffects.StateTerminalFailure) && state != string(runtimeeffects.StateOutcomeUncertain) {
			continue
		}
		failure, err := runtimefailures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return fmt.Errorf("decode recovered external effect failure: %w", err)
		}
		source, err := loadExternalEffectStorySource(ctx, tx, attemptID, postgres)
		if err != nil {
			return err
		}
		if err := recordExternalEffectStory(ctx, source, runtimeeffects.State(state), &failure, occurredAt); err != nil {
			return err
		}
	}
	return nil
}

func (e existingExternalAttempt) matchesAuthorityIdentity(authority runtimeeffects.Authority) bool {
	mode := string(authority.ExecutionMode)
	return e.authorityKind == string(authority.Kind) && e.authorityID == authority.ID &&
		e.operationMode == mode && e.attemptMode == mode
}

func (e existingExternalAttempt) matchesRetryAuthority(authority runtimeeffects.Authority) bool {
	if !e.matchesAuthorityIdentity(authority) || e.generation != authority.Generation() {
		return false
	}
	if authority.Kind == runtimeeffects.AuthorityNormalAgent {
		return e.agentID == authority.Normal.AgentID && e.epoch == authority.Normal.RuntimeEpoch
	}
	return e.agentID == "" && e.epoch == 0
}

func (e existingExternalAttempt) matchesRequest(req runtimeeffects.AuthorizeRequest) bool {
	return e.kind == string(req.Kind) && e.class == string(req.Class) && e.adapter == req.Adapter &&
		e.transport == req.Transport && e.fingerprint == req.RequestFingerprint
}

func loadExistingExternalAttemptPostgres(ctx context.Context, tx *sql.Tx, operationID string) (existingExternalAttempt, bool, error) {
	var existing existingExternalAttempt
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, o.execution_mode, a.execution_mode, o.effect_kind, o.effect_class, COALESCE(o.agent_id,''), COALESCE(o.runtime_epoch,0), o.generation,
		       o.request_fingerprint, o.state, a.attempt_id::text, a.adapter, a.transport, a.state,
		       a.attempt_ordinal, (a.launched_at IS NOT NULL), COALESCE(a.failure, '{}'::jsonb)::text
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		WHERE o.operation_id = $1::uuid
		ORDER BY a.attempt_ordinal DESC
		LIMIT 1
	`, operationID).Scan(&existing.authorityKind, &existing.authorityID, &existing.operationMode, &existing.attemptMode, &existing.kind, &existing.class, &existing.agentID, &existing.epoch, &existing.generation,
		&existing.fingerprint, &existing.operationState, &existing.attemptID, &existing.adapter, &existing.transport, &existing.attemptState,
		&existing.attemptOrdinal, &existing.launched, &existing.failureJSON)
	if err == sql.ErrNoRows {
		return existingExternalAttempt{}, false, nil
	}
	if err != nil {
		return existingExternalAttempt{}, false, fmt.Errorf("load external effect replay authority: %w", err)
	}
	return existing, true, nil
}

func loadExistingExternalAttemptSQLite(ctx context.Context, tx *sql.Tx, operationID string) (existingExternalAttempt, bool, error) {
	var existing existingExternalAttempt
	err := tx.QueryRowContext(ctx, `
		SELECT o.authority_kind, o.authority_id, o.execution_mode, a.execution_mode, o.effect_kind, o.effect_class, COALESCE(o.agent_id,''), COALESCE(o.runtime_epoch,0), o.generation,
		       o.request_fingerprint, o.state, a.attempt_id, a.adapter, a.transport, a.state,
		       a.attempt_ordinal, (a.launched_at IS NOT NULL), COALESCE(a.failure, '{}')
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		WHERE o.operation_id = ?
		ORDER BY a.attempt_ordinal DESC
		LIMIT 1
	`, operationID).Scan(&existing.authorityKind, &existing.authorityID, &existing.operationMode, &existing.attemptMode, &existing.kind, &existing.class, &existing.agentID, &existing.epoch, &existing.generation,
		&existing.fingerprint, &existing.operationState, &existing.attemptID, &existing.adapter, &existing.transport, &existing.attemptState,
		&existing.attemptOrdinal, &existing.launched, &existing.failureJSON)
	if err == sql.ErrNoRows {
		return existingExternalAttempt{}, false, nil
	}
	if err != nil {
		return existingExternalAttempt{}, false, fmt.Errorf("load sqlite external effect replay authority: %w", err)
	}
	return existing, true, nil
}

func authorizeClaudePrelaunchRetryPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, existing existingExternalAttempt) (runtimeeffects.Attempt, bool, error) {
	if !claudePrelaunchRetryEligible(authority, req, existing) {
		return runtimeeffects.Attempt{}, false, nil
	}
	return insertExternalRetryAttemptPostgres(ctx, tx, authority, req, existing.attemptOrdinal+1)
}

func authorizeClaudePrelaunchRetrySQLite(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, existing existingExternalAttempt) (runtimeeffects.Attempt, bool, error) {
	if !claudePrelaunchRetryEligible(authority, req, existing) {
		return runtimeeffects.Attempt{}, false, nil
	}
	attempt, err := insertExternalRetryAttemptSQLiteTx(ctx, tx, authority, req, existing.attemptOrdinal+1)
	return attempt, true, err
}

func claudePrelaunchRetryEligible(authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, existing existingExternalAttempt) bool {
	if req.Adapter != "claude_cli" || existing.operationState != string(runtimeeffects.StateTerminalFailure) ||
		existing.attemptState != string(runtimeeffects.StateTerminalFailure) {
		return false
	}
	if !existing.matchesRetryAuthority(authority) || !existing.matchesRequest(req) {
		return false
	}
	failure, err := runtimefailures.UnmarshalEnvelope([]byte(existing.failureJSON))
	if err != nil {
		return false
	}
	if !existing.launched {
		return failure.Retryable || failure.Detail.Code == "effect_recovery_prelaunch_abandoned"
	}
	launchRejected, _ := failure.Detail.Attributes["launch_rejected"].(bool)
	return launchRejected && failure.Retryable
}

func insertExternalRetryAttemptPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, ordinal int) (runtimeeffects.Attempt, bool, error) {
	attemptID, err := runtimeeffects.AttemptID(req.OperationID, ordinal)
	if err != nil {
		return runtimeeffects.Attempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
			usage_target_kind, usage_target_id, target_ordinal, state, authorized_at, updated_at
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, NULLIF($6,0), $7, $8, $9, $10, $11, NULLIF($12,''), NULLIF($13,'')::uuid, NULLIF($14,0), 'authorized', $15, $15)
	`, attemptID, req.OperationID, ordinal, req.Adapter, req.Transport, authority.RuntimeEpoch(), authority.ExecutionMode, authority.Generation(), authority.ExecutionOwner, authority.LeaseExpiresAt.UTC(), authority.FenceGeneration, string(authority.Target.Kind), authority.Target.ID, authority.Target.Ordinal, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, false, fmt.Errorf("insert external retry attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='authorized', completed_at=NULL, updated_at=$2 WHERE operation_id=$1::uuid`, req.OperationID, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, false, err
	}
	return externalAuthorizedAttempt(authority, req, attemptID, ordinal), true, nil
}

func insertExternalRetryAttemptSQLiteTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, ordinal int) (runtimeeffects.Attempt, error) {
	attemptID, err := runtimeeffects.AttemptID(req.OperationID, ordinal)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
			usage_target_kind, usage_target_id, target_ordinal, state, authorized_at, updated_at
		) VALUES (?, ?, ?, ?, ?, NULLIF(?,0), ?, ?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), NULLIF(?,0), 'authorized', ?, ?)
	`, attemptID, req.OperationID, ordinal, req.Adapter, req.Transport, authority.RuntimeEpoch(), authority.ExecutionMode, authority.Generation(), authority.ExecutionOwner, authority.LeaseExpiresAt.UTC(), authority.FenceGeneration, string(authority.Target.Kind), authority.Target.ID, authority.Target.Ordinal, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert sqlite external retry attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='authorized', completed_at=NULL, updated_at=? WHERE operation_id=?`, req.Now.UTC(), req.OperationID); err != nil {
		return runtimeeffects.Attempt{}, err
	}
	return externalAuthorizedAttempt(authority, req, attemptID, ordinal), nil
}

func externalEffectReplayRefusal(authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, existing existingExternalAttempt) error {
	detail := map[string]any{
		"operation_id": req.OperationID, "attempt_id": existing.attemptID,
		"operation_state": existing.operationState, "attempt_state": existing.attemptState,
	}
	if existing.generation != authority.Generation() {
		detail["existing_generation"] = existing.generation
		detail["authority_generation"] = authority.Generation()
	}
	if !existing.matchesAuthorityIdentity(authority) || !existing.matchesRequest(req) {
		detail["expected_fingerprint"] = existing.fingerprint
		detail["request_fingerprint"] = req.RequestFingerprint
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_replay_fingerprint_conflict", "external-effects", "authorize_attempt", detail)
	}
	switch runtimeeffects.State(existing.attemptState) {
	case runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved, runtimeeffects.StateOutcomeUncertain:
		return runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "external_effect_replay_outcome_uncertain", "external-effects", "authorize_attempt", detail)
	default:
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_replay_refused", "external-effects", "authorize_attempt", detail)
	}
}

func supersededExternalAttempt(token runtimeeffects.LifecycleToken, currentEpoch, currentGeneration int64, phase string) error {
	return runtimefailures.New(runtimefailures.ClassSupersededGeneration, "superseded_generation", "external-effects", "authorize_attempt", map[string]any{
		"agent_id": token.AgentID, "runtime_epoch": token.RuntimeEpoch, "generation": token.Generation,
		"current_runtime_epoch": currentEpoch, "current_generation": currentGeneration, "current_phase": strings.TrimSpace(phase),
	})
}

func insertExternalAttemptPostgres(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	lineage, _ := json.Marshal(req.Lineage)
	authorityEvidence, _ := json.Marshal(authority.Evidence())
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_operations (
			operation_id, effect_kind, effect_class, execution_mode, authority_kind, authority_id,
			agent_id, runtime_epoch, generation, selected_execution_id, fork_turn_id,
			authority_evidence, lineage, request_fingerprint, state, created_at, updated_at
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, NULLIF($7,''), NULLIF($8,0), $9,
		          NULLIF($10,'')::uuid, NULLIF($11,'')::uuid, $12::jsonb, $13::jsonb, $14, 'authorized', $15, $15)
	`, req.OperationID, string(req.Kind), string(req.Class), authority.ExecutionMode, string(authority.Kind), authority.ID,
		authority.Normal.AgentID, authority.RuntimeEpoch(), authority.Generation(), authority.SelectedFork.ExecutionID, authority.ForkChat.ForkTurnID,
		string(authorityEvidence), string(lineage), req.RequestFingerprint, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert external effect operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
			usage_target_kind, usage_target_id, target_ordinal, state, authorized_at, updated_at
		) VALUES ($1::uuid, $2::uuid, 1, $3, $4, NULLIF($5,0), $6, $7, $8, $9, $10,
		          NULLIF($11,''), NULLIF($12,'')::uuid, NULLIF($13,0), 'authorized', $14, $14)
	`, req.AttemptID, req.OperationID, req.Adapter, req.Transport, authority.RuntimeEpoch(), authority.ExecutionMode, authority.Generation(),
		authority.ExecutionOwner, authority.LeaseExpiresAt.UTC(), authority.FenceGeneration,
		string(authority.Target.Kind), authority.Target.ID, authority.Target.Ordinal, req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert external effect attempt: %w", err)
	}
	return externalAuthorizedAttempt(authority, req, req.AttemptID, 1), nil
}

func insertExternalAttemptSQLiteTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	lineage, _ := json.Marshal(req.Lineage)
	authorityEvidence, _ := json.Marshal(authority.Evidence())
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_operations (
			operation_id, effect_kind, effect_class, execution_mode, authority_kind, authority_id,
			agent_id, runtime_epoch, generation, selected_execution_id, fork_turn_id,
			authority_evidence, lineage, request_fingerprint, state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, NULLIF(?,''), NULLIF(?,0), ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, 'authorized', ?, ?)
	`, req.OperationID, string(req.Kind), string(req.Class), authority.ExecutionMode, string(authority.Kind), authority.ID,
		authority.Normal.AgentID, authority.RuntimeEpoch(), authority.Generation(), authority.SelectedFork.ExecutionID, authority.ForkChat.ForkTurnID,
		string(authorityEvidence), string(lineage), req.RequestFingerprint, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert sqlite external effect operation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runtime_external_effect_attempts (
			attempt_id, operation_id, attempt_ordinal, adapter, transport, runtime_epoch,
			execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
			usage_target_kind, usage_target_id, target_ordinal, state, authorized_at, updated_at
		) VALUES (?, ?, 1, ?, ?, NULLIF(?,0), ?, ?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''), NULLIF(?,0), 'authorized', ?, ?)
	`, req.AttemptID, req.OperationID, req.Adapter, req.Transport, authority.RuntimeEpoch(), authority.ExecutionMode, authority.Generation(),
		authority.ExecutionOwner, authority.LeaseExpiresAt.UTC(), authority.FenceGeneration,
		string(authority.Target.Kind), authority.Target.ID, authority.Target.Ordinal, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("insert sqlite external effect attempt: %w", err)
	}
	return externalAuthorizedAttempt(authority, req, req.AttemptID, 1), nil
}

func externalAuthorizedAttempt(authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest, attemptID string, ordinal int) runtimeeffects.Attempt {
	return runtimeeffects.Attempt{
		OperationID: req.OperationID, AttemptID: attemptID, Token: authority.Normal, Authority: authority,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: ordinal, AuthorizedAt: req.Now.UTC(),
	}
}

func (s *PostgresStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) error {
	return s.runAuthorActivityMutation(ctx, "postgres mark external attempt launched", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireExternalEffectAuthorityPostgres(txctx, tx, attempt.Authority, false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET state = 'launched', launched_at = $2, updated_at = $2 WHERE attempt_id = $1::uuid AND operation_id = $3::uuid AND execution_owner=$4 AND fence_generation=$5 AND state = 'authorized'`, attempt.AttemptID, now.UTC(), attempt.OperationID, attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
		if err := requireExternalAttemptTransition(res, err); err == nil {
			operationRes, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_operations SET state = 'launched', updated_at = $2 WHERE operation_id = $1::uuid AND state = 'authorized'`, attempt.OperationID, now.UTC())
			if err := requireExternalAttemptTransition(operationRes, err); err != nil {
				return err
			}
		} else {
			var state string
			var operationState string
			if queryErr := tx.QueryRowContext(txctx, `SELECT a.state, o.state FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id WHERE a.attempt_id = $1::uuid AND a.operation_id = $2::uuid`, attempt.AttemptID, attempt.OperationID).Scan(&state, &operationState); queryErr != nil || state != string(runtimeeffects.StateLaunched) || operationState != string(runtimeeffects.StateLaunched) {
				return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "launch_attempt", map[string]any{"attempt_id": attempt.AttemptID})
			}
		}
		launchedAt, err := loadExternalEffectLaunchTime(txctx, tx, attempt.AttemptID, attempt.OperationID, true)
		if err != nil {
			return err
		}
		return recordExternalEffectStory(txctx, externalEffectStorySourceFromAttempt(attempt), runtimeeffects.StateLaunched, nil, launchedAt)
	})
}

func (s *SQLiteRuntimeStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) error {
	return s.runAuthorActivityMutation(ctx, "sqlite mark external attempt launched", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, attempt.Authority, false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET state = 'launched', launched_at = ?, updated_at = ? WHERE attempt_id = ? AND operation_id = ? AND execution_owner=? AND fence_generation=? AND state = 'authorized'`, now.UTC(), now.UTC(), attempt.AttemptID, attempt.OperationID, attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
		if err := requireExternalAttemptTransition(res, err); err == nil {
			operationRes, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_operations SET state = 'launched', updated_at = ? WHERE operation_id = ? AND state = 'authorized'`, now.UTC(), attempt.OperationID)
			if err := requireExternalAttemptTransition(operationRes, err); err != nil {
				return err
			}
		} else {
			var state string
			var operationState string
			if queryErr := tx.QueryRowContext(txctx, `SELECT a.state, o.state FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id WHERE a.attempt_id = ? AND a.operation_id = ?`, attempt.AttemptID, attempt.OperationID).Scan(&state, &operationState); queryErr != nil || state != string(runtimeeffects.StateLaunched) || operationState != string(runtimeeffects.StateLaunched) {
				return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "launch_attempt", map[string]any{"attempt_id": attempt.AttemptID})
			}
		}
		launchedAt, err := loadExternalEffectLaunchTime(txctx, tx, attempt.AttemptID, attempt.OperationID, false)
		if err != nil {
			return err
		}
		return recordExternalEffectStory(txctx, externalEffectStorySourceFromAttempt(attempt), runtimeeffects.StateLaunched, nil, launchedAt)
	})
}

func loadExternalEffectLaunchTime(ctx context.Context, tx *sql.Tx, attemptID, operationID string, postgres bool) (time.Time, error) {
	query := `SELECT launched_at FROM runtime_external_effect_attempts WHERE attempt_id = ? AND operation_id = ?`
	if postgres {
		query = `SELECT launched_at FROM runtime_external_effect_attempts WHERE attempt_id = $1::uuid AND operation_id = $2::uuid`
	}
	var launchedAtRaw any
	if err := tx.QueryRowContext(ctx, query, strings.TrimSpace(attemptID), strings.TrimSpace(operationID)).Scan(&launchedAtRaw); err != nil {
		return time.Time{}, fmt.Errorf("load launched external effect author activity source: %w", err)
	}
	launchedAt, ok, err := sqliteTimeValue(launchedAtRaw)
	if err != nil || !ok {
		return time.Time{}, fmt.Errorf("load launched external effect time: %w", firstNonNilError(err, fmt.Errorf("launched_at is required")))
	}
	return launchedAt.UTC(), nil
}

func (s *PostgresStore) HeartbeatCompletionAttempt(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_heartbeat_lease_invalid", "external-effects", "heartbeat_attempt", map[string]any{"attempt_id": attempt.AttemptID})
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("heartbeat completion attempt begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireExternalEffectAuthorityPostgres(ctx, tx, attempt.Authority, false); err != nil {
		return err
	}
	expires := now.UTC().Add(lease)
	res, err := tx.ExecContext(ctx, `
		UPDATE runtime_external_effect_attempts
		SET lease_expires_at=GREATEST(lease_expires_at,$3), updated_at=$4
		WHERE attempt_id=$1::uuid AND operation_id=$2::uuid
		  AND execution_owner=$5 AND fence_generation=$6
		  AND state IN ('authorized','launched','response_observed')
	`, attempt.AttemptID, attempt.OperationID, expires, now.UTC(), attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_heartbeat_conflict", "external-effects", "heartbeat_attempt", map[string]any{"attempt_id": attempt.AttemptID}, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("heartbeat completion attempt commit: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) HeartbeatCompletionAttempt(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_heartbeat_lease_invalid", "external-effects", "heartbeat_attempt", map[string]any{"attempt_id": attempt.AttemptID})
	}
	return s.runRuntimeMutation(ctx, "sqlite heartbeat completion attempt", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, attempt.Authority, false); err != nil {
			return err
		}
		expires := now.UTC().Add(lease)
		res, err := tx.ExecContext(txctx, `
			UPDATE runtime_external_effect_attempts
			SET lease_expires_at=CASE WHEN lease_expires_at>? THEN lease_expires_at ELSE ? END, updated_at=?
			WHERE attempt_id=? AND operation_id=?
			  AND execution_owner=? AND fence_generation=?
			  AND state IN ('authorized','launched','response_observed')
		`, expires, expires, now.UTC(), attempt.AttemptID, attempt.OperationID, attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
		if err := requireExternalAttemptTransition(res, err); err != nil {
			return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_heartbeat_conflict", "external-effects", "heartbeat_attempt", map[string]any{"attempt_id": attempt.AttemptID}, err)
		}
		return nil
	})
}

func (s *PostgresStore) MarkExternalAttemptResponseObserved(ctx context.Context, attempt runtimeeffects.Attempt, evidence map[string]any, now time.Time) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireExternalEffectAuthorityPostgres(ctx, tx, attempt.Authority, false); err != nil {
		return err
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("marshal response-observed evidence: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET state='response_observed', evidence=$3::jsonb, response_observed_at=$4, updated_at=$4 WHERE attempt_id=$1::uuid AND operation_id=$2::uuid AND execution_owner=$5 AND fence_generation=$6 AND state='launched'`, attempt.AttemptID, attempt.OperationID, string(raw), now.UTC(), attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='response_observed', updated_at=$2 WHERE operation_id=$1::uuid AND state='launched'`, attempt.OperationID, now.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) MarkExternalAttemptResponseObserved(ctx context.Context, attempt runtimeeffects.Attempt, evidence map[string]any, now time.Time) error {
	return s.runRuntimeMutation(ctx, "sqlite mark external attempt response observed", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, attempt.Authority, false); err != nil {
			return err
		}
		raw, err := json.Marshal(evidence)
		if err != nil {
			return fmt.Errorf("marshal sqlite response-observed evidence: %w", err)
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET state='response_observed', evidence=?, response_observed_at=?, updated_at=? WHERE attempt_id=? AND operation_id=? AND execution_owner=? AND fence_generation=? AND state='launched'`, string(raw), now.UTC(), now.UTC(), attempt.AttemptID, attempt.OperationID, attempt.Authority.ExecutionOwner, attempt.Authority.FenceGeneration)
		if err := requireExternalAttemptTransition(res, err); err != nil {
			return err
		}
		_, err = tx.ExecContext(txctx, `UPDATE runtime_external_effect_operations SET state='response_observed', updated_at=? WHERE operation_id=? AND state='launched'`, now.UTC(), attempt.OperationID)
		return err
	})
}

func requireExternalAttemptTransition(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "transition_attempt", nil)
	}
	return nil
}

func (s *PostgresStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	return s.runAuthorActivityMutation(ctx, "postgres settle external attempt", func(txctx context.Context, tx *sql.Tx) error {
		if settlement.Authority.Valid() {
			if err := requireExternalEffectAuthorityPostgres(txctx, tx, settlement.Authority, false); err != nil {
				return err
			}
		}
		if err := settleExternalAttemptPostgres(txctx, tx, settlement); err != nil {
			return err
		}
		return recordSettledExternalEffectStory(txctx, tx, settlement, true)
	})
}

func (s *SQLiteRuntimeStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	return s.runAuthorActivityMutation(ctx, "sqlite settle external attempt", func(txctx context.Context, tx *sql.Tx) error {
		if settlement.Authority.Valid() {
			if err := requireExternalEffectAuthoritySQLite(txctx, tx, settlement.Authority, false); err != nil {
				return err
			}
		}
		if err := settleExternalAttemptSQLiteTx(txctx, tx, settlement); err != nil {
			return err
		}
		return recordSettledExternalEffectStory(txctx, tx, settlement, false)
	})
}

func requireProviderHeadLifecyclePostgres(ctx context.Context, tx *sql.Tx, req completionProviderHeadRequest) error {
	if !req.Token.Valid() || strings.TrimSpace(req.Token.AgentID) != strings.TrimSpace(req.Identity.AgentID) {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_head_lifecycle_token_invalid", "external-effects", "settle_provider_head", map[string]any{"agent_id": req.Identity.AgentID})
	}
	var epoch, generation int64
	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id=$1 FOR UPDATE`, req.Token.AgentID).Scan(&epoch, &generation, &phase); err != nil {
		if err == sql.ErrNoRows {
			return supersededExternalAttempt(req.Token, 0, 0, "absent")
		}
		return fmt.Errorf("lock provider-head lifecycle: %w", err)
	}
	if epoch != req.Token.RuntimeEpoch || generation != int64(req.Token.Generation) || strings.TrimSpace(phase) != "running" {
		return supersededExternalAttempt(req.Token, epoch, generation, phase)
	}
	return nil
}

func requireProviderHeadLifecycleSQLiteTx(ctx context.Context, tx *sql.Tx, req completionProviderHeadRequest) error {
	if !req.Token.Valid() || strings.TrimSpace(req.Token.AgentID) != strings.TrimSpace(req.Identity.AgentID) {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_head_lifecycle_token_invalid", "external-effects", "settle_provider_head", map[string]any{"agent_id": req.Identity.AgentID})
	}
	var epoch, generation int64
	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase FROM agents WHERE agent_id=?`, req.Token.AgentID).Scan(&epoch, &generation, &phase); err != nil {
		if err == sql.ErrNoRows {
			return supersededExternalAttempt(req.Token, 0, 0, "absent")
		}
		return fmt.Errorf("lock sqlite provider-head lifecycle: %w", err)
	}
	if epoch != req.Token.RuntimeEpoch || generation != int64(req.Token.Generation) || strings.TrimSpace(phase) != "running" {
		return supersededExternalAttempt(req.Token, epoch, generation, phase)
	}
	return nil
}

func promoteProviderHeadPostgres(ctx context.Context, tx *sql.Tx, req completionProviderHeadRequest) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object('provider_session_id', $1::text),
		    updated_at = $2
		WHERE session_id = $3::uuid
		  AND run_id = $4::uuid
		  AND agent_id = $5
		  AND flow_instance = $6
		  AND status = 'active'
		  AND lease_holder = $7
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at > $2
		  AND COALESCE(runtime_state->>'provider_session_id', '') = $8
	`, strings.TrimSpace(req.NewProviderHead), req.Now.UTC(), strings.TrimSpace(req.SessionID), req.Identity.RunID, req.Identity.AgentID, req.Identity.FlowInstance, strings.TrimSpace(req.LockOwner), strings.TrimSpace(req.ExpectedProviderHead))
	if err != nil {
		return fmt.Errorf("promote provider head: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		var currentHead, attemptState string
		err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(s.runtime_state->>'provider_session_id', ''), a.state
			FROM agent_sessions s, runtime_external_effect_attempts a
			WHERE s.session_id=$1::uuid AND a.attempt_id=$2::uuid AND a.operation_id=$3::uuid
		`, strings.TrimSpace(req.SessionID), req.AttemptID, req.OperationID).Scan(&currentHead, &attemptState)
		if err == nil && currentHead == strings.TrimSpace(req.NewProviderHead) && attemptState == string(runtimeeffects.StateSettled) {
			return nil
		}
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_head_cas_conflict", "external-effects", "settle_provider_head", map[string]any{"session_id": req.SessionID, "expected_provider_head": req.ExpectedProviderHead})
	}
	return nil
}

func promoteProviderHeadSQLiteTx(ctx context.Context, tx *sql.Tx, req completionProviderHeadRequest) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = json_set(COALESCE(runtime_state, '{}'), '$.provider_session_id', ?),
		    updated_at = ?
		WHERE session_id = ?
		  AND run_id = ?
		  AND agent_id = ?
		  AND flow_instance = ?
		  AND status = 'active'
		  AND lease_holder = ?
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at > ?
		  AND COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') = ?
	`, strings.TrimSpace(req.NewProviderHead), req.Now.UTC(), strings.TrimSpace(req.SessionID), req.Identity.RunID, req.Identity.AgentID, req.Identity.FlowInstance, strings.TrimSpace(req.LockOwner), req.Now.UTC(), strings.TrimSpace(req.ExpectedProviderHead))
	if err != nil {
		return fmt.Errorf("promote sqlite provider head: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		var currentHead, attemptState string
		err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(json_extract(s.runtime_state, '$.provider_session_id'), ''), a.state
			FROM agent_sessions s, runtime_external_effect_attempts a
			WHERE s.session_id=? AND a.attempt_id=? AND a.operation_id=?
		`, strings.TrimSpace(req.SessionID), req.AttemptID, req.OperationID).Scan(&currentHead, &attemptState)
		if err == nil && currentHead == strings.TrimSpace(req.NewProviderHead) && attemptState == string(runtimeeffects.StateSettled) {
			return nil
		}
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_head_cas_conflict", "external-effects", "settle_provider_head", map[string]any{"session_id": req.SessionID, "expected_provider_head": req.ExpectedProviderHead})
	}
	return nil
}

func externalSettlementPayload(settlement runtimeeffects.Settlement) ([]byte, []byte, error) {
	evidence, err := json.Marshal(settlement.Evidence)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal external effect evidence: %w", err)
	}
	var failure []byte
	if settlement.Failure != nil {
		failure, err = json.Marshal(settlement.Failure)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal external effect failure: %w", err)
		}
	}
	return evidence, failure, nil
}

func settleExternalAttemptPostgres(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	evidence, failure, err := externalSettlementPayload(settlement)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE runtime_external_effect_attempts
		SET state = $3, evidence = $4::jsonb, failure = $5::jsonb,
		    completed_at = $6, updated_at = $6
		WHERE attempt_id = $1::uuid AND operation_id = $2::uuid
		  AND state IN ('authorized', 'launched', 'response_observed')
	`, settlement.AttemptID, settlement.OperationID, string(settlement.State), string(evidence), nullableJSON(failure), settlement.Now.UTC())
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return acceptRepeatedPostgresSettlement(ctx, tx, settlement)
	}
	_, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state = $2, completed_at = $3, updated_at = $3 WHERE operation_id = $1::uuid`, settlement.OperationID, string(settlement.State), settlement.Now.UTC())
	return err
}

func settleExternalAttemptSQLiteTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	evidence, failure, err := externalSettlementPayload(settlement)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE runtime_external_effect_attempts
		SET state = ?, evidence = ?, failure = ?, completed_at = ?, updated_at = ?
		WHERE attempt_id = ? AND operation_id = ?
		  AND state IN ('authorized', 'launched', 'response_observed')
	`, string(settlement.State), string(evidence), sqliteNullableJSON(failure), settlement.Now.UTC(), settlement.Now.UTC(), settlement.AttemptID, settlement.OperationID)
	if err := requireExternalAttemptTransition(res, err); err != nil {
		return acceptRepeatedSQLiteSettlement(ctx, tx, settlement)
	}
	_, err = tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state = ?, completed_at = ?, updated_at = ? WHERE operation_id = ?`, string(settlement.State), settlement.Now.UTC(), settlement.Now.UTC(), settlement.OperationID)
	return err
}

func acceptRepeatedPostgresSettlement(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id = $1::uuid AND operation_id = $2::uuid`, settlement.AttemptID, settlement.OperationID).Scan(&state)
	if err == nil && state == string(settlement.State) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "settle_attempt", map[string]any{"attempt_id": settlement.AttemptID, "current_state": state, "target_state": settlement.State})
}

func acceptRepeatedSQLiteSettlement(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement) error {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id = ? AND operation_id = ?`, settlement.AttemptID, settlement.OperationID).Scan(&state)
	if err == nil && state == string(settlement.State) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "external-effects", "settle_attempt", map[string]any{"attempt_id": settlement.AttemptID, "current_state": state, "target_state": settlement.State})
}

func nullableJSON(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func sqliteNullableJSON(raw []byte) any { return nullableJSON(raw) }

func externalEffectRecoveryFailure(class runtimefailures.Class, code string, now time.Time) ([]byte, error) {
	err := runtimefailures.New(class, code, "external-effects", "startup_reconcile", map[string]any{"recovered_at": now.UTC().Format(time.RFC3339Nano)})
	envelope, ok := runtimefailures.EnvelopeFromError(err)
	if !ok {
		return nil, fmt.Errorf("construct external effect recovery failure")
	}
	return json.Marshal(envelope)
}

func reconcileExternalEffectAttemptsPostgres(ctx context.Context, tx *sql.Tx, now time.Time) (runtimeeffects.RecoverySummary, error) {
	completionSummary, err := reconcileCompletionAttemptsPostgres(ctx, tx, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	prelaunchFailure, err := externalEffectRecoveryFailure(runtimefailures.ClassLifecycleConflict, "effect_recovery_prelaunch_abandoned", now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	uncertainFailure, err := externalEffectRecoveryFailure(runtimefailures.ClassOutcomeUncertain, "effect_recovery_outcome_unconfirmed", now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='terminal_failure', completed_at=$1, updated_at=$1 WHERE state='authorized' AND operation_id IN (SELECT a.operation_id FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state='authorized' AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+postgresExternalEffectActiveOwnerPredicate+`)`, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	prelaunch, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts a SET state='terminal_failure', failure=$1::jsonb, completed_at=$2, updated_at=$2 FROM runtime_external_effect_operations o WHERE o.operation_id=a.operation_id AND a.state='authorized' AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+postgresExternalEffectActiveOwnerPredicate, string(prelaunchFailure), now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='outcome_uncertain', completed_at=$1, updated_at=$1 WHERE state IN ('launched','response_observed') AND operation_id IN (SELECT a.operation_id FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state IN ('launched','response_observed') AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+postgresExternalEffectActiveOwnerPredicate+`)`, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	uncertain, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts a SET state='outcome_uncertain', failure=$1::jsonb, completed_at=$2, updated_at=$2 FROM runtime_external_effect_operations o WHERE o.operation_id=a.operation_id AND a.state IN ('launched','response_observed') AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+postgresExternalEffectActiveOwnerPredicate, string(uncertainFailure), now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	genericSummary, err := externalEffectRecoverySummary(prelaunch, uncertain)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if err := reconcileCompletionParentAuthoritiesPostgres(ctx, tx, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	completionSummary.PrelaunchTerminal += genericSummary.PrelaunchTerminal
	completionSummary.OutcomeUncertain += genericSummary.OutcomeUncertain
	return completionSummary, nil
}

func reconcileExternalEffectAttemptsSQLiteTx(ctx context.Context, tx *sql.Tx, now time.Time) (runtimeeffects.RecoverySummary, error) {
	completionSummary, err := reconcileCompletionAttemptsSQLite(ctx, tx, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	prelaunchFailure, err := externalEffectRecoveryFailure(runtimefailures.ClassLifecycleConflict, "effect_recovery_prelaunch_abandoned", now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	uncertainFailure, err := externalEffectRecoveryFailure(runtimefailures.ClassOutcomeUncertain, "effect_recovery_outcome_unconfirmed", now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='terminal_failure', completed_at=?, updated_at=? WHERE state='authorized' AND operation_id IN (SELECT a.operation_id FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state='authorized' AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+sqliteExternalEffectActiveOwnerPredicate+`)`, now, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	prelaunch, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET state='terminal_failure', failure=?, completed_at=?, updated_at=? WHERE state='authorized' AND operation_id IN (SELECT o.operation_id FROM runtime_external_effect_operations o WHERE o.operation_id=runtime_external_effect_attempts.operation_id AND (o.effect_kind<>'provider_turn' OR runtime_external_effect_attempts.usage_target_kind IS NULL) AND `+sqliteExternalEffectActiveOwnerPredicate+`)`, string(prelaunchFailure), now, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_operations SET state='outcome_uncertain', completed_at=?, updated_at=? WHERE state IN ('launched','response_observed') AND operation_id IN (SELECT a.operation_id FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.state IN ('launched','response_observed') AND (o.effect_kind<>'provider_turn' OR a.usage_target_kind IS NULL) AND `+sqliteExternalEffectActiveOwnerPredicate+`)`, now, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	uncertain, err := tx.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET state='outcome_uncertain', failure=?, completed_at=?, updated_at=? WHERE state IN ('launched','response_observed') AND operation_id IN (SELECT o.operation_id FROM runtime_external_effect_operations o WHERE o.operation_id=runtime_external_effect_attempts.operation_id AND (o.effect_kind<>'provider_turn' OR runtime_external_effect_attempts.usage_target_kind IS NULL) AND `+sqliteExternalEffectActiveOwnerPredicate+`)`, string(uncertainFailure), now, now)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	genericSummary, err := externalEffectRecoverySummary(prelaunch, uncertain)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if err := reconcileCompletionParentAuthoritiesSQLite(ctx, tx, now); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	completionSummary.PrelaunchTerminal += genericSummary.PrelaunchTerminal
	completionSummary.OutcomeUncertain += genericSummary.OutcomeUncertain
	return completionSummary, nil
}

func externalEffectRecoverySummary(prelaunch, uncertain sql.Result) (runtimeeffects.RecoverySummary, error) {
	prelaunchRows, err := prelaunch.RowsAffected()
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	uncertainRows, err := uncertain.RowsAffected()
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return runtimeeffects.RecoverySummary{PrelaunchTerminal: int(prelaunchRows), OutcomeUncertain: int(uncertainRows)}, nil
}
