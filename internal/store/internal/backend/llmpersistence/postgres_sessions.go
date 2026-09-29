package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

var _ runtimesessions.Registry = (*LLMPostgresOwner)(nil)
var _ runtimellm.LiveSessionAcquirer = (*LLMPostgresOwner)(nil)

func (s *LLMPostgresOwner) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquirePostgresLiveSession(ctx, identity, lockOwner)
	return lease, err
}

func (s *LLMPostgresOwner) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquirePostgresLiveSession(ctx, identity, lockOwner)
}

func requirePostgresCurrentSessionGrantTx(ctx context.Context, tx *sql.Tx, lease *runtimesessions.Lease) error {
	var expiry time.Time
	err := tx.QueryRowContext(ctx, `
		SELECT lease_expires_at FROM agent_sessions
		WHERE session_id=$1::uuid AND run_id=$2::uuid AND lease_holder=$3
		  AND lease_grant_id=$4 AND status='active' FOR UPDATE
	`, lease.SessionID, lease.Identity.RunID, lease.LockOwner, lease.GrantID).Scan(&expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimesessions.ErrSessionLeased
	}
	if err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return fmt.Errorf("read post-lock session grant time: %w", err)
	}
	if !expiry.After(now) {
		return runtimesessions.ErrSessionLeased
	}
	return nil
}

func (s *LLMPostgresOwner) acquirePostgresLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	if lockOwner == "" {
		return nil, runtimellm.ConversationRecord{}, errors.New("lockOwner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	type acquired struct {
		lease  *runtimesessions.Lease
		record runtimellm.ConversationRecord
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (acquired, error) {
		var value acquired
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "acquire_hydrate", false); err != nil {
				return err
			}

			type row struct {
				sessionID, runID, status          string
				providerSessionID, retryReason    sql.NullString
				retriesFrom, leaseHolder, grantID sql.NullString
				leaseExpires                      sql.NullTime
				conversation, runtimeState        []byte
				turnCount                         int
			}
			var current row
			err := tx.QueryRowContext(sqlCtx, `
			SELECT session_id::text, run_id::text, status,
			       NULLIF(runtime_state->>'provider_session_id', ''), NULLIF(runtime_state->>'retry_reason', ''),
			       NULLIF(runtime_state->>'retries_from_session_id', ''), lease_holder, lease_grant_id, lease_expires_at,
			       COALESCE(conversation, '[]'::jsonb), COALESCE(runtime_state, '{}'::jsonb), COALESCE(turn_count, 0)
			FROM agent_sessions
			WHERE run_id = $1::uuid AND agent_id = $2 AND agent_name_owner = $3
			  AND agent_name_source = $4 AND agent_route_presence = $5
			  AND flow_scope_key = $6 AND flow_instance_id = $7 AND flow_instance = $8
			  AND status IN ('active', 'suspended')
			ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC
			LIMIT 1 FOR UPDATE
		`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(
				&current.sessionID, &current.runID, &current.status, &current.providerSessionID, &current.retryReason,
				&current.retriesFrom, &current.leaseHolder, &current.grantID, &current.leaseExpires,
				&current.conversation, &current.runtimeState, &current.turnCount,
			)
			var now time.Time
			if err := tx.QueryRowContext(sqlCtx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
				return fmt.Errorf("read selected-store session time: %w", err)
			}
			now = now.UTC().Truncate(time.Microsecond)
			newGrantID := uuid.NewString()
			if errors.Is(err, sql.ErrNoRows) {
				current.sessionID = uuid.NewString()
				current.status = "active"
				current.conversation = []byte("[]")
				current.runtimeState = []byte("{}")
				current.leaseHolder = sql.NullString{String: lockOwner, Valid: true}
				current.grantID = sql.NullString{String: newGrantID, Valid: true}
				current.leaseExpires = sql.NullTime{Time: now.Add(s.postgresSessionLockTTL()).Truncate(time.Microsecond), Valid: true}
				if err := tx.QueryRowContext(sqlCtx, `
				INSERT INTO agent_sessions (
					session_id, run_id, agent_id, agent_name_owner, agent_name_source,
					agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
					conversation, turn_count, runtime_state, lease_holder, lease_grant_id, lease_expires_at,
					status, created_at, updated_at
				) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', '[]'::jsonb, 0, '{}'::jsonb, $10, $11, $12, 'active', $13, $13)
				RETURNING session_id::text, run_id::text
			`, current.sessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
					fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lockOwner, newGrantID, current.leaseExpires.Time, now).Scan(&current.sessionID, &current.runID); err != nil {
					return fmt.Errorf("insert live session: %w", err)
				}
			} else if err != nil {
				return fmt.Errorf("load live session: %w", err)
			} else {
				if current.status == "suspended" {
					return runtimesessions.ErrSessionSuspended
				}
				if current.leaseHolder.Valid && current.leaseExpires.Valid && current.leaseExpires.Time.After(now) && current.leaseHolder.String != lockOwner {
					return runtimesessions.ErrSessionLeased
				}
				current.leaseHolder = sql.NullString{String: lockOwner, Valid: true}
				current.grantID = sql.NullString{String: newGrantID, Valid: true}
				current.leaseExpires = sql.NullTime{Time: now.Add(s.postgresSessionLockTTL()).Truncate(time.Microsecond), Valid: true}
				if _, err := tx.ExecContext(sqlCtx, `UPDATE agent_sessions SET lease_holder=$1, lease_grant_id=$2, lease_expires_at=$3, updated_at=$4 WHERE session_id=$5::uuid`, lockOwner, newGrantID, current.leaseExpires.Time, now, current.sessionID); err != nil {
					return fmt.Errorf("update live session lease: %w", err)
				}
			}
			value.record, err = decodeLiveConversationRecord(identity, current.sessionID, current.status, current.conversation, current.runtimeState, current.turnCount)
			if err != nil {
				return err
			}
			nextWake, err := storerunlifecycle.PostgresRunSessionNextWakeTx(sqlCtx, tx, identity.RunID, now)
			if err != nil {
				return err
			}
			if nextWake == nil {
				return errors.New("acquired live session has no exact lease expiry")
			}
			if _, err := attempt.RequestCompletion(sqlCtx, s.lifecycle, identity.RunID, nextWake); err != nil {
				return err
			}
			value.lease = &runtimesessions.Lease{
				SessionID: current.sessionID, ProviderSessionID: strings.TrimSpace(current.providerSessionID.String), Identity: identity,
				RetryReason: strings.TrimSpace(current.retryReason.String), RetriesFromSessionID: strings.TrimSpace(current.retriesFrom.String),
				LockOwner: lockOwner, GrantID: current.grantID.String, ExpiresAt: current.leaseExpires.Time,
			}
			return addAgentSessionFacts(attempt, current.runID, current.sessionID)
		})
		return value, err
	})
	if value, ok := result.Value(); ok {
		return value.lease, value.record, result.Err()
	}
	return nil, runtimellm.ConversationRecord{}, result.Err()
}

func (s *LLMPostgresOwner) Renew(ctx context.Context, lease *runtimesessions.Lease) (*runtimesessions.Lease, error) {
	if lease == nil || strings.TrimSpace(lease.GrantID) == "" {
		return nil, errors.New("exact session grant is required")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return nil, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (*runtimesessions.Lease, error) {
		var renewed *runtimesessions.Lease
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "renew", false); err != nil {
				return err
			}
			if err := requirePostgresCurrentSessionGrantTx(sqlCtx, tx, lease); err != nil {
				return err
			}
			var expiry time.Time
			err := tx.QueryRowContext(sqlCtx, `UPDATE agent_sessions SET lease_expires_at=GREATEST(lease_expires_at,clock_timestamp()+($1 * INTERVAL '1 microsecond')),updated_at=clock_timestamp()
				WHERE session_id=$2::uuid AND run_id=$3::uuid AND agent_id=$4 AND agent_name_owner=$5 AND agent_name_source=$6 AND agent_route_presence=$7 AND flow_scope_key=$8 AND flow_instance_id=$9 AND flow_instance=$10
				AND lease_holder=$11 AND lease_grant_id=$12 AND status='active'
				RETURNING lease_expires_at`, s.postgresSessionLockTTL().Microseconds(), lease.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lease.LockOwner, lease.GrantID).Scan(&expiry)
			if errors.Is(err, sql.ErrNoRows) {
				return runtimesessions.ErrSessionLeased
			}
			if err != nil {
				return err
			}
			copy := *lease
			copy.ExpiresAt = expiry.UTC()
			renewed = &copy
			if err := addAgentSessionFacts(attempt, identity.RunID, lease.SessionID); err != nil {
				return err
			}
			_, err = attempt.RequestCompletion(sqlCtx, s.lifecycle, identity.RunID, &expiry)
			return err
		})
		return renewed, err
	})
	if renewed, ok := result.Value(); ok {
		return renewed, result.Err()
	}
	return nil, result.Err()
}

func (s *LLMPostgresOwner) ReleaseOutcome(ctx context.Context, lease *runtimesessions.Lease) (runtimesessions.ReleaseResult, error) {
	if lease == nil {
		return runtimesessions.ReleaseResult{}, errors.New("nil lease")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimesessions.ReleaseResult{}, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return runtimesessions.ReleaseResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			var storedSessionID, storedRunID string
			err := tx.QueryRowContext(sqlCtx, `
			UPDATE agent_sessions SET lease_holder=NULL, lease_grant_id=NULL, lease_expires_at=NULL, updated_at=now()
			WHERE run_id=$1::uuid AND agent_id=$2 AND agent_name_owner=$3
			  AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6
			  AND flow_instance_id=$7 AND flow_instance=$8 AND session_id=$9::uuid
			  AND lease_holder=$10 AND lease_grant_id=$11 AND status='active'
			RETURNING session_id::text, run_id::text
		`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lease.SessionID, lease.LockOwner, lease.GrantID).Scan(&storedSessionID, &storedRunID)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("no active lease to release for agent=%s session=%s", identity.AgentID(), lease.SessionID)
			}
			if err != nil {
				return fmt.Errorf("release live session lease: %w", err)
			}
			if _, err := attempt.RequestCompletion(sqlCtx, s.lifecycle, identity.RunID, nil); err != nil {
				return err
			}
			return addAgentSessionFacts(attempt, storedRunID, storedSessionID)
		})
		return struct{}{}, err
	})
	if !result.Acknowledged() {
		return runtimesessions.ReleaseResult{}, result.Err()
	}
	return runtimesessions.ReleaseResult{Acknowledged: true}, result.Err()
}

func (s *LLMPostgresOwner) Rotate(ctx context.Context, leaseInput *runtimesessions.Lease, rotation runtimesessions.RotationMetadata) (*runtimesessions.Lease, error) {
	if leaseInput == nil || strings.TrimSpace(leaseInput.GrantID) == "" {
		return nil, errors.New("exact session grant is required")
	}
	identity, lockOwner := leaseInput.Identity, leaseInput.LockOwner
	request, err := runtimesessions.NormalizeRotationRequest(identity, lockOwner, rotation)
	if err != nil {
		return nil, err
	}
	identity, lockOwner, rotation = request.Identity, request.LockOwner, request.Metadata
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return nil, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (*runtimesessions.Lease, error) {
		var lease *runtimesessions.Lease
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "rotate", false); err != nil {
				return err
			}
			var receipt *runtimesessions.RotationReceipt
			if rotation.OperationID != "" {
				// Serialize the store-wide key before reading the receipt or current row.
				if _, err := tx.ExecContext(sqlCtx, `SELECT pg_advisory_xact_lock(2458, hashtext($1))`, rotation.OperationID); err != nil {
					return err
				}
				var digest string
				var raw []byte
				err := tx.QueryRowContext(sqlCtx, `SELECT rotation_request_digest, rotation_result FROM agent_sessions WHERE rotation_operation_id=$1`, rotation.OperationID).Scan(&digest, &raw)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil {
					receipt = &runtimesessions.RotationReceipt{OperationID: rotation.OperationID, RequestDigest: digest}
					if err := runtimesessions.CheckRotationReceiptRequest(request, receipt); err != nil {
						return err
					}
					if err := json.Unmarshal(raw, &receipt.Result); err != nil {
						return fmt.Errorf("decode rotation receipt: %w", err)
					}
				}
			}
			var currentID, currentRunID string
			var existingOwner sql.NullString
			var existingGrant sql.NullString
			var existingExpiry sql.NullTime
			var runtimeStateRaw []byte
			currentErr := tx.QueryRowContext(sqlCtx, `
			SELECT session_id::text, run_id::text, lease_holder, lease_grant_id, lease_expires_at, runtime_state
			FROM agent_sessions
			WHERE run_id=$1::uuid AND agent_id=$2 AND agent_name_owner=$3
			  AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6
			  AND flow_instance_id=$7 AND flow_instance=$8 AND status='active'
			ORDER BY created_at DESC LIMIT 1 FOR UPDATE
		`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&currentID, &currentRunID, &existingOwner, &existingGrant, &existingExpiry, &runtimeStateRaw)
			if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
				return currentErr
			}
			var now time.Time
			if err := tx.QueryRowContext(sqlCtx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
				return fmt.Errorf("read selected-store session time: %w", err)
			}
			now = now.UTC()
			if receipt != nil {
				var current *runtimesessions.Lease
				if currentErr == nil {
					var state struct {
						ProviderSessionID    string `json:"provider_session_id"`
						RetryReason          string `json:"retry_reason"`
						RetriesFromSessionID string `json:"retries_from_session_id"`
					}
					if err := json.Unmarshal(runtimeStateRaw, &state); err != nil {
						return fmt.Errorf("decode active session runtime state: %w", err)
					}
					current = &runtimesessions.Lease{SessionID: currentID, ProviderSessionID: state.ProviderSessionID, Identity: identity,
						RetryReason: state.RetryReason, RetriesFromSessionID: state.RetriesFromSessionID,
						LockOwner: existingOwner.String, GrantID: existingGrant.String, ExpiresAt: existingExpiry.Time}
				}
				lease, err = runtimesessions.ReplayRotation(request, receipt, current, now)
				return err
			}
			if currentErr != nil {
				return fmt.Errorf("no active session to rotate for agent=%s", identity.AgentID())
			}
			if currentID != leaseInput.SessionID || existingOwner.String != lockOwner || existingGrant.String != leaseInput.GrantID || !existingExpiry.Time.After(now) {
				return runtimesessions.ErrSessionLeased
			}
			newID := uuid.NewString()
			retryReason := rotation.RetryReason
			if _, err := tx.ExecContext(sqlCtx, `
			UPDATE agent_sessions SET status='terminated', termination_reason=$2, termination_detail=NULLIF($3,''),
			terminated_at=$4, successor_session_id=NULL, lease_holder=NULL, lease_grant_id=NULL, lease_expires_at=NULL, updated_at=$4
			WHERE session_id=$1::uuid AND status='active'
		`, currentID, rotation.TerminationReason.String(), retryReason, now); err != nil {
				return err
			}
			runtimeState, err := json.Marshal(map[string]any{"summary": rotation.CheckpointSummary, "retry_reason": retryReason, "retries_from_session_id": currentID})
			if err != nil {
				return err
			}
			conversation, err := json.Marshal(runtimellm.RotationCheckpointConversation(rotation.CheckpointSummary))
			if err != nil {
				return err
			}
			expires := now.Add(s.postgresSessionLockTTL()).Truncate(time.Microsecond)
			newGrantID := uuid.NewString()
			lease = &runtimesessions.Lease{SessionID: newID, Identity: identity, RetryReason: retryReason, RetriesFromSessionID: currentID, LockOwner: lockOwner, GrantID: newGrantID, ExpiresAt: expires}
			var receiptJSON []byte
			if rotation.OperationID != "" {
				receiptJSON, err = json.Marshal(lease)
				if err != nil {
					return err
				}
			}
			var newRunID string
			if err := tx.QueryRowContext(sqlCtx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source, conversation, turn_count, runtime_state,
				lease_holder, lease_grant_id, lease_expires_at, status, created_at, updated_at,
				rotation_operation_id, rotation_request_digest, rotation_result
			) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored',$10::jsonb,0,$11::jsonb,$12,$13,$14,'active',$15,$15,$16,$17,$18::jsonb)
			RETURNING session_id::text, run_id::text
		`, newID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, string(conversation), string(runtimeState), lockOwner, newGrantID, expires, now,
				rotationReceiptValue(rotation.OperationID, rotation.OperationID != ""), rotationReceiptValue(request.Digest, rotation.OperationID != ""), receiptJSON).Scan(&newID, &newRunID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(sqlCtx, `UPDATE agent_sessions SET successor_session_id=$2::uuid, updated_at=$3 WHERE session_id=$1::uuid AND status='terminated'`, currentID, newID, now); err != nil {
				return err
			}
			nextWake, err := storerunlifecycle.PostgresRunSessionNextWakeTx(sqlCtx, tx, identity.RunID, now)
			if err != nil {
				return err
			}
			if nextWake == nil {
				return errors.New("rotated live session has no exact lease expiry")
			}
			if _, err := attempt.RequestCompletion(sqlCtx, s.lifecycle, identity.RunID, nextWake); err != nil {
				return err
			}
			if err := addAgentSessionFacts(attempt, currentRunID, currentID); err != nil {
				return err
			}
			if err := addAgentSessionFacts(attempt, newRunID, newID); err != nil {
				return err
			}
			return nil
		})
		return lease, err
	})
	if lease, ok := result.Value(); ok {
		return lease, result.Err()
	}
	return nil, result.Err()
}

func (s *LLMPostgresOwner) IncrementTurnOutcome(ctx context.Context, lease *runtimesessions.Lease) (runtimesessions.TurnIncrementResult, error) {
	if lease == nil || strings.TrimSpace(lease.GrantID) == "" {
		return runtimesessions.TurnIncrementResult{}, errors.New("exact session grant is required")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimesessions.TurnIncrementResult{}, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return runtimesessions.TurnIncrementResult{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "increment_turn", false); err != nil {
				return err
			}
			if err := requirePostgresCurrentSessionGrantTx(sqlCtx, tx, lease); err != nil {
				return err
			}
			var storedSessionID, storedRunID string
			err := tx.QueryRowContext(sqlCtx, `
			UPDATE agent_sessions SET turn_count=turn_count+1, updated_at=now()
			WHERE run_id=$1::uuid AND agent_id=$2 AND agent_name_owner=$3
			  AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6
			  AND flow_instance_id=$7 AND flow_instance=$8 AND session_id=$9::uuid AND status='active'
			  AND lease_holder=$10 AND lease_grant_id=$11
			RETURNING session_id::text, run_id::text
		`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lease.SessionID, lease.LockOwner, lease.GrantID).Scan(&storedSessionID, &storedRunID)
			if errors.Is(err, sql.ErrNoRows) {
				return runtimesessions.ErrSessionLeased
			}
			if err != nil {
				return err
			}
			if err := addAgentSessionFacts(attempt, storedRunID, storedSessionID); err != nil {
				return err
			}
			var now time.Time
			if err := tx.QueryRowContext(sqlCtx, `SELECT CURRENT_TIMESTAMP`).Scan(&now); err != nil {
				return fmt.Errorf("read selected-store session time: %w", err)
			}
			nextWake, err := storerunlifecycle.PostgresRunSessionNextWakeTx(sqlCtx, tx, identity.RunID, now.UTC())
			if err != nil {
				return err
			}
			_, err = attempt.RequestCompletion(sqlCtx, s.lifecycle, identity.RunID, nextWake)
			return err
		})
		return struct{}{}, err
	})
	if !result.Acknowledged() {
		return runtimesessions.TurnIncrementResult{}, result.Err()
	}
	return runtimesessions.TurnIncrementResult{Acknowledged: true}, result.Err()
}

func (s *LLMPostgresOwner) ResetAll(metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.backend == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	source := strings.TrimSpace(metadata.Source)
	ctx := context.Background()
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (runtimesessions.ResetSummary, error) {
		var summary runtimesessions.ResetSummary
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(sqlCtx, `
			WITH affected AS (
				SELECT session_id, run_id, agent_id, flow_instance, status FROM agent_sessions
				WHERE status IN ('active', 'suspended') FOR UPDATE
			), updated AS (
				UPDATE agent_sessions AS current SET status='terminated', termination_reason='orphaned', termination_detail=NULLIF($1,''),
				terminated_at=COALESCE(current.terminated_at,now()), lease_holder=NULL, lease_grant_id=NULL, lease_expires_at=NULL, updated_at=now()
				FROM affected WHERE current.session_id=affected.session_id
				RETURNING affected.session_id::text, affected.run_id::text, affected.agent_id, affected.flow_instance, affected.status
			)
			SELECT session_id, run_id, agent_id, flow_instance, status FROM updated ORDER BY run_id, agent_id, flow_instance, session_id
		`, source)
			if err != nil {
				return fmt.Errorf("reset postgres live sessions: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var d runtimesessions.ResetDisposition
				if err := rows.Scan(&d.SessionID, &d.RunID, &d.AgentID, &d.FlowInstance, &d.PreviousStatus); err != nil {
					return fmt.Errorf("scan postgres live session reset: %w", err)
				}
				d.TerminationReason = runtimesessions.TerminationReasonOrphaned.String()
				d.TerminationDetail = source
				summary.OrphanedSessions = append(summary.OrphanedSessions, d)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("read postgres live session reset: %w", err)
			}
			if err := rows.Close(); err != nil {
				return fmt.Errorf("close postgres live session reset: %w", err)
			}
			seenRuns := make(map[string]struct{}, len(summary.OrphanedSessions))
			for _, disposition := range summary.OrphanedSessions {
				if err := addAgentSessionFacts(attempt, disposition.RunID, disposition.SessionID); err != nil {
					return err
				}
				if _, exists := seenRuns[disposition.RunID]; exists {
					continue
				}
				seenRuns[disposition.RunID] = struct{}{}
				if _, err := attempt.RequestCompletion(sqlCtx, s.lifecycle, disposition.RunID, nil); err != nil {
					return err
				}
			}
			return nil
		})
		return summary, err
	})
	if summary, ok := result.Value(); ok {
		return summary, result.Err()
	}
	return runtimesessions.ResetSummary{}, result.Err()
}

func (s *LLMPostgresOwner) postgresSessionLockTTL() time.Duration {
	if s == nil || s.sessionLockTTL <= 0 {
		return 120 * time.Second
	}
	return s.sessionLockTTL
}

func decodeLiveConversationRecord(identity agentmemory.Identity, sessionID, status string, rawMessages, runtimeStateRaw []byte, turnCount int) (runtimellm.ConversationRecord, error) {
	record := runtimellm.ConversationRecord{
		SessionID: sessionID, AgentID: identity.AgentID(), Identity: identity, Memory: agentmemory.Authored(true),
		TurnCount: turnCount, Status: status,
	}
	state, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("decode exact live session runtime_state: %w", err)
	}
	record.Summary = state.Summary
	record.RetryReason = state.RetryReason
	record.RetriesFromSessionID = state.RetriesFromSessionID
	if state.Watchdog != nil {
		record.Watchdog = &runtimellm.ConversationWatchdog{
			State: state.Watchdog.State, BlockingLayer: state.Watchdog.BlockingLayer, Action: state.Watchdog.Action,
			Outcome: state.Watchdog.Outcome, LastOutputAt: state.Watchdog.LastOutputAt, RecordedAt: state.Watchdog.RecordedAt,
		}
	}
	if len(rawMessages) == 0 {
		rawMessages = []byte("[]")
	}
	if err := json.Unmarshal(rawMessages, &record.Messages); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("decode exact live session conversation: %w", err)
	}
	return record, nil
}
