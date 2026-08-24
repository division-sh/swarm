package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
)

var _ runtimeeffects.CompletionContinuationStore = (*EffectPostgresOwner)(nil)
var _ runtimeeffects.CompletionContinuationStore = (*EffectSQLiteOwner)(nil)

type completionContinuationRow struct {
	operationID        string
	effectClass        string
	requestFingerprint string
	planFingerprint    string
	bundleHash         string
	attemptID          string
	attemptOrdinal     int
	adapter            string
	transport          string
	authorizedAt       time.Time
	evidence           string
	phase              string
	successor          string
	turnID             string
	runID              string
	agentID            string
	nameOwner          string
	nameSource         string
	routePresence      string
	flowScopeKey       string
	flowInstanceID     string
	flowInstance       string
	sessionID          string
	memoryEnabled      bool
	memorySource       string
	entityID           string
	surface            string
}

func (s *EffectPostgresOwner) RecoverCompletionContinuation(ctx context.Context, req runtimeeffects.CompletionContinuationRequest) (runtimeeffects.Attempt, bool, error) {
	var attempt runtimeeffects.Attempt
	var found bool
	err := s.runRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := requireCompletionContinuationRequest(req); err != nil {
			return err
		}
		if err := requireExternalEffectAuthorityPostgres(txctx, tx, req.Authority, false); err != nil {
			return err
		}
		if s.delivery == nil {
			return errors.New("completion continuation PostgreSQL delivery owner is not bound")
		}
		if err := s.delivery.ValidateProviderOriginTx(txctx, tx, req.Origin.Delivery); err != nil {
			return err
		}
		rows, err := loadCompletionContinuationRowsPostgres(txctx, tx, req.Origin)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if len(rows) != 1 {
			return fmt.Errorf("delivery %s has ambiguous settled completion continuations", req.Origin.Delivery.DeliveryID())
		}
		attempt, err = admitCompletionContinuationRow(txctx, req, rows[0])
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return attempt, found, err
}

func (s *EffectSQLiteOwner) RecoverCompletionContinuation(ctx context.Context, req runtimeeffects.CompletionContinuationRequest) (runtimeeffects.Attempt, bool, error) {
	var attempt runtimeeffects.Attempt
	var found bool
	err := s.runRuntimeMutation(ctx, "sqlite recover exact completion continuation", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireCompletionContinuationRequest(req); err != nil {
			return err
		}
		if err := requireExternalEffectAuthoritySQLite(txctx, tx, req.Authority, false); err != nil {
			return err
		}
		if s.delivery == nil {
			return errors.New("completion continuation SQLite delivery owner is not bound")
		}
		if err := s.delivery.ValidateProviderOriginTx(txctx, tx, req.Origin.Delivery); err != nil {
			return err
		}
		rows, err := loadCompletionContinuationRowsSQLite(txctx, tx, req.Origin)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if len(rows) != 1 {
			return fmt.Errorf("delivery %s has ambiguous settled completion continuations", req.Origin.Delivery.DeliveryID())
		}
		attempt, err = admitCompletionContinuationRow(txctx, req, rows[0])
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return attempt, found, err
}

func requireCompletionContinuationRequest(req runtimeeffects.CompletionContinuationRequest) error {
	if !req.Authority.Valid() || req.Authority.Kind != runtimeeffects.AuthorityNormalAgent ||
		req.Origin.Kind != runtimeeffects.CompletionOriginDelivery || req.Origin.Validate() != nil ||
		strings.TrimSpace(req.ExecutionAuthorityID) == "" || strings.TrimSpace(req.SessionID) == "" {
		return errors.New("completion continuation requires current normal authority, delivery claim, and session")
	}
	_, err := req.Memory.Normalize()
	return err
}

const postgresCompletionContinuationSelect = `
	SELECT o.operation_id::text,o.effect_class,o.request_fingerprint,COALESCE(o.capability_plan_fingerprint,''),o.bundle_hash,
	       a.attempt_id::text,a.attempt_ordinal,a.adapter,a.transport,a.authorized_at,a.evidence::text,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn::text,''),
	       t.turn_id::text,t.run_id::text,t.agent_id,t.agent_name_owner,t.agent_name_source,t.agent_route_presence,
	       t.flow_scope_key,t.flow_instance_id,t.flow_instance,t.session_id::text,t.memory_enabled,t.memory_source,
	       COALESCE(t.entity_id::text,''),surface.surface::text
	FROM runtime_external_effect_attempts a
	JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
	JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id
	JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
	WHERE a.origin_delivery_id=$1::uuid AND a.origin_run_id=$2::uuid AND a.origin_route_identity=$3
	  AND a.origin_subscriber_type=$4 AND a.origin_subscriber_id=$5
	  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
	  AND a.completion_continuation_active=TRUE
	LIMIT 2`

const sqliteCompletionContinuationSelect = `
	SELECT o.operation_id,o.effect_class,o.request_fingerprint,COALESCE(o.capability_plan_fingerprint,''),o.bundle_hash,
	       a.attempt_id,a.attempt_ordinal,a.adapter,a.transport,a.authorized_at,a.evidence,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn,''),
	       t.turn_id,t.run_id,t.agent_id,t.agent_name_owner,t.agent_name_source,t.agent_route_presence,
	       t.flow_scope_key,t.flow_instance_id,t.flow_instance,t.session_id,t.memory_enabled,t.memory_source,
	       COALESCE(t.entity_id,''),surface.surface
	FROM runtime_external_effect_attempts a
	JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
	JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id
	JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
	WHERE a.origin_delivery_id=? AND a.origin_run_id=? AND a.origin_route_identity=?
	  AND a.origin_subscriber_type=? AND a.origin_subscriber_id=?
	  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
	  AND a.completion_continuation_active=1
	LIMIT 2`

func loadCompletionContinuationRowsPostgres(ctx context.Context, tx *sql.Tx, origin runtimeeffects.CompletionOrigin) ([]completionContinuationRow, error) {
	claim := origin.Delivery
	rows, err := tx.QueryContext(ctx, postgresCompletionContinuationSelect, claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), string(claim.SubscriberClass()), claim.SubscriberID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCompletionContinuationRows(rows)
}

func loadCompletionContinuationRowsSQLite(ctx context.Context, tx *sql.Tx, origin runtimeeffects.CompletionOrigin) ([]completionContinuationRow, error) {
	claim := origin.Delivery
	rows, err := tx.QueryContext(ctx, sqliteCompletionContinuationSelect, claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), string(claim.SubscriberClass()), claim.SubscriberID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCompletionContinuationRows(rows)
}

func scanCompletionContinuationRows(rows *sql.Rows) ([]completionContinuationRow, error) {
	var result []completionContinuationRow
	for rows.Next() {
		var row completionContinuationRow
		var authorizedAt conversationForkTimeValue
		if err := rows.Scan(
			&row.operationID, &row.effectClass, &row.requestFingerprint, &row.planFingerprint, &row.bundleHash,
			&row.attemptID, &row.attemptOrdinal, &row.adapter, &row.transport, &authorizedAt, &row.evidence, &row.phase, &row.successor,
			&row.turnID, &row.runID, &row.agentID, &row.nameOwner, &row.nameSource, &row.routePresence,
			&row.flowScopeKey, &row.flowInstanceID, &row.flowInstance, &row.sessionID, &row.memoryEnabled, &row.memorySource,
			&row.entityID, &row.surface,
		); err != nil {
			return nil, err
		}
		if !authorizedAt.Valid {
			return nil, errors.New("settled completion continuation is missing authorized_at")
		}
		row.authorizedAt = authorizedAt.Time
		result = append(result, row)
	}
	return result, rows.Err()
}

func admitCompletionContinuationRow(ctx context.Context, req runtimeeffects.CompletionContinuationRequest, row completionContinuationRow) (runtimeeffects.Attempt, error) {
	bundleHash, err := requiredExternalEffectBundleHash(ctx, req.Authority)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	identity, err := storeagent.IdentityFromColumns(row.agentID, row.nameOwner, row.nameSource, row.routePresence, row.flowScopeKey, row.flowInstanceID, row.flowInstance)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	currentIdentity := req.Authority.Normal.Identity.Normalize()
	if equal, equalErr := runtimeagentidentity.Equal(identity, currentIdentity); equalErr != nil || !equal {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation actor identity is stale")
	}
	plan, err := agentmemory.NewPlan(row.memoryEnabled, agentmemory.Source(row.memorySource))
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	requestedPlan, err := req.Memory.Normalize()
	if err != nil || requestedPlan != plan || (plan.Enabled && strings.TrimSpace(req.SessionID) != strings.TrimSpace(row.sessionID)) || row.bundleHash != bundleHash {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation session, memory, or bundle identity is stale")
	}
	surface, err := storemanagedcapability.Decode([]byte(row.surface))
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	planFingerprint, err := managedCapabilityPlanFingerprint(&surface)
	if err != nil || planFingerprint != row.planFingerprint {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation capability plan is invalid")
	}
	projectedSessionID := row.sessionID
	if !plan.Enabled {
		projectedSessionID = strings.TrimSpace(req.SessionID)
	}
	surface, err = surface.ProjectNormalContinuation(req.ExecutionAuthorityID, projectedSessionID)
	if err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("project settled completion continuation: %w", err)
	}
	projectedFingerprint, err := managedCapabilityPlanFingerprint(&surface)
	if err != nil || projectedFingerprint != row.planFingerprint {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation projection changed the capability plan")
	}
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: row.turnID, RunID: row.runID, AgentID: row.agentID,
		AgentIdentity: identity, SessionID: projectedSessionID, Memory: plan, FlowInstance: row.flowInstance, EntityID: row.entityID,
	}
	if !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(target, surface) {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation target does not match capability surface")
	}
	authority := req.Authority
	authority.Target = target
	attempt := runtimeeffects.Attempt{
		OperationID: row.operationID, AttemptID: row.attemptID, Token: authority.Normal, Authority: authority,
		Kind: runtimeeffects.KindProviderTurn, Class: runtimeeffects.EffectClass(row.effectClass), Adapter: row.adapter,
		Transport: row.transport, Ordinal: row.attemptOrdinal, AuthorizedAt: row.authorizedAt.UTC(), Origin: req.Origin,
	}
	return runtimeeffects.AdmitCompletionContinuation(attempt, json.RawMessage(row.evidence), row.requestFingerprint, surface, runtimeeffects.CompletionProjectionPhase(row.phase), json.RawMessage(row.successor))
}

type lockedCompletionContinuation struct {
	requestFingerprint string
	planFingerprint    string
	bundleHash         string
	evidence           string
	phase              runtimeeffects.CompletionProjectionPhase
	successor          string
	surface            managedcapabilities.Surface
}

func admitSettledCompletionContinuation(ctx context.Context, attempt runtimeeffects.Attempt, row lockedCompletionContinuation, surfaceRaw string) (runtimeeffects.Attempt, error) {
	bundleHash, err := requiredExternalEffectBundleHash(ctx, attempt.Authority)
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	if row.bundleHash != bundleHash || row.phase != runtimeeffects.CompletionProjectionResponseSettled {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation bundle or phase is stale")
	}
	surface, err := storemanagedcapability.Decode([]byte(surfaceRaw))
	if err != nil {
		return runtimeeffects.Attempt{}, err
	}
	planFingerprint, err := managedCapabilityPlanFingerprint(&surface)
	if err != nil || planFingerprint != row.planFingerprint {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation capability plan is invalid")
	}
	if !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(attempt.Authority.Target, surface) {
		return runtimeeffects.Attempt{}, errors.New("settled completion continuation target does not match persisted capability surface")
	}
	return runtimeeffects.AdmitCompletionContinuation(attempt, json.RawMessage(row.evidence), row.requestFingerprint, surface, row.phase, nil)
}

func loadSettledCompletionContinuationPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (runtimeeffects.Attempt, error) {
	var row lockedCompletionContinuation
	var surfaceRaw string
	err := tx.QueryRowContext(ctx, `
		SELECT o.request_fingerprint,o.capability_plan_fingerprint,o.bundle_hash,a.evidence::text,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn::text,''),surface.surface::text
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
		WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid AND a.origin_delivery_id=$3::uuid
		  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
		  AND a.completion_continuation_active=TRUE
	`, attempt.AttemptID, attempt.OperationID, attempt.Origin.Delivery.DeliveryID()).Scan(&row.requestFingerprint, &row.planFingerprint, &row.bundleHash, &row.evidence, &row.phase, &row.successor, &surfaceRaw)
	if err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("load committed completion continuation: %w", err)
	}
	return admitSettledCompletionContinuation(ctx, attempt, row, surfaceRaw)
}

func loadSettledCompletionContinuationSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (runtimeeffects.Attempt, error) {
	var row lockedCompletionContinuation
	var surfaceRaw string
	err := tx.QueryRowContext(ctx, `
		SELECT o.request_fingerprint,o.capability_plan_fingerprint,o.bundle_hash,a.evidence,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn,''),surface.surface
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
		WHERE a.attempt_id=? AND a.operation_id=? AND a.origin_delivery_id=?
		  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
		  AND a.completion_continuation_active=1
	`, attempt.AttemptID, attempt.OperationID, attempt.Origin.Delivery.DeliveryID()).Scan(&row.requestFingerprint, &row.planFingerprint, &row.bundleHash, &row.evidence, &row.phase, &row.successor, &surfaceRaw)
	if err != nil {
		return runtimeeffects.Attempt{}, fmt.Errorf("load committed sqlite completion continuation: %w", err)
	}
	return admitSettledCompletionContinuation(ctx, attempt, row, surfaceRaw)
}

func (s *EffectPostgresOwner) ProjectCompletionConversation(ctx context.Context, attempt runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection) error {
	return s.runRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationPostgres(txctx, tx, attempt)
		if err != nil {
			return err
		}
		if err := validateCompletionProjection(attempt, projection, locked); err != nil {
			return err
		}
		if locked.phase == runtimeeffects.CompletionProjectionConversationProjected || locked.phase == runtimeeffects.CompletionProjectionResponseConsumed {
			return nil
		}
		if locked.phase != runtimeeffects.CompletionProjectionResponseSettled {
			return fmt.Errorf("completion continuation has invalid projection phase %q", locked.phase)
		}
		if projection.Memory.Enabled {
			record, err := completionConversationRecord(projection)
			if err != nil {
				return err
			}
			if err := s.llm.ProjectCompletionConversationTx(txctx, tx, record, projection.ExpectedTurnCount); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET completion_projection_phase='conversation_projected',updated_at=now() WHERE attempt_id=$1::uuid AND operation_id=$2::uuid AND completion_projection_phase='response_settled'`, attempt.AttemptID, attempt.OperationID)
		return requireExternalAttemptTransition(res, err)
	})
}

func (s *EffectSQLiteOwner) ProjectCompletionConversation(ctx context.Context, attempt runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection) error {
	return s.runRuntimeMutation(ctx, "sqlite project exact completion conversation", func(txctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationSQLite(txctx, tx, attempt)
		if err != nil {
			return err
		}
		if err := validateCompletionProjection(attempt, projection, locked); err != nil {
			return err
		}
		if locked.phase == runtimeeffects.CompletionProjectionConversationProjected || locked.phase == runtimeeffects.CompletionProjectionResponseConsumed {
			return nil
		}
		if locked.phase != runtimeeffects.CompletionProjectionResponseSettled {
			return fmt.Errorf("completion continuation has invalid projection phase %q", locked.phase)
		}
		now := time.Now().UTC()
		if projection.Memory.Enabled {
			record, err := completionConversationRecord(projection)
			if err != nil {
				return err
			}
			if err := s.llm.ProjectCompletionConversationTx(txctx, tx, record, projection.ExpectedTurnCount, now); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET completion_projection_phase='conversation_projected',updated_at=? WHERE attempt_id=? AND operation_id=? AND completion_projection_phase='response_settled'`, now, attempt.AttemptID, attempt.OperationID)
		return requireExternalAttemptTransition(res, err)
	})
}

func validateCompletionProjection(attempt runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection, locked lockedCompletionContinuation) error {
	if !attempt.Authority.Valid() || attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent || attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return errors.New("completion projection requires current normal delivery authority")
	}
	if projection.ExpectedTurnCount < 0 || projection.TurnCount != projection.ExpectedTurnCount+1 ||
		strings.TrimSpace(projection.SessionID) != attempt.Authority.Target.SessionID ||
		projection.Identity.Normalize() != (agentmemory.Identity{RunID: attempt.Authority.Target.RunID, Agent: attempt.Authority.Target.AgentIdentity}).Normalize() {
		return errors.New("completion projection session, identity, or turn transition is invalid")
	}
	plan, err := projection.Memory.Normalize()
	if err != nil || plan != attempt.Authority.Target.Memory {
		return errors.New("completion projection memory plan is invalid")
	}
	if len(bytes.TrimSpace(projection.Payload)) == 0 || !json.Valid(projection.Payload) || len(bytes.TrimSpace(projection.Messages)) == 0 || !json.Valid(projection.Messages) {
		return errors.New("completion projection requires exact continuation and conversation payloads")
	}
	admitted, err := runtimeeffects.AdmitCompletionContinuation(attempt, json.RawMessage(locked.evidence), locked.requestFingerprint, locked.surface, locked.phase, json.RawMessage(locked.successor))
	if err != nil {
		return err
	}
	snapshot, ok := admitted.CompletionContinuation()
	if !ok || !bytes.Equal(bytes.TrimSpace(snapshot.Payload), bytes.TrimSpace(projection.Payload)) {
		return errors.New("completion projection payload does not match immutable settlement")
	}
	return nil
}

func completionConversationRecord(projection runtimeeffects.CompletionConversationProjection) (runtimellm.ConversationRecord, error) {
	var messages []runtimellm.Message
	if err := json.Unmarshal(projection.Messages, &messages); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("decode completion conversation projection: %w", err)
	}
	return runtimellm.ConversationRecord{
		SessionID: projection.SessionID, AgentID: projection.Identity.AgentID(), Identity: projection.Identity,
		Memory: projection.Memory, Messages: messages, TurnCount: projection.TurnCount, Status: "active",
	}, nil
}

func (s *EffectPostgresOwner) ConsumeCompletionResponse(ctx context.Context, attempt runtimeeffects.Attempt, successor *agentframe.ToolContinuation) error {
	return s.runRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationPostgres(txctx, tx, attempt)
		if err != nil {
			return err
		}
		successorRaw, err := validateCompletionSuccessor(attempt, successor)
		if err != nil {
			return err
		}
		if locked.phase == runtimeeffects.CompletionProjectionResponseConsumed {
			return requireMatchingCompletionSuccessor(locked.successor, successor)
		}
		if locked.phase != runtimeeffects.CompletionProjectionConversationProjected {
			return fmt.Errorf("completion response cannot be consumed from phase %q", locked.phase)
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET completion_projection_phase='response_consumed',completion_successor_turn=$3::jsonb,updated_at=now() WHERE attempt_id=$1::uuid AND operation_id=$2::uuid AND completion_projection_phase='conversation_projected'`, attempt.AttemptID, attempt.OperationID, successorRaw)
		return requireExternalAttemptTransition(res, err)
	})
}

func (s *EffectSQLiteOwner) ConsumeCompletionResponse(ctx context.Context, attempt runtimeeffects.Attempt, successor *agentframe.ToolContinuation) error {
	return s.runRuntimeMutation(ctx, "sqlite consume exact completion response", func(txctx context.Context, tx *sql.Tx) error {
		locked, err := s.lockCompletionContinuationSQLite(txctx, tx, attempt)
		if err != nil {
			return err
		}
		successorRaw, err := validateCompletionSuccessor(attempt, successor)
		if err != nil {
			return err
		}
		if locked.phase == runtimeeffects.CompletionProjectionResponseConsumed {
			return requireMatchingCompletionSuccessor(locked.successor, successor)
		}
		if locked.phase != runtimeeffects.CompletionProjectionConversationProjected {
			return fmt.Errorf("completion response cannot be consumed from phase %q", locked.phase)
		}
		res, err := tx.ExecContext(txctx, `UPDATE runtime_external_effect_attempts SET completion_projection_phase='response_consumed',completion_successor_turn=?,updated_at=? WHERE attempt_id=? AND operation_id=? AND completion_projection_phase='conversation_projected'`, successorRaw, time.Now().UTC(), attempt.AttemptID, attempt.OperationID)
		return requireExternalAttemptTransition(res, err)
	})
}

func validateCompletionSuccessor(attempt runtimeeffects.Attempt, successor *agentframe.ToolContinuation) (any, error) {
	if successor == nil {
		return nil, nil
	}
	snapshot, ok := attempt.CompletionContinuation()
	if !ok {
		return nil, errors.New("completion response successor requires immutable continuation evidence")
	}
	if err := runtimellm.ValidateCompletionToolContinuation(snapshot.Payload, attempt.Adapter, *successor); err != nil {
		return nil, err
	}
	raw, err := successor.Encode()
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

func requireMatchingCompletionSuccessor(raw string, successor *agentframe.ToolContinuation) error {
	raw = strings.TrimSpace(raw)
	if raw == "" && successor == nil {
		return nil
	}
	if raw == "" || successor == nil {
		return errors.New("completion response successor does not match committed transition")
	}
	committed, err := agentframe.DecodeToolContinuation(json.RawMessage(raw))
	if err != nil || committed.ParentFrameID() != successor.ParentFrameID() || !bytes.Equal(committed.ToolResult(), successor.ToolResult()) {
		return errors.New("completion response successor does not match committed transition")
	}
	return nil
}

func (s *EffectPostgresOwner) lockCompletionContinuationPostgres(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (lockedCompletionContinuation, error) {
	if err := requireExternalEffectAuthorityPostgres(ctx, tx, attempt.Authority, false); err != nil {
		return lockedCompletionContinuation{}, err
	}
	if s.delivery == nil {
		return lockedCompletionContinuation{}, errors.New("completion continuation PostgreSQL delivery owner is not bound")
	}
	if err := s.delivery.ValidateProviderOriginTx(ctx, tx, attempt.Origin.Delivery); err != nil {
		return lockedCompletionContinuation{}, err
	}
	var row lockedCompletionContinuation
	var surfaceRaw string
	err := tx.QueryRowContext(ctx, `
		SELECT o.request_fingerprint,o.capability_plan_fingerprint,o.bundle_hash,a.evidence::text,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn::text,''),surface.surface::text
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
		WHERE a.attempt_id=$1::uuid AND a.operation_id=$2::uuid AND a.origin_delivery_id=$3::uuid
		  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
		  AND a.completion_continuation_active=TRUE
		FOR UPDATE
	`, attempt.AttemptID, attempt.OperationID, attempt.Origin.Delivery.DeliveryID()).Scan(&row.requestFingerprint, &row.planFingerprint, &row.bundleHash, &row.evidence, &row.phase, &row.successor, &surfaceRaw)
	if err != nil {
		return lockedCompletionContinuation{}, fmt.Errorf("lock completion continuation: %w", err)
	}
	return validateLockedCompletionContinuation(ctx, attempt, row, surfaceRaw)
}

func (s *EffectSQLiteOwner) lockCompletionContinuationSQLite(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (lockedCompletionContinuation, error) {
	if err := requireExternalEffectAuthoritySQLite(ctx, tx, attempt.Authority, false); err != nil {
		return lockedCompletionContinuation{}, err
	}
	if s.delivery == nil {
		return lockedCompletionContinuation{}, errors.New("completion continuation SQLite delivery owner is not bound")
	}
	if err := s.delivery.ValidateProviderOriginTx(ctx, tx, attempt.Origin.Delivery); err != nil {
		return lockedCompletionContinuation{}, err
	}
	var row lockedCompletionContinuation
	var surfaceRaw string
	err := tx.QueryRowContext(ctx, `
		SELECT o.request_fingerprint,o.capability_plan_fingerprint,o.bundle_hash,a.evidence,COALESCE(a.completion_projection_phase,''),COALESCE(a.completion_successor_turn,''),surface.surface
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN managed_agent_capability_surfaces surface ON surface.surface_id=a.capability_surface_id
		WHERE a.attempt_id=? AND a.operation_id=? AND a.origin_delivery_id=?
		  AND a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND o.authority_kind='normal_agent'
		  AND a.completion_continuation_active=1
	`, attempt.AttemptID, attempt.OperationID, attempt.Origin.Delivery.DeliveryID()).Scan(&row.requestFingerprint, &row.planFingerprint, &row.bundleHash, &row.evidence, &row.phase, &row.successor, &surfaceRaw)
	if err != nil {
		return lockedCompletionContinuation{}, fmt.Errorf("lock sqlite completion continuation: %w", err)
	}
	return validateLockedCompletionContinuation(ctx, attempt, row, surfaceRaw)
}

func validateLockedCompletionContinuation(ctx context.Context, attempt runtimeeffects.Attempt, row lockedCompletionContinuation, surfaceRaw string) (lockedCompletionContinuation, error) {
	bundleHash, err := requiredExternalEffectBundleHash(ctx, attempt.Authority)
	if err != nil {
		return lockedCompletionContinuation{}, err
	}
	if row.bundleHash != bundleHash || !runtimeeffects.CompletionProjectionPhase(row.phase).Valid() {
		return lockedCompletionContinuation{}, errors.New("completion continuation bundle or projection phase is stale")
	}
	surface, err := storemanagedcapability.Decode([]byte(surfaceRaw))
	if err != nil {
		return lockedCompletionContinuation{}, err
	}
	persistedFingerprint, err := managedCapabilityPlanFingerprint(&surface)
	if err != nil || persistedFingerprint != row.planFingerprint {
		return lockedCompletionContinuation{}, errors.New("completion continuation capability plan is invalid")
	}
	snapshot, ok := attempt.CompletionContinuation()
	if !ok {
		return lockedCompletionContinuation{}, errors.New("completion continuation attempt is missing immutable evidence")
	}
	surface, err = surface.ProjectNormalContinuation(snapshot.Surface.Authority.ExecutionAuthorityID, attempt.Authority.Target.SessionID)
	if err != nil {
		return lockedCompletionContinuation{}, fmt.Errorf("project locked completion continuation: %w", err)
	}
	projectedFingerprint, err := managedCapabilityPlanFingerprint(&surface)
	if err != nil || projectedFingerprint != row.planFingerprint {
		return lockedCompletionContinuation{}, errors.New("completion continuation projection changed the capability plan")
	}
	if !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(attempt.Authority.Target, surface) {
		return lockedCompletionContinuation{}, errors.New("completion continuation target does not match persisted capability surface")
	}
	row.surface = surface
	row.phase = runtimeeffects.CompletionProjectionPhase(row.phase)
	return row, nil
}
