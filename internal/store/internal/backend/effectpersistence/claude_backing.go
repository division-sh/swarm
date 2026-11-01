package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func (s *EffectPostgresOwner) ClaudeBackingOrigin(ctx context.Context, identity agentidentity.Identity, session, head string) (string, error) {
	if err := s.requireCurrent(); err != nil {
		return "", err
	}
	return readClaudeBackingOrigin(ctx, s.backend.RunReadTransaction, identity, session, head, `s.runtime_state->>'provider_session_id'`)
}

func (s *EffectSQLiteOwner) ClaudeBackingOrigin(ctx context.Context, identity agentidentity.Identity, session, head string) (string, error) {
	if err := s.requireCurrent(); err != nil {
		return "", err
	}
	return readClaudeBackingOrigin(ctx, s.backend.RunReadTransaction, identity, session, head, `json_extract(s.runtime_state,'$.provider_session_id')`)
}

func claudeBackingLineageError(reason string) error {
	return failures.New(failures.ClassLifecycleConflict, "claude_provider_backing_lineage_invalid", "external-effects", "resolve_claude_backing", map[string]any{"reason": reason})
}

type claudeBackingLink struct {
	bundle, parent string
}

// One snapshot reads the exact session's CLI turns. Only explicit parent links
// reachable from its confirmed physical head select backing, never timestamps.
func readClaudeBackingOrigin(ctx context.Context, read func(context.Context, func(context.Context, *sql.Tx) error) error, identity agentidentity.Identity, session, head, currentHead string) (string, error) {
	fields, err := identity.StorageFields()
	if err != nil {
		return "", claudeBackingLineageError("invalid_actor")
	}
	for _, id := range []string{session, head} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return "", claudeBackingLineageError("invalid_session_or_head")
		}
	}
	links := map[string]claudeBackingLink{}
	err = read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
			SELECT CAST(a.attempt_id AS TEXT), o.bundle_hash, t.request_payload, o.agent_frame_bytes,
			       a.state='settled' AND o.state='settled' AND o.effect_kind='provider_turn' AND t.failure IS NULL
			       AND t.memory_enabled=TRUE AND a.usage_target_kind='agent_turn' AND a.usage_target_id=t.turn_id
			       AND t.run_id=s.run_id AND t.agent_id=s.agent_id
			       AND t.agent_name_owner=s.agent_name_owner AND t.agent_name_source=s.agent_name_source
			       AND t.agent_route_presence=s.agent_route_presence AND t.flow_scope_key=s.flow_scope_key
			       AND t.flow_instance_id=s.flow_instance_id AND t.flow_instance=s.flow_instance
			       AND t.agent_frame_bytes=o.agent_frame_bytes
			FROM agent_sessions s
			JOIN agent_turns t ON t.session_id=s.session_id
			JOIN runtime_external_effect_attempts a ON a.attempt_id=t.completion_attempt_id
			JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			WHERE s.session_id=$1 AND %s=$2 AND s.status='active'
			  AND s.run_id=$3 AND s.agent_id=$4 AND s.agent_name_owner=$5 AND s.agent_name_source=$6
			  AND s.agent_route_presence=$7 AND s.flow_scope_key=$8 AND s.flow_instance_id=$9 AND s.flow_instance=$10
			  AND a.adapter='claude_cli'`, currentHead), session, head, fields.RunID, fields.AgentID,
			fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, bundle string
			var request, frame []byte
			var matches bool
			if err := rows.Scan(&id, &bundle, &request, &frame, &matches); err != nil {
				return err
			}
			var input struct {
				Parent *string `json:"provider_session_id"`
			}
			stored, decodeErr := agentframe.DecodeDurable(frame)
			if !matches || decodeErr != nil || stored.Session.AgentIdentity.Normalize() != identity.Normalize() ||
				stored.Session.SourceArtifact.Hash != bundle || !json.Valid(request) || json.Unmarshal(request, &input) != nil || input.Parent == nil {
				continue
			}
			links[id] = claudeBackingLink{bundle: bundle, parent: *input.Parent}
		}
		return rows.Err()
	})
	if err != nil {
		return "", err
	}
	for head != "" {
		link, found := links[head]
		if !found {
			return "", claudeBackingLineageError("missing_foreign_contradictory_or_cyclic_link")
		}
		delete(links, head)
		if link.parent == "" {
			return link.bundle, nil
		}
		head = link.parent
	}
	return "", claudeBackingLineageError("missing_root")
}
