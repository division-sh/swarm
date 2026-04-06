package store

// CanonicalTaskConversationVisibilitySourceSQL returns the explicit mixed-rollout
// task-conversation visibility contract: canonical audit rows are authoritative,
// and legacy task rows in agent_sessions are only visible until the same
// session_id has been adopted into agent_conversation_audits.
func CanonicalTaskConversationVisibilitySourceSQL(caps ConversationSchemaCapabilities) string {
	if caps.Audits != SchemaFlavorCanonical {
		return ""
	}
	auditSource := `
		SELECT
			session_id::text AS session_id,
			agent_id,
			COALESCE(scope_key, '') AS scope_key,
			COALESCE(scope, '') AS scope,
			COALESCE(runtime_mode, '') AS runtime_mode,
			COALESCE(status, '') AS status,
			COALESCE(turn_count, 0) AS turn_count,
			COALESCE(runtime_state, '{}'::jsonb) AS runtime_state,
			COALESCE(conversation, '[]'::jsonb) AS conversation,
			updated_at,
			created_at
		FROM agent_conversation_audits
		WHERE status = 'active'
	`
	if caps.Sessions != SchemaFlavorCanonical {
		return auditSource
	}
	return auditSource + `
		UNION ALL
		SELECT
			legacy.session_id::text AS session_id,
			legacy.agent_id,
			COALESCE(legacy.scope_key, '') AS scope_key,
			COALESCE(legacy.scope, '') AS scope,
			COALESCE(legacy.runtime_mode, '') AS runtime_mode,
			COALESCE(legacy.status, '') AS status,
			COALESCE(legacy.turn_count, 0) AS turn_count,
			COALESCE(legacy.runtime_state, '{}'::jsonb) AS runtime_state,
			COALESCE(legacy.conversation, '[]'::jsonb) AS conversation,
			legacy.updated_at,
			legacy.created_at
		FROM agent_sessions legacy
		WHERE legacy.status = 'active'
		  AND legacy.runtime_mode = 'task'
		  AND NOT EXISTS (
			SELECT 1
			FROM agent_conversation_audits canonical
			WHERE canonical.session_id = legacy.session_id
		  )
	`
}
