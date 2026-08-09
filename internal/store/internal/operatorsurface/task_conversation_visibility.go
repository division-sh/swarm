package operatorsurface

// CanonicalStatelessConversationVisibilitySourceSQL returns the canonical
// stateless conversation visibility contract: audits are visible only from
// agent_conversation_audits.
func CanonicalStatelessConversationVisibilitySourceSQL() string {
	return `
		SELECT
			session_id::text AS session_id,
			agent_id,
			agent_name_owner,
			agent_name_source,
			agent_route_presence,
			flow_scope_key,
			flow_instance_id,
			COALESCE(run_id::text, '') AS run_id,
			flow_instance,
			memory_enabled,
			memory_source,
			COALESCE(status, '') AS status,
			COALESCE(turn_count, 0) AS turn_count,
			COALESCE(runtime_state, '{}'::jsonb) AS runtime_state,
			COALESCE(conversation, '[]'::jsonb) AS conversation,
			updated_at,
			created_at
		FROM agent_conversation_audits
		WHERE status = 'active'
	`
}
