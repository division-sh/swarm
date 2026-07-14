package store

// CanonicalStatelessConversationVisibilitySourceSQL returns the canonical
// stateless conversation visibility contract: audits are visible only from
// agent_conversation_audits.
func CanonicalStatelessConversationVisibilitySourceSQL(caps ConversationSchemaCapabilities) string {
	if caps.Audits != SchemaFlavorCanonical {
		return ""
	}
	runID := "''"
	if caps.AuditRunID {
		runID = "COALESCE(run_id::text, '')"
	}
	// Keep run_id in the projection even when the audit table lacks that
	// column so shared conversation readers can always select it safely.
	return `
		SELECT
			session_id::text AS session_id,
			agent_id,
			` + runID + ` AS run_id,
			COALESCE(flow_instance, '') AS flow_instance,
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
