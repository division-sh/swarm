package store

// CanonicalTaskConversationVisibilitySourceSQL returns the canonical task
// conversation visibility contract: task conversations are visible only from
// agent_conversation_audits.
func CanonicalTaskConversationVisibilitySourceSQL(caps ConversationSchemaCapabilities) string {
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
}
