package runforkrevision

// replyContextProjectionSpec retains the native reply owner's complete record.
// origin_route includes the request's exact return admissions, not a new route.
func replyContextProjectionSpec() projectionSpec {
	const query = `SELECT r.reply_context_id, CAST(r.run_id AS TEXT),
		CAST(r.request_event_id AS TEXT), r.requester_flow_id,
		r.request_output_pin, r.reply_input_pin, r.provider_flow_id,
		r.provider_input_pin, r.provider_output_pin, r.origin_route,
		r.request_correlation_id, COALESCE(r.correlation_key, ''), r.state,
		COALESCE(CAST(r.accepted_reply_event_id AS TEXT), ''),
		r.created_at, r.updated_at, r.terminal_at
		FROM reply_contexts r WHERE r.run_id = $1`
	return projectionSpec{
		query: query, source: "reply_contexts r", runAlias: "r",
		columns: typedColumns(map[string]valueKind{
			"origin_route": valueJSON,
			"created_at":   valueTime, "updated_at": valueTime, "terminal_at": valueTime,
		}, "reply_context_id", "run_id", "request_event_id", "requester_flow_id",
			"request_output_pin", "reply_input_pin", "provider_flow_id",
			"provider_input_pin", "provider_output_pin", "origin_route",
			"request_correlation_id", "correlation_key", "state",
			"accepted_reply_event_id", "created_at", "updated_at", "terminal_at"),
		parts: []projectionPart{{query: query, alias: "r"}},
	}
}
