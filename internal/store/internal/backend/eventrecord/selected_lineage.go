package eventrecord

// SelectedForkLineageSQL is the complete existing lineage-owner relation used
// by live event readback and fixed-revision capture on both native stores.
const SelectedForkLineageSQL = `LEFT JOIN (
	SELECT candidate.*,
		COUNT(*) OVER (PARTITION BY candidate.fork_event_id) AS lineage_owner_count
	FROM (
		SELECT fork_event_id, source_run_id, source_event_id, selection_authority
		FROM run_fork_selected_contract_executions
		UNION ALL
		SELECT fork_event_id, source_run_id, source_event_id, selection_authority
		FROM run_fork_delivery_event_replays
		GROUP BY fork_event_id, source_run_id, source_event_id, selection_authority
	) candidate
) sf ON sf.fork_event_id = e.event_id`
