package llmpersistence

import (
	"context"
	"database/sql"
)

type MemoryEnabledSessionStorage struct {
	SessionID, RunID, AgentID, FlowInstance, FlowTemplate string
	TurnCount                                             int
}

func ReadMemoryEnabledSessionsStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) ([]MemoryEnabledSessionStorage, error) {
	query := `SELECT s.session_id, s.run_id, s.agent_id, s.flow_instance, COALESCE(fi.flow_template, ''), s.turn_count
		FROM agent_sessions s
		LEFT JOIN flow_instances fi ON fi.run_id = s.run_id AND fi.instance_path = s.flow_instance
		WHERE s.memory_enabled = 1
		ORDER BY s.flow_instance`
	if postgres {
		query = `SELECT s.session_id::text, s.run_id::text, s.agent_id, s.flow_instance, COALESCE(fi.flow_template, ''), s.turn_count
			FROM agent_sessions s
			LEFT JOIN flow_instances fi ON fi.run_id = s.run_id AND fi.instance_path = s.flow_instance
			WHERE s.memory_enabled
			ORDER BY s.flow_instance`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemoryEnabledSessionStorage
	for rows.Next() {
		var row MemoryEnabledSessionStorage
		if err := rows.Scan(&row.SessionID, &row.RunID, &row.AgentID, &row.FlowInstance, &row.FlowTemplate, &row.TurnCount); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
