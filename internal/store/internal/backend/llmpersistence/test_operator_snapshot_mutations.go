package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// This is a storage-snapshot fixture, not a second implementation of Rotate:
// the original native session must be active and have no outstanding grant.
func ReplaceOperatorSnapshotSessionTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, predecessor, successor string, postgres, renamed bool) error {
	fields, err := identity.StorageFields()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	query := `UPDATE agent_sessions SET status='terminated',termination_reason='normal',terminated_at=$1,updated_at=$1 WHERE session_id=$2 AND run_id=$3 AND agent_id=$4 AND agent_name_owner=$5 AND agent_name_source=$6 AND agent_route_presence=$7 AND flow_scope_key=$8 AND flow_instance_id=$9 AND flow_instance=$10 AND status='active' AND lease_holder IS NULL AND lease_grant_id IS NULL`
	if renamed {
		query = strings.Replace(query, "UPDATE agent_sessions ", "UPDATE agent_sessions_snapshot_source ", 1)
	}
	result, err := tx.ExecContext(ctx, query, now, predecessor, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err := requireOperatorSnapshotOneRow(result, err); err != nil {
		return err
	}
	// Copy the issued session's exact identity and memory mode; do not fabricate
	// either a lifecycle token or lease to produce the atomicity cut.
	query = `INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,conversation,turn_count,runtime_state,status,created_at,updated_at)
 SELECT $1,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,'[]',0,'{}','active',$2,$2 FROM agent_sessions WHERE session_id=$3 AND run_id=$4 AND status='terminated'`
	if renamed {
		query = strings.ReplaceAll(query, "agent_sessions ", "agent_sessions_snapshot_source ")
	}
	result, err = tx.ExecContext(ctx, query, successor, now, predecessor, fields.RunID)
	return requireOperatorSnapshotOneRow(result, err)
}

func ReplaceOperatorSnapshotTurnSummaryTx(ctx context.Context, tx *sql.Tx, turnID, output string, postgres, renamed bool) error {
	raw, err := json.Marshal([]struct {
		Kind string `json:"kind"`
		Data struct {
			Output  string `json:"assistant_visible_output"`
			Outcome string `json:"outcome"`
		} `json:"data"`
	}{{Kind: "turn_summary", Data: struct {
		Output  string `json:"assistant_visible_output"`
		Outcome string `json:"outcome"`
	}{output, "completed"}}})
	if err != nil {
		return err
	}
	return replaceOperatorSnapshotTurnBlocksTx(ctx, tx, turnID, string(raw), postgres, renamed)
}

func CorruptOperatorSnapshotTurnBlocksTx(ctx context.Context, tx *sql.Tx, turnID string, postgres bool) error {
	return replaceOperatorSnapshotTurnBlocksTx(ctx, tx, turnID, `{}`, postgres, false)
}

func replaceOperatorSnapshotTurnBlocksTx(ctx context.Context, tx *sql.Tx, turnID, raw string, postgres, renamed bool) error {
	query := `UPDATE agent_turns SET turn_blocks=$1 WHERE turn_id=$2`
	if postgres {
		query = `UPDATE agent_turns SET turn_blocks=$1::jsonb WHERE turn_id=$2`
	}
	if renamed {
		query = strings.Replace(query, "UPDATE agent_turns ", "UPDATE agent_turns_snapshot_source ", 1)
	}
	result, err := tx.ExecContext(ctx, query, raw, turnID)
	return requireOperatorSnapshotOneRow(result, err)
}

func CorruptOperatorSnapshotSessionStateTx(ctx context.Context, tx *sql.Tx, sessionID string, postgres bool) error {
	value := "$1"
	if postgres {
		value += "::jsonb"
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET runtime_state=`+value+` WHERE session_id=$2 AND status='active'`, `"invalid"`, sessionID)
	return requireOperatorSnapshotOneRow(result, err)
}

func requireOperatorSnapshotOneRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("operator snapshot mutation changed%d rows, want1", n)
	}
	return nil
}
