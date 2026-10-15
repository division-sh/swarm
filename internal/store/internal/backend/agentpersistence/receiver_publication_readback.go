package agentpersistence

import (
	"context"
	"database/sql"
)

type ReceiverAgentIdentityStorage struct {
	ID, Config, Descriptor, Tools, Permissions string
	Memory                                     bool
	Model                                      string
}

func ReadReceiverAgentIdentityStorageTx(ctx context.Context, tx *sql.Tx, runID, instance string) (ReceiverAgentIdentityStorage, error) {
	var out ReceiverAgentIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT agent_id,CAST(config AS TEXT),CAST(runtime_descriptor AS TEXT),CAST(tools AS TEXT),CAST(permissions AS TEXT),memory_enabled,model FROM agents WHERE run_id=$1 AND flow_instance=$2`, runID, instance).Scan(&out.ID, &out.Config, &out.Descriptor, &out.Tools, &out.Permissions, &out.Memory, &out.Model)
	return out, err
}
