package apiidempotency

import (
	"context"
	"database/sql"
)

type GateCompletionAPIStorage struct{ Method, ActorKind, Actor, Key, Hash, ResourceID, Response string }

func ReadGateCompletionAPIStorageTx(ctx context.Context, tx *sql.Tx, cardID, method, key string) ([]GateCompletionAPIStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT method,actor_kind,actor_id,idempotency_key,request_hash,COALESCE(resource_id,''),CAST(response AS TEXT)
		FROM api_idempotency WHERE resource_id=$1 OR (method=$2 AND idempotency_key=$3) ORDER BY method,actor_kind,actor_id,idempotency_key`, cardID, method, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GateCompletionAPIStorage
	for rows.Next() {
		var row GateCompletionAPIStorage
		if err := rows.Scan(&row.Method, &row.ActorKind, &row.Actor, &row.Key, &row.Hash, &row.ResourceID, &row.Response); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
