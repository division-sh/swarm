package llmpersistence

import (
	"context"
	"database/sql"
)

type PostgresSessionTerminationStorage struct {
	Status string
	Reason string
	Detail string
}

func ReadPostgresSessionTerminationStorageTx(ctx context.Context, tx *sql.Tx, sessionID string) (PostgresSessionTerminationStorage, error) {
	var out PostgresSessionTerminationStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(termination_reason, ''), COALESCE(termination_detail, '') FROM agent_sessions WHERE session_id = $1::uuid`, sessionID).Scan(&out.Status, &out.Reason, &out.Detail)
	if err != nil {
		return PostgresSessionTerminationStorage{}, err
	}
	return out, nil
}
