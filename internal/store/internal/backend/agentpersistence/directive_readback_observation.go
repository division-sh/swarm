package agentpersistence

import (
	"context"
	"database/sql"
)

type DirectiveIdentityStorage struct{ OperationID, EventID, RunID, State string }
type DirectiveFailureStorage struct {
	State   string
	Failure []byte
}
type DirectiveReplayStorage struct {
	OperationID, EventID, State string
	Response, Failure           sql.NullString
}

func ReadDirectiveIdentityStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, key string) (DirectiveIdentityStorage, error) {
	query := `SELECT operation_id, directive_event_id, resolved_run_id, state FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`
	if postgres {
		query = `SELECT operation_id::text, directive_event_id::text, resolved_run_id::text, state FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`
	}
	var out DirectiveIdentityStorage
	err := tx.QueryRowContext(ctx, query, key).Scan(&out.OperationID, &out.EventID, &out.RunID, &out.State)
	return out, err
}

func CountKeyedDirectiveOperationsStorageTx(ctx context.Context, tx *sql.Tx, key string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&count)
	return count, err
}

func ReadDirectiveFailureStorageTx(ctx context.Context, tx *sql.Tx, key string) (DirectiveFailureStorage, error) {
	var out DirectiveFailureStorage
	err := tx.QueryRowContext(ctx, `SELECT state, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&out.State, &out.Failure)
	return out, err
}

func ReadDirectiveReplayStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, key string) (DirectiveReplayStorage, error) {
	query := `SELECT operation_id, directive_event_id, state, response, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`
	if postgres {
		query = `SELECT operation_id::text, directive_event_id::text, state, response, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`
	}
	var out DirectiveReplayStorage
	err := tx.QueryRowContext(ctx, query, key).Scan(&out.OperationID, &out.EventID, &out.State, &out.Response, &out.Failure)
	return out, err
}
