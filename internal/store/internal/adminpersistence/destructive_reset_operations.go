package adminpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

const resetOperationColumns = "operation_id, actor_token_id, idempotency_key, revision, phase, active_slot, record"

// These functions run inside the selected retained process transaction. The
// reset family, not a backend adapter, validates identities and transitions.
func LookupResetOperationTx(ctx context.Context, tx *sql.Tx, req destructivereset.Request) (*destructivereset.Operation, error) {
	initial, err := destructivereset.NewOperation(req)
	if err != nil {
		return nil, err
	}
	req = initial.Request
	query := `SELECT ` + resetOperationColumns + ` FROM runtime_reset_operations WHERE operation_id = $1`
	args := []any{req.OperationID}
	if req.IdempotencyKey != "" {
		query = `SELECT ` + resetOperationColumns + ` FROM runtime_reset_operations WHERE operation_id = $1 OR (actor_token_id = $2 AND idempotency_key = $3)`
		args = append(args, req.ActorTokenID, req.IdempotencyKey)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var found *destructivereset.Operation
	for rows.Next() {
		op, err := scanResetOperation(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if found != nil || !op.Matches(req) {
			rows.Close()
			return nil, &destructivereset.OperationConflictError{OperationID: op.Request.OperationID, OriginalRequestHash: op.Request.RequestHash, ConflictingRequestHash: req.RequestHash}
		}
		found = &op
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if found != nil {
		return found, nil
	}
	return nil, nil
}

func AdmitResetOperationTx(ctx context.Context, tx *sql.Tx, req destructivereset.Request, sourceSet *agenttopology.SourceSetPlan, sqlite bool) (destructivereset.Operation, error) {
	initial, err := destructivereset.NewOperation(req)
	if err != nil {
		return destructivereset.Operation{}, err
	}
	initial.SourceSet = sourceSet
	if err := initial.Validate(); err != nil {
		return destructivereset.Operation{}, err
	}
	req = initial.Request
	var pending string
	err = tx.QueryRowContext(ctx, `SELECT operation_id FROM runtime_reset_operations WHERE active_slot = 1`).Scan(&pending)
	if err == nil {
		return destructivereset.Operation{}, fmt.Errorf("%w: %s", destructivereset.ErrOperationInProgress, pending)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return destructivereset.Operation{}, err
	}
	raw, err := json.Marshal(initial)
	if err != nil {
		return destructivereset.Operation{}, err
	}
	var key any
	if req.IdempotencyKey != "" {
		key = req.IdempotencyKey
	}
	query := `INSERT INTO runtime_reset_operations (operation_id, actor_token_id, idempotency_key, revision, phase, active_slot, record) VALUES ($1, $2, $3, 1, $4, 1, $5::jsonb)`
	if sqlite {
		query = `INSERT INTO runtime_reset_operations (operation_id, actor_token_id, idempotency_key, revision, phase, active_slot, record) VALUES ($1, $2, $3, 1, $4, 1, $5)`
	}
	_, err = tx.ExecContext(ctx, query, req.OperationID, req.ActorTokenID, key, initial.Phase, string(raw))
	return initial, err
}

func ReadResetOperationTx(ctx context.Context, tx *sql.Tx, id string) (destructivereset.Operation, error) {
	return scanResetOperation(tx.QueryRowContext(ctx, `SELECT `+resetOperationColumns+` FROM runtime_reset_operations WHERE operation_id = $1`, id))
}

func PendingResetOperationsTx(ctx context.Context, tx *sql.Tx) ([]destructivereset.Operation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+resetOperationColumns+` FROM runtime_reset_operations WHERE active_slot = 1 ORDER BY operation_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []destructivereset.Operation
	for rows.Next() {
		op, err := scanResetOperation(rows)
		if err != nil {
			return nil, err
		}
		if op.Phase == destructivereset.PhaseCompleted || len(out) != 0 {
			return nil, errors.New("invalid active reset operation ledger")
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func AdvanceResetOperationTx(ctx context.Context, tx *sql.Tx, before, after destructivereset.Operation, sqlite bool) error {
	if (after.Phase == destructivereset.PhaseCleanupCommitted || after.Phase == destructivereset.PhaseQuiesced) && after.Phase != before.Phase {
		return errors.New("reset effect phase must commit inside its effect transaction")
	}
	return advanceResetOperationTx(ctx, tx, before, after, sqlite)
}

func advanceResetOperationTx(ctx context.Context, tx *sql.Tx, before, after destructivereset.Operation, sqlite bool) error {
	if err := destructivereset.ValidateOperationTransition(before, after); err != nil {
		return err
	}
	current, err := ReadResetOperationTx(ctx, tx, before.Request.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, before) {
		return destructivereset.ErrOperationChanged
	}
	raw, err := json.Marshal(after)
	if err != nil {
		return err
	}
	var active any = 1
	if after.Phase == destructivereset.PhaseCompleted {
		active = nil
	}
	query := `UPDATE runtime_reset_operations SET revision = $1, phase = $2, active_slot = $3, record = $4::jsonb WHERE operation_id = $5 AND revision = $6`
	if sqlite {
		query = `UPDATE runtime_reset_operations SET revision = $1, phase = $2, active_slot = $3, record = $4 WHERE operation_id = $5 AND revision = $6`
	}
	result, err := tx.ExecContext(ctx, query, after.Revision, after.Phase, active, string(raw), before.Request.OperationID, before.Revision)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return destructivereset.ErrOperationChanged
	}
	return nil
}

// ReadResetCleanupTx reconciles an earlier commit before any new cleanup SQL.
func ReadResetCleanupTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest) (*destructivereset.CleanupResult, error) {
	op, err := ReadResetOperationTx(ctx, tx, req.OperationID)
	if err != nil {
		return nil, err
	}
	if op.Request.ActorTokenID != req.ActorTokenID || !reflect.DeepEqual(op.Plan, &req.Result) || !reflect.DeepEqual(op.Quiescence, &req.Quiescence) {
		return nil, errors.New("cleanup does not match durable reset evidence")
	}
	if op.Cleanup != nil {
		return op.Cleanup, nil
	}
	if op.Phase != destructivereset.PhaseQuiesced {
		return nil, errors.New("reset is not durably quiesced")
	}
	return nil, nil
}

// CommitResetCleanupTx records the commit witness in the transaction that
// changed the cleanup catalog and source topology, never in a later save.
func CommitResetCleanupTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, result destructivereset.CleanupResult, sqlite bool) error {
	op, err := ReadResetOperationTx(ctx, tx, req.OperationID)
	if err != nil {
		return err
	}
	if op.Phase != destructivereset.PhaseQuiesced || op.Request.ActorTokenID != req.ActorTokenID ||
		!reflect.DeepEqual(op.Plan, &req.Result) || !reflect.DeepEqual(op.Quiescence, &req.Quiescence) {
		return errors.New("cleanup does not match the durable reset plan and quiescence")
	}
	next := op
	next.Phase = destructivereset.PhaseCleanupCommitted
	next.Revision++
	next.Cleanup = &result
	return advanceResetOperationTx(ctx, tx, op, next, sqlite)
}

func CommitResetQuiescenceTx(ctx context.Context, tx *sql.Tx, req destructivereset.QuiescenceRequest, result destructivereset.QuiescenceResult, sqlite bool) error {
	op, err := ReadResetOperationTx(ctx, tx, req.OperationID)
	if err != nil {
		return err
	}
	if op.Phase != destructivereset.PhasePlanned || op.Request.ActorTokenID != req.ActorTokenID || !reflect.DeepEqual(op.Plan, &req.Result) {
		return errors.New("quiescence does not match the durable reset plan")
	}
	next := op
	next.Phase = destructivereset.PhaseQuiesced
	next.Revision++
	next.Quiescence = &result
	return advanceResetOperationTx(ctx, tx, op, next, sqlite)
}

func scanResetOperation(row interface{ Scan(...any) error }) (destructivereset.Operation, error) {
	var op destructivereset.Operation
	var id, actor, phase string
	var key sql.NullString
	var revision int64
	var active sql.NullInt64
	var raw []byte
	if err := row.Scan(&id, &actor, &key, &revision, &phase, &active, &raw); err != nil {
		return op, err
	}
	if err := json.Unmarshal(raw, &op); err != nil {
		return op, fmt.Errorf("decode durable reset operation: %w", err)
	}
	if op.Request.OperationID != id || op.Request.ActorTokenID != actor || op.Request.IdempotencyKey != key.String ||
		key.Valid != (op.Request.IdempotencyKey != "") || op.Revision != revision || string(op.Phase) != phase ||
		active.Valid != (op.Phase != destructivereset.PhaseCompleted) || (active.Valid && active.Int64 != 1) {
		return op, errors.New("durable reset operation index contradicts its evidence")
	}
	return op, op.Validate()
}
