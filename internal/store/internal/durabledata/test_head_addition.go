package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

type NonMutatingHeadHistoryAddition struct {
	marker     *ReceiptCorruption
	evaluation runtimedata.SourceEvaluationContext
	completed  any
}

func CaptureNonMutatingHeadHistoryTx(ctx context.Context, tx *sql.Tx, source string) (*NonMutatingHeadHistoryAddition, error) {
	if uuid.Validate(source) != nil {
		return nil, errors.New("non-mutating head history requires its exact source invocation")
	}
	wire, err := ReadSourceEvaluationResultWireTx(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	var evaluation runtimedata.SourceEvaluationContext
	var result runtimedata.SourceOperationResult
	if err := json.Unmarshal(wire.Evaluation, &evaluation); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(wire.Result, &result); err != nil {
		return nil, err
	}
	if result.Operation != "check" || result.Outcome != "accepted" || evaluation.Base.Head.After.State != "version" {
		return nil, errors.New("head history negative requires the accepted non-mutating version check")
	}
	f := &NonMutatingHeadHistoryAddition{
		marker:     &ReceiptCorruption{id: source, change: receiptCorruptionChange{owner: headHistoryReceiptCorruption}},
		evaluation: evaluation, completed: result.CompletedAt,
	}
	if _, _, err := f.marker.readTx(ctx, tx, false); !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("non-mutating source already has head history")
	}
	return f, nil
}

func (f *NonMutatingHeadHistoryAddition) installTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	if _, _, err := f.marker.readTx(ctx, tx, postgres); !errors.Is(err, sql.ErrNoRows) {
		return errors.New("non-mutating source head history is no longer absent")
	}
	ref, head := f.evaluation.Declaration.Ref, f.evaluation.Base.Head
	if _, err := tx.ExecContext(ctx, `INSERT INTO resource_head_history
		(flow_path,event_name,revision,before_version_id,after_version_id,operation_kind,operation_id,committed_at)
		VALUES ($1,$2,$3,$4,$4,'source_import',$5,$6)`, ref.FlowPath, ref.EventName, head.Revision+1, head.After.VersionID, f.marker.id, f.completed); err != nil {
		return err
	}
	var err error
	f.marker.after, _, err = f.marker.readTx(ctx, tx, false)
	return err
}

func (f *NonMutatingHeadHistoryAddition) restoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	current, _, err := f.marker.readTx(ctx, tx, postgres)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(f.marker.after) == 0 || !bytes.Equal(current, f.marker.after) {
		return errors.New("head history replacement refuses historical cleanup")
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM resource_head_history WHERE operation_id=$1`, f.marker.id)
	return err
}
