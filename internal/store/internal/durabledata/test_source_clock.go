package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// CaptureSourceClockRewriteTx retains the original five-row atomic clock cut.
// Its inputs are independently observed wires, not caller-supplied mutations.
func CaptureSourceClockRewriteTx(ctx context.Context, tx *sql.Tx, id string, source SourceEvaluationResultWire, provenanceWire []byte, completed time.Time) (*receiptAtomicCorruption, error) {
	current, err := ReadSourceEvaluationResultWireTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	currentProvenance, err := ReadImportProvenanceWireTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(current.Evaluation, source.Evaluation) || !bytes.Equal(current.Result, source.Result) || !bytes.Equal(currentProvenance, provenanceWire) {
		return nil, errors.New("source clock observations changed before exact capture")
	}
	var evaluation runtimedata.SourceEvaluationContext
	var result runtimedata.SourceOperationResult
	var provenance runtimedata.Provenance
	if json.Unmarshal(source.Evaluation, &evaluation) != nil || json.Unmarshal(source.Result, &result) != nil || json.Unmarshal(provenanceWire, &provenance) != nil {
		return nil, errors.New("decode source operation time projections")
	}
	evaluation.CompletedAt, result.CompletedAt, provenance.CommittedAt = completed, completed, completed
	evaluationJSON, err := json.Marshal(evaluation)
	if err != nil {
		return nil, err
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	provenanceJSON, err := json.Marshal(provenance)
	if err != nil {
		return nil, err
	}
	f := &receiptAtomicCorruption{}
	ref := evaluation.Declaration.Ref
	for _, change := range []struct {
		owner             receiptCorruptionOwner
		id, child, column string
		value             any
	}{
		{sourceReceiptCorruption, id, "", "evaluation_json", evaluationJSON},
		{sourceReceiptCorruption, id, "", "result_json", resultJSON},
		{sourceReceiptCorruption, id, "", "completed_at", completed},
		{importProvenanceCorruption, id, "", "provenance_json", provenanceJSON},
		{importProvenanceCorruption, id, "", "committed_at", completed},
		{headHistoryReceiptCorruption, id, "", "committed_at", completed},
		{versionReceiptCorruption, string(result.Candidate.VersionID), "", "created_at", completed},
		{headReceiptCorruption, ref.FlowPath, ref.EventName, "updated_at", completed},
	} {
		field, err := captureReceiptCorruptionTx(ctx, tx, change.id, change.child, receiptCorruptionChange{
			owner: change.owner, column: change.column, scalar: func() any { return change.value },
		})
		if err != nil {
			return nil, err
		}
		f.fields = append(f.fields, field)
	}
	return f, nil
}
