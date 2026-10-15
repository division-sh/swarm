package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

type SourceContextCorruption struct {
	receiptAtomicCorruption
	Result runtimedata.SourceOperationResult
}

func CaptureSourceContextCorruptionTx(ctx context.Context, tx *sql.Tx, id, kind string) (*SourceContextCorruption, error) {
	if uuid.Validate(id) != nil {
		return nil, fmt.Errorf("source context fault requires its exact invocation")
	}
	source, err := ReadSourceEvaluationResultWireTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	raw, err := ReadSourceRequestWireTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	var evaluation runtimedata.SourceEvaluationContext
	var command runtimedata.SourceCommand
	if err := json.Unmarshal(source.Evaluation, &evaluation); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &command); err != nil {
		return nil, err
	}
	switch kind {
	case "replacement schema":
		schema := []byte(`{"type":"object","additionalProperties":false,"required":["score"],"properties":{"score":{"type":"integer"}}}`)
		ref := evaluation.Declaration.Ref
		evaluation.Declaration = runtimedata.Declaration{
			Name: ref.EventName, Ref: ref, OwnerFlowID: "root", SchemaDigest: runtimedata.SchemaDigestFor(schema), CanonicalSchema: schema,
		}
	case "historical head":
		evaluation.Base = runtimedata.SourceEvaluationBase{
			State: "absent", Head: runtimedata.HeadResult{Before: runtimedata.AbsentHead(), After: runtimedata.AbsentHead()},
		}
	default:
		return nil, fmt.Errorf("unrecognized source context integrity case %q", kind)
	}
	_, result, evidence, err := evaluation.Evaluate(command)
	if err != nil {
		return nil, err
	}
	f := &SourceContextCorruption{Result: result}
	for _, change := range []struct {
		column string
		value  any
	}{{"evaluation_json", evaluation}, {"result_json", result}, {"evidence_json", evidence}} {
		raw, err := json.Marshal(change.value)
		if err != nil {
			return nil, err
		}
		field, err := captureReceiptCorruptionTx(ctx, tx, id, "", receiptCorruptionChange{
			owner: sourceReceiptCorruption, column: change.column, scalar: func() any { return raw },
		})
		if err != nil {
			return nil, err
		}
		f.fields = append(f.fields, field)
	}
	return f, nil
}
