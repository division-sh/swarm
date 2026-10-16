package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// receiptAtomicCorruption keeps complete pre/post images for the fixed writes
// of one named domain fault. Callers cannot supply its relations or changes.
type receiptAtomicCorruption struct{ fields []*ReceiptCorruption }

func (f *receiptAtomicCorruption) matchesTx(ctx context.Context, tx *sql.Tx, postgres, original bool) (bool, error) {
	match := true
	for _, field := range f.fields {
		current, _, err := field.readTx(ctx, tx, postgres)
		if err != nil {
			return false, err
		}
		want := field.after
		if original {
			want = field.before
		}
		match = match && len(want) > 0 && bytes.Equal(current, want)
	}
	return match, nil
}

func (f *receiptAtomicCorruption) installTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	match, err := f.matchesTx(ctx, tx, postgres, true)
	if err != nil {
		return err
	}
	if !match {
		return errors.New("coordinated resource receipt changed before fault installation")
	}
	for _, field := range f.fields {
		if err := field.replaceTx(ctx, tx, field.corrupt); err != nil {
			return err
		}
	}
	for _, field := range f.fields {
		field.after, _, err = field.readTx(ctx, tx, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *receiptAtomicCorruption) restoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	original, err := f.matchesTx(ctx, tx, postgres, true)
	if err != nil || original {
		return err
	}
	match, err := f.matchesTx(ctx, tx, postgres, false)
	if err != nil {
		return err
	}
	if !match {
		return errors.New("coordinated resource receipt replacement changed before cleanup")
	}
	for i := len(f.fields) - 1; i >= 0; i-- {
		if err := f.fields[i].replaceTx(ctx, tx, f.fields[i].original); err != nil {
			return err
		}
	}
	match, err = f.matchesTx(ctx, tx, postgres, true)
	if err == nil && !match {
		err = errors.New("coordinated resource receipt cleanup lost its complete original")
	}
	return err
}

type FailedFusedContextCorruption struct {
	receiptAtomicCorruption
	Projection runtimedata.FusedChildEvaluation
	Defects    []runtimedata.ValidationDefect
}

// The two wires come from the journey's independent observations. Installation
// still compares both complete rows and commits all four fields atomically.
func CaptureFailedFusedContextCorruptionTx(ctx context.Context, tx *sql.Tx, run, source string, contextWire []byte, parentWire RunCreationRequestEvidenceWire) (*FailedFusedContextCorruption, error) {
	var evaluation runtimedata.SourceEvaluationContext
	var command runtimedata.RunCreationCommand
	var evidence runtimedata.RunCreationEvidence
	if err := json.Unmarshal(contextWire, &evaluation); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(parentWire.Request, &command); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(parentWire.Evidence, &evidence); err != nil {
		return nil, err
	}
	schema := []byte(`{"type":"object","additionalProperties":false,"required":["alternate"],"properties":{"alternate":{"type":"string"}},"x-swarm-dataset-key":"alternate"}`)
	ref := evaluation.Declaration.Ref
	evaluation.Declaration = runtimedata.Declaration{
		Name: ref.EventName, Ref: ref, OwnerFlowID: evaluation.Declaration.OwnerFlowID,
		BusinessKey: "alternate", SchemaDigest: runtimedata.SchemaDigestFor(schema), CanonicalSchema: schema,
	}
	projection, defects, err := evaluation.FusedProjection(runtimedata.SourceCommand{
		Operation: "import", SourceInvocationID: source, ParentRunID: run, Actor: command.Actor,
		BundleHash: command.BundleHash, Declaration: ref, ExpectedHead: runtimedata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{}\n"),
	})
	if err != nil {
		return nil, err
	}
	evidence.ChildEvaluations = []runtimedata.FusedChildEvaluation{projection}
	evidence.ChildDefects = nil
	for _, defect := range defects {
		evidence.ChildDefects = append(evidence.ChildDefects, runtimedata.FusedChildDefect{SourceInvocationID: source, Defect: defect})
	}
	f := &FailedFusedContextCorruption{Projection: projection, Defects: defects}
	for _, change := range []struct {
		owner  receiptCorruptionOwner
		column string
		value  any
	}{
		{childReceiptCorruption, "context_json", evaluation},
		{childReceiptCorruption, "evaluation_json", projection},
		{childReceiptCorruption, "defects_json", defects},
		{runCreationReceiptCorruption, "evidence_json", evidence},
	} {
		raw, err := json.Marshal(change.value)
		if err != nil {
			return nil, err
		}
		child := ""
		if change.owner == childReceiptCorruption {
			child = source
		}
		field, err := captureReceiptCorruptionTx(ctx, tx, run, child, receiptCorruptionChange{
			owner: change.owner, column: change.column, scalar: func() any { return raw },
		})
		if err != nil {
			return nil, err
		}
		f.fields = append(f.fields, field)
	}
	currentContext, err := ReadRunCreationChildContextWireTx(ctx, tx, run, source)
	if err != nil {
		return nil, err
	}
	currentParent, err := ReadRunCreationRequestEvidenceWireTx(ctx, tx, run)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(currentContext, contextWire) || !bytes.Equal(currentParent.Request, parentWire.Request) || !bytes.Equal(currentParent.Evidence, parentWire.Evidence) {
		return nil, errors.New("failed fused observations changed before exact capture")
	}
	return f, nil
}
