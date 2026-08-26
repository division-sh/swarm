package durabledata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const SourceEvaluationFormat = "swarm.resource.source.evaluation.v1"

type SourceEvaluationBase struct {
	State           string     `json:"state"`
	Head            HeadResult `json:"head"`
	Manifest        *Manifest  `json:"manifest,omitempty"`
	BusinessKey     string     `json:"business_key,omitempty"`
	CanonicalSchema []byte     `json:"canonical_schema,omitempty"`
	CanonicalJSONL  []byte     `json:"canonical_jsonl,omitempty"`
}

type SourceEvaluationContext struct {
	Format      string               `json:"format"`
	Declaration Declaration          `json:"declaration"`
	Base        SourceEvaluationBase `json:"base"`
	CompletedAt time.Time            `json:"completed_at"`
}

type sourceEvaluationFacts struct {
	Compiled CompiledVersion
	Result   SourceOperationResult
	Evidence SourceEvidence
}

func (d Declaration) Validate() error {
	if d.Name == "" || d.Name != d.Ref.EventName {
		return fmt.Errorf("resource declaration name must equal its canonical authored event name")
	}
	if err := d.Ref.Validate(); err != nil {
		return err
	}
	if err := d.SchemaDigest.Validate(); err != nil {
		return err
	}
	if SchemaDigestFor(d.CanonicalSchema) != d.SchemaDigest {
		return fmt.Errorf("resource declaration schema digest is contradictory")
	}
	var schema map[string]any
	if err := json.Unmarshal(d.CanonicalSchema, &schema); err != nil || strings.TrimSpace(fmt.Sprint(schema["type"])) != "object" {
		return fmt.Errorf("resource declaration canonical schema must be one JSON object schema")
	}
	declared, _ := schema["x-swarm-dataset-key"].(string)
	if declared != d.BusinessKey {
		return fmt.Errorf("resource declaration business key contradicts canonical schema")
	}
	if d.BusinessKey != "" {
		properties, ok := schema["properties"].(map[string]any)
		if !ok || properties[d.BusinessKey] == nil {
			return fmt.Errorf("resource declaration business key is absent from canonical schema")
		}
	}
	return nil
}

func (b SourceEvaluationBase) Validate(declaration DeclarationRef) ([]Row, error) {
	if err := b.Head.Validate(); err != nil {
		return nil, err
	}
	if !b.Head.Before.Equal(b.Head.After) || b.Head.Changed {
		return nil, fmt.Errorf("source evaluation base head must be unchanged")
	}
	switch b.State {
	case "absent":
		if b.Head.Before.State != "absent" || b.Head.Revision != 0 || b.Manifest != nil || b.BusinessKey != "" ||
			b.CanonicalSchema != nil || b.CanonicalJSONL != nil {
			return nil, fmt.Errorf("absent source evaluation base carries version facts")
		}
		return nil, nil
	case "version":
		if b.Head.Before.State != "version" || b.Head.Revision == 0 || b.Manifest == nil || b.CanonicalSchema == nil || b.CanonicalJSONL == nil {
			return nil, fmt.Errorf("version source evaluation base is incomplete")
		}
	default:
		return nil, fmt.Errorf("source evaluation base state must be absent or version")
	}
	if err := b.Manifest.Validate(); err != nil {
		return nil, err
	}
	if b.Manifest.Declaration != declaration || b.Manifest.SchemaDigest != SchemaDigestFor(b.CanonicalSchema) {
		return nil, fmt.Errorf("source evaluation base manifest contradicts declaration or schema")
	}
	var schema map[string]any
	if err := json.Unmarshal(b.CanonicalSchema, &schema); err != nil {
		return nil, fmt.Errorf("decode source evaluation base schema: %w", err)
	}
	declared, _ := schema["x-swarm-dataset-key"].(string)
	if declared != b.BusinessKey {
		return nil, fmt.Errorf("source evaluation base business key contradicts schema")
	}
	compiled, defects := CompileJSONL(declaration, schema, b.BusinessKey, b.CanonicalJSONL)
	versionID, err := b.Manifest.VersionID()
	if err != nil || len(defects) != 0 || compiled.VersionID != versionID || versionID != b.Head.Before.VersionID ||
		!reflect.DeepEqual(compiled.Manifest, *b.Manifest) || !bytes.Equal(compiled.CanonicalSchema, b.CanonicalSchema) {
		return nil, fmt.Errorf("source evaluation base payload contradicts immutable version facts")
	}
	return compiled.Rows, nil
}

func (c SourceEvaluationContext) evaluate(command SourceCommand) (sourceEvaluationFacts, error) {
	if c.Format != SourceEvaluationFormat {
		return sourceEvaluationFacts{}, fmt.Errorf("source evaluation format must be %q", SourceEvaluationFormat)
	}
	if err := command.Validate(); err != nil {
		return sourceEvaluationFacts{}, err
	}
	if err := c.Declaration.Validate(); err != nil {
		return sourceEvaluationFacts{}, err
	}
	if c.Declaration.Ref != command.Declaration {
		return sourceEvaluationFacts{}, fmt.Errorf("source evaluation declaration contradicts request")
	}
	if err := validateCompletedAt(c.CompletedAt); err != nil {
		return sourceEvaluationFacts{}, err
	}
	baseRows, err := c.Base.Validate(command.Declaration)
	if err != nil {
		return sourceEvaluationFacts{}, err
	}
	baseHead := c.Base.Head.Before
	compiled, defects := CompileJSONL(command.Declaration, mustDecodeEvaluationSchema(c.Declaration.CanonicalSchema), c.Declaration.BusinessKey, command.Input)
	result := SourceOperationResult{
		SourceInvocationID: command.SourceInvocationID,
		Operation:          command.Operation,
		BundleHash:         command.BundleHash,
		SchemaDigest:       c.Declaration.SchemaDigest,
		Declaration:        command.Declaration,
		ExpectedHead:       command.ExpectedHead,
		ObservedHead:       baseHead,
		Head:               c.Base.Head,
		Defects:            FirstEvidencePage([]ValidationDefect{}),
		CompletedAt:        c.CompletedAt,
	}
	evidence := SourceEvidence{}
	if len(defects) != 0 {
		result.Outcome = "validation_rejected"
		result.Candidate = CandidateVersion{State: "none"}
		result.Delta = UncomputedDelta("validation_rejected")
		result.Defects = FirstEvidencePage(defects)
		evidence.Defects = defects
		return sourceEvaluationFacts{Compiled: compiled, Result: result, Evidence: evidence}, nil
	}
	manifest := compiled.Manifest
	result.Candidate = CandidateVersion{State: "candidate", VersionID: compiled.VersionID, Manifest: &manifest}
	if !command.ExpectedHead.Equal(baseHead) {
		result.Outcome = "head_conflict"
		result.Delta = UncomputedDelta("head_conflict")
		return sourceEvaluationFacts{Compiled: compiled, Result: result, Evidence: evidence}, nil
	}
	if c.Base.State == "version" && c.Base.Manifest.SchemaDigest != c.Declaration.SchemaDigest {
		result.Outcome = "accepted"
		result.Delta = NotComparableDelta(baseHead, "schema_changed")
		return sourceEvaluationFacts{Compiled: compiled, Result: result, Evidence: evidence}, nil
	}
	delta, added, removed, changed := Delta(baseRows, compiled.Rows, c.Declaration.BusinessKey != "")
	rowIdentity := DeltaRowIdentityPosition
	if c.Declaration.BusinessKey != "" {
		rowIdentity = DeltaRowIdentityBusinessKey
	}
	result.Outcome = "accepted"
	result.Delta = ComputedDelta(baseHead, delta, rowIdentity)
	evidence = SourceEvidence{DeltaAdded: sourceDeltaKeys(added), DeltaRemoved: sourceDeltaKeys(removed), DeltaChanged: sourceDeltaKeys(changed)}
	return sourceEvaluationFacts{Compiled: compiled, Result: result, Evidence: evidence}, nil
}

func (c SourceEvaluationContext) Evaluate(command SourceCommand) (CompiledVersion, SourceOperationResult, SourceEvidence, error) {
	facts, err := c.evaluate(command)
	if err != nil {
		return CompiledVersion{}, SourceOperationResult{}, SourceEvidence{}, err
	}
	return facts.Compiled, facts.Result, facts.Evidence, nil
}

func (c SourceEvaluationContext) ValidateRecord(command SourceCommand, record SourceOperationRecord) error {
	facts, err := c.evaluate(command)
	if err != nil {
		return err
	}
	expected := facts.Result
	if expected.Outcome == "accepted" && command.Operation == "import" {
		if record.Result.Candidate.State != "version" {
			return fmt.Errorf("accepted import requires a committed version candidate")
		}
		if _, err := ParseVersionAlias(record.Result.Candidate.Alias); err != nil {
			return err
		}
		expected.Candidate.State = "version"
		expected.Candidate.Alias = record.Result.Candidate.Alias
		expected.Head.After = VersionHead(facts.Compiled.VersionID)
		expected.Head.Changed = !expected.Head.Before.Equal(expected.Head.After)
		if expected.Head.Changed {
			expected.Head.Revision++
		}
	}
	if !reflect.DeepEqual(record.Result, expected) || !reflect.DeepEqual(record.Evidence, facts.Evidence) {
		return fmt.Errorf("source result or evidence contradicts canonical evaluation")
	}
	return nil
}

func (c SourceEvaluationContext) FusedProjection(command SourceCommand) (FusedChildEvaluation, []ValidationDefect, error) {
	facts, err := c.evaluate(command)
	if err != nil {
		return FusedChildEvaluation{}, nil, err
	}
	outcome := facts.Result.Outcome
	if outcome == "accepted" {
		outcome = "ready"
	}
	projection := FusedChildEvaluation{
		SourceInvocationID: command.SourceInvocationID,
		BundleHash:         command.BundleHash,
		SchemaDigest:       c.Declaration.SchemaDigest,
		Declaration:        command.Declaration,
		ExpectedHead:       command.ExpectedHead,
		ObservedHead:       facts.Result.ObservedHead,
		Candidate:          facts.Result.Candidate,
		Delta:              facts.Result.Delta,
		DefectCount:        len(facts.Evidence.Defects),
		Outcome:            outcome,
	}
	return projection, append([]ValidationDefect(nil), facts.Evidence.Defects...), nil
}

func sourceDeltaKeys(keys []BusinessKey) []DeltaKey {
	if len(keys) == 0 {
		return nil
	}
	out := make([]DeltaKey, len(keys))
	for index, key := range keys {
		out[index] = DeltaKey{Key: key}
	}
	return out
}

func mustDecodeEvaluationSchema(raw []byte) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return map[string]any{"type": "invalid"}
	}
	return schema
}
