package fanoutobligation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestRunSummarySemanticRejectionEvidenceIsClosedAndExplicit(t *testing.T) {
	failure, ok := failures.EnvelopeFromError(failures.New(
		failures.ClassSchemaInvalid, "emit_payload_contract_violation", "runtime.engine", "fan_out.emit",
		map[string]any{"event": "company.registered", "path": "$.gem_score", "constraint": "type", "expected": "number", "actual": "string"},
	))
	if !ok {
		t.Fatal("construct typed semantic rejection")
	}
	summary := RunSummary{
		RunID: uuid.NewString(), Intents: 1, Cardinality: 1, Cursor: 1, SemanticRejected: 1,
		SemanticRejectionSample: &FanOutSemanticRejectionSample{
			TriggeringDeliveryID: uuid.NewString(), PackageKey: "root", ElementID: uuid.NewString(), Ordinal: 0, Failure: failure,
		},
		BlockedIntents: []BlockedIntentDiagnosis{}, MinNextChunk: InitialChunkSize, MaxNextChunk: InitialChunkSize,
	}
	if err := summary.Validate(); err != nil {
		t.Fatalf("RunSummary.Validate: %v", err)
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"semantic_rejected":1`) || !strings.Contains(text, `"semantic_rejection_sample":{`) || strings.Contains(text, `"rejected":`) {
		t.Fatalf("run summary JSON = %s", text)
	}

	withoutSample := summary
	withoutSample.SemanticRejectionSample = nil
	if err := withoutSample.Validate(); err == nil {
		t.Fatal("semantic rejection count without sample validated")
	}
	withoutCount := summary
	withoutCount.SemanticRejected = 0
	if err := withoutCount.Validate(); err == nil {
		t.Fatal("semantic rejection sample without count validated")
	}
	outOfRange := summary
	outOfRange.SemanticRejectionSample = &FanOutSemanticRejectionSample{
		TriggeringDeliveryID: summary.SemanticRejectionSample.TriggeringDeliveryID,
		PackageKey:           summary.SemanticRejectionSample.PackageKey,
		ElementID:            summary.SemanticRejectionSample.ElementID,
		Ordinal:              summary.Cardinality,
		Failure:              failure,
	}
	if err := outOfRange.Validate(); err == nil {
		t.Fatal("out-of-range semantic rejection sample validated")
	}
}

func TestRunSummaryZeroSemanticRejectionsSerializesExplicitNullSample(t *testing.T) {
	summary := RunSummary{RunID: uuid.NewString(), BlockedIntents: []BlockedIntentDiagnosis{}}
	if err := summary.Validate(); err != nil {
		t.Fatalf("zero RunSummary.Validate: %v", err)
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"semantic_rejection_sample":null`) {
		t.Fatalf("zero run summary omitted explicit null sample: %s", raw)
	}
}
