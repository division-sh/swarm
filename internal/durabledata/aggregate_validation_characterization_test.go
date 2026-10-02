package durabledata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

type aggregateValidationCase[T any] struct {
	name   string
	base   T
	mutate func(*T)
	want   string
}

func runAggregateValidationCases[T any](t *testing.T, validate func(T) error, cases []aggregateValidationCase[T]) {
	t.Helper()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := cloneJSON(t, test.base)
			if test.mutate != nil {
				test.mutate(&value)
			}
			before, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			if err := validate(value); err != nil {
				got = err.Error()
			}
			if got != test.want {
				t.Fatalf("error = %q, want %q", got, test.want)
			}
			after, err := json.Marshal(value)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("validation changed input: before=%s after=%s error=%v", before, after, err)
			}
		})
	}
}

func TestDurableDataAggregateValidationCharacterization(t *testing.T) {
	command, source := validSourceAggregate(t)
	_, created := validRunCreationAggregate(t, command, source)
	t.Run("source", func(t *testing.T) { characterizeSourceAggregate(t, source.Result) })
	t.Run("child", func(t *testing.T) { characterizeFusedChild(t, source.Result) })
	t.Run("run", func(t *testing.T) { characterizeRunAggregate(t, created, source.Result) })
	t.Run("created_binding", func(t *testing.T) { characterizeCreatedBinding(t, created) })
	t.Run("prune", func(t *testing.T) { characterizePruneAggregate(t, source.Result, created) })
}

func characterizationSource(source SourceOperationResult, operation, outcome string) SourceOperationResult {
	source.Operation, source.Outcome = operation, outcome
	if operation == "check" || outcome != "accepted" {
		source.Head = HeadResult{Before: source.ObservedHead, After: source.ObservedHead}
		source.Candidate.State, source.Candidate.Alias = "candidate", ""
	}
	switch outcome {
	case "validation_rejected":
		source.Candidate = CandidateVersion{State: "none"}
		source.Delta = UncomputedDelta("validation_rejected")
		source.Defects = FirstEvidencePage([]ValidationDefect{{Code: "invalid", Message: "invalid row"}})
	case "head_conflict":
		source.ExpectedHead = VersionHead(source.Candidate.VersionID)
		source.Delta = UncomputedDelta("head_conflict")
	}
	return source
}

func characterizeSourceAggregate(t *testing.T, source SourceOperationResult) {
	check := characterizationSource(source, "check", "accepted")
	rejected := characterizationSource(source, "import", "validation_rejected")
	conflict := characterizationSource(source, "import", "head_conflict")
	cases := []aggregateValidationCase[SourceOperationResult]{
		{name: "import/accepted", base: source},
		{name: "check/accepted", base: check},
		{name: "import/validation_rejected", base: rejected},
		{name: "check/validation_rejected", base: characterizationSource(source, "check", "validation_rejected")},
		{name: "import/head_conflict", base: conflict},
		{name: "check/head_conflict", base: characterizationSource(source, "check", "head_conflict")},
		{name: "uuid", base: source, mutate: func(r *SourceOperationResult) { r.SourceInvocationID = "" }, want: "source_invocation_id must be one canonical non-zero UUID"},
		{name: "bundle", base: source, mutate: func(r *SourceOperationResult) { r.BundleHash = "" }, want: "bundle_hash must be non-empty"},
		{name: "schema", base: source, mutate: func(r *SourceOperationResult) { r.SchemaDigest = "" }, want: "resource-schema-v1:sha256: must be resource-schema-v1:sha256:<64 lowercase hex>"},
		{name: "declaration", base: source, mutate: func(r *SourceOperationResult) { r.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
		{name: "expected_head", base: source, mutate: func(r *SourceOperationResult) { r.ExpectedHead.State = "unknown" }, want: "expected head: expected head state must be absent or version"},
		{name: "observed_head", base: source, mutate: func(r *SourceOperationResult) { r.ObservedHead.State = "unknown" }, want: "observed head: expected head state must be absent or version"},
		{name: "operation", base: source, mutate: func(r *SourceOperationResult) { r.Operation = "unknown" }, want: "source operation must be check or import"},
		{name: "outcome", base: source, mutate: func(r *SourceOperationResult) { r.Outcome = "unknown" }, want: "source outcome is unsupported"},
		{name: "candidate_commit", base: check, mutate: func(r *SourceOperationResult) { r.Candidate.State = "version"; r.Candidate.Alias = "v1" }, want: "source check accepted requires candidate state candidate"},
		{name: "candidate_schema", base: source, mutate: func(r *SourceOperationResult) {
			r.SchemaDigest = SchemaDigest("resource-schema-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		}, want: "candidate manifest contradicts declaration or schema digest"},
		{name: "head_before_local", base: source, mutate: func(r *SourceOperationResult) { r.Head.Before.State = "unknown" }, want: "head before: expected head state must be absent or version"},
		{name: "head_after_local", base: source, mutate: func(r *SourceOperationResult) { r.Head.After.State = "unknown" }, want: "head after: expected head state must be absent or version"},
		{name: "head_changed", base: source, mutate: func(r *SourceOperationResult) { r.Head.Changed = false }, want: "head changed contradicts before and after"},
		{name: "head_revision", base: source, mutate: func(r *SourceOperationResult) { r.Head.Revision = 0 }, want: "changed head requires a positive revision"},
		{name: "head_observation", base: source, mutate: func(r *SourceOperationResult) { r.Head.Before = r.Head.After; r.Head.Changed = false }, want: "head before contradicts observed head"},
		{name: "delta_local", base: source, mutate: func(r *SourceOperationResult) { r.Delta.State = "unknown" }, want: "delta state must be computed, not_computed, or not_comparable"},
		{name: "nil_defects", base: source, mutate: func(r *SourceOperationResult) { r.Defects.Items = nil }, want: "source defects: page items must be an array"},
		{name: "defect_count", base: source, mutate: func(r *SourceOperationResult) { r.Defects.ItemCount++ }, want: "source defects: page item_count contradicts items"},
		{name: "defect_bytes", base: source, mutate: func(r *SourceOperationResult) { r.Defects.EncodedItemsBytes++ }, want: "source defects: page encoded_items_bytes contradicts items"},
		{name: "defect_continuation", base: source, mutate: func(r *SourceOperationResult) { r.Defects.Continuation.Cursor = "1" }, want: "source defects: end page continuation forbids cursor"},
		{name: "defect_local", base: rejected, mutate: func(r *SourceOperationResult) { r.Defects = FirstEvidencePage([]ValidationDefect{{Code: "invalid"}}) }, want: "validation defect requires code and message"},
		{name: "completion_zero", base: source, mutate: func(r *SourceOperationResult) { r.CompletedAt = time.Time{} }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
		{name: "completion_precision", base: source, mutate: func(r *SourceOperationResult) { r.CompletedAt = r.CompletedAt.Add(time.Nanosecond) }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
		{name: "completion_location", base: source, mutate: func(r *SourceOperationResult) { r.CompletedAt = r.CompletedAt.In(time.FixedZone("not-UTC", 0)) }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
		{name: "rejected_without_defects", base: rejected, mutate: func(r *SourceOperationResult) { r.Defects = FirstEvidencePage([]ValidationDefect{}) }, want: "validation_rejected source result has contradictory defects, delta, or head"},
		{name: "rejected_delta_reason", base: rejected, mutate: func(r *SourceOperationResult) { r.Delta = UncomputedDelta("head_conflict") }, want: "validation_rejected source result has contradictory defects, delta, or head"},
		{name: "rejected_changed_head", base: rejected, mutate: func(r *SourceOperationResult) {
			r.Head.After = source.Head.After
			r.Head.Changed = true
			r.Head.Revision = 1
		}, want: "validation_rejected source result has contradictory defects, delta, or head"},
		{name: "conflict_equal_heads", base: conflict, mutate: func(r *SourceOperationResult) { r.ExpectedHead = r.ObservedHead }, want: "head_conflict source result has contradictory head, delta, or defects"},
		{name: "conflict_defects", base: conflict, mutate: func(r *SourceOperationResult) { r.Defects = rejected.Defects }, want: "head_conflict source result has contradictory head, delta, or defects"},
		{name: "conflict_delta", base: conflict, mutate: func(r *SourceOperationResult) { r.Delta = source.Delta }, want: "head_conflict source result has contradictory head, delta, or defects"},
		{name: "conflict_changed_head", base: conflict, mutate: func(r *SourceOperationResult) { r.Head = source.Head }, want: "head_conflict source result has contradictory head, delta, or defects"},
		{name: "accepted_expected_head", base: source, mutate: func(r *SourceOperationResult) { r.ExpectedHead = r.Head.After }, want: "accepted source result has contradictory head, delta, or defects"},
		{name: "accepted_defects", base: source, mutate: func(r *SourceOperationResult) { r.Defects = rejected.Defects }, want: "accepted source result has contradictory head, delta, or defects"},
		{name: "accepted_uncomputed", base: source, mutate: func(r *SourceOperationResult) { r.Delta = UncomputedDelta("head_conflict") }, want: "accepted source result has contradictory head, delta, or defects"},
		{name: "accepted_delta_against", base: source, mutate: func(r *SourceOperationResult) { *r.Delta.Against = r.Head.After }, want: "accepted source result has contradictory head, delta, or defects"},
		{name: "check_changes_head", base: check, mutate: func(r *SourceOperationResult) { r.Head = source.Head }, want: "accepted check must not change head"},
		{name: "import_wrong_head", base: source, mutate: func(r *SourceOperationResult) { r.Head.After = r.Head.Before; r.Head.Changed = false }, want: "accepted import head must select candidate version"},
		{name: "schema_changed_delta", base: source, mutate: func(r *SourceOperationResult) { r.Delta = NotComparableDelta(r.ObservedHead, "schema_changed") }},
	}
	runAggregateValidationCases(t, validateSourceOperationResult, cases)
}

func characterizationChild(source SourceOperationResult, outcome string) FusedChildEvaluation {
	sourceOutcome := outcome
	if outcome == "ready" {
		sourceOutcome = "accepted"
	}
	r := characterizationSource(source, "check", sourceOutcome)
	return FusedChildEvaluation{SourceInvocationID: r.SourceInvocationID, Outcome: outcome, BundleHash: r.BundleHash,
		SchemaDigest: r.SchemaDigest, Declaration: r.Declaration, ExpectedHead: r.ExpectedHead, ObservedHead: r.ObservedHead,
		Candidate: r.Candidate, Delta: r.Delta, DefectCount: r.Defects.ItemCount}
}

func characterizeFusedChild(t *testing.T, source SourceOperationResult) {
	ready := characterizationChild(source, "ready")
	rejected := characterizationChild(source, "validation_rejected")
	conflict := characterizationChild(source, "head_conflict")
	cases := []aggregateValidationCase[FusedChildEvaluation]{
		{name: "ready", base: ready}, {name: "validation_rejected", base: rejected}, {name: "head_conflict", base: conflict},
		{name: "uuid", base: ready, mutate: func(e *FusedChildEvaluation) { e.SourceInvocationID = "" }, want: "source_invocation_id must be one canonical non-zero UUID"},
		{name: "bundle", base: ready, mutate: func(e *FusedChildEvaluation) { e.BundleHash = "" }, want: "bundle_hash must be non-empty"},
		{name: "schema", base: ready, mutate: func(e *FusedChildEvaluation) { e.SchemaDigest = "" }, want: "resource-schema-v1:sha256: must be resource-schema-v1:sha256:<64 lowercase hex>"},
		{name: "declaration", base: ready, mutate: func(e *FusedChildEvaluation) { e.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
		{name: "expected_head", base: ready, mutate: func(e *FusedChildEvaluation) { e.ExpectedHead.State = "unknown" }, want: "expected head state must be absent or version"},
		{name: "observed_head", base: ready, mutate: func(e *FusedChildEvaluation) { e.ObservedHead.State = "unknown" }, want: "expected head state must be absent or version"},
		{name: "candidate", base: ready, mutate: func(e *FusedChildEvaluation) { e.Candidate = CandidateVersion{State: "none"} }, want: "fused child ready requires candidate state candidate"},
		{name: "candidate_schema", base: ready, mutate: func(e *FusedChildEvaluation) {
			e.SchemaDigest = SchemaDigest("resource-schema-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		}, want: "fused child candidate contradicts declaration or schema"},
		{name: "delta", base: ready, mutate: func(e *FusedChildEvaluation) { e.Delta.State = "unknown" }, want: "delta state must be computed, not_computed, or not_comparable"},
		{name: "negative_count", base: ready, mutate: func(e *FusedChildEvaluation) { e.DefectCount = -1 }, want: "fused child defect_count must not be negative"},
		{name: "unsupported_outcome", base: ready, mutate: func(e *FusedChildEvaluation) { e.Outcome = "unknown" }, want: "fused child outcome is unsupported"},
		{name: "ready_defects", base: ready, mutate: func(e *FusedChildEvaluation) { e.DefectCount = 1 }, want: "ready fused child has contradictory head, delta, or defects"},
		{name: "ready_head", base: ready, mutate: func(e *FusedChildEvaluation) { e.ExpectedHead = VersionHead(e.Candidate.VersionID) }, want: "ready fused child has contradictory head, delta, or defects"},
		{name: "ready_delta", base: ready, mutate: func(e *FusedChildEvaluation) { e.Delta = UncomputedDelta("head_conflict") }, want: "ready fused child has contradictory head, delta, or defects"},
		{name: "ready_against", base: ready, mutate: func(e *FusedChildEvaluation) { *e.Delta.Against = VersionHead(e.Candidate.VersionID) }, want: "ready fused child has contradictory head, delta, or defects"},
		{name: "conflict_defects", base: conflict, mutate: func(e *FusedChildEvaluation) { e.DefectCount = 1 }, want: "head-conflict fused child has contradictory head, delta, or defects"},
		{name: "conflict_head", base: conflict, mutate: func(e *FusedChildEvaluation) { e.ExpectedHead = e.ObservedHead }, want: "head-conflict fused child has contradictory head, delta, or defects"},
		{name: "conflict_delta", base: conflict, mutate: func(e *FusedChildEvaluation) { e.Delta = ready.Delta }, want: "head-conflict fused child has contradictory head, delta, or defects"},
		{name: "conflict_reason", base: conflict, mutate: func(e *FusedChildEvaluation) { e.Delta = UncomputedDelta("validation_rejected") }, want: "head-conflict fused child has contradictory head, delta, or defects"},
		{name: "rejected_count", base: rejected, mutate: func(e *FusedChildEvaluation) { e.DefectCount = 0 }, want: "validation-rejected fused child has contradictory delta or defects"},
		{name: "rejected_delta", base: rejected, mutate: func(e *FusedChildEvaluation) { e.Delta = ready.Delta }, want: "validation-rejected fused child has contradictory delta or defects"},
		{name: "rejected_reason", base: rejected, mutate: func(e *FusedChildEvaluation) { e.Delta = UncomputedDelta("head_conflict") }, want: "validation-rejected fused child has contradictory delta or defects"},
		{name: "schema_changed_delta", base: ready, mutate: func(e *FusedChildEvaluation) { e.Delta = NotComparableDelta(e.ObservedHead, "schema_changed") }},
	}
	runAggregateValidationCases(t, FusedChildEvaluation.Validate, cases)
}

func characterizationRejectedRun(created RunCreationOperationRecord, child FusedChildEvaluation) RunCreationOperationRecord {
	summary := created.Summary
	summary.Outcome, summary.EventID, summary.Status, summary.PinCount, summary.ImportCount = "data_rejected", "", "", 0, 1
	summary.Rejection = RunCreationRejection{State: "rejected", Code: RunCreationRejectionFusedValidation}
	defects := []FusedChildDefect{{SourceInvocationID: child.SourceInvocationID, Defect: ValidationDefect{Code: "invalid", Message: "invalid row"}}}
	if child.Outcome == "head_conflict" {
		summary.Outcome, summary.Rejection.Code, defects = "head_conflict", RunCreationRejectionFusedHead, nil
	}
	return RunCreationOperationRecord{Summary: summary, Binding: DataBinding{State: "none"},
		Evidence: RunCreationEvidence{ChildEvaluations: []FusedChildEvaluation{child}, ChildDefects: defects}}
}

func characterizeRunAggregate(t *testing.T, created RunCreationOperationRecord, source SourceOperationResult) {
	child := characterizationChild(source, "validation_rejected")
	rejected := characterizationRejectedRun(created, child)
	conflict := characterizationRejectedRun(created, characterizationChild(source, "head_conflict"))
	cases := []aggregateValidationCase[RunCreationOperationRecord]{
		{name: "created", base: created}, {name: "rejected", base: rejected}, {name: "head_conflict", base: conflict},
		{name: "feed_only", base: created, mutate: func(r *RunCreationOperationRecord) { r.Summary.EventID = "" }},
		{name: "unbound", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Summary.PinCount, r.Summary.ImportCount = 0, 0
			r.Binding = DataBinding{State: "none"}
			r.Evidence = RunCreationEvidence{}
		}},
		{name: "summary", base: created, mutate: func(r *RunCreationOperationRecord) { r.Summary.Kind = "unknown" }, want: "run-creation summary kind must be run_creation"},
		{name: "binding", base: created, mutate: func(r *RunCreationOperationRecord) { r.Binding.State = "unknown" }, want: "data binding state must be none or bound"},
		{name: "child_local", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildEvaluations[0].BundleHash = "" }, want: "fused child " + child.SourceInvocationID + ": bundle_hash must be non-empty"},
		{name: "child_invocation_duplicate", base: rejected, mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.ChildEvaluations = append(r.Evidence.ChildEvaluations, r.Evidence.ChildEvaluations[0])
		}, want: "run creation repeats fused child source_invocation_id " + child.SourceInvocationID},
		{name: "child_declaration_duplicate", base: rejected, mutate: func(r *RunCreationOperationRecord) {
			other := r.Evidence.ChildEvaluations[0]
			other.SourceInvocationID = uuid.NewString()
			r.Evidence.ChildEvaluations = append(r.Evidence.ChildEvaluations, other)
		}, want: "run creation repeats fused child declaration " + child.Declaration.Key()},
		{name: "child_order", base: rejected, mutate: func(r *RunCreationOperationRecord) {
			other := r.Evidence.ChildEvaluations[0]
			other.SourceInvocationID = uuid.NewString()
			other.Declaration.EventName = "aaa.loaded"
			r.Evidence.ChildEvaluations = append(r.Evidence.ChildEvaluations, other)
		}, want: "fused child evaluations must be strictly declaration-sorted"},
		{name: "defect_local", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildDefects[0].Defect.Message = "" }, want: "validation defect requires code and message"},
		{name: "defect_foreign", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildDefects[0].SourceInvocationID = uuid.NewString() }, want: "child defect references unknown source invocation"},
		{name: "defect_count", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildDefects = nil }, want: "fused child " + child.SourceInvocationID + " defect_count contradicts evidence"},
		{name: "run_item_local", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Kind = "unknown" }, want: "run-binding item kind is unsupported"},
		{name: "rejected_child_count", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Summary.ImportCount++ }, want: "rejected run creation has contradictory binding or child count"},
		{name: "rejected_binding", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Binding = created.Binding }, want: "rejected run creation has contradictory binding or child count"},
		{name: "rejected_committed_items", base: rejected, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding = created.Evidence.RunBinding }, want: "rejected run creation has contradictory binding or child count"},
		{name: "created_failed_children", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.ChildEvaluations = rejected.Evidence.ChildEvaluations
			r.Evidence.ChildDefects = rejected.Evidence.ChildDefects
		}, want: "created run creation forbids pre-commit child evidence"},
		{name: "unbound_with_imports", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Summary.PinCount = 0
			r.Binding = DataBinding{State: "none"}
			r.Evidence = RunCreationEvidence{}
		}, want: "unbound created run has contradictory counts or evidence"},
		{name: "binding_parent", base: created, mutate: func(r *RunCreationOperationRecord) { r.Binding.RunID = uuid.NewString() }, want: "created run summary, binding, and evidence disagree"},
		{name: "binding_summary_count", base: created, mutate: func(r *RunCreationOperationRecord) { r.Summary.PinCount++ }, want: "created run summary, binding, and evidence disagree"},
		{name: "binding_page", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding = nil }, want: "created run summary, binding, and evidence disagree"},
	}
	runAggregateValidationCases(t, RunCreationOperationRecord.Validate, cases)
}

func characterizeCreatedBinding(t *testing.T, created RunCreationOperationRecord) {
	cases := []aggregateValidationCase[RunCreationOperationRecord]{
		{name: "fused", base: created},
		{name: "explicit", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding = r.Evidence.RunBinding[:1]
			r.Evidence.RunBinding[0].Pin.Selection = "explicit"
			r.Summary.ImportCount = 0
		}},
		{name: "first_item", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding = r.Evidence.RunBinding[1:] }, want: "run binding must group each declaration under its pin"},
		{name: "nil_pin", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin = nil }, want: "run binding must group each declaration under its pin"},
		{name: "foreign_run", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin.RunID = uuid.NewString() }, want: "run-binding pin contradicts parent summary"},
		{name: "foreign_status", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin.RunState = "paused" }, want: "run-binding pin contradicts parent summary"},
		{name: "fork_selection", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin.Selection = "fork_inherited" }, want: "run-binding pin contradicts parent summary"},
		{name: "duplicate_pin", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding = append(r.Evidence.RunBinding, r.Evidence.RunBinding[0])
		}, want: "run-binding pins must be strictly declaration-sorted"},
		{name: "missing_import", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding = r.Evidence.RunBinding[:1] }, want: "fused-import pin requires its source summary"},
		{name: "nil_import", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import = nil }, want: "bound import contradicts its fused-import pin"},
		{name: "import_operation", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.Operation = "check" }, want: "bound import contradicts its fused-import pin"},
		{name: "import_outcome", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.Outcome = "head_conflict" }, want: "bound import contradicts its fused-import pin"},
		{name: "import_bundle", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.BundleHash = "" }, want: "bound import contradicts its fused-import pin"},
		{name: "import_declaration", base: created, mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding[1].Import.Declaration.EventName = "different.loaded"
		}, want: "bound import contradicts its fused-import pin"},
		{name: "import_selection", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin.Selection = "explicit" }, want: "bound import contradicts its fused-import pin"},
		{name: "import_version", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.Candidate.VersionID = "" }, want: "bound import contradicts its fused-import pin"},
		{name: "import_schema", base: created, mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.SchemaDigest = "" }, want: "bound import contradicts its fused-import pin"},
		{name: "duplicate_source", base: created, mutate: func(r *RunCreationOperationRecord) {
			pin := *r.Evidence.RunBinding[0].Pin
			imported := *r.Evidence.RunBinding[1].Import
			pin.Declaration.EventName = "zzz.loaded"
			imported.Declaration = pin.Declaration
			r.Evidence.RunBinding = append(r.Evidence.RunBinding, RunCreationDataItem{Kind: "pin", Pin: &pin}, RunCreationDataItem{Kind: "import", Import: &imported})
		}, want: "run binding repeats import source_invocation_id " + created.Evidence.RunBinding[1].Import.SourceInvocationID},
		{name: "pin_count", base: created, mutate: func(r *RunCreationOperationRecord) { r.Summary.PinCount++ }, want: "run-binding item counts contradict summary"},
		{name: "import_count", base: created, mutate: func(r *RunCreationOperationRecord) { r.Summary.ImportCount++ }, want: "run-binding item counts contradict summary"},
	}
	runAggregateValidationCases(t, validateCreatedRunBinding, cases)
}

func characterizationPrune(source SourceOperationResult, created RunCreationOperationRecord, outcome string) PruneOperationResult {
	r := PruneOperationResult{PruneInvocationID: uuid.NewString(), Declaration: source.Declaration,
		VersionID: source.Candidate.VersionID, ExpectedHead: AbsentHead(), ObservedHead: AbsentHead(),
		CompletedAt: source.CompletedAt, Outcome: outcome}
	switch outcome {
	case "rejected":
		page := FirstEvidencePage([]PruneDefect{{Code: "version_not_found", Message: "missing"}})
		r.Defects = &page
	case "head_conflict":
		r.ExpectedHead = VersionHead(r.VersionID)
	case "refused_current":
		r.ExpectedHead, r.ObservedHead, r.CurrentVersionID = VersionHead(r.VersionID), VersionHead(r.VersionID), r.VersionID
	case "refused_pinned":
		page := FirstEvidencePage([]Pin{*created.Evidence.RunBinding[0].Pin})
		r.Pins, r.PinCount = &page, 1
	case "already_pruned":
		r.PayloadBefore, r.PayloadAfter = "pruned", "pruned"
	case "pruned":
		r.PayloadBefore, r.PayloadAfter = "materialized", "pruned"
	}
	return r
}

func characterizePruneAggregate(t *testing.T, source SourceOperationResult, created RunCreationOperationRecord) {
	pruned := characterizationPrune(source, created, "pruned")
	rejected := characterizationPrune(source, created, "rejected")
	pinned := characterizationPrune(source, created, "refused_pinned")
	cases := []aggregateValidationCase[PruneOperationResult]{
		{name: "uuid", base: pruned, mutate: func(r *PruneOperationResult) { r.PruneInvocationID = "" }, want: "prune_invocation_id must be one canonical non-zero UUID"},
		{name: "declaration", base: pruned, mutate: func(r *PruneOperationResult) { r.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
		{name: "version", base: pruned, mutate: func(r *PruneOperationResult) { r.VersionID = "" }, want: "resource-version-v1:sha256: must be resource-version-v1:sha256:<64 lowercase hex>"},
		{name: "expected_head", base: pruned, mutate: func(r *PruneOperationResult) { r.ExpectedHead.State = "unknown" }, want: "expected head state must be absent or version"},
		{name: "observed_head", base: pruned, mutate: func(r *PruneOperationResult) { r.ObservedHead.State = "unknown" }, want: "expected head state must be absent or version"},
		{name: "completion", base: pruned, mutate: func(r *PruneOperationResult) { r.CompletedAt = time.Time{} }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
		{name: "negative_pins", base: pruned, mutate: func(r *PruneOperationResult) { r.PinCount = -1 }, want: "prune pin_count must not be negative"},
		{name: "nil_pin_items", base: pinned, mutate: func(r *PruneOperationResult) { r.Pins.Items = nil }, want: "page items must be an array"},
		{name: "pin_local", base: pinned, mutate: func(r *PruneOperationResult) { r.Pins.Items[0].RunID = ""; *r.Pins = FirstEvidencePage(r.Pins.Items) }, want: "pin run_id must be one canonical non-zero UUID"},
		{name: "pin_target", base: pinned, mutate: func(r *PruneOperationResult) {
			r.Pins.Items[0].Declaration.EventName = "different.loaded"
			*r.Pins = FirstEvidencePage(r.Pins.Items)
		}, want: "prune pin summary contradicts target"},
		{name: "nil_defect_items", base: rejected, mutate: func(r *PruneOperationResult) { r.Defects.Items = nil }, want: "page items must be an array"},
		{name: "defect_local", base: rejected, mutate: func(r *PruneOperationResult) {
			r.Defects.Items[0].Code = "unknown"
			*r.Defects = FirstEvidencePage(r.Defects.Items)
		}, want: "prune defect is unsupported or incomplete"},
		{name: "defect_not_complete", base: rejected, mutate: func(r *PruneOperationResult) { r.Defects.Continuation = PageContinuation{State: "more", Cursor: "1"} }, want: "prune defect page is not complete canonical evidence"},
		{name: "unsupported", base: pruned, mutate: func(r *PruneOperationResult) { r.Outcome = "unknown" }, want: "prune outcome is unsupported"},
	}
	for _, outcome := range []string{"rejected", "head_conflict", "refused_current", "refused_pinned", "already_pruned", "pruned"} {
		base := characterizationPrune(source, created, outcome)
		want := map[string]string{"rejected": "rejected prune has contradictory decision facts", "head_conflict": "head-conflict prune has contradictory decision facts", "refused_current": "current-version prune refusal has contradictory decision facts", "refused_pinned": "pinned prune refusal has contradictory decision facts", "already_pruned": "already-pruned result has contradictory decision facts", "pruned": "pruned result has contradictory decision facts"}[outcome]
		cases = append(cases, aggregateValidationCase[PruneOperationResult]{name: outcome + "/valid", base: base})
		cases = append(cases, aggregateValidationCase[PruneOperationResult]{name: outcome + "/foreign_payload", base: base, mutate: func(r *PruneOperationResult) { r.PayloadAfter = "wrong" }, want: want})
		if outcome != "refused_pinned" {
			cases = append(cases, aggregateValidationCase[PruneOperationResult]{name: outcome + "/pins", base: base, mutate: func(r *PruneOperationResult) { r.PinCount, r.Pins = pinned.PinCount, pinned.Pins }, want: want})
		}
		if outcome != "rejected" {
			cases = append(cases, aggregateValidationCase[PruneOperationResult]{name: outcome + "/defects", base: base, mutate: func(r *PruneOperationResult) { r.Defects = rejected.Defects }, want: want})
		}
	}
	runAggregateValidationCases(t, PruneOperationResult.Validate, cases)
	t.Run("complete_pins", func(t *testing.T) {
		pins := []Pin{*created.Evidence.RunBinding[0].Pin}
		if err := pinned.ValidateWithPins(pins); err != nil {
			t.Fatal(err)
		}
		if err := pinned.ValidateWithPins(nil); err == nil || err.Error() != "prune pin evidence is incomplete" {
			t.Fatalf("missing complete pins: %v", err)
		}
		if err := pruned.ValidateWithPins([]Pin{}); err == nil || err.Error() != "prune pin evidence is not canonically ordered" {
			t.Fatalf("noncanonical nonnil empty evidence: %v", err)
		}
		if err := pruned.ValidateWithPins(nil); err != nil {
			t.Fatalf("canonical nil evidence: %v", err)
		}
	})
}

func TestDurableDataAggregateValidationErrorPrecedence(t *testing.T) {
	command, source := validSourceAggregate(t)
	_, created := validRunCreationAggregate(t, command, source)
	child := characterizationChild(source.Result, "ready")
	prune := characterizationPrune(source.Result, created, "pruned")
	t.Run("source", func(t *testing.T) {
		stages := []aggregateValidationCase[SourceOperationResult]{
			{name: "identity", mutate: func(r *SourceOperationResult) { r.SourceInvocationID = "" }, want: "source_invocation_id must be one canonical non-zero UUID"},
			{name: "bundle", mutate: func(r *SourceOperationResult) { r.BundleHash = "" }, want: "bundle_hash must be non-empty"},
			{name: "declaration", mutate: func(r *SourceOperationResult) { r.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
			{name: "expected", mutate: func(r *SourceOperationResult) { r.ExpectedHead.State = "unknown" }, want: "expected head: expected head state must be absent or version"},
			{name: "observed", mutate: func(r *SourceOperationResult) { r.ObservedHead.State = "unknown" }, want: "observed head: expected head state must be absent or version"},
			{name: "candidate", mutate: func(r *SourceOperationResult) { r.Candidate.State = "unknown" }, want: "candidate state must be none, candidate, or version"},
			{name: "head", mutate: func(r *SourceOperationResult) { r.Head.Changed = false }, want: "head changed contradicts before and after"},
			{name: "delta", mutate: func(r *SourceOperationResult) { r.Delta.State = "unknown" }, want: "delta state must be computed, not_computed, or not_comparable"},
			{name: "page", mutate: func(r *SourceOperationResult) { r.Defects.Items = nil }, want: "source defects: page items must be an array"},
			{name: "completion", mutate: func(r *SourceOperationResult) { r.CompletedAt = time.Time{} }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
			{name: "decision", mutate: func(r *SourceOperationResult) { r.ExpectedHead = r.Head.After }, want: "accepted source result has contradictory head, delta, or defects"},
		}
		characterizeStagePrecedence(t, source.Result, validateSourceOperationResult, stages)
	})
	t.Run("child", func(t *testing.T) {
		characterizeStagePrecedence(t, child, FusedChildEvaluation.Validate, []aggregateValidationCase[FusedChildEvaluation]{
			{name: "identity", mutate: func(e *FusedChildEvaluation) { e.SourceInvocationID = "" }, want: "source_invocation_id must be one canonical non-zero UUID"},
			{name: "bundle", mutate: func(e *FusedChildEvaluation) { e.BundleHash = "" }, want: "bundle_hash must be non-empty"},
			{name: "declaration", mutate: func(e *FusedChildEvaluation) { e.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
			{name: "head", mutate: func(e *FusedChildEvaluation) { e.ObservedHead.State = "unknown" }, want: "expected head state must be absent or version"},
			{name: "candidate", mutate: func(e *FusedChildEvaluation) { e.Candidate.State = "unknown" }, want: "candidate state must be none, candidate, or version"},
			{name: "delta", mutate: func(e *FusedChildEvaluation) { e.Delta.State = "unknown" }, want: "delta state must be computed, not_computed, or not_comparable"},
			{name: "count", mutate: func(e *FusedChildEvaluation) { e.DefectCount = -1 }, want: "fused child defect_count must not be negative"},
			{name: "decision", mutate: func(e *FusedChildEvaluation) { e.ExpectedHead = VersionHead(e.Candidate.VersionID) }, want: "ready fused child has contradictory head, delta, or defects"},
		})
	})
	t.Run("prune", func(t *testing.T) {
		characterizeStagePrecedence(t, prune, PruneOperationResult.Validate, []aggregateValidationCase[PruneOperationResult]{
			{name: "identity", mutate: func(r *PruneOperationResult) { r.PruneInvocationID = "" }, want: "prune_invocation_id must be one canonical non-zero UUID"},
			{name: "declaration", mutate: func(r *PruneOperationResult) { r.Declaration.EventName = "" }, want: "event name \"\" must be one canonical qualified event name"},
			{name: "version", mutate: func(r *PruneOperationResult) { r.VersionID = "" }, want: "resource-version-v1:sha256: must be resource-version-v1:sha256:<64 lowercase hex>"},
			{name: "head", mutate: func(r *PruneOperationResult) { r.ObservedHead.State = "unknown" }, want: "expected head state must be absent or version"},
			{name: "completion", mutate: func(r *PruneOperationResult) { r.CompletedAt = time.Time{} }, want: "completed_at must be one non-zero UTC microsecond timestamp"},
			{name: "count", mutate: func(r *PruneOperationResult) { r.PinCount = -1 }, want: "prune pin_count must not be negative"},
			{name: "page", mutate: func(r *PruneOperationResult) { p := FirstEvidencePage([]Pin{}); p.Items = nil; r.Pins = &p }, want: "page items must be an array"},
			{name: "decision", mutate: func(r *PruneOperationResult) { r.PayloadBefore = "wrong" }, want: "pruned result has contradictory decision facts"},
		})
	})
	t.Run("run", func(t *testing.T) {
		rejected := characterizationRejectedRun(created, characterizationChild(source.Result, "validation_rejected"))
		characterizeStagePrecedence(t, rejected, RunCreationOperationRecord.Validate, []aggregateValidationCase[RunCreationOperationRecord]{
			{name: "summary", mutate: func(r *RunCreationOperationRecord) { r.Summary.Kind = "unknown" }, want: "run-creation summary kind must be run_creation"},
			{name: "binding", mutate: func(r *RunCreationOperationRecord) { r.Binding.State = "unknown" }, want: "data binding state must be none or bound"},
			{name: "child", mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildEvaluations[0].BundleHash = "" }, want: "fused child " + source.Result.SourceInvocationID + ": bundle_hash must be non-empty"},
			{name: "defect", mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildDefects[0].Defect.Message = "" }, want: "validation defect requires code and message"},
			{name: "counts", mutate: func(r *RunCreationOperationRecord) { r.Evidence.ChildEvaluations[0].DefectCount++ }, want: "fused child " + source.Result.SourceInvocationID + " defect_count contradicts evidence"},
			{name: "decision", mutate: func(r *RunCreationOperationRecord) { r.Summary.ImportCount++ }, want: "rejected run creation has contradictory binding or child count"},
		})
	})
	t.Run("created_binding", func(t *testing.T) {
		characterizeStagePrecedence(t, created, validateCreatedRunBinding, []aggregateValidationCase[RunCreationOperationRecord]{
			{name: "pin", mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Kind = "unknown" }, want: "run binding must group each declaration under its pin"},
			{name: "parent", mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Pin.RunID = "" }, want: "run-binding pin contradicts parent summary"},
			{name: "import", mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[1].Import.BundleHash = "" }, want: "bound import contradicts its fused-import pin"},
			{name: "counts", mutate: func(r *RunCreationOperationRecord) { r.Summary.PinCount++ }, want: "run-binding item counts contradict summary"},
		})
	})
}

func characterizeStagePrecedence[T any](t *testing.T, base T, validate func(T) error, stages []aggregateValidationCase[T]) {
	t.Helper()
	cases := make([]aggregateValidationCase[T], 0)
	for i, first := range stages {
		cases = append(cases, aggregateValidationCase[T]{name: first.name + "/alone", base: base, mutate: first.mutate, want: first.want})
		for _, later := range stages[i+1:] {
			cases = append(cases, aggregateValidationCase[T]{name: fmt.Sprintf("%s/before/%s", first.name, later.name), base: base,
				mutate: func(value *T) { later.mutate(value); first.mutate(value) }, want: first.want})
		}
	}
	runAggregateValidationCases(t, validate, cases)
}
