package durabledata

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const aggregateTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestCandidateVersionClosedStates(t *testing.T) {
	ref, err := ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	compiled, defects := CompileJSONL(ref, map[string]any{
		"type": "object", "additionalProperties": false,
	}, "", nil)
	if len(defects) != 0 {
		t.Fatalf("CompileJSONL defects = %#v", defects)
	}
	manifest := compiled.Manifest
	for _, test := range []struct {
		name      string
		candidate CandidateVersion
		valid     bool
	}{
		{name: "none", candidate: CandidateVersion{State: "none"}, valid: true},
		{name: "uncommitted candidate", candidate: CandidateVersion{State: "candidate", VersionID: compiled.VersionID, Manifest: &manifest}, valid: true},
		{name: "committed version", candidate: CandidateVersion{State: "version", VersionID: compiled.VersionID, Alias: "v1", Manifest: &manifest}, valid: true},
		{name: "candidate with alias", candidate: CandidateVersion{State: "candidate", VersionID: compiled.VersionID, Alias: "v1", Manifest: &manifest}},
		{name: "version without alias", candidate: CandidateVersion{State: "version", VersionID: compiled.VersionID, Manifest: &manifest}},
		{name: "noncanonical alias", candidate: CandidateVersion{State: "version", VersionID: compiled.VersionID, Alias: "v01", Manifest: &manifest}},
		{name: "manifest contradiction", candidate: CandidateVersion{State: "candidate", VersionID: VersionID("resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), Manifest: &manifest}},
		{name: "unknown state", candidate: CandidateVersion{State: "pending", VersionID: compiled.VersionID, Manifest: &manifest}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.candidate.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("Validate error = nil, want closed-union rejection")
			}
		})
	}
}

func TestRunCreationRejectionClosedStates(t *testing.T) {
	ref, err := ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	version := VersionID("resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	expected := SchemaDigest("resource-schema-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	selected := SchemaDigest("resource-schema-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	refPointer := func() *DeclarationRef {
		copy := ref
		return &copy
	}
	for _, test := range []struct {
		name      string
		outcome   string
		rejection RunCreationRejection
		valid     bool
	}{
		{name: "created", outcome: "created", rejection: NoRunCreationRejection(), valid: true},
		{name: "fused validation", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionFusedValidation}, valid: true},
		{name: "fused head", outcome: "head_conflict", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionFusedHead}, valid: true},
		{name: "missing pin", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing, Declaration: refPointer(), VersionID: version}, valid: true},
		{name: "pruned pin", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionPruned, Declaration: refPointer(), VersionID: version}, valid: true},
		{name: "schema mismatch", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionSchemaMismatch, Declaration: refPointer(), VersionID: version, ExpectedSchemaDigest: expected, SelectedSchemaDigest: selected}, valid: true},
		{name: "created with rejection", outcome: "created", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing, Declaration: refPointer(), VersionID: version}},
		{name: "missing target facts", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing}},
		{name: "schema mismatch without both schemas", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionSchemaMismatch, Declaration: refPointer(), VersionID: version, ExpectedSchemaDigest: expected}},
		{name: "schema mismatch with equal schemas", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionSchemaMismatch, Declaration: refPointer(), VersionID: version, ExpectedSchemaDigest: expected, SelectedSchemaDigest: expected}},
		{name: "fused rejection with target facts", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionFusedValidation, Declaration: refPointer(), VersionID: version}},
		{name: "unknown code", outcome: "data_rejected", rejection: RunCreationRejection{State: "rejected", Code: "other"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.rejection.ValidateForOutcome(test.outcome)
			if test.valid && err != nil {
				t.Fatalf("ValidateForOutcome: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("ValidateForOutcome error = nil, want closed-union rejection")
			}
		})
	}

	fusedJSON, err := json.Marshal(RunCreationRejection{State: "rejected", Code: RunCreationRejectionFusedValidation})
	if err != nil || string(fusedJSON) != `{"state":"rejected","code":"fused_validation_rejected"}` {
		t.Fatalf("fused rejection JSON = %s, %v", fusedJSON, err)
	}
	pinJSON, err := json.Marshal(RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing, Declaration: refPointer(), VersionID: version})
	if err != nil {
		t.Fatal(err)
	}
	var pinObject map[string]any
	if err := json.Unmarshal(pinJSON, &pinObject); err != nil || len(pinObject) != 4 {
		t.Fatalf("pin rejection JSON = %s, %v", pinJSON, err)
	}
}

func TestSourceCandidateStateAgreesWithOperationOutcome(t *testing.T) {
	ref, err := ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	compiled, defects := CompileJSONL(ref, map[string]any{"type": "object", "additionalProperties": false}, "", nil)
	if len(defects) != 0 {
		t.Fatalf("CompileJSONL defects = %#v", defects)
	}
	manifest := compiled.Manifest
	candidate := CandidateVersion{State: "candidate", VersionID: compiled.VersionID, Manifest: &manifest}
	version := CandidateVersion{State: "version", VersionID: compiled.VersionID, Alias: "v1", Manifest: &manifest}
	none := CandidateVersion{State: "none"}
	for _, test := range []struct {
		operation string
		outcome   string
		candidate CandidateVersion
		valid     bool
	}{
		{operation: "check", outcome: "accepted", candidate: candidate, valid: true},
		{operation: "import", outcome: "accepted", candidate: version, valid: true},
		{operation: "check", outcome: "head_conflict", candidate: candidate, valid: true},
		{operation: "import", outcome: "head_conflict", candidate: candidate, valid: true},
		{operation: "import", outcome: "validation_rejected", candidate: none, valid: true},
		{operation: "import", outcome: "accepted", candidate: candidate},
		{operation: "check", outcome: "accepted", candidate: version},
		{operation: "check", outcome: "validation_rejected", candidate: candidate},
		{operation: "unknown", outcome: "validation_rejected", candidate: none},
		{operation: "unknown", outcome: "head_conflict", candidate: candidate},
	} {
		err := validateSourceCandidateState(test.operation, test.outcome, test.candidate)
		if test.valid && err != nil {
			t.Fatalf("%s %s ValidateCandidateState: %v", test.operation, test.outcome, err)
		}
		if !test.valid && err == nil {
			t.Fatalf("%s %s accepted candidate state %s", test.operation, test.outcome, test.candidate.State)
		}
	}
}

func TestParseVersionAliasRejectsAlternateSpellings(t *testing.T) {
	for _, test := range []struct {
		alias string
		valid bool
	}{
		{alias: "v1", valid: true},
		{alias: "v42", valid: true},
		{alias: "v0"},
		{alias: "v01"},
		{alias: "v+1"},
		{alias: "V1"},
		{alias: "v 1"},
		{alias: "v1 "},
	} {
		t.Run(test.alias, func(t *testing.T) {
			_, err := ParseVersionAlias(test.alias)
			if test.valid && err != nil {
				t.Fatalf("ParseVersionAlias(%q): %v", test.alias, err)
			}
			if !test.valid && err == nil {
				t.Fatalf("ParseVersionAlias(%q) accepted noncanonical spelling", test.alias)
			}
		})
	}
}

func TestRunCreationEnvelopeRejectsDuplicateFusedChildInvocationIDsBeforeHashing(t *testing.T) {
	first, err := ParseDeclarationRef(".", "first.loaded")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ParseDeclarationRef(".", "second.loaded")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	envelope := RunCreationDataEnvelope{Imports: []FusedImport{
		{SourceInvocationID: id, Declaration: first, ExpectedHead: AbsentHead(), InputFormat: "jsonl"},
		{SourceInvocationID: id, Declaration: second, ExpectedHead: AbsentHead(), InputFormat: "jsonl", Input: []byte("not-json")},
	}}
	if _, err := envelope.Canonical(); err == nil {
		t.Fatal("Canonical accepted duplicate child invocation IDs")
	}
	command := RunCreationCommand{
		RunID: uuid.NewString(), Actor: "operator", BundleHash: aggregateTestBundleHash, EventID: uuid.NewString(),
		InitialEvent: json.RawMessage(`{"type":"start"}`), Data: envelope,
	}
	if _, _, _, err := command.RequestHash(); err == nil {
		t.Fatal("RequestHash accepted duplicate child invocation IDs")
	}
}

func TestPermanentReceiptAggregateValidatorsRejectHostileContradictions(t *testing.T) {
	sourceCommand, source := validSourceAggregate(t)
	for _, test := range []struct {
		name   string
		mutate func(*SourceOperationRecord)
	}{
		{name: "invocation identity", mutate: func(r *SourceOperationRecord) { r.Result.SourceInvocationID = uuid.NewString() }},
		{name: "schema identity", mutate: func(r *SourceOperationRecord) {
			r.Result.SchemaDigest = SchemaDigest("resource-schema-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
		}},
		{name: "expected observed agreement", mutate: func(r *SourceOperationRecord) { r.Result.ExpectedHead = VersionHead(r.Result.Candidate.VersionID) }},
		{name: "head transition", mutate: func(r *SourceOperationRecord) { r.Result.Head.Changed = false }},
		{name: "delta state", mutate: func(r *SourceOperationRecord) { r.Result.Delta = UncomputedDelta("head_conflict") }},
		{name: "delta row identity", mutate: func(r *SourceOperationRecord) { r.Result.Delta.RowIdentity = "" }},
		{name: "defect page evidence", mutate: func(r *SourceOperationRecord) {
			r.Evidence.Defects = []ValidationDefect{{Code: "hostile", Message: "hostile"}}
		}},
		{name: "completion", mutate: func(r *SourceOperationRecord) { r.Result.CompletedAt = time.Time{} }},
	} {
		t.Run("source/"+test.name, func(t *testing.T) {
			hostile := cloneJSON(t, source)
			hostile.Evaluation = source.Evaluation
			test.mutate(&hostile)
			if err := hostile.ValidateForCommand(sourceCommand); err == nil {
				t.Fatal("hostile source aggregate validated")
			}
		})
	}
	missingEvidence := cloneJSON(t, source)
	missingEvidence.Evaluation = source.Evaluation
	missingEvidence.Evidence.DeltaAdded = nil
	if err := missingEvidence.ValidateForCommand(sourceCommand); err == nil {
		t.Fatal("keyed source aggregate accepted deleted delta evidence")
	}
	positionalWithKeys := cloneJSON(t, source)
	positionalWithKeys.Result.Delta.RowIdentity = DeltaRowIdentityPosition
	if err := positionalWithKeys.ValidateForCommand(sourceCommand); err == nil {
		t.Fatal("positional source aggregate accepted business-key evidence")
	}

	runCommand, runRecord := validRunCreationAggregate(t, sourceCommand, source)
	for _, test := range []struct {
		name   string
		mutate func(*RunCreationOperationRecord)
	}{
		{name: "summary kind", mutate: func(r *RunCreationOperationRecord) { r.Summary.Kind = "event_publish" }},
		{name: "summary count", mutate: func(r *RunCreationOperationRecord) { r.Summary.PinCount++ }},
		{name: "summary status", mutate: func(r *RunCreationOperationRecord) { r.Summary.Status = "bogus" }},
		{name: "binding state", mutate: func(r *RunCreationOperationRecord) { r.Binding = DataBinding{State: "none"} }},
		{name: "binding page", mutate: func(r *RunCreationOperationRecord) { r.Binding.Evidence.ItemCount-- }},
		{name: "run binding item union", mutate: func(r *RunCreationOperationRecord) { r.Evidence.RunBinding[0].Import = r.Evidence.RunBinding[1].Import }},
		{name: "bound import identity", mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding[1].Import.SourceInvocationID = uuid.NewString()
		}},
		{name: "bound import operation", mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding[1].Import.Operation = "check"
			r.Binding.Evidence.Items[1].Import.Operation = "check"
		}},
		{name: "bound import parent bundle", mutate: func(r *RunCreationOperationRecord) {
			r.Evidence.RunBinding[1].Import.BundleHash = "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			r.Binding.Evidence.Items[1].Import.BundleHash = "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}},
		{name: "event identity", mutate: func(r *RunCreationOperationRecord) { r.Summary.EventID = uuid.NewString() }},
	} {
		t.Run("run_creation/"+test.name, func(t *testing.T) {
			hostile := cloneJSON(t, runRecord)
			test.mutate(&hostile)
			if err := hostile.ValidateForCommand(runCommand); err == nil {
				t.Fatal("hostile run-creation aggregate validated")
			}
		})
	}

	for _, status := range []string{"running", "paused", "completed", "failed", "cancelled", "forked"} {
		t.Run("run_creation/status/"+status, func(t *testing.T) {
			summary := runRecord.Summary
			summary.Status = status
			if err := summary.Validate(); err != nil {
				t.Fatalf("canonical status rejected: %v", err)
			}
			pin := *runRecord.Evidence.RunBinding[0].Pin
			pin.RunState = status
			if err := pin.Validate(); err != nil {
				t.Fatalf("canonical pin run_state rejected: %v", err)
			}
		})
	}
	for _, status := range []string{"", "bogus", "Running", " running "} {
		t.Run("run_creation/status/reject/"+status, func(t *testing.T) {
			summary := runRecord.Summary
			summary.Status = status
			if err := summary.Validate(); err == nil {
				t.Fatal("noncanonical summary status validated")
			}
			pin := *runRecord.Evidence.RunBinding[0].Pin
			pin.RunState = status
			if err := pin.Validate(); err == nil {
				t.Fatal("noncanonical pin run_state validated")
			}
		})
	}

	pruneCommand := PruneCommand{
		PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: source.Result.Declaration,
		VersionID: source.Result.Candidate.VersionID, ExpectedHead: AbsentHead(),
	}
	prune := PruneOperationResult{
		Outcome: "pruned", PruneInvocationID: pruneCommand.PruneInvocationID, Declaration: pruneCommand.Declaration,
		VersionID: pruneCommand.VersionID, ExpectedHead: AbsentHead(), ObservedHead: AbsentHead(),
		PayloadBefore: "materialized", PayloadAfter: "pruned", CompletedAt: source.Result.CompletedAt,
	}
	if err := prune.ValidateForCommand(pruneCommand, nil); err != nil {
		t.Fatalf("valid prune aggregate: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*PruneOperationResult)
	}{
		{name: "invocation identity", mutate: func(r *PruneOperationResult) { r.PruneInvocationID = uuid.NewString() }},
		{name: "outcome", mutate: func(r *PruneOperationResult) { r.Outcome = "already_pruned" }},
		{name: "head", mutate: func(r *PruneOperationResult) { r.ObservedHead = VersionHead(r.VersionID) }},
		{name: "payload before", mutate: func(r *PruneOperationResult) { r.PayloadBefore = "pruned" }},
		{name: "payload after", mutate: func(r *PruneOperationResult) { r.PayloadAfter = "materialized" }},
		{name: "pin facts", mutate: func(r *PruneOperationResult) { r.PinCount = 1 }},
		{name: "defect facts", mutate: func(r *PruneOperationResult) {
			page := FirstEvidencePage([]PruneDefect{{Code: "version_not_found", Message: "missing"}})
			r.Defects = &page
		}},
	} {
		t.Run("prune/"+test.name, func(t *testing.T) {
			hostile := prune
			test.mutate(&hostile)
			if err := hostile.ValidateForCommand(pruneCommand, nil); err == nil {
				t.Fatal("hostile prune aggregate validated")
			}
		})
	}
}

func TestSourceEvaluationRejectsCoordinatedSemanticMutations(t *testing.T) {
	command, record := validSourceAggregate(t)

	changedInput := command
	changedInput.Input = []byte("{\"slug\":\"beta\"}\n")
	if err := record.ValidateForCommand(changedInput); err == nil {
		t.Fatal("source evaluation accepted a different exact input with retained result")
	}

	positional := cloneJSON(t, record)
	positional.Evaluation = record.Evaluation
	positional.Result.Delta.RowIdentity = DeltaRowIdentityPosition
	positional.Evidence = SourceEvidence{}
	if err := positional.Validate(); err != nil {
		t.Fatalf("coordinated positional receipt is structurally valid: %v", err)
	}
	if err := positional.ValidateForCommand(command); err == nil {
		t.Fatal("keyed evaluation accepted coordinated positional downgrade")
	}

	schema := mustDecodeEvaluationSchema(record.Evaluation.Declaration.CanonicalSchema)
	other, defects := CompileJSONL(command.Declaration, schema, "slug", []byte("{\"slug\":\"beta\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile hostile candidate: %#v", defects)
	}
	coordinatedCandidate := cloneJSON(t, record)
	coordinatedCandidate.Evaluation = record.Evaluation
	manifest := other.Manifest
	coordinatedCandidate.Result.Candidate.VersionID = other.VersionID
	coordinatedCandidate.Result.Candidate.Manifest = &manifest
	coordinatedCandidate.Result.Head.After = VersionHead(other.VersionID)
	if err := coordinatedCandidate.Validate(); err != nil {
		t.Fatalf("coordinated candidate receipt is structurally valid: %v", err)
	}
	if err := coordinatedCandidate.ValidateForCommand(command); err == nil {
		t.Fatal("source evaluation accepted coordinated candidate/manifest mutation")
	}

	changedDelta := cloneJSON(t, record)
	changedDelta.Evaluation = record.Evaluation
	changedDelta.Result.Delta.Summary = &DeltaSummary{Removed: 1}
	changedDelta.Evidence = SourceEvidence{DeltaRemoved: []DeltaKey{{Key: BusinessKey(`"alpha"`)}}}
	if err := changedDelta.Validate(); err != nil {
		t.Fatalf("coordinated delta receipt is structurally valid: %v", err)
	}
	if err := changedDelta.ValidateForCommand(command); err == nil {
		t.Fatal("source evaluation accepted coordinated delta class/count mutation")
	}

	rejectedCommand := command
	rejectedCommand.SourceInvocationID = uuid.NewString()
	rejectedCommand.Input = []byte("{}\n")
	_, rejectedResult, rejectedEvidence, err := record.Evaluation.Evaluate(rejectedCommand)
	if err != nil {
		t.Fatal(err)
	}
	rejected := SourceOperationRecord{Evaluation: record.Evaluation, Result: rejectedResult, Evidence: rejectedEvidence}
	if err := rejected.ValidateForCommand(rejectedCommand); err != nil {
		t.Fatalf("canonical validation rejection: %v", err)
	}
	hostileDefects := []ValidationDefect{{Row: 1, Path: "$.slug", Code: "schema_rejected", Message: "coordinated false defect"}}
	rejected.Result.Defects = FirstEvidencePage(hostileDefects)
	rejected.Evidence.Defects = hostileDefects
	if err := rejected.Validate(); err != nil {
		t.Fatalf("coordinated rejection is structurally valid: %v", err)
	}
	if err := rejected.ValidateForCommand(rejectedCommand); err == nil {
		t.Fatal("source evaluation accepted coordinated validation-defect mutation")
	}
}

func TestProvenanceValidationClosesEveryProducerArm(t *testing.T) {
	versionID := VersionID("resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	now := time.Date(2026, time.August, 26, 12, 0, 0, 123000, time.UTC)
	for _, test := range []struct {
		kind string
		id   string
	}{
		{kind: "import", id: uuid.NewString()},
		{kind: "normal_run", id: uuid.NewString()},
		{kind: "fork_candidate_promotion", id: uuid.NewString()},
	} {
		ref, err := NewProvenanceRef(test.kind, test.id)
		if err != nil {
			t.Fatalf("NewProvenanceRef(%s): %v", test.kind, err)
		}
		provenance := Provenance{Sequence: 1, VersionID: versionID, ProducerRef: ref, Actor: "operator", CommittedAt: now}
		if err := provenance.Validate(); err != nil {
			t.Fatalf("%s provenance: %v", test.kind, err)
		}
	}
	invalid := []Provenance{
		{VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "import", SourceInvocationID: uuid.NewString()}, Actor: "operator", CommittedAt: now},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "import", SourceInvocationID: "NOT-A-UUID"}, Actor: "operator", CommittedAt: now},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "normal_run", RunID: strings.ToUpper(uuid.NewString())}, Actor: "operator", CommittedAt: now},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "fork_candidate_promotion", PromotionInvocationID: uuid.Nil.String()}, Actor: "operator", CommittedAt: now},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "import", SourceInvocationID: uuid.NewString()}, Actor: " ", CommittedAt: now},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "import", SourceInvocationID: uuid.NewString()}, Actor: "operator"},
		{Sequence: 1, VersionID: versionID, ProducerRef: ProvenanceRef{Kind: "import", SourceInvocationID: uuid.NewString()}, Actor: "operator", CommittedAt: now.Add(time.Nanosecond)},
	}
	for index, provenance := range invalid {
		if err := provenance.Validate(); err == nil {
			t.Fatalf("invalid provenance %d validated: %#v", index, provenance)
		}
	}
}

func validSourceAggregate(t *testing.T) (SourceCommand, SourceOperationRecord) {
	t.Helper()
	ref, err := ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"slug"},
		"properties": map[string]any{"slug": map[string]any{"type": "string"}},
	}
	input := []byte("{\"slug\":\"alpha\"}\n")
	compiled, defects := CompileJSONL(ref, schema, "slug", input)
	if len(defects) != 0 {
		t.Fatalf("CompileJSONL defects = %#v", defects)
	}
	manifest := compiled.Manifest
	command := SourceCommand{
		Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: aggregateTestBundleHash,
		Declaration: ref, ExpectedHead: AbsentHead(), InputFormat: "jsonl", Input: input,
	}
	result := SourceOperationResult{
		SourceInvocationID: command.SourceInvocationID, Operation: "import", Outcome: "accepted", BundleHash: command.BundleHash,
		SchemaDigest: manifest.SchemaDigest, Declaration: ref, ExpectedHead: AbsentHead(), ObservedHead: AbsentHead(),
		Candidate: CandidateVersion{State: "version", VersionID: compiled.VersionID, Alias: "v1", Manifest: &manifest},
		Head:      HeadResult{Before: AbsentHead(), After: VersionHead(compiled.VersionID), Changed: true, Revision: 1},
		Delta:     ComputedDelta(AbsentHead(), DeltaSummary{Added: 1}, DeltaRowIdentityBusinessKey), Defects: FirstEvidencePage([]ValidationDefect{}),
		CompletedAt: time.Date(2026, time.August, 26, 0, 0, 0, 0, time.UTC),
	}
	record := SourceOperationRecord{
		Evaluation: SourceEvaluationContext{
			Format: SourceEvaluationFormat,
			Declaration: Declaration{
				Name: ref.EventName, Ref: ref, BusinessKey: "slug", SchemaDigest: manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
			},
			Base:        SourceEvaluationBase{State: "absent", Head: HeadResult{Before: AbsentHead(), After: AbsentHead()}},
			CompletedAt: result.CompletedAt,
		},
		Result: result, Evidence: SourceEvidence{DeltaAdded: []DeltaKey{{Key: BusinessKey(`"alpha"`)}}},
	}
	if err := record.ValidateForCommand(command); err != nil {
		t.Fatalf("valid source aggregate: %v", err)
	}
	return command, record
}

func validRunCreationAggregate(t *testing.T, sourceCommand SourceCommand, source SourceOperationRecord) (RunCreationCommand, RunCreationOperationRecord) {
	t.Helper()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	command := RunCreationCommand{
		RunID: runID, Actor: "operator", BundleHash: sourceCommand.BundleHash, EventID: eventID,
		InitialEvent: json.RawMessage(`{"type":"start"}`),
		Data: RunCreationDataEnvelope{Imports: []FusedImport{{
			SourceInvocationID: sourceCommand.SourceInvocationID, Declaration: sourceCommand.Declaration,
			ExpectedHead: sourceCommand.ExpectedHead, InputFormat: sourceCommand.InputFormat, Input: sourceCommand.Input,
		}}},
	}
	pin := Pin{
		RunID: runID, RunState: "running", Declaration: source.Result.Declaration, SchemaDigest: source.Result.SchemaDigest,
		VersionID: source.Result.Candidate.VersionID, Selection: "fused_import",
	}
	summary := SummarizeSource(source)
	items := []RunCreationDataItem{{Kind: "pin", Pin: &pin}, {Kind: "import", Import: &summary}}
	page := FirstEvidencePage(items)
	record := RunCreationOperationRecord{
		Summary: RunCreationOperationSummary{
			Kind: "run_creation", Outcome: "created", RunID: runID, BundleHash: command.BundleHash, EventID: eventID,
			Status: "running", PinCount: 1, ImportCount: 1, Rejection: NoRunCreationRejection(), CompletedAt: source.Result.CompletedAt,
		},
		Binding:  DataBinding{State: "bound", RunID: runID, PinCount: 1, ImportCount: 1, Evidence: &page},
		Evidence: RunCreationEvidence{RunBinding: items},
	}
	if err := record.ValidateForCommand(command); err != nil {
		t.Fatalf("valid run-creation aggregate: %v", err)
	}
	return command, record
}

func cloneJSON[T any](t *testing.T, value T) T {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var copy T
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}
