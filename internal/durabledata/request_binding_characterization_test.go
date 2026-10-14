package durabledata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func requestBindingFixture(t *testing.T) (RunCreationCommand, RunCreationOperationRecord, RunCreationRequestBinding) {
	t.Helper()
	sourceCommand, source := validSourceAggregate(t)
	command, record := validRunCreationAggregate(t, sourceCommand, source)
	ref := DeclarationRef{FlowPath: ".", EventName: "scores.loaded"}
	command.Data.Pins = []ExplicitPin{{Declaration: ref, VersionID: source.Result.Candidate.VersionID}}
	pin := *record.Evidence.RunBinding[0].Pin
	pin.Declaration, pin.Selection = ref, "explicit"
	record.Evidence.RunBinding = append(record.Evidence.RunBinding, RunCreationDataItem{Kind: "pin", Pin: &pin})
	record.Summary.PinCount, record.Binding.PinCount = 2, 2
	page := FirstEvidencePage(record.Evidence.RunBinding)
	record.Binding.Evidence = &page
	binding, err := BindRunCreationRequest(command, record)
	if err != nil {
		t.Fatalf("combined request fixture: %v", err)
	}
	return command, record, binding
}

func requestBindingRejectedFixture(t *testing.T) (RunCreationCommand, RunCreationOperationRecord, RunCreationRequestBinding) {
	t.Helper()
	sourceCommand, source := validSourceAggregate(t)
	command, created := validRunCreationAggregate(t, sourceCommand, source)
	ref := DeclarationRef{FlowPath: ".", EventName: "scores.loaded"}
	command.Data.Pins = []ExplicitPin{{Declaration: ref, VersionID: source.Result.Candidate.VersionID}}
	candidate := source.Result.Candidate
	candidate.State, candidate.Alias = "candidate", ""
	record := RunCreationOperationRecord{
		Summary: RunCreationOperationSummary{
			Kind: "run_creation", Outcome: "data_rejected", RunID: command.RunID,
			BundleHash: command.BundleHash, ImportCount: 1, CompletedAt: created.Summary.CompletedAt,
			Rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing,
				Declaration: &ref, VersionID: source.Result.Candidate.VersionID},
		},
		Binding: DataBinding{State: "none"},
		Evidence: RunCreationEvidence{ChildEvaluations: []FusedChildEvaluation{{
			SourceInvocationID: sourceCommand.SourceInvocationID, BundleHash: sourceCommand.BundleHash,
			SchemaDigest: source.Result.SchemaDigest, Declaration: sourceCommand.Declaration,
			ExpectedHead: sourceCommand.ExpectedHead, ObservedHead: source.Result.ObservedHead,
			Candidate: candidate, Delta: source.Result.Delta, Outcome: "ready",
		}}},
	}
	binding, err := BindRunCreationRequest(command, record)
	if err != nil {
		t.Fatalf("rejected request fixture: %v", err)
	}
	return command, record, binding
}

func requireRequestBindingError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestRequestBindingR01Identity(t *testing.T) {
	_, _, binding := requestBindingFixture(t)
	for _, test := range []struct {
		name string
		edit func(*RunCreationRequestBinding)
		want string
	}{
		{"run_absent", func(b *RunCreationRequestBinding) { b.RunID = "" }, "request binding has invalid run_id"},
		{"run_zero", func(b *RunCreationRequestBinding) { b.RunID = uuid.Nil.String() }, "request binding has invalid run_id"},
		{"run_uppercase", func(b *RunCreationRequestBinding) { b.RunID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" }, "request binding has invalid run_id"},
		{"event_zero", func(b *RunCreationRequestBinding) { b.EventID = uuid.Nil.String() }, "request binding has invalid event_id"},
		{"event_uppercase", func(b *RunCreationRequestBinding) { b.EventID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" }, "request binding has invalid event_id"},
		{"hash_prefix", func(b *RunCreationRequestBinding) { b.RequestHash = strings.TrimPrefix(b.RequestHash, "resource-") }, "request binding has invalid request_hash"},
		{"hash_short", func(b *RunCreationRequestBinding) { b.RequestHash = b.RequestHash[:len(b.RequestHash)-1] }, "request binding has invalid request_hash"},
		{"hash_uppercase", func(b *RunCreationRequestBinding) {
			b.RequestHash = "resource-run-creation-request-v1:sha256:" + strings.Repeat("A", 64)
		}, "request binding has invalid request_hash"},
		{"hash_nonhex", func(b *RunCreationRequestBinding) {
			b.RequestHash = "resource-run-creation-request-v1:sha256:" + strings.Repeat("g", 64)
		}, "request binding has invalid request_hash"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := cloneJSON(t, binding)
			test.edit(&b)
			requireRequestBindingError(t, b.Validate(), test.want)
		})
	}
	for _, hash := range []string{"", "weak", "bundle-v2:sha256:" + strings.Repeat("A", 64)} {
		b := binding
		b.BundleHash = hash
		if err := b.Validate(); err == nil {
			t.Fatalf("invalid bundle %q admitted", hash)
		}
	}
}

func TestRequestBindingR02InitiationAndBound(t *testing.T) {
	_, _, binding := requestBindingFixture(t)
	for _, mode := range []string{"event_only", "data_only", "combined"} {
		t.Run(mode, func(t *testing.T) {
			b := binding
			if mode == "event_only" {
				b.Imports, b.Pins = nil, nil
			} else if mode == "data_only" {
				b.EventID = ""
			}
			if err := b.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	b := binding
	b.EventID, b.Imports, b.Pins = "", nil, nil
	requireRequestBindingError(t, b.Validate(), "request binding has no initiation")
	for _, imports := range []bool{false, true} {
		b := binding
		b.EventID, b.Imports, b.Pins = "", nil, nil
		for index := 0; index < 257; index++ {
			ref := DeclarationRef{FlowPath: ".", EventName: fmt.Sprintf("item%03d.loaded", index)}
			if imports {
				item := binding.Imports[0]
				item.Declaration, item.SourceInvocationID = ref, fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
				b.Imports = append(b.Imports, item)
			} else {
				b.Pins = append(b.Pins, ExplicitPin{Declaration: ref, VersionID: binding.Pins[0].VersionID})
			}
			if index == 255 {
				if err := b.Validate(); err != nil {
					t.Fatalf("256 selections (imports=%v): %v", imports, err)
				}
			}
		}
		requireRequestBindingError(t, b.Validate(), "request binding exceeds declaration bound")
	}
	b = binding
	b.Pins = make([]ExplicitPin, 256)
	requireRequestBindingError(t, b.Validate(), "request binding exceeds declaration bound")
}

func TestRequestBindingR03Imports(t *testing.T) {
	_, _, binding := requestBindingFixture(t)
	for _, test := range []struct {
		name string
		edit func(*RunCreationRequestBinding)
		want string
	}{
		{"source", func(b *RunCreationRequestBinding) { b.Imports[0].SourceInvocationID = "invalid" }, "request binding has invalid source_invocation_id"},
		{"head_state", func(b *RunCreationRequestBinding) { b.Imports[0].ExpectedHead.State = "unknown" }, "expected head state must be absent or version"},
		{"absent_head_version", func(b *RunCreationRequestBinding) { b.Imports[0].ExpectedHead.VersionID = b.Pins[0].VersionID }, "absent head forbids version_id"},
		{"duplicate_declaration", func(b *RunCreationRequestBinding) { b.Imports = append(b.Imports, b.Imports[0]) }, "request imports are not declaration-sorted"},
		{"descending", func(b *RunCreationRequestBinding) {
			item := b.Imports[0]
			item.Declaration.EventName = "aaa.loaded"
			b.Imports = append(b.Imports, item)
		}, "request imports are not declaration-sorted"},
		{"duplicate_source", func(b *RunCreationRequestBinding) {
			item := b.Imports[0]
			item.Declaration.EventName = "zzz.loaded"
			b.Imports = append(b.Imports, item)
		}, "request imports repeat source_invocation_id"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := cloneJSON(t, binding)
			test.edit(&b)
			requireRequestBindingError(t, b.Validate(), test.want)
		})
	}
	for _, field := range []string{"declaration", "schema", "version_head"} {
		b := cloneJSON(t, binding)
		switch field {
		case "declaration":
			b.Imports[0].Declaration.EventName = ""
		case "schema":
			b.Imports[0].SchemaDigest = "weak"
		case "version_head":
			b.Imports[0].ExpectedHead = VersionHead("weak")
		}
		if err := b.Validate(); err == nil {
			t.Fatalf("invalid import %s admitted", field)
		}
	}
	b := cloneJSON(t, binding)
	item := b.Imports[0]
	item.Declaration.EventName, item.SourceInvocationID = "zzz.loaded", uuid.NewString()
	b.Imports = append(b.Imports, item)
	if err := b.Validate(); err != nil {
		t.Fatalf("sorted distinct imports: %v", err)
	}
}

func TestRequestBindingR04Pins(t *testing.T) {
	_, _, binding := requestBindingFixture(t)
	for _, test := range []struct {
		name string
		edit func(*RunCreationRequestBinding)
		want string
	}{
		{"duplicate", func(b *RunCreationRequestBinding) { b.Pins = append(b.Pins, b.Pins[0]) }, "request pins are not declaration-sorted"},
		{"descending", func(b *RunCreationRequestBinding) {
			item := b.Pins[0]
			item.Declaration.EventName = "aaa.loaded"
			b.Pins = append(b.Pins, item)
		}, "request pins are not declaration-sorted"},
		{"overlap", func(b *RunCreationRequestBinding) { b.Pins[0].Declaration = b.Imports[0].Declaration }, "request pin overlaps another selection"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := cloneJSON(t, binding)
			test.edit(&b)
			requireRequestBindingError(t, b.Validate(), test.want)
		})
	}
	for _, field := range []string{"declaration", "version"} {
		b := cloneJSON(t, binding)
		if field == "declaration" {
			b.Pins[0].Declaration.EventName = ""
		} else {
			b.Pins[0].VersionID = "weak"
		}
		if err := b.Validate(); err == nil {
			t.Fatalf("invalid pin %s admitted", field)
		}
	}
}

func TestRequestBindingR05CreatedSummary(t *testing.T) {
	_, record, binding := requestBindingFixture(t)
	for _, field := range []string{"run", "bundle", "import_count", "event", "pin_count"} {
		t.Run(field, func(t *testing.T) {
			b := cloneJSON(t, binding)
			want := "request binding contradicts operation summary"
			switch field {
			case "run":
				b.RunID = uuid.NewString()
			case "bundle":
				b.BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64)
			case "import_count":
				b.Imports = nil
			case "event":
				b.EventID = uuid.NewString()
				want = "request binding contradicts created run"
			case "pin_count":
				b.Pins = nil
				want = "request binding contradicts created run"
			}
			requireRequestBindingError(t, b.ValidateForRecord(record), want)
		})
	}
	r := cloneJSON(t, record)
	r.Evidence.RunBinding[1].Import = nil
	if err := binding.ValidateForRecord(r); err == nil {
		t.Fatal("incomplete created evidence admitted")
	}
}

func TestRequestBindingR06CreatedRelationships(t *testing.T) {
	_, record, binding := requestBindingFixture(t)
	for _, test := range []struct {
		name string
		edit func(*RunCreationRequestBinding)
		want string
	}{
		{"source", func(b *RunCreationRequestBinding) { b.Imports[0].SourceInvocationID = uuid.NewString() }, "bound import contradicts request"},
		{"schema", func(b *RunCreationRequestBinding) {
			b.Imports[0].SchemaDigest = SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("f", 64))
		}, "bound import contradicts request"},
		{"head", func(b *RunCreationRequestBinding) { b.Imports[0].ExpectedHead = VersionHead(b.Pins[0].VersionID) }, "bound import contradicts request"},
		{"absent_pin", func(b *RunCreationRequestBinding) { b.Pins[0].Declaration.EventName = "zzz.loaded" }, "bound pin is absent from request"},
		{"explicit_version", func(b *RunCreationRequestBinding) {
			b.Pins[0].VersionID = VersionID("resource-version-v1:sha256:" + strings.Repeat("f", 64))
		}, "bound explicit pin contradicts request"},
		{"selection", func(b *RunCreationRequestBinding) {
			b.Imports[0].Declaration, b.Pins[0].Declaration = b.Pins[0].Declaration, b.Imports[0].Declaration
		}, "bound pin selection contradicts request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := cloneJSON(t, binding)
			test.edit(&b)
			requireRequestBindingError(t, b.ValidateForRecord(record), test.want)
		})
	}
}

func TestRequestBindingR07RejectedChildren(t *testing.T) {
	_, record, binding := requestBindingRejectedFixture(t)
	for _, field := range []string{"source", "declaration", "schema", "head", "bundle"} {
		t.Run(field, func(t *testing.T) {
			b, r := cloneJSON(t, binding), cloneJSON(t, record)
			switch field {
			case "source":
				b.Imports[0].SourceInvocationID = uuid.NewString()
			case "declaration":
				b.Imports[0].Declaration.EventName = "aaa.loaded"
			case "schema":
				b.Imports[0].SchemaDigest = SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("f", 64))
			case "head":
				b.Imports[0].ExpectedHead = VersionHead(b.Pins[0].VersionID)
			case "bundle":
				r.Evidence.ChildEvaluations[0].BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64)
			}
			requireRequestBindingError(t, b.ValidateForRecord(r), "request import contradicts rejected child")
		})
	}
	r := cloneJSON(t, record)
	r.Evidence.ChildEvaluations = nil
	requireRequestBindingError(t, binding.ValidateForRecord(r), "rejected run creation has contradictory binding or child count")
	r = cloneJSON(t, record)
	second := cloneJSON(t, r.Evidence.ChildEvaluations[0])
	second.SourceInvocationID, second.Declaration.EventName = uuid.NewString(), "reports.loaded"
	second.Candidate.Manifest.Declaration = second.Declaration
	version, err := second.Candidate.Manifest.VersionID()
	if err != nil {
		t.Fatal(err)
	}
	second.Candidate.VersionID = version
	r.Summary.ImportCount = 2
	r.Evidence.ChildEvaluations = append(r.Evidence.ChildEvaluations, second)
	b := cloneJSON(t, binding)
	item := b.Imports[0]
	item.SourceInvocationID, item.Declaration = second.SourceInvocationID, second.Declaration
	b.Imports = append(b.Imports, item)
	if err := b.ValidateForRecord(r); err != nil {
		t.Fatalf("ordered rejected children: %v", err)
	}
	b.Imports[0].SourceInvocationID, b.Imports[1].SourceInvocationID = b.Imports[1].SourceInvocationID, b.Imports[0].SourceInvocationID
	requireRequestBindingError(t, b.ValidateForRecord(r), "request import contradicts rejected child")
	r.Evidence.ChildEvaluations[0], r.Evidence.ChildEvaluations[1] = r.Evidence.ChildEvaluations[1], r.Evidence.ChildEvaluations[0]
	requireRequestBindingError(t, b.ValidateForRecord(r), "fused child evaluations must be strictly declaration-sorted")
}

func TestRequestBindingR08RejectedPins(t *testing.T) {
	command, record, _ := requestBindingRejectedFixture(t)
	command.Data.Imports = nil
	command.Data.Pins = append(command.Data.Pins, ExplicitPin{Declaration: DeclarationRef{FlowPath: ".", EventName: "aaa.loaded"}, VersionID: command.Data.Pins[0].VersionID})
	record.Summary.ImportCount, record.Evidence.ChildEvaluations = 0, nil
	b, err := BindRunCreationRequest(command, record)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Pins) != 2 || b.Pins[0].Declaration.EventName != "aaa.loaded" || len(record.Evidence.RunBinding) != 0 {
		t.Fatalf("complete canonical rejected request not retained: %+v", b)
	}
	for _, field := range []string{"missing_target", "wrong_version"} {
		bad := cloneJSON(t, b)
		if field == "missing_target" {
			bad.Pins = bad.Pins[:1]
		} else {
			bad.Pins[1].VersionID = VersionID("resource-version-v1:sha256:" + strings.Repeat("f", 64))
		}
		requireRequestBindingError(t, bad.ValidateForRecord(record), "rejection target is absent from requested pins")
	}
}

func TestRequestBindingR09ProjectionAuthority(t *testing.T) {
	command, record, binding := requestBindingFixture(t)
	hash, _, canonical, err := command.RequestHash()
	if err != nil || binding.RequestHash != hash || !reflect.DeepEqual(binding.Pins, canonical.Data.Pins) {
		t.Fatalf("canonical hash/selections lost: %+v, %v", binding, err)
	}
	badCommand, badRecord := cloneJSON(t, command), cloneJSON(t, record)
	badCommand.RunID, badRecord.Summary.Kind = "invalid", "invalid"
	_, err = BindRunCreationRequest(badCommand, badRecord)
	requireRequestBindingError(t, err, "run_id must be one canonical non-zero UUID")
	_, err = BindRunCreationRequest(command, badRecord)
	requireRequestBindingError(t, err, "run-creation summary kind must be run_creation")
	badCommand = cloneJSON(t, command)
	badCommand.Data.Imports[0].ExpectedHead = VersionHead(binding.Pins[0].VersionID)
	_, err = BindRunCreationRequest(badCommand, record)
	requireRequestBindingError(t, err, "bound import contradicts requested fused import")
	// Payload and actor equality belong to the retained canonical command. The
	// DTO deliberately checks only the request hash's shape, not those bytes.
	badCommand = cloneJSON(t, command)
	badCommand.Actor = "another-operator"
	other, err := BindRunCreationRequest(badCommand, record)
	if err != nil || other.RequestHash == binding.RequestHash {
		t.Fatalf("request projection did not retain changed command hash: %v", err)
	}
	command.Data.Imports, command.Data.Pins = nil, nil
	record.Summary.ImportCount, record.Summary.PinCount = 0, 0
	record.Binding, record.Evidence = DataBinding{State: "none"}, RunCreationEvidence{}
	eventOnly, err := BindRunCreationRequest(command, record)
	if err != nil || eventOnly.Imports == nil || eventOnly.Pins == nil {
		t.Fatalf("event-only request projection empty-array contract: %+v, %v", eventOnly, err)
	}
}

func TestRequestBindingR10WireDetachmentAndPrecedence(t *testing.T) {
	command, record, binding := requestBindingFixture(t)
	before := cloneJSON(t, binding)
	command.Data.Pins[0].VersionID = "changed"
	command.Data.Imports[0].Input[0] = '!'
	record.Evidence.RunBinding[1].Import.SchemaDigest = "changed"
	if !reflect.DeepEqual(binding, before) {
		t.Fatal("metadata aliases command or evidence")
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 || string(fields["imports"]) == "null" || string(fields["pins"]) == "null" {
		t.Fatalf("binding wire shape changed: %s", raw)
	}
	for _, forbidden := range []string{"actor", "initial_event", "content_base64", "input", "operator", "alpha", "slug"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("binding exposed %q: %s", forbidden, raw)
		}
	}
	_, r, b := requestBindingFixture(t)
	r.Summary.Kind, b.RunID = "invalid", "invalid"
	requireRequestBindingError(t, b.ValidateForRecord(r), "request binding has invalid run_id")
	_, r, b = requestBindingFixture(t)
	r.Summary.Kind, b.RunID = "invalid", uuid.NewString()
	requireRequestBindingError(t, b.ValidateForRecord(r), "run-creation summary kind must be run_creation")
	_, r, b = requestBindingFixture(t)
	b.RunID, b.Imports[0].SourceInvocationID = uuid.NewString(), uuid.NewString()
	requireRequestBindingError(t, b.ValidateForRecord(r), "request binding contradicts operation summary")
	_, _, b = requestBindingFixture(t)
	b.EventID, b.RequestHash = "invalid", "invalid"
	requireRequestBindingError(t, b.Validate(), "request binding has invalid event_id")
	_, _, b = requestBindingFixture(t)
	b.Imports[0].SourceInvocationID, b.Pins[0].VersionID = "invalid", "invalid"
	requireRequestBindingError(t, b.Validate(), "request binding has invalid source_invocation_id")
	_, r, b = requestBindingFixture(t)
	b.EventID, b.Imports[0].SourceInvocationID = uuid.NewString(), uuid.NewString()
	requireRequestBindingError(t, b.ValidateForRecord(r), "request binding contradicts created run")
	_, r, b = requestBindingRejectedFixture(t)
	b.RunID, b.Imports[0].SourceInvocationID = uuid.NewString(), uuid.NewString()
	requireRequestBindingError(t, b.ValidateForRecord(r), "request binding contradicts operation summary")
}
