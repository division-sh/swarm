package durabledata

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRunCreationRequestBindingMetadataAndRelationships(t *testing.T) {
	sourceCommand, source := validSourceAggregate(t)
	command, record := validRunCreationAggregate(t, sourceCommand, source)
	binding, err := BindRunCreationRequest(command, record)
	if err != nil {
		t.Fatalf("bind created request: %v", err)
	}
	if err := binding.Validate(); err != nil {
		t.Fatalf("standalone created binding: %v", err)
	}
	hash, _, _, err := command.RequestHash()
	if err != nil || binding.RequestHash != hash || binding.RunID != command.RunID || binding.BundleHash != command.BundleHash ||
		binding.EventID != command.EventID || len(binding.Imports) != 1 ||
		binding.Imports[0].SchemaDigest != source.Result.SchemaDigest || binding.Imports[0].ExpectedHead != sourceCommand.ExpectedHead {
		t.Fatalf("created binding = %#v, hash error %v", binding, err)
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"operator", "initial_event", "input", "content_base64", "slug", "alpha"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("request binding leaked %q: %s", forbidden, raw)
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*RunCreationRequestBinding)
	}{
		{"run", func(b *RunCreationRequestBinding) { b.RunID = uuid.NewString() }},
		{"bundle", func(b *RunCreationRequestBinding) {
			b.BundleHash = "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}},
		{"event", func(b *RunCreationRequestBinding) { b.EventID = uuid.NewString() }},
		{"hash", func(b *RunCreationRequestBinding) { b.RequestHash = "weak" }},
		{"expected head", func(b *RunCreationRequestBinding) {
			b.Imports[0].ExpectedHead = VersionHead(source.Result.Candidate.VersionID)
		}},
		{"source", func(b *RunCreationRequestBinding) { b.Imports[0].SourceInvocationID = uuid.NewString() }},
		{"schema", func(b *RunCreationRequestBinding) {
			b.Imports[0].SchemaDigest = SchemaDigest("resource-schema-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hostile := binding
			hostile.Imports = append([]RunCreationRequestImport{}, binding.Imports...)
			test.mutate(&hostile)
			if err := hostile.ValidateForRecord(record); err == nil {
				t.Fatal("contradictory request binding validated")
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*RunCreationRequestBinding)
	}{
		{"missing initiation", func(b *RunCreationRequestBinding) { b.EventID = ""; b.Imports = nil }},
		{"invalid source", func(b *RunCreationRequestBinding) { b.Imports[0].SourceInvocationID = "not-a-uuid" }},
		{"invalid schema", func(b *RunCreationRequestBinding) { b.Imports[0].SchemaDigest = "" }},
		{"invalid head", func(b *RunCreationRequestBinding) { b.Imports[0].ExpectedHead.State = "head" }},
		{"duplicate import", func(b *RunCreationRequestBinding) { b.Imports = append(b.Imports, b.Imports[0]) }},
		{"overlapping pin", func(b *RunCreationRequestBinding) {
			b.Pins = []ExplicitPin{{Declaration: b.Imports[0].Declaration, VersionID: source.Result.Candidate.VersionID}}
		}},
	} {
		t.Run("standalone/"+test.name, func(t *testing.T) {
			hostile := binding
			hostile.Imports = append([]RunCreationRequestImport{}, binding.Imports...)
			test.mutate(&hostile)
			if err := hostile.Validate(); err == nil {
				t.Fatal("invalid standalone request binding validated")
			}
		})
	}
}

func TestRejectedTwoPinRequestBindingRetainsBothPins(t *testing.T) {
	sourceCommand, source := validSourceAggregate(t)
	other, err := ParseDeclarationRef(".", "scores.loaded")
	if err != nil {
		t.Fatal(err)
	}
	version := source.Result.Candidate.VersionID
	command := RunCreationCommand{
		RunID: uuid.NewString(), Actor: "operator", BundleHash: sourceCommand.BundleHash,
		EventID: uuid.NewString(), InitialEvent: json.RawMessage(`{"type":"start"}`),
		Data: RunCreationDataEnvelope{Pins: []ExplicitPin{
			{Declaration: sourceCommand.Declaration, VersionID: version},
			{Declaration: other, VersionID: version},
		}},
	}
	record := RunCreationOperationRecord{
		Summary: RunCreationOperationSummary{
			Kind: "run_creation", Outcome: "data_rejected", RunID: command.RunID, BundleHash: command.BundleHash,
			Rejection: RunCreationRejection{State: "rejected", Code: RunCreationRejectionVersionMissing,
				Declaration: &other, VersionID: version}, CompletedAt: time.Now().UTC().Truncate(time.Microsecond),
		},
		Binding: DataBinding{State: "none"},
	}
	binding, err := BindRunCreationRequest(command, record)
	if err != nil {
		t.Fatalf("bind rejected request: %v", err)
	}
	if err := binding.Validate(); err != nil {
		t.Fatalf("standalone rejected binding: %v", err)
	}
	if len(binding.Pins) != 2 || len(record.Evidence.RunBinding) != 0 || binding.Pins[0].Declaration != sourceCommand.Declaration ||
		binding.Pins[1].Declaration != other {
		t.Fatalf("rejected two-pin binding = %#v", binding)
	}
	hostile := binding
	hostile.Pins = append([]ExplicitPin{}, binding.Pins[:1]...)
	if err := hostile.ValidateForRecord(record); err == nil {
		t.Fatal("rejection target absent from request binding validated")
	}
}
