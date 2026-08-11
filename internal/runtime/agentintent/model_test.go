package agentintent

import (
	"strings"
	"testing"

	runtimeflowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	"golang.org/x/text/unicode/norm"
)

func TestResolvedIntentIdentityBindsExactCanonicalFacts(t *testing.T) {
	content := "  preserve exact intent bytes  \n"
	resolved, err := Resolve(SourceInline, "inline", "flows/review/agents.yaml#agents.reviewer.intent", content)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Content != content {
		t.Fatalf("content = %q, want exact %q", resolved.Content, content)
	}
	if !strings.HasPrefix(resolved.ContentHash, "sha256:") || !strings.HasPrefix(resolved.Identity, "agent-intent:v1:sha256:") {
		t.Fatalf("resolved hashes = content %q identity %q", resolved.ContentHash, resolved.Identity)
	}
	if err := resolved.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	changedContent, err := Resolve(resolved.Kind, resolved.Coordinate, resolved.Provenance, content+"changed")
	if err != nil {
		t.Fatal(err)
	}
	if changedContent.ContentHash == resolved.ContentHash || changedContent.Identity == resolved.Identity {
		t.Fatal("exact content change did not change canonical hashes")
	}
	changedProvenance, err := Resolve(resolved.Kind, resolved.Coordinate, "agents.yaml#agents.reviewer.intent", content)
	if err != nil {
		t.Fatal(err)
	}
	if changedProvenance.ContentHash != resolved.ContentHash || changedProvenance.Identity == resolved.Identity {
		t.Fatal("provenance must change identity without changing exact content hash")
	}
}

func TestResolvedIntentRejectsTamperedArtifact(t *testing.T) {
	resolved, err := Resolve(SourceLocal, "flows/review/intent/reviewer.md", "flows/review/agents.yaml#agents.reviewer.intent", "Review the request.\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Resolved){
		func(value *Resolved) { value.Content += "tampered" },
		func(value *Resolved) { value.ContentHash = "sha256:tampered" },
		func(value *Resolved) { value.Identity = "agent-intent:v1:sha256:tampered" },
		func(value *Resolved) { value.Provenance = "agents.yaml#agents.other.intent" },
		func(value *Resolved) { value.Coordinate += " " },
		func(value *Resolved) { value.Provenance += " " },
	} {
		candidate := resolved
		mutate(&candidate)
		if err := candidate.Validate(); err == nil {
			t.Fatalf("tampered artifact validated: %#v", candidate)
		}
	}
}

func TestResolvedIntentRejectsImpossibleCanonicalFactsAtConstruction(t *testing.T) {
	nonNFC := "flows/caf" + string([]rune{'e', '\u0301'}) + "/intent.md"
	if norm.NFC.IsNormalString(nonNFC) {
		t.Fatal("test coordinate unexpectedly NFC-normalized")
	}
	tests := []struct {
		name       string
		kind       SourceKind
		coordinate string
		provenance string
	}{
		{name: "inline_file_coordinate", kind: SourceInline, coordinate: "intent.md", provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_traversal", kind: SourceLocal, coordinate: "../outside.md", provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_absolute", kind: SourceLocal, coordinate: "/outside.md", provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_windows_absolute", kind: SourceLocal, coordinate: `C:\outside.md`, provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_backslash", kind: SourceLocal, coordinate: `flows\worker.md`, provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_nul", kind: SourceLocal, coordinate: "flows/worker\x00.md", provenance: "agents.yaml#agents.worker.intent"},
		{name: "local_non_nfc", kind: SourceLocal, coordinate: nonNFC, provenance: "agents.yaml#agents.worker.intent"},
		{name: "arbitrary_provenance", kind: SourceInline, coordinate: "inline", provenance: "operator-input"},
		{name: "wrong_declaration_file", kind: SourceInline, coordinate: "inline", provenance: "flows/review/config.yaml#agents.worker.intent"},
		{name: "provenance_traversal", kind: SourceInline, coordinate: "inline", provenance: "../agents.yaml#agents.worker.intent"},
		{name: "provenance_backslash", kind: SourceInline, coordinate: "inline", provenance: `flows\review\agents.yaml#agents.worker.intent`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Resolve(tc.kind, tc.coordinate, tc.provenance, "Business intent."); err == nil {
				t.Fatalf("Resolve accepted impossible facts kind=%q coordinate=%q provenance=%q", tc.kind, tc.coordinate, tc.provenance)
			}
		})
	}
}

func TestResolvedIntentValidateRejectsImpossiblePersistedFactsBeforeHashIdentity(t *testing.T) {
	valid, err := Resolve(SourceInline, "inline", "agents.yaml#agents.worker.intent", "Business intent.")
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Resolved){
		"inline_file_coordinate": func(value *Resolved) { value.Coordinate = "intent.md" },
		"local_traversal": func(value *Resolved) {
			value.Kind = SourceLocal
			value.Coordinate = "../outside.md"
		},
		"arbitrary_provenance": func(value *Resolved) { value.Provenance = "operator-input" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			candidate.ContentHash = "sha256:recomputed-by-hostile-producer"
			candidate.Identity = "agent-intent:v1:sha256:recomputed-by-hostile-producer"
			if err := candidate.Validate(); err == nil {
				t.Fatalf("Validate accepted impossible persisted facts: %#v", candidate)
			}
		})
	}
}

func TestContractCriteriaPromptRejectsArbitraryRenderedSuffix(t *testing.T) {
	intent, err := Resolve(SourceInline, "inline", "agents.yaml#agents.reviewer.intent", "Review the proposal.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := ContractCriteriaPrompt(intent, []string{"quality"}, map[string]runtimeflowmodel.PolicyCriteriaSet{
		"quality": {
			Classes: map[string]runtimeflowmodel.PolicyCriteriaClass{"hard": {Disposition: "reject"}},
			Rules: []runtimeflowmodel.PolicyCriteriaRule{{
				ID: "QUALITY-01", Class: "hard", Text: "Reject incomplete proposals.",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	prompt.text += "\nArbitrary unowned suffix."
	if err := prompt.Validate(intent, []string{"quality"}); err == nil || !strings.Contains(err.Error(), "rendering is not canonical") {
		t.Fatalf("Validate error = %v, want arbitrary criteria suffix rejection", err)
	}
}
