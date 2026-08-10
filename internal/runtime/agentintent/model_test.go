package agentintent

import (
	"strings"
	"testing"
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
