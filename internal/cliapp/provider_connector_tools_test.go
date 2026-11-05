package cliapp

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestProviderConnectorSurfaceMessageIncludesGeneratedReviewEvidence(t *testing.T) {
	guarantee, err := packs.NewGuarantee(packs.GuaranteeActivityJournal)
	if err != nil {
		t.Fatal(err)
	}
	message := packs.RenderSubject(packs.Subject{
		ID: "github.create_issue", Kind: packs.SubjectProviderConnector, Provider: "github", Action: "create_issue",
		Source: "connector_pack_import", Applicability: "effective", Status: packs.StatusReady,
		Capabilities: []packs.Capability{{Code: packs.CapabilityCallProviderAction, Target: "create GitHub issues"}},
		Guarantees:   []packs.Guarantee{guarantee},
		Evidence: []packs.Evidence{{Kind: "generation", Fields: map[string]string{
			"generator": "swarm-openapi-gen/v1", "source": "catalog/sources/github.json.gz", "source_hash": "sha256:source",
			"profile": "catalog/generator-profiles/github.yaml", "profile_hash": "sha256:profile", "manifest_hash": "sha256:manifest",
			"operation": "issues/create", "permissions": "issues:write:GitHub App Issues permission at write level",
			"fixture": "github/issues-create:passing", "review": "approved",
		}}},
	}, true)
	for _, want := range []string{
		"operation=issues/create",
		"permissions=issues:write:GitHub App Issues permission at write level",
		"source_hash=sha256:source",
		"profile_hash=sha256:profile",
		"manifest_hash=sha256:manifest",
		"fixture=github/issues-create:passing",
		"review=approved",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("message = %q, want %q", message, want)
		}
	}
}
