package main

import (
	"os"
	"path/filepath"
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

func TestProviderCapabilitySurfaceRetiresDuplicateOwnersAndTargetBindingGuess(t *testing.T) {
	files := map[string][]string{
		"internal/packs/envelope.go": {
			"func (e Envelope) Surface", "type CapabilitySurface", "type RequirementStatus",
		},
		"internal/providerconnectors/providerconnectors.go": {
			"type Surface struct", "SurfacesWithOptions", "type RequirementStatus", "ResolveInboundTarget", "SQLiteRuntimeStore", "PostgresRuntimeStore",
		},
		"internal/providertriggers/providertriggers.go": {
			"CapabilitySurface(", "ResolveInboundTarget", "SQLiteRuntimeStore", "PostgresRuntimeStore",
		},
		"cmd/swarm/provider_connector_tools.go": {
			"providerConnectorSurfaceMessage", "formatProviderConnectorSurfaceVerbs", "formatProviderConnectorRequirements",
		},
		"cmd/swarm/provider_trigger_packs.go": {
			"providerTriggerPackSurfaceMessage", "formatProviderTriggerPackRequirements", "ResolveInboundTarget",
		},
		"cmd/swarm/doctor.go": {"appendProviderTriggerPackSurfaceFindings"},
	}
	for name, forbidden := range files {
		body, err := os.ReadFile(filepath.Join(repoRoot(), name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(body), token) {
				t.Fatalf("%s retains forbidden provider capability owner or target-binding seam %q", name, token)
			}
		}
	}
}
