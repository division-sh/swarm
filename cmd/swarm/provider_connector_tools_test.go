package main

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
)

func TestProviderConnectorSurfaceMessageIncludesGeneratedReviewEvidence(t *testing.T) {
	message := providerConnectorSurfaceMessage(providerconnectors.Surface{
		ToolID:   "github.create_issue",
		Can:      []string{"create GitHub issues"},
		Cannot:   []string{"bypass activity_attempts"},
		Requires: nil,
		Generation: &providerconnectors.GenerationSurface{
			GeneratorVersion: "swarm-openapi-gen/v1",
			SourcePath:       "catalog/sources/github.json.gz",
			SourceSHA256:     "sha256:source",
			ProfilePath:      "catalog/generator-profiles/github.yaml",
			ProfileSHA256:    "sha256:profile",
			ManifestSHA256:   "sha256:manifest",
			OperationID:      "issues/create",
			Permissions: []providerconnectors.GenerationPermission{
				{ID: "issues:write", Note: "GitHub App Issues permission at write level"},
			},
			FixtureID:     "github/issues-create",
			FixtureStatus: "passing",
			ReviewStatus:  "approved",
		},
	})
	for _, want := range []string{
		"operation=issues/create",
		"permissions=[issues:write:GitHub App Issues permission at write level]",
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
