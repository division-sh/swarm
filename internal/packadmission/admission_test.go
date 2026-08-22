package packadmission

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"gopkg.in/yaml.v3"
)

func TestAdmitRejectsMalformedBodiesForEveryPackKind(t *testing.T) {
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	var platform runtimecontracts.PlatformSpecDocument
	platformBody, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(platformBody, &platform); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		packID  string
		wantErr string
	}{
		{name: "trigger", packID: "provider.telegram", wantErr: "admit provider trigger packs"},
		{name: "connector", packID: "provider.telegram.connector", wantErr: "admit provider connector packs"},
		{name: "channel", packID: "provider.telegram.hitl_channel", wantErr: "admit channel packs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := base.Lookup(tc.packID)
			if !ok {
				t.Fatalf("embedded pack %q is missing", tc.packID)
			}
			envelope := entry.Envelope()
			envelope.Provenance.Source = packartifact.ProvenanceProject
			envelope.ManifestHash = packartifact.ManifestHashDerived
			envelopeBody, err := yaml.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			effective, err := packartifact.NewEffectivePackInventory(base, []packartifact.ProjectPackSource{{
				Path: tc.packID, EnvelopeBody: envelopeBody, ManifestBody: []byte("unknown_field: true\n"),
				Origin: packartifact.ImportOrigin{
					Source: packartifact.ProvenanceEmbedded, ID: entry.ID(), Version: entry.Version(), ManifestHash: entry.ManifestHash(),
					EnvelopeHash: manifesthash.FromBytes(envelopeBody).String(),
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Admit(effective, platform); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("malformed %s body error = %v", tc.name, err)
			}
		})
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
