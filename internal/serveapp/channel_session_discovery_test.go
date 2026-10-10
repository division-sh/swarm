package serveapp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestServeSessionDiscoveryRetainsDeclarationWithoutAuthority(t *testing.T) {
	catalog := sessionDiscoveryCatalog(t)
	plan := packfixture.WhatsAppSessionChannel(t, filepath.Join(repoRootForTest(), defaultPlatformSpecPath), catalog)
	for _, flow := range []string{".", "support"} {
		t.Run(flow, func(t *testing.T) {
			trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "reception", Provider: "whatsapp"})
			if err != nil {
				t.Fatal(err)
			}
			declarations := []runtimepkg.StandingTargetDeclaration{{FlowPath: flow, Alias: "reception",
				Ingress: []runtimepkg.StandingIngressBinding{{Provider: "whatsapp", AdmissionPlan: trigger}}}}
			definition := runtimepkg.BundleContext{SourceArtifactFact: sourceartifactfixture.Fact(), RuntimeInstanceID: uuid.NewString(),
				PublicationGeneration: 1, PackInventoryDigest: "sha256:discovery-inventory"}
			candidates, err := serveChannelCandidatesForPlan(definition, "discovery-fixture", declarations, plan)
			if err != nil || len(candidates) != 1 {
				t.Fatal("session declaration could not be discovered", candidates, err)
			}
			candidate := candidates[0]
			if candidate.ValidateDeclaration() != nil || candidate.Target.Selector != "ingress:"+flow+":whatsapp" ||
				candidate.Target.FlowPath != flow || candidate.Target.Alias != "reception" ||
				!candidate.Target.AdmissionGeneration.Equal(trigger.Generation()) || candidate.Target.Generation != 0 ||
				candidate.Target.PublicationSequence != 0 || candidate.Coordinate.TargetGeneration != 0 ||
				candidate.ProviderCredentialRole != "" || candidate.SigningCredentialRole != "" || candidate.Validate() == nil {
				t.Fatal("session discovery dropped declaration scope or invented execution", candidate)
			}
			if _, err := channelonboarding.NewCandidateCatalog(candidates); err != nil {
				t.Fatal("real discovery cannot enter the canonical onboarding catalog", err)
			}
			missing, err := serveChannelCandidatesForPlan(definition, "discovery-fixture", nil, plan)
			if err != nil || len(missing) != 0 {
				t.Fatal("session discovery fabricated a target without an ingress declaration", missing, err)
			}
		})
	}
}

func TestServeSessionBootstrapConstructionDoesNotOpenState(t *testing.T) {
	directory := t.TempDir()
	owner, err := newServeSessionBootstrap(nil, nil, nil, directory)
	if err == nil && owner == nil {
		t.Fatal("missing bootstrap owner did not return its typed unavailability")
	}
	if owner != nil && owner.QualifySessionPlan(channelonboarding.Candidate{}) == nil {
		t.Fatal("missing runtime and selected owners qualified a session implementation")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("discovery/owner construction opened provider state", entries, err)
	}
}

func sessionDiscoveryCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest, err := providertriggers.ParseManifest([]byte(`provider: whatsapp
transport: session
ack: {mode: durable_before_dispatch}
payload_object_required: true
delivery_id: {json_path: $.provider_message_reference, required: true}
event_type: {json_path: $.kind, required: true}
event_name: {literal: inbound.whatsapp}
normalized_events:
  - event: inbound.whatsapp.message
    when: {equals: {kind: message}}
    author_subject: {type: chat, field: conversation_reference}
    fields:
      text: {from: text, schema: {type: string, minLength: 1}}
      external_account_reference: {from: external_account_reference, schema: {type: string, minLength: 1}}
      conversation_reference: {from: conversation_reference, schema: {type: string, minLength: 1}}
      conversation_scope: {from: conversation_scope, schema: {type: string, enum: [direct, shared]}}
      provider_message_reference: {from: provider_message_reference, schema: {type: string, minLength: 1}}
      reply_to_message_reference: {from: reply_to_message_reference, optional: true, schema: {type: string, minLength: 1}}
      entry_invocation:
        from: entry_invocation
        optional: true
        schema:
          type: object
          additionalProperties: false
          required: [reference]
          properties:
            reference: {type: string, minLength: 1, maxLength: 32}
            address: {type: string, minLength: 5, maxLength: 32}
`))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
