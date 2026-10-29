package serveapp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestServeSessionDiscoveryRetainsDeclarationWithoutAuthority(t *testing.T) {
	for _, flow := range []string{".", "support"} {
		t.Run(flow, func(t *testing.T) {
			catalog, plan, rooted := sessionDiscoveryCapturedProvider(t, flow)
			trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "reception", Provider: "whatsapp"})
			if err != nil {
				t.Fatal(err)
			}
			declarations := []runtimepkg.StandingTargetDeclaration{{FlowPath: flow, Alias: "reception",
				Ingress: []runtimepkg.StandingIngressBinding{{Provider: "whatsapp", AdmissionPlan: trigger}}}}
			definition := runtimepkg.BundleContext{SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()), RootAdmission: rooted.Admission(), RuntimeInstanceID: uuid.NewString(),
				PublicationGeneration: 1, PackInventoryDigest: "sha256:discovery-inventory"}
			candidates, err := serveChannelCandidatesForPlan(definition, "discovery-fixture", declarations, plan)
			if err != nil || len(candidates) != 1 {
				t.Fatal("session declaration could not be discovered", candidates, err)
			}
			candidate := candidates[0]
			if candidate.ValidateDeclaration() != nil || candidate.Target.Selector != "ingress:"+flow+":whatsapp" ||
				candidate.Target.FlowPath != flow || candidate.Target.Alias != "reception" ||
				!candidate.Target.AdmissionGeneration.Equal(trigger.Generation()) ||
				candidate.ProviderCredentialRole != "" || candidate.SigningCredentialRole != "" || candidate.Validate() != nil {
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

func sessionDiscoveryCapturedProvider(t *testing.T, flow string) (*providertriggers.CatalogSnapshot, packs.SatisfactionPlan, sourceartifact.RootedSource) {
	t.Helper()
	standalone, _ := sessionDiscoveryProvider(t)
	entry, present := standalone.EntryByProvider("whatsapp")
	if !present {
		t.Fatal("session discovery requires its authored WhatsApp trigger")
	}
	members := map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
	}
	members[filepath.ToSlash(filepath.Join(flow, "manifest.yaml"))] = members["manifest.yaml"]
	members[filepath.ToSlash(filepath.Join(flow, "schema.yaml"))] = []byte("stages: []\npins: {inputs: [inbound.whatsapp.raw, inbound.whatsapp.message]}\n")
	members[filepath.ToSlash(filepath.Join(flow, "policy.yaml"))] = []byte("operator_account: '+15551234567'\n")
	for name, body := range packfixture.WhatsAppSessionProviderMembers(t, entry.Manifest.SourceBytes(), []string{"raw", "message"}) {
		members[filepath.ToSlash(filepath.Join(flow, "whatsapp", name))] = body
	}
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "reception", artifact)
	repo := repoRootForTest()
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(flow)
	if err != nil {
		t.Fatal(err)
	}
	definition, present := scope.Provider("whatsapp")
	if !present || definition.Provider().DeclaringFlow() != flow {
		t.Fatal("session discovery has no exact captured holder")
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	trigger, hasTrigger := catalog.PackDescriptorForProvider("whatsapp")
	connector, hasConnector := definition.ConnectorDescriptor()
	if !hasTrigger || !hasConnector {
		t.Fatal("session discovery requires its captured trigger and connector")
	}
	registry, err := packs.NewInterfaceRegistry(bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := packs.CompileProviderChannel(registry, definition.Provider(), trigger, connector)
	if err != nil {
		t.Fatal(err)
	}
	return catalog, plan, rooted
}

func TestServeSessionBootstrapConstructionDoesNotOpenState(t *testing.T) {
	directory := t.TempDir()
	owner, err := newServeSessionBootstrap(nil, nil, nil, directory, nil, nil)
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

func sessionDiscoveryProvider(t *testing.T) (*providertriggers.CatalogSnapshot, packs.SatisfactionPlan) {
	t.Helper()
	return packfixture.WhatsAppSessionProvider(t, filepath.Join(repoRootForTest(), defaultPlatformSpecPath), []byte(`transport: session
ack: {mode: durable_before_dispatch}
payload_object_required: true
delivery_id: {json_path: $.provider_message_reference, required: true}
event_type: {json_path: $.kind, required: true}
event_name: {literal: raw}
normalized_events:
  - event: message
    when: {equals: {kind: message}}
    author_subject: {type: chat, field: conversation_reference}
    fields:
      text: {from: text, schema: {type: string, minLength: 1}}
      external_account: {from: external_account, schema: {type: string, minLength: 1}}
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
`), []string{"raw", "message"})
}
