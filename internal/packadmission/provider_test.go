package packadmission

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func providerDefinitionPlatform(t *testing.T) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	body, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	platform, err := runtimecontracts.ParsePlatformSpecDocument(body, "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return platform
}

func TestS1ProviderDirectoryAdmissionOwnsAllRoleInputs(t *testing.T) {
	platform := providerDefinitionPlatform(t)
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		t.Run(key, func(t *testing.T) {
			provider, err := packartifact.AdmitProviderMembers(key, packfixture.NeutralProviderMembers())
			if err != nil {
				t.Fatal(err)
			}
			definition, err := AdmitProviderDirectory(provider, platform)
			if err != nil || !definition.Valid() || definition.Provider().Key() != key {
				t.Fatalf("captured provider did not admit: %+v %v", definition, err)
			}
			catalog, hasTrigger := definition.TriggerCatalog()
			connector, hasConnector := definition.ConnectorDescriptor()
			plan, hasChannel := definition.ChannelPlan()
			if !hasTrigger || !hasConnector || !hasChannel || plan.Provider() != key || connector.Provider != key {
				t.Fatal("directory lost a role or borrowed a different provider")
			}
			if _, present := catalog.PackDescriptorForProvider(key); !present {
				t.Fatal("exact provider descriptor is unavailable")
			}
			if _, present := catalog.PackDescriptorForProvider(" " + key); present {
				t.Fatal("catalog repaired a foreign provider selector")
			}
			delete(connector.Tools, key+".send_message")
			repeated, _ := definition.ConnectorDescriptor()
			if _, present := repeated.Tools[key+".send_message"]; !present {
				t.Fatal("readback map mutated canonical provider declaration")
			}
			tool, err := plan.OperationTool("deliver")
			if err != nil || len(tool.Credentials()) != 1 || tool.Credentials()[0] != key+".bot_token" {
				t.Fatal("actual channel plan lost its captured connector contract")
			}
			body, _ := provider.Body(packmodel.BodyChannel)
			copy := body.Bytes()
			copy[0] = 'X'
			other, err := AdmitProviderDirectory(provider, platform)
			if err != nil {
				t.Fatal(err)
			}
			otherPlan, _ := other.ChannelPlan()
			generation, _ := plan.Generation()
			otherGeneration, _ := otherPlan.Generation()
			if !generation.Equal(otherGeneration) || plan.RequireExecutableProvider() == nil {
				t.Fatal("readback changed frozen plan or declaration fabricated installed session execution")
			}
		})
	}
}

func TestS1ProviderDirectoryAdmissionReturnsNoPartialDefinition(t *testing.T) {
	platform := providerDefinitionPlatform(t)
	for _, cell := range []struct{ name, label, old, replacement string }{
		{"trigger", "trigger.yaml", "event_name:", "unknown_field:"},
		{"connector", "connector.yaml", "handler_type: http", "handler_type: unknown"},
		{"channel structural reference", "channel.yaml", "tool: send_message", "tool: missing"},
		{"platform", "manifest.yaml", "platform_version: '*'", "platform_version: '>=99.0.0'"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			members := packfixture.NeutralProviderMembers()
			members[cell.label] = bytes.Replace(members[cell.label], []byte(cell.old), []byte(cell.replacement), 1)
			provider, err := packartifact.AdmitProviderMembers("team-bot", members)
			if err != nil {
				t.Fatal(err)
			}
			definition, err := AdmitProviderDirectory(provider, platform)
			if err == nil || definition.Valid() {
				t.Fatalf("failed role returned an admitted definition: %v", err)
			}
			if _, present := definition.TriggerCatalog(); present {
				t.Fatal("failed role leaked a partial trigger catalog")
			}
			if _, present := definition.ConnectorDescriptor(); present {
				t.Fatal("failed role leaked a partial connector")
			}
			if _, present := definition.ChannelPlan(); present {
				t.Fatal("failed role leaked a partial channel")
			}
		})
	}
	if definition, err := AdmitProviderDirectory(packartifact.AdmittedProvider{}, platform); err == nil || definition.Valid() {
		t.Fatal("zero provider became an admitted declaration")
	}
}

func TestS1ProviderDirectoryNearestScopeNeverBorrowsMissingRoles(t *testing.T) {
	members := map[string][]byte{"child/agents.yaml": []byte("{}\n"), "child/schema.yaml": []byte("stages: []\n")}
	for label, body := range packfixture.NeutralProviderMembers() {
		members["team-bot/"+label] = body
		if label == "connector.yaml" {
			members["child/team-bot/"+label] = body
		}
	}
	members["child/team-bot/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n")
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	platform := providerDefinitionPlatform(t)
	for _, source := range []*sourceartifact.AdmittedSourceArtifact{artifact, retained} {
		for _, scope := range []string{".", "child"} {
			selected, err := source.ProviderDirectoriesInScope(scope)
			if err != nil || len(selected) != 1 {
				t.Fatalf("scope %s selected=%+v: %v", scope, selected, err)
			}
			provider, err := packartifact.ProviderFromArtifact(source, selected[0].Path())
			if err != nil {
				t.Fatal(err)
			}
			definition, err := AdmitProviderDirectory(provider, platform)
			if err != nil {
				t.Fatal(err)
			}
			_, hasTrigger := definition.TriggerCatalog()
			_, hasChannel := definition.ChannelPlan()
			connector, hasConnector := definition.ConnectorDescriptor()
			if !hasConnector || hasTrigger != (scope == ".") || hasChannel != (scope == ".") || (scope == "child" && !strings.HasPrefix(connector.Identity.Source().Path(), "child/")) {
				t.Fatal("nearer provider borrowed an ancestor role or source")
			}
		}
	}
}
