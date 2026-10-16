package packs_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func s1ChannelProviderDescriptors(t testing.TB, key string, members map[string][]byte) (packartifact.AdmittedProvider, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	provider, err := packartifact.AdmitProviderMembers(key, members)
	if err != nil {
		t.Fatal(err)
	}
	trigger, connector := s1AcceptedChannelRoleDescriptors(t, provider)
	return provider, trigger, connector
}

func s1AcceptedChannelRoleDescriptors(t testing.TB, provider packartifact.AdmittedProvider) (packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	triggerBody, _ := provider.Body(packmodel.BodyTrigger)
	trigger, err := providertriggers.AdmitBody(triggerBody)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshotFromAdmittedBodies(trigger)
	if err != nil {
		t.Fatal(err)
	}
	connectorBody, _ := provider.Body(packmodel.BodyConnector)
	connector, err := providerconnectors.AdmitBody(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	connectorIdentity, err := packs.NewProviderBodyIdentity(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, present := catalog.PackDescriptorForProvider(provider.Key())
	if !present {
		t.Fatal("captured trigger descriptor disappeared")
	}
	return descriptor, packs.ConnectorPackDescriptor{Identity: connectorIdentity, Provider: connector.Provider, Tools: connector.Tools}
}

func TestS1ProviderChannelCompilesCapturedCoLocatedRoles(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		t.Run(key, func(t *testing.T) {
			members := packfixture.NeutralProviderMembers()
			provider, trigger, connector := s1ChannelProviderDescriptors(t, key, members)
			plan, err := packs.CompileProviderChannel(registry, provider, trigger, connector)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := plan.InterfaceIdentity()
			if err != nil || identity.InterfaceRef != "swarm.hitl-channel/v2" || plan.Provider() != key || plan.ChannelIdentity().ID() != key || plan.ChannelIdentity().Source().Provenance() != "" || plan.ChannelIdentity().Source().Diagnostic() != key+"/channel.yaml" {
				t.Fatalf("captured structural identity changed or invented provenance: %+v %v", identity, err)
			}
			bounds, err := plan.PresentationBounds()
			if err != nil || bounds.TextRunes != 1000 {
				t.Fatalf("structural selection lost actual connector bounds: %+v %v", bounds, err)
			}
			input, err := plan.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": "Inbox"}}, map[string]any{"destination": "room-1"})
			if err != nil || input["room"] != "room-1" || input["text"] != "Inbox" || len(input) != 2 {
				t.Fatalf("structural mapping=%+v: %v", input, err)
			}
			tool, err := plan.OperationTool("deliver")
			if err != nil {
				t.Fatal(err)
			}
			execution, _ := tool.HTTPExecution()
			prepared, err := execution.Prepare(input, map[string]any{key + ".bot_token": "fixture-token"})
			if err != nil || prepared.Headers().Get("Authorization") != "Bearer fixture-token" {
				t.Fatalf("same BODY role did not reach the compiled HTTP owner: %+v %v", prepared, err)
			}
			profile, ok := plan.OnboardingProfile()
			if !ok || profile.ProviderCredential() != key+".bot_token" || profile.ConnectionHealth() != "provider_connection" || plan.RequireExecutableProvider() == nil {
				t.Fatal("structural satisfaction fabricated session installation or lost real credential policy")
			}
			body, _ := provider.Body(packmodel.BodyChannel)
			if !bytes.Equal(body.Bytes(), members["channel.yaml"]) {
				t.Fatal("compiler rewrote captured channel bytes")
			}
		})
	}
}

func TestS1ProviderChannelRefusesCrossedRoleIdentity(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	provider, trigger, connector := s1ChannelProviderDescriptors(t, "team-bot", packfixture.NeutralProviderMembers())
	_, foreignTrigger, foreignConnector := s1ChannelProviderDescriptors(t, "team_bot", packfixture.NeutralProviderMembers())
	changed := packfixture.NeutralProviderMembers()
	changed["connector.yaml"] = bytes.ReplaceAll(changed["connector.yaml"], []byte("example.test"), []byte("replacement.test"))
	_, _, replacementConnector := s1ChannelProviderDescriptors(t, "team-bot", changed)
	versioned := packfixture.NeutralProviderMembers()
	versioned["manifest.yaml"] = bytes.Replace(versioned["manifest.yaml"], []byte("version: 1.0.0"), []byte("version: 1.0.1"), 1)
	_, versionedTrigger, _ := s1ChannelProviderDescriptors(t, "team-bot", versioned)
	members := map[string][]byte{"shop/agents.yaml": []byte("{}\n"), "shop/schema.yaml": []byte("stages: []\n")}
	for label, body := range packfixture.NeutralProviderMembers() {
		members["shop/team-bot/"+label] = body
	}
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	nested, err := packartifact.ProviderFromArtifact(artifact, "shop/team-bot")
	if err != nil {
		t.Fatal(err)
	}
	nestedTrigger, _ := s1AcceptedChannelRoleDescriptors(t, nested)
	wrongRole := trigger
	channelBody, _ := provider.Body(packmodel.BodyChannel)
	wrongRole.Identity, err = packs.NewProviderBodyIdentity(channelBody)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct {
		name      string
		trigger   packs.TriggerPackDescriptor
		connector packs.ConnectorPackDescriptor
	}{
		{"foreign trigger", foreignTrigger, connector},
		{"foreign connector", trigger, foreignConnector},
		{"same-key different-body connector", trigger, replacementConnector},
		{"same-key different-version trigger", versionedTrigger, connector},
		{"same-key foreign-source trigger", nestedTrigger, connector},
		{"same-key wrong-role trigger", wrongRole, connector},
	} {
		t.Run(cell.name, func(t *testing.T) {
			plan, err := packs.CompileProviderChannel(registry, provider, cell.trigger, cell.connector)
			if err == nil || !strings.Contains(err.Error(), "captured BODY identity") {
				t.Fatalf("crossed accepted role gained a structural plan: %v", err)
			}
			if _, err := plan.Generation(); err == nil {
				t.Fatal("refusal returned a live plan generation")
			}
		})
	}
}

func TestS1ProviderChannelUsesOneNamedPlatformInterfaceWithoutFirstFit(t *testing.T) {
	provider, trigger, connector := s1ChannelProviderDescriptors(t, "team-bot", packfixture.NeutralProviderMembers())
	if _, err := packs.CompileProviderChannel(nil, provider, trigger, connector); err == nil {
		t.Fatal("missing platform owner supplied a channel interface")
	}
	platform := loadChannelPlatformSpec(t)
	platform.Interfaces["unrelated"] = platform.Interfaces["swarm.hitl-channel"]
	delete(platform.Interfaces, "swarm.hitl-channel")
	registry, err := packs.NewInterfaceRegistry(platform)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := packs.CompileProviderChannel(registry, provider, trigger, connector); err == nil || !strings.Contains(err.Error(), "swarm.hitl-channel/v2") {
		t.Fatalf("compatible but differently named interface was guessed: %v", err)
	}
	registry = loadChannelInterfaceRegistry(t)
	if _, err := packs.CompileProviderChannel(registry, packartifact.AdmittedProvider{}, trigger, connector); err == nil {
		t.Fatal("zero provider acquired structural satisfaction")
	}
	for _, role := range []string{"channel.yaml", "channel.yaml and other roles"} {
		members := packfixture.NeutralProviderMembers()
		delete(members, "channel.yaml")
		if role == "channel.yaml and other roles" {
			delete(members, "connector.yaml")
		}
		without, err := packartifact.AdmitProviderMembers("team-bot", members)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := packs.CompileProviderChannel(registry, without, trigger, connector); err == nil || !strings.Contains(err.Error(), "channel.yaml") {
			t.Fatalf("missing channel role gained a plan: %v", err)
		}
	}
}

func TestS1ProviderBodyIdentityUsesOnlyCapturedMetadataAndBytes(t *testing.T) {
	if _, err := packs.NewProviderBodyIdentity(packartifact.ProviderBody{}); err == nil {
		t.Fatal("zero BODY acquired role identity")
	}
	for _, key := range []string{"team-bot", "team_bot"} {
		provider, _, _ := s1ChannelProviderDescriptors(t, key, packfixture.NeutralProviderMembers())
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			body, _ := provider.Body(role)
			identity, err := packs.NewProviderBodyIdentity(body)
			if err != nil || identity.ID() != key || identity.Type() != role.String() || identity.ManifestHash() != body.Hash() || identity.Version() != body.Metadata().Version || identity.Source().Path() != body.Label() || identity.Source().Provenance() != "" {
				t.Fatalf("body identity guessed a coordinate: %+v %v", identity, err)
			}
			readback := body.Bytes()
			readback[0] = 'X'
			repeated, err := packs.NewProviderBodyIdentity(body)
			if err != nil || repeated != identity {
				t.Fatal("mutable bytes changed the captured identity")
			}
		}
	}
}

func TestS1ProviderChannelPlanRetainsFrozenToolsAfterDescriptorMutation(t *testing.T) {
	provider, trigger, connector := s1ChannelProviderDescriptors(t, "team-bot", packfixture.NeutralProviderMembers())
	plan, err := packs.CompileProviderChannel(loadChannelInterfaceRegistry(t), provider, trigger, connector)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	delete(connector.Tools, "team-bot.send_message")
	delete(trigger.Events, "inbound.team-bot.text_message")
	tool, err := plan.OperationTool("deliver")
	if err != nil || len(tool.Credentials()) != 1 || tool.Credentials()[0] != "team-bot.bot_token" {
		t.Fatal("descriptor readback mutation changed compiled operation")
	}
	input, err := plan.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": "Inbox"}}, map[string]any{"destination": "room-1"})
	if err != nil || input["room"] != "room-1" || input["text"] != "Inbox" {
		t.Fatal("descriptor readback mutation changed compiled mapping")
	}
	repeated, err := plan.Generation()
	if err != nil || !repeated.Equal(generation) {
		t.Fatal("descriptor readback mutation changed frozen plan generation")
	}
}
