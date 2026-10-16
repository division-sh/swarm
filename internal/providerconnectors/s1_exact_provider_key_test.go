package providerconnectors

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func s1ConnectorPack(key string) LoadedPack {
	return LoadedPack{
		Envelope: packs.Envelope{
			ID: key + ".connector", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("a", 64),
			Provenance: packs.Provenance{Source: packs.ProvenancePlatform},
		},
		Source: "fixture:" + key,
		Manifest: ConnectorManifest{Provider: key, Tools: map[string]runtimecontracts.ToolSchemaEntry{
			key + ".send_message": telegramConnectorTool("https://api.telegram.org"),
		}},
	}
}

func TestS1ConnectorRegistryKeepsExactPortableProviderKeys(t *testing.T) {
	registry, err := NewPackRegistry(s1ConnectorPack("team-bot"), s1ConnectorPack("team_bot"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"team-bot", "team_bot"} {
		toolID := key + ".send_message"
		pack, found := registry.Lookup(key, toolID)
		if !found || pack.Manifest.Provider != key {
			t.Fatalf("exact connector lookup changed key: %q found=%t provider=%q", key, found, pack.Manifest.Provider)
		}
		for _, other := range []string{"team-bot", "team_bot", "TEAM-BOT", "team bot", " team-bot", "team-bot "} {
			if other != key {
				if _, found := registry.Lookup(other, toolID); found {
					t.Errorf("provider %q borrowed tool %q", other, toolID)
				}
			}
		}
		for _, alias := range []string{" " + toolID, toolID + " "} {
			if _, found := registry.Lookup(key, alias); found {
				t.Errorf("tool identity alias %q selected %q", alias, toolID)
			}
		}
	}
	for _, installed := range registry.Inventory() {
		if installed.Provider != installed.Pack.Manifest.Provider || !strings.HasPrefix(installed.ToolID, installed.Provider+".") {
			t.Errorf("inventory key contradicts installed pack/tool: %q %q %q", installed.Provider, installed.Pack.Manifest.Provider, installed.ToolID)
		}
	}
}

func TestS1ConnectorProviderAdmissionRejectsNonportableKeys(t *testing.T) {
	for _, key := range []string{"", "TEAM", "team bot", " team", "team ", "team\t", "_team", "con", "com1", strings.Repeat("a", 101), "a-" + strings.Repeat("b", 99)} {
		t.Run(key, func(t *testing.T) {
			if _, err := NewPackRegistry(s1ConnectorPack(key)); err == nil {
				t.Fatalf("connector admitted nonportable provider key %q", key)
			}
		})
	}
}

func TestS1ConnectorToolIDSplitRetainsExactNames(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot", "9-team", strings.Repeat("a", 100), "a-" + strings.Repeat("b", 98)} {
		for _, action := range []string{"send_message", "send-message"} {
			provider, gotAction, ok := splitToolID(key + "." + action)
			if !ok || provider != key || gotAction != action {
				t.Errorf("compiled tool identity changed: key=%q action=%q got=%q.%q valid=%t", key, action, provider, gotAction, ok)
			}
		}
	}
	for _, raw := range []string{" team.send", "team .send", "TEAM.send", "team bot.send", "_team.send", "con.send", "team.send ", "team..send"} {
		if _, _, ok := splitToolID(raw); ok {
			t.Errorf("malformed tool ID %q was repaired", raw)
		}
	}
}

func TestS1ConnectorMockResponseRequiresExactToolIdentity(t *testing.T) {
	responses := map[string]map[string]any{
		"team-bot.send_message": {"provider": "team-bot"},
		"team_bot.send_message": {"provider": "team_bot"},
	}
	plan, err := NewMockResponsePlan(responses)
	if err != nil {
		t.Fatal(err)
	}
	tool := telegramConnectorTool("https://api.telegram.org")
	for _, key := range []string{"team-bot", "team_bot"} {
		toolID := key + ".send_message"
		admitted, err := plan.Admit(toolID, tool)
		if err != nil {
			t.Fatal(err)
		}
		value, err := admitted.Materialize()
		if err != nil || value.(map[string]any)["provider"] != key {
			t.Fatalf("response borrowed another provider key: %+v err=%v", value, err)
		}
		for _, alias := range []string{" " + toolID, toolID + " ", strings.ToUpper(key) + ".send_message"} {
			if _, err := plan.Admit(alias, tool); err == nil {
				t.Errorf("mock responder adopted tool alias %q", alias)
			}
		}
	}
	for _, invalid := range []string{"TEAM.send", "_team.send", "con.send", strings.Repeat("a", 101) + ".send"} {
		if _, err := NewMockResponsePlan(map[string]any{invalid: map[string]any{}}); err == nil {
			t.Errorf("mock response plan admitted nonportable provider key in %q", invalid)
		}
	}
}
