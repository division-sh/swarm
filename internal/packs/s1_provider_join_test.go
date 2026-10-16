package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestS1ChannelCompilerRejectsProviderKeyRepair(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	if _, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector}); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"channel", "trigger", "connector"} {
		for _, key := range []string{" mock", "mock "} {
			t.Run(owner+"/"+key, func(t *testing.T) {
				channel, trigger, connector := mockChannelSatisfier()
				switch owner {
				case "channel":
					channel.Manifest.Provider = key
				case "trigger":
					trigger.Provider = key
				case "connector":
					connector.Provider = key
				}
				if _, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector}); err == nil {
					t.Fatalf("%s provider key %q borrowed another key's channel authority", owner, key)
				}
			})
		}
	}
}

func TestS1ChannelOnboardingRetainsExactProviderKey(t *testing.T) {
	profile := packs.ChannelOnboardingProfile{
		Ceremony: "authenticated_text_challenge", Confirmation: "deliver", ConnectionHealth: "provider_connection",
		LearnedDestination: map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}},
	}
	for _, key := range []string{"team-bot", "team_bot", "9-team", " team", "team ", "TEAM", "_team", "con", strings.Repeat("a", 101)} {
		compiled, err := packs.CompileChannelOnboardingProfile(key, packs.ChannelTransportSession, profile, []string{"deliver"})
		valid := key == "team-bot" || key == "team_bot" || key == "9-team"
		if !valid {
			if err == nil {
				t.Errorf("onboarding projection repaired invalid key %q", key)
			}
			continue
		}
		if err != nil || compiled.Provider() != key {
			t.Fatalf("onboarding projection changed key %q to %q: %v", key, compiled.Provider(), err)
		}
	}
}
