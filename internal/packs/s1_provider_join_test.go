package packs_test

import (
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
