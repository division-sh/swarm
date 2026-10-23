package packs_test

import (
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
)

// This paper port describes BODY contracts, not an installed Gateway adapter.
func discordPaperPort(t *testing.T) (packs.SatisfactionPlan, packs.TriggerPackDescriptor) {
	t.Helper()
	provider, err := packartifact.LoadProviderFS(os.DirFS("testdata/discord-paper-port"), "discord", "0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	trigger, connector := s1AcceptedChannelRoleDescriptors(t, provider)
	plan, err := packs.CompileProviderChannel(loadChannelInterfaceRegistry(t), provider, trigger, connector)
	if err != nil {
		t.Fatal(err)
	}
	return plan, trigger
}
