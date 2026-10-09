package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestChannelTransportMustMatchVerifiedTrigger(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	for _, cell := range []struct {
		name             string
		channel, trigger packs.ChannelTransport
		valid            bool
	}{
		{"webhook control", packs.ChannelTransportWebhook, packs.ChannelTransportWebhook, true},
		{"webhook with session", packs.ChannelTransportWebhook, packs.ChannelTransportSession, false},
		{"session with webhook", packs.ChannelTransportSession, packs.ChannelTransportWebhook, false},
		{"webhook missing trigger transport", packs.ChannelTransportWebhook, "", false},
		{"session missing trigger transport", packs.ChannelTransportSession, "", false},
		{"unknown trigger transport", packs.ChannelTransportWebhook, "socket", false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			channel.Manifest.Transport, trigger.Transport = cell.channel, cell.trigger
			plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if cell.valid {
				if err != nil || plan.Transport() != cell.channel {
					t.Fatalf("valid transport = %+v, %v", plan, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "transport") {
				t.Fatalf("mismatched/missing transport admitted: %+v, %v", plan, err)
			}
		})
	}
	// The standing Discord paper port supplies a real session descriptor and
	// still uses the neutral interface without installing an executable adapter.
	plan, trigger := discordPaperPort(t)
	if plan.Transport() != packs.ChannelTransportSession || trigger.Transport != plan.Transport() {
		t.Fatal("session paper port lost its explicit trigger transport")
	}
}
