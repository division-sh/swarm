package packs_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
)

// The descriptor is a paper port, not an installed Gateway adapter. Its typed
// normalized catalog and HTTP operation consume the existing channel compiler.

func TestDiscordPaperPortFitsExistingNeutralInterface(t *testing.T) {
	plan, trigger := discordPaperPort(t)
	vector := plan.Capabilities().Vector()
	if plan.Transport() != packs.ChannelTransportSession || plan.HasNativeInbox() ||
		!vector.CardRender || !vector.ReplyToReference || !vector.ActionsAsText || !vector.InboxListing ||
		vector.Edit || vector.ActionsAsButtons || vector.Acknowledgment {
		t.Fatal("paper port changed its declared session/text baseline")
	}
	profile, found := plan.OnboardingProfile()
	if !found || profile.IdentityCeremony() != packs.ChannelCeremonyAuthenticatedTextChallenge ||
		profile.ProviderCredential() != "discord.discord_bot_token" || profile.SigningCredential() != "" {
		t.Fatal("Gateway readiness replaced the real credential or human claim")
	}
	bounds, err := plan.PresentationBounds()
	if err != nil || bounds.TextRunes != 2000 {
		t.Fatalf("Discord text bound = %+v, %v", bounds, err)
	}
	input, err := plan.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": "Inbox"}}, map[string]any{"destination": "123456789012345678"})
	if err != nil || input["channel_id"] != "123456789012345678" || input["content"] != "Inbox" || len(input) != 2 {
		t.Fatalf("session ingress cannot coexist with HTTP delivery: %v, %v", input, err)
	}
	for _, operation := range []string{"edit", "acknowledge_interaction", "install_inbox_entry", "read_default_inbox_launcher"} {
		if _, _, err := plan.ConnectorOperation(operation); err == nil {
			t.Fatalf("paper port fabricated optional operation %s", operation)
		}
	}
	var unavailable *operatorchannel.SessionProviderUnavailableError
	if err := plan.RequireExecutableProvider(); !errors.As(err, &unavailable) {
		t.Fatalf("uninstalled Gateway transport was executable: %v", err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(channelonboarding.Candidate{Provider: "discord", Plan: plan,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{PlanGeneration: generation},
		Target:     channelonboarding.CandidateTarget{AdmissionGeneration: trigger.Generation}})
	if err != nil {
		t.Fatal(err)
	}
	var readback struct {
		Capabilities map[string]bool `json:"capabilities"`
	}
	if err := json.Unmarshal(raw, &readback); err != nil || len(readback.Capabilities) != 7 ||
		readback.Capabilities["edit"] || readback.Capabilities["actions_as_buttons"] ||
		readback.Capabilities["acknowledgment"] || !readback.Capabilities["inbox_listing"] {
		t.Fatalf("candidate readback omitted or fabricated vector fields: %s: %v", raw, err)
	}
}

// These are the expected adapter outputs specified in the paper transcript,
// not a new implementation of Discord's raw-message interpreter.
