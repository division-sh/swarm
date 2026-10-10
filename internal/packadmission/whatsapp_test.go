package packadmission

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEmbeddedWhatsAppPackConsumesExactInventoryWithoutInventingExecution(t *testing.T) {
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(contracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	platform, err := contracts.ParsePlatformSpecDocument(body, "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	projection, err := Admit(inventory, platform)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"provider.whatsapp", "provider.whatsapp.connector", "provider.whatsapp.hitl_channel"} {
		entry, found := projection.Inventory.Lookup(id)
		if !found || entry.Source() != packartifact.ProvenanceEmbedded || entry.ManifestHash() != packs.ManifestHash(entry.ManifestBody()) ||
			len(entry.Envelope().Requires.Secrets) != 0 || len(entry.Envelope().Requires.ManagedCredentials) != 0 {
			t.Fatal("installed WhatsApp pack lost exact bytes or acquired fabricated credentials", id)
		}
	}
	trigger, found := projection.ProviderTriggers.EntryByProvider("whatsapp")
	if !found || trigger.Identity.ID != "provider.whatsapp" || trigger.Manifest.Transport() != packs.ChannelTransportSession {
		t.Fatal("installed native trigger did not consume verified inventory provenance")
	}
	for _, input := range []string{"/inbox", "hello", "/inbox@foreign", "/boite"} {
		payload, err := json.Marshal(map[string]any{"kind": "message", "text": input,
			"external_account_reference": "customer@s.whatsapp.net", "conversation_reference": "customer@s.whatsapp.net",
			"conversation_scope": "direct", "provider_message_reference": "message-1", "provider_timestamp_ms": 1, "ephemeral": false})
		if err != nil {
			t.Fatal(err)
		}
		outputs, err := trigger.Manifest.ProjectNormalizedPayload(payload)
		if err != nil || len(outputs) != 1 || outputs[0].Name != "inbound.whatsapp.message" {
			t.Fatal("installed native input lost its normalized text contract", input, outputs, err)
		}
		entry, present := outputs[0].Payload["entry_invocation"]
		if present != (input == "/inbox") {
			t.Fatal("installed pack guessed an Inbox entry or required a provider menu", input, entry)
		}
		if present && entry.(map[string]any)["reference"] != "inbox" {
			t.Fatal("installed pack lost its explicit text entry reference", entry)
		}
	}
	count := 0
	for _, plan := range projection.ChannelPlans {
		if plan.Provider() != "whatsapp" {
			continue
		}
		count++
		vector := plan.Capabilities().Vector()
		if plan.Transport() != packs.ChannelTransportSession || plan.HasNativeInbox() || !vector.CardRender || !vector.ReplyToReference ||
			!vector.ActionsAsText || !vector.InboxListing || vector.ActionsAsButtons || vector.Edit || vector.Acknowledgment {
			t.Fatal("installed native channel invented Telegram extension capabilities")
		}
		profile, ok := plan.OnboardingProfile()
		if !ok || profile.ProviderCredential() != "" || profile.SigningCredential() != "" ||
			profile.IdentityCeremony() != packs.ChannelCeremonyAuthenticatedTextChallenge {
			t.Fatal("pairing replaced human claim or acquired a dummy credential")
		}
		id, tool, err := plan.ConnectorOperation("deliver")
		target, native := tool.InProcess()
		if err != nil || id != "whatsapp.send_text" || !native || target != contracts.ToolInProcessWhatsAppSendText {
			t.Fatal("installed delivery lost exact native implementation", id, err)
		}
		var unavailable *operatorchannel.SessionProviderUnavailableError
		if err := plan.RequireExecutableProvider(); !errors.As(err, &unavailable) {
			t.Fatal("pack declaration alone granted connection execution", err)
		}
	}
	if count != 1 {
		t.Fatal("installed inventory must compile exactly one WhatsApp channel", count)
	}
}
