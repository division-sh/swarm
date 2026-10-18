package packfixture

import (
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// WhatsAppSessionChannel is the compiler-only descriptor shared by SDK and
// discovery proofs. It grants no account, connection or executable authority.
func WhatsAppSessionChannel(t testing.TB, platformSpecPath string, catalog *providertriggers.CatalogSnapshot) packs.SatisfactionPlan {
	t.Helper()
	body := []byte(`provider: whatsapp
transport: session
capabilities: {card_render: true, reply_to_reference: true, actions_as_buttons: false, actions_as_text: true, edit: false, acknowledgment: false, inbox_listing: true}
onboarding:
  ceremony: authenticated_text_challenge
  confirmation: deliver
  connection_health: provider_connection
  learned_destination: {destination: conversation_reference}
opaque_types:
  destination: {type: string, minLength: 1}
  delivery_reference: {type: string, minLength: 1}
  external_account_reference: {type: string, minLength: 1}
  conversation_reference: {type: string, minLength: 1}
operations:
  deliver:
    tool: whatsapp.fixture_send
    input: {destination: context.destination, text: input.presentation.text}
    output: {delivery_reference: result.id}
events:
  text:
    event: inbound.whatsapp.message
    fields:
      text: event.text
      external_account_reference: event.external_account_reference
      conversation_reference: event.conversation_reference
      conversation_scope: event.conversation_scope
      provider_message_reference: event.provider_message_reference
      reply_to_message_reference: event.reply_to_message_reference
      entry_invocation: event.entry_invocation
`)
	manifest, err := packs.ParseChannelManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	pack := packs.LoadedChannelPack{Envelope: packs.Envelope{ID: "provider.whatsapp.hitl_channel", Version: "0.1.0", Type: packs.TypeChannel,
		ManifestHash: packs.ManifestHash(body), Implements: []string{operatorchannel.InterfaceHITLChannelV2}, Provenance: packs.Provenance{Source: "external"},
		Requires: packs.Requires{Packs: map[string]string{packs.TypeTrigger: "provider.whatsapp.input", packs.TypeConnector: "provider.whatsapp.fixture_connector"}}},
		Manifest: manifest, Source: packs.MustPackSource("test", "active-native-input")}
	text := contracts.MustToolInputSchema(contracts.ToolSchemaString, contracts.ToolSchemaMinLength(1))
	presentationText := contracts.MustToolInputSchema(contracts.ToolSchemaString, contracts.ToolSchemaMinLength(1), contracts.ToolSchemaMaxLength(2000))
	object := func(properties map[string]contracts.ToolInputSchema, required ...string) contracts.ToolInputSchema {
		return contracts.MustToolInputSchema(contracts.ToolSchemaObject, contracts.ToolSchemaProperties(properties), contracts.ToolSchemaRequired(required...))
	}
	connector := packs.ConnectorPackDescriptor{Identity: packs.MustPackIdentity("provider.whatsapp.fixture_connector", "0.1.0", packs.ManifestHash([]byte("fixture-connector")), packs.TypeConnector, packs.MustPackSource("test", "active-native-input")),
		Provider: "whatsapp", Tools: map[string]contracts.ToolSchemaEntry{"whatsapp.fixture_send": contracts.MustToolSchemaEntry(
			contracts.WithToolCategory(contracts.ToolCategoryProviderConnector.String()), contracts.WithToolHandler(contracts.ToolHandlerInProcess),
			contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
			contracts.WithToolSchemas(object(map[string]contracts.ToolInputSchema{"destination": text, "text": presentationText}, "destination", "text"), object(map[string]contracts.ToolInputSchema{"id": text}, "id")),
			contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))}}
	snapshot, err := yamlsource.LoadFile(platformSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := packs.CompileChannel(registry, pack, catalog.PackDescriptors(), []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
