package packfixture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// WhatsAppSessionProvider captures the authored fixture roles together. It
// grants no account, connection, runtime root or executable authority.
func WhatsAppSessionProvider(t testing.TB, platformSpecPath string, trigger []byte) (*providertriggers.CatalogSnapshot, packs.SatisfactionPlan) {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, "whatsapp")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {}, cannot: [bypass activity_attempts]}\n"),
		"trigger.yaml":  trigger,
		"connector.yaml": []byte(`tools:
  fixture_send:
    category: provider_connector
    handler_type: in_process
    in_process: whatsapp.send_text
    effect_class: non_idempotent_write
    input_schema:
      type: object
      additionalProperties: false
      required: [destination, text]
      properties:
        destination: {type: string, minLength: 1}
        text: {type: string, minLength: 1, maxLength: 2000}
    output_schema:
      type: object
      additionalProperties: false
      required: [id]
      properties:
        id: {type: string, minLength: 1}
`),
		"channel.yaml": []byte(`transport: session
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
    tool: fixture_send
    input: {destination: context.destination, text: input.presentation.text}
    output: {delivery_reference: result.id}
events:
  text:
    event: message
    fields:
      text: event.text
      external_account_reference: event.external_account_reference
      conversation_reference: event.conversation_reference
      conversation_scope: event.conversation_scope
      provider_message_reference: event.provider_message_reference
      reply_to_message_reference: event.reply_to_message_reference
      entry_invocation: event.entry_invocation
`),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	provider, err := packartifact.LoadProviderFS(os.DirFS(root), "whatsapp", PlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := yamlsource.LoadFile(platformSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	triggerBody, _ := provider.Body(packmodel.BodyTrigger)
	manifest, err := providertriggers.AdmitBody(triggerBody)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshotFromAdmittedBodies(manifest)
	if err != nil {
		t.Fatal(err)
	}
	connectorBody, _ := provider.Body(packmodel.BodyConnector)
	connector, err := providerconnectors.AdmitBody(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := packs.NewProviderBodyIdentity(connectorBody)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	triggerDescriptor, _ := catalog.PackDescriptorForProvider("whatsapp")
	plan, err := packs.CompileProviderChannel(registry, provider, triggerDescriptor, packs.ConnectorPackDescriptor{
		Identity: identity, Provider: "whatsapp", Tools: connector.Tools,
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog, plan
}
