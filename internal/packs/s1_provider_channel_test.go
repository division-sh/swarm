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
)

func s1ChannelProviderMembers() map[string][]byte {
	return map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw, text_message], call_provider_actions: [send_message]}, cannot: [bypass journal]}\n"),
		"trigger.yaml": []byte(`secret: {required: true}
signature: {type: token_equality, header: X-Signature}
event_name: {literal: raw}
normalized_events:
  - event: text_message
    fields:
      text: {from: text, schema: {type: string, minLength: 1}}
      account: {from: account, schema: {type: string, minLength: 1, maxLength: 32}}
      room: {from: room, schema: {type: string, minLength: 1, maxLength: 32}}
      message: {from: message, schema: {type: string, minLength: 1, maxLength: 32}}
      reply: {from: reply, optional: true, schema: {type: string, minLength: 1, maxLength: 32}}
      scope: {from: scope, schema: {type: string, enum: [direct, shared]}}
      entry:
        from: entry
        optional: true
        schema:
          type: object
          additionalProperties: false
          required: [reference]
          properties:
            reference: {type: string, minLength: 1, maxLength: 32}
            address: {type: string, minLength: 5, maxLength: 32}
`),
		"connector.yaml": []byte(`tools:
  send_message:
    category: provider_connector
    handler_type: http
    effect_class: non_idempotent_write
    credentials: [bot_token]
    input_schema:
      type: object
      additionalProperties: false
      required: [room, text]
      properties:
        room: {type: string, minLength: 1, maxLength: 32}
        text: {type: string, minLength: 1, maxLength: 1000}
    output_schema:
      type: object
      additionalProperties: false
      required: [id]
      properties:
        id: {type: string, minLength: 1, maxLength: 32}
    http:
      method: POST
      url: 'https://example.test/messages'
      headers: {Authorization: 'Bearer {{credentials.bot_token}}'}
      body: {room: '{{input.room}}', text: '{{input.text}}'}
    response_success: {kind: http_status_2xx}
    response_mapping: {id: '{{response.body.id}}'}
`),
		"channel.yaml": []byte(`transport: session
capabilities:
  card_render: true
  reply_to_reference: true
  actions_as_buttons: false
  actions_as_text: true
  edit: false
  acknowledgment: false
  inbox_listing: true
onboarding:
  ceremony: authenticated_text_challenge
  provider_credential: bot_token
  confirmation: deliver
  connection_health: provider_connection
  learned_destination: {destination: conversation_reference}
opaque_types:
  destination: {type: string, minLength: 1, maxLength: 32}
  delivery_reference: {type: string, minLength: 1, maxLength: 32}
  external_account_reference: {type: string, minLength: 1, maxLength: 32}
  conversation_reference: {type: string, minLength: 1, maxLength: 32}
operations:
  deliver:
    tool: send_message
    input: {room: context.destination, text: input.presentation.text}
    output: {delivery_reference: result.id}
events:
  text:
    event: text_message
    fields:
      text: event.text
      external_account_reference: event.account
      conversation_reference: event.room
      conversation_scope: event.scope
      provider_message_reference: event.message
      reply_to_message_reference: event.reply
      entry_invocation: event.entry
`),
	}
}

func s1ChannelProviderDescriptors(t testing.TB, key string, members map[string][]byte) (packartifact.AdmittedProvider, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	provider, err := packartifact.AdmitProviderMembers(key, members)
	if err != nil {
		t.Fatal(err)
	}
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
	return provider, catalog.PackDescriptors()[0], packs.ConnectorPackDescriptor{Identity: connectorIdentity, Provider: connector.Provider, Tools: connector.Tools}
}

func TestS1ProviderChannelCompilesCapturedCoLocatedRoles(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		t.Run(key, func(t *testing.T) {
			members := s1ChannelProviderMembers()
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
	provider, trigger, connector := s1ChannelProviderDescriptors(t, "team-bot", s1ChannelProviderMembers())
	_, foreignTrigger, foreignConnector := s1ChannelProviderDescriptors(t, "team_bot", s1ChannelProviderMembers())
	changed := s1ChannelProviderMembers()
	changed["connector.yaml"] = bytes.ReplaceAll(changed["connector.yaml"], []byte("example.test"), []byte("replacement.test"))
	_, _, replacementConnector := s1ChannelProviderDescriptors(t, "team-bot", changed)
	for _, cell := range []struct {
		name      string
		trigger   packs.TriggerPackDescriptor
		connector packs.ConnectorPackDescriptor
	}{
		{"foreign trigger", foreignTrigger, connector},
		{"foreign connector", trigger, foreignConnector},
		{"same-key different-body connector", trigger, replacementConnector},
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
