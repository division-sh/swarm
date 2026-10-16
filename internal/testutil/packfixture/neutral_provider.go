package packfixture

// NeutralProviderMembers returns a fresh authored three-BODY fixture, not an
// installed provider or an execution permission.
func NeutralProviderMembers() map[string][]byte {
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
