package canonicalrouting

import "testing"

// CopyProviderReceiverInitialization keeps the checked Telegram schema import
// but uses only a system node: no producer/connect hop or provider transport.
func CopyProviderReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := CopyTelegramChatWithoutIngress(t)
	removeClosedVariantFiles(t, root, "telegram-chat/agents.yaml", "telegram-chat/tools.yaml")
	writeClosedVariantFile(t, root, "telegram-chat/schema.yaml", `name: telegram-chat
imports:
  provider_trigger_events:
    - {provider: telegram, event: inbound.telegram.text_message}
mode: template
instance: conversation_reference
initial_state: active
states: [active]
instance_variables:
  variables:
    initial_text: text
    message_number: integer
    enabled: {type: boolean, default: false}
auto_emit_on_create:
  event: chat.initialized
pins:
  inputs:
    events:
      - event: inbound.telegram.text_message
        source: external
        resolution: {mode: select-or-create}
        initialize:
          initial_text: payload.text
          message_number: payload.provider_message_reference
`)
	writeClosedVariantFile(t, root, "telegram-chat/entities.yaml", `chat:
  conversation_reference: {type: text, _unused_reason: canonical receiver key}
  initial_text: text
  message_number: integer
  enabled: boolean
`)
	writeClosedVariantFile(t, root, "telegram-chat/events.yaml", `chat.initialized:
  conversation_reference: text
  initial_text: text
  message_number: integer
  enabled: boolean
`)
	writeClosedVariantFile(t, root, "telegram-chat/nodes.yaml", `receiver:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message, chat.initialized]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: "payload.conversation_reference != ''"}
    chat.initialized:
      data_accumulation:
        writes: [initial_text, message_number, enabled]
`)
	return root
}
