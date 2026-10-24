package canonicalrouting

import "testing"

// CopyProviderReceiverInitialization keeps the checked Telegram schema import
// and enters the private receiver through the selected root's connect edge.
func CopyProviderReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := CopyTelegramChatWithoutIngress(t)
	removeClosedVariantFiles(t, root, "telegram-chat/agents.yaml", "telegram-chat/tools.yaml")
	writeClosedVariantFile(t, root, "telegram-chat/schema.yaml", `name: telegram-chat
imports:
  provider_trigger_events:
    - {provider: telegram, event: inbound.telegram.text_message}
instance: conversation_reference
auto_emit_on_create:
  event: chat.initialized
inputs:
  - inbound.telegram.text_message
`)
	writeClosedVariantFile(t, root, "telegram-chat/state.yaml", `conversation_reference: text
text: text
provider_message_reference: integer
enabled: false
`)
	writeClosedVariantFile(t, root, "telegram-chat/events.yaml", `chat.initialized:
  conversation_reference: text
  text: text
  provider_message_reference: integer
  enabled: boolean
`)
	writeClosedVariantFile(t, root, "telegram-chat/nodes.yaml", `receiver:
  inbound.telegram.text_message:
    guard: payload.conversation_reference != ''
  chat.initialized:
    data_accumulation:
      writes: [text, provider_message_reference, enabled]
`)
	return root
}
