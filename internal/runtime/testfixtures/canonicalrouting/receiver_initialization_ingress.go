package canonicalrouting

import "testing"

// CopyProviderReceiverInitialization keeps the captured Telegram declaration
// and enters the private receiver through the selected root's connect edge.
func CopyProviderReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := CopyTelegramChatWithoutIngress(t)
	removeClosedVariantFiles(t, root, "telegram-chat/agents.yaml", "telegram-chat/tools.yaml")
	writeClosedVariantFile(t, root, "telegram-chat/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "telegram-chat/schema.yaml", `# fixture: telegram-chat
instance: conversation_reference
auto_emit_on_create:
  event: chat.initialized
pins:
  inputs:
    - inbound.telegram.text_message
`)
	writeClosedVariantFile(t, root, "telegram-chat/entities.yaml", `chat:
  conversation_reference: {type: text, _unused_reason: canonical receiver key}
  text: text
  provider_message_reference: integer
  enabled: {type: boolean, initial: false}
`)
	writeClosedVariantFile(t, root, "telegram-chat/events.yaml", `chat.initialized:
  conversation_reference: text
  text: text
  provider_message_reference: integer
  enabled: boolean
`)
	writeClosedVariantFile(t, root, "telegram-chat/nodes.yaml", `receiver:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message, chat.initialized]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: payload.conversation_reference != ''}
    chat.initialized:
      data_accumulation:
        writes: [text, provider_message_reference, enabled]
`)
	return root
}
