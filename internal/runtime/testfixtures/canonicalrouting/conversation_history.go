package canonicalrouting

import "testing"

// ConversationForkConstructionDocuments supplies the fixed historical witness's
// actual keyed constructor and declared advance. It has no agents or autonomous work.
func ConversationForkConstructionDocuments() map[string]string {
	return map[string]string{
		"flow/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"flow/schema.yaml":   "instance: name\nstages:\n  draft: {}\n  after: {}\npins:\n  inputs: [conversation.created]\n",
		"flow/entities.yaml": "default:\n  name: text\n  current_state: {type: 'text?', _unused_reason: Fixed historical same-spelling business-field witness.}\n  gates: {type: 'json?', _unused_reason: Fixed historical same-spelling business-field witness.}\n  accumulator: {type: 'json?', _unused_reason: Fixed historical same-spelling business-field witness.}\n  bookkeeping: {type: 'json?', _unused_reason: Fixed historical same-spelling business-field witness.}\n",
		"flow/nodes.yaml":    "advance:\n  execution_type: system_node\n  event_handlers:\n    conversation.created:\n      advances_to: after\n",
	}
}

func ConversationForkHistoryRootDocument() string {
	return "stages:\n  waiting: {}\npins:\n  inputs: [conversation.created]\n  outputs: [conversation.created]\nconnect:\n  - {event: conversation.created, from: ., to: flow, resolution: create}\n"
}

func CopyConversationForkHistory(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", ConversationForkHistoryRootDocument())
	writeClosedVariantFile(t, root, "events.yaml", "conversation.created:\n  name: text\n")
	addConversationForkConstruction(t, root)
	return root
}

func addConversationForkConstruction(t testing.TB, root string) {
	t.Helper()
	for path, source := range ConversationForkConstructionDocuments() {
		writeClosedVariantFile(t, root, path, source)
	}
}
