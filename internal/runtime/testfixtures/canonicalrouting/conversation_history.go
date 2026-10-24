package canonicalrouting

import "testing"

// ConversationForkConstructionDocuments supplies the fixed historical witness's
// actual keyed constructor and declared advance. It has no agents or autonomous work.
func ConversationForkConstructionDocuments() map[string]string {
	return map[string]string{
		"flow/schema.yaml": "name: flow\ninstance: name\nstages:\n  draft: {}\n  after: {}\ninputs: [conversation.created]\n",
		"flow/state.yaml":  "name: text\ncurrent_state: text?\ngates: json?\naccumulator: json?\nbookkeeping: json?\n",
		"flow/nodes.yaml":  "advance:\n  conversation.created:\n    advances_to: after\n",
	}
}

func ConversationForkHistoryRootDocument() string {
	return "name: conversation-fork-history\nstages:\n  waiting: {}\ninputs: [conversation.created]\noutputs: [conversation.created]\nconnect:\n  - {event: conversation.created, from: ., to: flow, resolution: create}\n"
}

func CopyConversationForkHistory(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
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
