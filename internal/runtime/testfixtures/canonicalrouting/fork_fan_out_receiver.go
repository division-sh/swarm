package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkFanOutReceiverOwnership emits real handler ordinals to two authored
// receivers. A checkpoint input can capture a settled/pending recipient cut.
func CopyForkFanOutReceiverOwnership(t testing.TB) string {
	t.Helper()
	root := CopyForkReceiverOwnership(t, []ForkReceiver{
		{Path: "consumer", Policy: ForkReceiverRequiredExisting},
		{Path: "settled", Policy: ForkReceiverRequiredExisting},
	}, false)
	applyClosedReplacement(t, filepath.Join(root, "consumer/entities.yaml"),
		"  marker: text\n", "  marker: text\n  processed_token: text\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"),
		"          - target_field: marker\n            value: \"consumer-owned\"\n",
		"          - target_field: marker\n            value: \"consumer-owned\"\n          - {target_field: processed_token, value: 'seeded'}\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"), `      emit:
        event: receiver.finished
        fields:
          owner: "consumer"
          token: payload.token
`, `      data_accumulation:
        writes:
          - {target_field: processed_token, value: payload.token}
`)
	applyClosedReplacement(t, filepath.Join(root, "settled/nodes.yaml"), `      emit:
        event: receiver.finished
        fields:
          owner: "settled"
          token: payload.token
`, "")
	for _, receiver := range []string{"consumer", "settled"} {
		applyClosedReplacement(t, filepath.Join(root, receiver, "schema.yaml"), "  outputs:\n    - receiver.finished\n", "")
		removeClosedVariantFiles(t, root, receiver+"/events.yaml")
		removeForkReceiverFinishedConnection(t, root, receiver)
	}
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"start.requested:\n  token: text\n", "start.requested:\n  token: text\n  tokens: '[text]'\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"work.requested:\n  token: text\n", "work.requested:\n  token: text\n  tokens: '[text]'\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"        fields: {token: payload.token}\n    start.closed:",
		"        fields: {token: payload.token, tokens: payload.tokens}\n    start.closed:")
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"),
		"      emit:\n        event: work.ready\n        fields: {token: payload.token}\n", `      fan_out:
        items_from: payload.tokens
        as: entry
        identity: entry
        emit:
          event: work.ready
          fields: {token: entry}
`)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"    - start.closed\n", "    - start.closed\n    - start.checkpointed\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"    - start.closed\n", "    - start.closed\n    - start.checkpointed\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"),
		"start.closed:\n", "start.closed:\nstart.checkpointed:\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"    start.closed:\n      advances_to: done\n", "    start.closed:\n      advances_to: done\n    start.checkpointed: {}\n")
	return root
}
