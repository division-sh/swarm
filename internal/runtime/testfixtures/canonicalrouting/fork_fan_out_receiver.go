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
`, `      data_accumulation:
        writes:
          - {target_field: number, value: int(payload.token)}
`)
	applyClosedReplacement(t, filepath.Join(root, "settled/entities.yaml"),
		"  marker: text\n", "  marker: text\n  number: {type: integer, initial: 0}\n")
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
		"start.closed:\n", "start.closed:\nstart.checkpointed:\nwork.cut:\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"  outputs: [work.requested, receiver.closed, receiver.seeded]\n",
		"  outputs: [work.requested, receiver.closed, receiver.seeded, work.cut]\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"    start.closed:\n      advances_to: done\n", "    start.closed:\n      advances_to: done\n    start.checkpointed:\n      emit:\n        event: work.cut\n")
	return root
}

// CopyForkFanOutBarrierReceiver keeps the mixed-recipient cut and adds a real
// producer-owned all-settled outcome with a separate root business consumer.
func CopyForkFanOutBarrierReceiver(t testing.TB) string {
	t.Helper()
	root := CopyForkFanOutReceiverOwnership(t)
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"), `      advances_to: active
      data_accumulation:
        writes:
          - target_field: marker
            value: "producer-owned"
`, "")
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"),
		"          fields: {token: entry}\n", `          fields: {token: entry}
      join:
        id: all-receivers-settled
        members: {from_fan_out: true}
        on_complete:
          advances_to: active
          data_accumulation:
            writes: [{target_field: marker, value: 'producer-owned'}]
          emit:
            event: batch.completed
            fields: {total: join.total}
`)
	applyClosedReplacement(t, filepath.Join(root, "producer/events.yaml"),
		"work.ready:\n  token: text\n", "work.ready:\n  token: text\nbatch.completed:\n  total: integer\n")
	applyClosedReplacement(t, filepath.Join(root, "producer/schema.yaml"),
		"outputs: [work.ready]", "outputs: [work.ready, batch.completed]")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"connect:\n", "connect:\n  - {event: batch.completed, from: producer, to: .}\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"    - start.checkpointed\n", "    - start.checkpointed\n    - batch.completed\n")
	applyClosedReplacement(t, filepath.Join(root, "entities.yaml"),
		"  marker: text\n", "  marker: text\n  completed_total: {type: integer, initial: 0}\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"    start.checkpointed:\n", `    batch.completed:
      data_accumulation:
        writes: [{target_field: completed_total, value: payload.total}]
    start.checkpointed:
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"),
		"    - start.checkpointed\n", "    - start.checkpointed\n    - batch.completed\n")
	return root
}

// This counterfactual keeps the same barrier contract but fails while evaluating
// its completion write, before the stage, business state or output can commit.
func CopyForkFanOutBarrierCompletionFailure(t testing.TB) string {
	t.Helper()
	root := CopyForkFanOutBarrierReceiver(t)
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"),
		"writes: [{target_field: marker, value: 'producer-owned'}]",
		"writes: [{target_field: marker, value: string(1 / (join.total - join.total))}]")
	return root
}
