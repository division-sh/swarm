package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkReceiverAcquisitionWithoutFinishedEmission preserves the authored
// creation, stage and field writes; only the later finished emission is removed.
func CopyForkReceiverAcquisitionWithoutFinishedEmission(t testing.TB, policy ForkReceiverPolicy) string {
	t.Helper()
	if policy != ForkReceiverAutoMaterializing && policy != ForkReceiverExplicitCreate {
		t.Fatalf("effect-only acquisition requires an authored acquiring receiver: %d", policy)
	}
	root := CopyForkReceiverOwnership(t, []ForkReceiver{{Path: "consumer", Policy: policy}}, false)
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"), `      emit:
        event: receiver.finished
        fields:
          owner: {literal: consumer}
          token: {expression: payload.token}
`, "")
	applyClosedReplacement(t, filepath.Join(root, "consumer/schema.yaml"), "  outputs:\n    events: [receiver.finished]\n", "")
	applyClosedReplacement(t, filepath.Join(root, "consumer/events.yaml"), "receiver.finished:\n  owner: text\n  token: text\n  swarm:\n    consumer: external\n", "")
	return root
}
