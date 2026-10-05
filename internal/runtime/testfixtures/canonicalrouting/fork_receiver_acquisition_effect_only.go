package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyForkReceiverAcquisitionWithoutFinishedEmission preserves the authored
// constructor-owned receiver, stage and field writes; only the later finished emission is removed.
func CopyForkReceiverAcquisitionWithoutFinishedEmission(t testing.TB, policy ForkReceiverPolicy) string {
	t.Helper()
	if policy != ForkReceiverConstructorOwned {
		t.Fatalf("effect-only acquisition requires a constructor-owned receiver: %d", policy)
	}
	root := CopyForkReceiverOwnership(t, []ForkReceiver{{Path: "consumer", Policy: policy}}, false)
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"), `      emit:
        event: receiver.finished
        fields:
          owner: "consumer"
          token: payload.token
`, "")
	applyClosedReplacement(t, filepath.Join(root, "consumer/schema.yaml"), "  outputs:\n    - receiver.finished\n", "")
	removeClosedVariantFiles(t, root, "consumer/events.yaml")
	removeForkReceiverFinishedConnection(t, root, "consumer")
	return root
}
