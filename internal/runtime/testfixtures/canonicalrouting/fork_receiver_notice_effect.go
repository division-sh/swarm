package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"testing"
)

// CopyForkReceiverNoticeOwnership changes only the final business effect.
// Payload-only mailbox writes do not add an entity dependency to observers.
func CopyForkReceiverNoticeOwnership(t testing.TB, receivers []ForkReceiver, entitylessProducer, nested, repeated bool) string {
	t.Helper()
	if nested && repeated {
		t.Fatal("nested repeated ownership is not a declared fixture geometry")
	}
	if entitylessProducer && (nested || repeated) {
		t.Fatal("nested/repeated fixtures require their authored stateful producer")
	}
	root := CopyForkReceiverOwnership(t, receivers, entitylessProducer)
	prefix := ""
	if nested {
		root = CopyForkReceiverNestedOwnership(t, receivers)
		prefix = "branch/"
	}
	if repeated {
		root = CopyForkReceiverRepeatedOwnership(t, receivers)
	}
	for _, receiver := range receivers {
		path := prefix + receiver.Path
		applyClosedReplacement(t, filepath.Join(root, path, "nodes.yaml"), fmt.Sprintf(`      emit:
        event: receiver.finished
        fields:
          owner: {literal: %s}
          token: {expression: payload.token}
`, receiver.Path), fmt.Sprintf(`      action:
        id: mailbox_write
        mailbox:
          item_type: {literal: receiver_processed}
          severity: {literal: normal}
          summary: {literal: %s processed}
          payload:
            owner: {literal: %s}
            token: {ref: payload.token}
`, path, path))
		applyClosedReplacement(t, filepath.Join(root, path, "schema.yaml"), "  outputs:\n    events: [receiver.finished]\n", "")
		applyClosedReplacement(t, filepath.Join(root, path, "events.yaml"), "receiver.finished:\n  owner: text\n  token: text\n  swarm:\n    consumer: external\n", "")
		if receiver.Policy == ForkReceiverOptionalAbsent {
			removeClosedVariantFiles(t, root, path+"/events.yaml")
		}
	}
	return root
}
