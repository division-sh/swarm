package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"testing"
)

// CopyForkReceiverExecutionOwnership preserves optional receivers through a
// payload-only guard. State-owning receivers perform a contained business write;
// neither surface emits post-frontier work or uses the retired mailbox action.
func CopyForkReceiverExecutionOwnership(t testing.TB, receivers []ForkReceiver, entitylessProducer, nested, repeated bool) string {
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
		replacement := "      guard:\n        id: payload_observed\n        check: payload.token == 'receiver-proof'\n"
		if receiver.Policy != ForkReceiverOptionalAbsent && receiver.Policy != ForkReceiverOptionalExisting {
			applyClosedReplacement(t, filepath.Join(root, path, "entities.yaml"), "  marker: text\n", "  marker: text\n  processed_count: {type: integer, initial: 0}\n")
			replacement = "          - {target_field: processed_count, expression: entity.processed_count + 1}\n"
			if receiver.Policy == ForkReceiverRequiredExisting || receiver.Policy == ForkReceiverRequiredMissing {
				replacement = "      data_accumulation:\n        writes:\n" + replacement
			}
		}
		applyClosedReplacement(t, filepath.Join(root, path, "nodes.yaml"), fmt.Sprintf(`      emit:
        event: receiver.finished
        fields:
          owner: {literal: %s}
          token: {expression: payload.token}
`, receiver.Path), replacement)
		applyClosedReplacement(t, filepath.Join(root, path, "schema.yaml"), "  outputs:\n    events: [receiver.finished]\n", "")
		applyClosedReplacement(t, filepath.Join(root, path, "events.yaml"), "receiver.finished:\n  owner: text\n  token: text\n  swarm:\n    consumer: external\n", "")
		if receiver.Policy == ForkReceiverOptionalAbsent {
			removeClosedVariantFiles(t, root, path+"/events.yaml")
		}
	}
	return root
}
