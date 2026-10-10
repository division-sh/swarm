package canonicalrouting

import "testing"

// CopyPipelineDeliveryAuthority owns the unconditional queued-to-done source
// shared by native delivery admission, carrier and settlement witnesses.
func CopyPipelineDeliveryAuthority(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: delivery-authority\nstages:\n  queued: {}\n  done: {final: true}\n")
	writeClosedVariantFile(t, root, "entities.yaml", "test_entity: {}\n")
	writeClosedVariantFile(t, root, "events.yaml", "source.evt:\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n")
	return root
}

// CopyPipelineConnectedDeliveryCollision retains both renamed inputs for the
// exact stamped-claim refusal and replay witness.
func CopyPipelineConnectedDeliveryCollision(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: connected-collision\nconnect:\n  - {event: deploy.done, from: producer, to: receiver, rename: deploy.accepted}\n  - {event: deploy.done, from: producer, to: receiver, rename: deploy.audited}\n")
	writeClosedVariantFile(t, root, "entities.yaml", "test_entity: {}\n")
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\npins:\n  outputs: [deploy.done]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "deploy.done:\n")
	writeClosedVariantFile(t, root, "receiver/schema.yaml", "name: receiver\npins:\n  inputs: [deploy.accepted, deploy.audited]\n")
	writeClosedVariantFile(t, root, "receiver/nodes.yaml", "receiver-node:\n  execution_type: system_node\n  subscribes_to: [deploy.accepted, deploy.audited]\n  event_handlers:\n    deploy.accepted: {}\n    deploy.audited: {}\n")
	return root
}

// CopyPipelineDeliveryRetry owns the unchanged publication-preparation failure
// source, including the selected-store thirty-second retry policy.
func CopyPipelineDeliveryRetry(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: delivery-retry\nstages:\n  queued: {}\n  done: {final: true}\n")
	writeClosedVariantFile(t, root, "entities.yaml", "test_entity: {}\n")
	writeClosedVariantFile(t, root, "events.yaml", "source.evt:\nnode.completed:\n")
	writeClosedVariantFile(t, root, "policy.yaml", "handler_retry_base_seconds: 30\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      emit: node.completed\n")
	return root
}
