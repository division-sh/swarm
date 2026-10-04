package canonicalrouting

import (
	"path/filepath"
	"testing"
)

func CopyRetiredPinMarker(t testing.TB, input bool) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	direction, field := "outputs", "sink"
	if input {
		direction, field = "inputs", "source"
	}
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"  "+direction+":\n    - work.requested\n",
		"  "+direction+":\n    - {event: work.requested, "+field+": harness}\n")
	return root
}

// ApplyCompositionConnectReceiverPinCollisionMutation creates two distinct
// receiver-local edges that collapse onto one durable event x subscriber row.
func ApplyCompositionConnectReceiverPinCollisionMutation(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"),
		"    rename: deploy.completed\n",
		"    rename: deploy.completed\n  - event: deploy.done\n    from: producer\n    to: consumer\n    rename: deploy.audited\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer", "schema.yaml"),
		"    - deploy.completed\n",
		"    - deploy.completed\n    - deploy.audited\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer", "nodes.yaml"),
		"  subscribes_to: [deploy.completed]\n",
		"  subscribes_to: [deploy.completed, deploy.audited]\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer", "nodes.yaml"),
		"      advances_to: done\n",
		"      advances_to: done\n    deploy.audited:\n      create_entity: true\n      advances_to: done\n")
}

// TemplateSelectOrCreateNegativeMutation is the closed fail-closed matrix for
// the canonical select-or-create route.
type TemplateSelectOrCreateNegativeMutation uint8

const (
	TemplateSelectOrCreateRetiredInstanceKey TemplateSelectOrCreateNegativeMutation = iota + 1
	TemplateSelectOrCreateOptionalIdentitySource
	TemplateSelectOrCreateReceiverSelector
	TemplateSelectOrCreateProducerTarget
	TemplateSelectOrCreateProducerBroadcast
)

func ApplyTemplateSelectOrCreateNegativeMutation(t testing.TB, root string, mutation TemplateSelectOrCreateNegativeMutation) {
	t.Helper()
	receiverSchema := filepath.Join(root, "account", "schema.yaml")
	receiverNodes := filepath.Join(root, "account", "nodes.yaml")
	producerNodes := filepath.Join(root, "producer", "nodes.yaml")
	switch mutation {
	case TemplateSelectOrCreateRetiredInstanceKey:
		applyClosedReplacement(t, receiverSchema, "    - account.ready\n", "    - event: account.ready\n      resolution:\n        mode: select-or-create\n        instance_key: account_id\n")
	case TemplateSelectOrCreateOptionalIdentitySource:
		applyClosedReplacement(t, filepath.Join(root, "producer", "events.yaml"),
			"account.ready:\n  key: account_id\n  account_id: text\n", "account.ready:\n  key: account_id\n  account_id: text?\n")
	case TemplateSelectOrCreateReceiverSelector:
		applyClosedReplacement(t, receiverNodes,
			"    account.ready:\n      data_accumulation:\n",
			"    account.ready:\n      select_entity:\n        by:\n          account_id: payload.account_id\n      data_accumulation:\n")
	case TemplateSelectOrCreateProducerTarget:
		applyClosedReplacement(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        target:\n          flow: account\n          match:\n            account_id: payload.account_id\n        fields:\n")
	case TemplateSelectOrCreateProducerBroadcast:
		applyClosedReplacement(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        broadcast: true\n        fields:\n")
	default:
		t.Fatalf("unsupported select-or-create negative mutation %d", mutation)
	}
}

// TemplateReplyNegativeMutation is the closed malformed-pairing matrix for the
// canonical explicit-correlation reply variant.
type TemplateReplyNegativeMutation uint8

const (
	TemplateReplyMissingRepliesTo TemplateReplyNegativeMutation = iota + 1
	TemplateReplyMissingCorrelationField
	TemplateReplyAmbiguousRequestEdge
	TemplateReplyMismatchedProvider
)

func ApplyTemplateReplyNegativeMutation(t testing.TB, root string, mutation TemplateReplyNegativeMutation) {
	t.Helper()
	compositionFile := filepath.Join(root, "schema.yaml")
	switch mutation {
	case TemplateReplyMissingRepliesTo:
		applyClosedReplacement(t, compositionFile, "    replies_to: provider.requested\n", "")
	case TemplateReplyMissingCorrelationField:
		applyClosedReplacement(t, filepath.Join(root, "requester", "events.yaml"),
			"  key: provider_request_id\n  provider_request_id: text\n", "")
	case TemplateReplyAmbiguousRequestEdge:
		applyClosedReplacement(t, compositionFile,
			"  - event: provider.requested\n    from: requester\n    to: provider\n",
			"  - event: provider.requested\n    from: requester\n    to: provider\n  - event: provider.requested\n    from: requester\n    to: provider\n")
	case TemplateReplyMismatchedProvider:
		applyClosedReplacement(t, compositionFile,
			"  - event: provider.replied\n    from: provider\n",
			"  - event: provider.replied\n    from: other-provider\n")
		duplicateFlowForNegativeMutation(t, root, "provider", "other-provider")
		applyClosedReplacement(t, filepath.Join(root, "other-provider", "schema.yaml"), "name: provider\n", "name: other-provider\n")
	default:
		t.Fatalf("unsupported template reply negative mutation %d", mutation)
	}
}
