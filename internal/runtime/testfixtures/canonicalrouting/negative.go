package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// TemplateSelectOrCreateNegativeMutation is the closed fail-closed matrix for
// the canonical select-or-create route.
type TemplateSelectOrCreateNegativeMutation uint8

const (
	TemplateSelectOrCreateBadConnectMapping TemplateSelectOrCreateNegativeMutation = iota + 1
	TemplateSelectOrCreateDuplicateConnectMapping
	TemplateSelectOrCreateMissingInstanceKey
	TemplateSelectOrCreateMissingCarry
	TemplateSelectOrCreateReceiverSelector
	TemplateSelectOrCreateProducerTarget
	TemplateSelectOrCreateProducerBroadcast
)

func ApplyTemplateSelectOrCreateNegativeMutation(t testing.TB, root string, mutation TemplateSelectOrCreateNegativeMutation) {
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	receiverSchema := filepath.Join(root, "flows", "account", "schema.yaml")
	receiverNodes := filepath.Join(root, "flows", "account", "nodes.yaml")
	producerNodes := filepath.Join(root, "flows", "producer", "nodes.yaml")
	switch mutation {
	case TemplateSelectOrCreateBadConnectMapping:
		applyClosedReplacement(t, packageFile,
			"  - from: producer.account_ready\n    to: account.account_ready\n",
			"  - from: producer.account_ready\n    to: account.account_ready\n    using:\n      instance:\n        source: missing_account_id\n        target: account_id\n")
	case TemplateSelectOrCreateDuplicateConnectMapping:
		applyClosedReplacement(t, packageFile,
			"  - from: producer.account_ready\n    to: account.account_ready\n",
			"  - from: producer.account_ready\n    to: account.account_ready\n    using:\n      instance:\n        source: [account_id, account_id]\n        target: [account_id, account_id]\n")
	case TemplateSelectOrCreateMissingInstanceKey:
		applyClosedReplacement(t, receiverSchema, "          instance_key: account_id\n", "")
	case TemplateSelectOrCreateMissingCarry:
		applyClosedReplacement(t, receiverSchema,
			"        carries:\n          account_id:\n            from: payload.account_id\n            type: text\n", "")
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
	TemplateReplyMissingCorrelationCarry
	TemplateReplyAmbiguousRequestEdge
	TemplateReplyMismatchedProvider
)

func ApplyTemplateReplyNegativeMutation(t testing.TB, root string, mutation TemplateReplyNegativeMutation) {
	t.Helper()
	requesterSchema := filepath.Join(root, "flows", "requester", "schema.yaml")
	packageFile := filepath.Join(root, "package.yaml")
	switch mutation {
	case TemplateReplyMissingRepliesTo:
		applyClosedReplacement(t, requesterSchema, "          replies_to: provider_requested\n", "")
	case TemplateReplyMissingCorrelationCarry:
		applyClosedReplacement(t, requesterSchema, "        carries: [provider_request_id]\n", "")
	case TemplateReplyAmbiguousRequestEdge:
		applyClosedReplacement(t, packageFile,
			"  - from: requester.provider_requested\n    to: provider.provider_requested\n",
			"  - from: requester.provider_requested\n    to: provider.provider_requested\n  - from: requester.provider_requested\n    to: provider.provider_requested\n")
	case TemplateReplyMismatchedProvider:
		applyClosedReplacement(t, packageFile,
			"  - id: provider\n    flow: provider\n    mode: static\n",
			"  - id: provider\n    flow: provider\n    mode: static\n  - id: other-provider\n    flow: other-provider\n    mode: static\n")
		applyClosedReplacement(t, packageFile,
			"  - from: provider.provider_replied\n    to: requester.provider_replied\n",
			"  - from: other-provider.provider_replied\n    to: requester.provider_replied\n")
		duplicateFlowForNegativeMutation(t, root, "provider", "other-provider")
		applyClosedReplacement(t, filepath.Join(root, "flows", "other-provider", "schema.yaml"), "name: provider\n", "name: other-provider\n")
	default:
		t.Fatalf("unsupported template reply negative mutation %d", mutation)
	}
}
