package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// ApplyRetiredResolutionInstanceKeyMutation restores only the deterministic
// pre-#2021 spelling so loader and codemod tests can prove its retirement.
func ApplyRetiredResolutionInstanceKeyMutation(t testing.TB, root string, id ArtifactID) {
	t.Helper()
	insertCarried := func(path, mode string) {
		applyClosedReplacement(t, path,
			"          mode: "+mode+"\n",
			"          mode: "+mode+"\n          instance_key: account_id\n")
	}
	switch id {
	case TemplateSelectExisting:
		path := filepath.Join(root, "flows/account/schema.yaml")
		insertCarried(path, "select-or-create")
		insertCarried(path, "select")
	case TemplateSelectOrCreate:
		insertCarried(filepath.Join(root, "flows/account/schema.yaml"), "select-or-create")
	case TemplateReply:
		path := filepath.Join(root, "flows/requester/schema.yaml")
		applyClosedReplacement(t, path, "          mode: select-or-create\n", "          mode: select-or-create\n          instance_key: account_id\n")
		applyClosedReplacement(t, path, "          mode: select\n", "          mode: select\n          instance_key: account_id\n")
	case TemplateCreateMintedKey:
		path := filepath.Join(root, "flows/validator/schema.yaml")
		applyClosedReplacement(t, path, "          mode: create\n", "          mode: create\n          instance_key:\n            mint: uuid\n            as: validation_case_id\n")
		applyClosedReplacement(t, path, "            from: generated.uuid\n", "            from: instance.key.validation_case_id\n")
	case FanInStream, FanInBarrier:
		path := filepath.Join(root, "flows/operating/schema.yaml")
		applyClosedReplacement(t, path, "          mode: create\n", "          mode: create\n          instance_key:\n            mint: event_id\n            as: operating_id\n")
		applyClosedReplacement(t, path, "            from: event.id\n", "            from: instance.key.operating_id\n")
	case ArtifactID("examples/routing/notify-all-children"):
		path := filepath.Join(root, "flows/account/schema.yaml")
		insertCarried(path, "select-or-create")
		insertCarried(path, "select")
	default:
		t.Fatalf("artifact %q has no deterministic retired resolution.instance_key mutation", id)
	}
}

type RetiredResolutionInstanceKeyBlocker uint8

const (
	RetiredResolutionInstanceKeyMismatch RetiredResolutionInstanceKeyBlocker = iota + 1
	RetiredResolutionInstanceKeyUnknownMint
	RetiredResolutionInstanceKeySelectingSyntheticSource
)

func ApplyRetiredResolutionInstanceKeyBlocker(t testing.TB, root string, blocker RetiredResolutionInstanceKeyBlocker) {
	t.Helper()
	switch blocker {
	case RetiredResolutionInstanceKeyMismatch:
		path := filepath.Join(root, "flows/account/schema.yaml")
		applyClosedReplacement(t, path, "          instance_key: account_id\n", "          instance_key: wrong_id\n")
	case RetiredResolutionInstanceKeyUnknownMint:
		path := filepath.Join(root, "flows/validator/schema.yaml")
		applyClosedReplacement(t, path, "            mint: uuid\n", "            mint: random\n")
	case RetiredResolutionInstanceKeySelectingSyntheticSource:
		path := filepath.Join(root, "flows/account/schema.yaml")
		applyClosedReplacement(t, path, "            from: payload.account_id\n", "            from: generated.uuid\n")
	default:
		t.Fatalf("unsupported retired resolution.instance_key blocker %d", blocker)
	}
}

// ApplyCompositionConnectReceiverPinCollisionMutation creates two distinct
// receiver-local edges that collapse onto one durable event x subscriber row.
func ApplyCompositionConnectReceiverPinCollisionMutation(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"    rename: deploy.completed\n",
		"    rename: deploy.completed\n  - event: deploy.done\n    from: producer\n    to: consumer\n    rename: deploy.audited\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "schema.yaml"),
		"      - deploy.completed\n",
		"      - deploy.completed\n      - deploy.audited\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"),
		"  subscribes_to: [deploy.completed]\n",
		"  subscribes_to: [deploy.completed, deploy.audited]\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"),
		"      advances_to: done\n",
		"      advances_to: done\n    deploy.audited:\n      create_entity: true\n      advances_to: done\n")
}

// ApplyRetiredConnectDeliveryOneMutation creates the single deterministic
// retired spelling accepted by the migration command and rejected by loaders.
func ApplyRetiredConnectDeliveryOneMutation(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - event: work.ready\n    from: producer\n    to: consumer\n",
		"  - event: work.ready\n    from: producer\n    to: consumer\n    delivery: one\n")
}

// ApplyRetiredConnectDeliveryOnePairMutation creates two independently
// removable rows without exposing raw positive bundle construction.
func ApplyRetiredConnectDeliveryOnePairMutation(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - event: work.ready\n    from: producer\n    to: consumer\n",
		"  - event: work.ready\n    from: producer\n    to: consumer\n    delivery: one\n  - event: work.ready\n    from: producer\n    to: consumer\n    delivery: one\n")
}

type RetiredConnectMigrationBlocker uint8

const (
	RetiredConnectDeliveryMany RetiredConnectMigrationBlocker = iota + 1
	RetiredConnectDeliveryBroadcast
	RetiredConnectDeliveryReply
	RetiredConnectReplyMap
)

func ApplyRetiredConnectMigrationBlocker(t testing.TB, root string, blocker RetiredConnectMigrationBlocker) {
	t.Helper()
	field := retiredConnectMigrationBlockerField(t, blocker)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - event: work.ready\n    from: producer\n    to: consumer\n",
		"  - event: work.ready\n    from: producer\n    to: consumer\n"+field)
}

// ApplyRetiredConnectDeliveryOneThenBlockerMutation proves that a later manual
// decision prevents any earlier deterministic removal from reaching disk.
func ApplyRetiredConnectDeliveryOneThenBlockerMutation(t testing.TB, root string, blocker RetiredConnectMigrationBlocker) {
	t.Helper()
	field := retiredConnectMigrationBlockerField(t, blocker)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - event: work.ready\n    from: producer\n    to: consumer\n",
		"  - event: work.ready\n    from: producer\n    to: consumer\n    delivery: one\n  - event: work.ready\n    from: producer\n    to: consumer\n"+field)
}

func retiredConnectMigrationBlockerField(t testing.TB, blocker RetiredConnectMigrationBlocker) string {
	t.Helper()
	switch blocker {
	case RetiredConnectDeliveryMany:
		return "    delivery: many\n"
	case RetiredConnectDeliveryBroadcast:
		return "    delivery: broadcast\n"
	case RetiredConnectDeliveryReply:
		return "    delivery: reply\n"
	case RetiredConnectReplyMap:
		return "    reply:\n      correlation_id: event.correlation_id\n"
	default:
		t.Fatalf("unsupported retired connect migration blocker %d", blocker)
	}
	return ""
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
	receiverSchema := filepath.Join(root, "flows", "account", "schema.yaml")
	receiverNodes := filepath.Join(root, "flows", "account", "nodes.yaml")
	producerNodes := filepath.Join(root, "flows", "producer", "nodes.yaml")
	switch mutation {
	case TemplateSelectOrCreateRetiredInstanceKey:
		applyClosedReplacement(t, receiverSchema, "          mode: select-or-create\n", "          mode: select-or-create\n          instance_key: account_id\n")
	case TemplateSelectOrCreateOptionalIdentitySource:
		applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"),
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
	requesterSchema := filepath.Join(root, "flows", "requester", "schema.yaml")
	packageFile := filepath.Join(root, "package.yaml")
	switch mutation {
	case TemplateReplyMissingRepliesTo:
		applyClosedReplacement(t, requesterSchema, "          replies_to: provider.requested\n", "")
	case TemplateReplyMissingCorrelationField:
		applyClosedReplacement(t, filepath.Join(root, "flows", "requester", "events.yaml"),
			"  key: provider_request_id\n  provider_request_id: text\n", "")
	case TemplateReplyAmbiguousRequestEdge:
		applyClosedReplacement(t, packageFile,
			"  - event: provider.requested\n    from: requester\n    to: provider\n",
			"  - event: provider.requested\n    from: requester\n    to: provider\n  - event: provider.requested\n    from: requester\n    to: provider\n")
	case TemplateReplyMismatchedProvider:
		applyClosedReplacement(t, packageFile,
			"  - id: provider\n    flow: provider\n    mode: static\n",
			"  - id: provider\n    flow: provider\n    mode: static\n  - id: other-provider\n    flow: other-provider\n    mode: static\n")
		applyClosedReplacement(t, packageFile,
			"  - event: provider.replied\n    from: provider\n    to: requester\n",
			"  - event: provider.replied\n    from: other-provider\n    to: requester\n")
		duplicateFlowForNegativeMutation(t, root, "provider", "other-provider")
		applyClosedReplacement(t, filepath.Join(root, "flows", "other-provider", "schema.yaml"), "name: provider\n", "name: other-provider\n")
	default:
		t.Fatalf("unsupported template reply negative mutation %d", mutation)
	}
}
