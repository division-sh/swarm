package canonicalrouting

import (
	"path/filepath"
	"testing"
)

type SealedParentConnectOptions struct {
	OmitConsumerInputBind      bool
	OmitConsumerPolicyBind     bool
	OmitConsumerCredentialBind bool
	ForbiddenSiblingWildcard   bool
	InvalidConnectReceiver     bool
}

// CopySealedParentConnect derives the closed package-boundary conformance
// matrix from the checked-in parent-connect route.
func CopySealedParentConnect(t testing.TB, opts SealedParentConnectOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	packageFile := filepath.Join(root, "package.yaml")
	applyClosedReplacement(t, packageFile,
		"  - id: producer\n    flow: producer\n    mode: static\n",
		"  - id: producer\n    flow: producer\n    mode: static\n    bind:\n      inputs:\n        work.requested: parent.producer_start\n      outputs:\n        work.ready: parent.producer_done\n      policy:\n        runtime.profile: parent.policy.producer.runtime.profile\n      credentials:\n        shared_token: producer_deployment_token\n")
	applyClosedReplacement(t, packageFile,
		"  - id: consumer\n    flow: consumer\n    mode: static\n",
		sealedConsumerFlow(opts))
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "schema.yaml"), "        source: external\n", "")
	if opts.InvalidConnectReceiver {
		applyClosedReplacement(t, packageFile, "    to: consumer.work_ready\n", "    to: consumer.missing_work_ready\n")
	}
	addSealedRootDependencies(t, root)
	addSealedProducerDependencies(t, root)
	addSealedConsumerDependencies(t, root, opts)
	return root
}

func sealedConsumerFlow(opts SealedParentConnectOptions) string {
	result := "  - id: consumer\n    flow: consumer\n    mode: static\n    bind:\n"
	if !opts.OmitConsumerInputBind {
		result += "      inputs:\n        control.start: parent.consumer_start\n"
	}
	if !opts.OmitConsumerPolicyBind {
		result += "      policy:\n        runtime.profile: parent.policy.consumer.runtime.profile\n"
	}
	if !opts.OmitConsumerCredentialBind {
		result += "      credentials:\n        shared_token: consumer_deployment_token\n"
	}
	return result
}

func addSealedRootDependencies(t testing.TB, root string) {
	t.Helper()
	SetOverlayFile(t, root, "policy.yaml", "producer:\n  runtime:\n    profile: producer-bound\nconsumer:\n  runtime:\n    profile: consumer-bound\nruntime:\n  profile: ambient-root\n  ambient: should-not-leak\n")
	SetOverlayFile(t, root, "events.yaml", "parent.producer_start:\n  work_id: text\nparent.producer_done:\n  work_id: text\nparent.consumer_start:\n  work_id: text\n")
}

func addSealedProducerDependencies(t testing.TB, root string) {
	t.Helper()
	writeClosedVariantFile(t, root, "flows/producer/package.yaml", "name: producer\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nrequires:\n  inputs: [work.requested]\n  outputs: [work.ready]\n  policy: [runtime.profile]\n  credentials: [shared_token]\n")
	SetOverlayFile(t, root, "flows/producer/policy.yaml", "runtime:\n  profile: producer-local\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "events.yaml"),
		"work.ready:\n  work_id: text\n",
		"work.ready:\n  work_id: text\naudit.seen:\n  work_id: text\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "  produces: [work.ready]\n", "  produces: [work.ready, audit.seen]\n")
}

func addSealedConsumerDependencies(t testing.TB, root string, opts SealedParentConnectOptions) {
	t.Helper()
	writeClosedVariantFile(t, root, "flows/consumer/package.yaml", "name: consumer\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nrequires:\n  inputs: [control.start]\n  outputs: []\n  policy: [runtime.profile]\n  credentials: [shared_token]\n")
	SetOverlayFile(t, root, "flows/consumer/policy.yaml", "runtime:\n  profile: consumer-local\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "schema.yaml"),
		"      - name: work_ready\n        event: work.ready\n",
		"      - name: work_ready\n        event: work.ready\n      - name: control_start\n        event: control.start\n")
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "events.yaml"),
		"work.ready:\n  work_id: text\n",
		"work.ready:\n  work_id: text\ncontrol.start:\n  work_id: text\naudit.seen:\n  work_id: text\n")
	wildcard := "**/audit.seen"
	if opts.ForbiddenSiblingWildcard {
		wildcard = "producer/**/audit.seen"
	}
	applyClosedReplacement(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"),
		"  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready: {}\n",
		"  subscribes_to: [work.ready, \""+wildcard+"\"]\n  event_handlers:\n    work.ready: {}\n    \""+wildcard+"\": {}\n")
}
