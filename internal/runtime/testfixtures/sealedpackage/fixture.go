package sealedpackage

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Options toggles package-boundary variants on top of the canonical parent-connect route.
type Options struct {
	OmitConsumerInputBind      bool
	OmitConsumerPolicyBind     bool
	OmitConsumerCredentialBind bool
	ForbiddenSiblingWildcard   bool
	InvalidConnectReceiver     bool
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	root := Write(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		root,
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	packageFile := filepath.Join(root, "package.yaml")

	producerBind := `  - id: producer
    flow: producer
    mode: static
    bind:
      inputs:
        work.requested: parent.producer_start
      outputs:
        work.ready: parent.producer_done
      policy:
        runtime.profile: parent.policy.producer.runtime.profile
      credentials:
        shared_token: producer_deployment_token
`
	canonicalrouting.ReplaceFile(t, packageFile, `  - id: producer
    flow: producer
    mode: static
`, producerBind)
	canonicalrouting.ReplaceFile(t, packageFile, `  - id: consumer
    flow: consumer
    mode: static
`, consumerFlowYAML(opts))
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/producer/schema.yaml"), "        source: external\n", "")
	if opts.InvalidConnectReceiver {
		canonicalrouting.ReplaceFile(t, packageFile, "    to: consumer.work_ready\n", "    to: consumer.missing_work_ready\n")
	}

	addRootDependencies(t, root)
	addProducerDependencies(t, root)
	addConsumerDependencies(t, root, opts)
	return root
}

func consumerFlowYAML(opts Options) string {
	result := `  - id: consumer
    flow: consumer
    mode: static
    bind:
`
	if !opts.OmitConsumerInputBind {
		result += `      inputs:
        control.start: parent.consumer_start
`
	}
	if !opts.OmitConsumerPolicyBind {
		result += `      policy:
        runtime.profile: parent.policy.consumer.runtime.profile
`
	}
	if !opts.OmitConsumerCredentialBind {
		result += `      credentials:
        shared_token: consumer_deployment_token
`
	}
	return result
}

func addRootDependencies(t testing.TB, root string) {
	t.Helper()
	canonicalrouting.WriteFile(t, root, "policy.yaml", `
producer:
  runtime:
    profile: producer-bound
consumer:
  runtime:
    profile: consumer-bound
runtime:
  profile: ambient-root
  ambient: should-not-leak
`)
	canonicalrouting.WriteFile(t, root, "events.yaml", `
parent.producer_start:
  work_id: text
parent.producer_done:
  work_id: text
parent.consumer_start:
  work_id: text
`)
}

func addProducerDependencies(t testing.TB, root string) {
	t.Helper()
	canonicalrouting.WriteFile(t, root, "flows/producer/package.yaml", `
name: producer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
requires:
  inputs: [work.requested]
  outputs: [work.ready]
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	canonicalrouting.WriteFile(t, root, "flows/producer/policy.yaml", `
runtime:
  profile: producer-local
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/producer/events.yaml"), `work.ready:
  work_id: text
`, `work.ready:
  work_id: text
audit.seen:
  work_id: text
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/producer/nodes.yaml"), "  produces: [work.ready]\n", "  produces: [work.ready, audit.seen]\n")
}

func addConsumerDependencies(t testing.TB, root string, opts Options) {
	t.Helper()
	canonicalrouting.WriteFile(t, root, "flows/consumer/package.yaml", `
name: consumer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
requires:
  inputs: [control.start]
  outputs: []
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	canonicalrouting.WriteFile(t, root, "flows/consumer/policy.yaml", `
runtime:
  profile: consumer-local
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/consumer/schema.yaml"), `      - name: work_ready
        event: work.ready
`, `      - name: work_ready
        event: work.ready
      - name: control_start
        event: control.start
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/consumer/events.yaml"), `work.ready:
  work_id: text
`, `work.ready:
  work_id: text
control.start:
  work_id: text
audit.seen:
  work_id: text
`)
	wildcard := "**/audit.seen"
	if opts.ForbiddenSiblingWildcard {
		wildcard = "producer/**/audit.seen"
	}
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows/consumer/nodes.yaml"), `  subscribes_to: [work.ready]
  event_handlers:
    work.ready: {}
`, `  subscribes_to: [work.ready, "`+wildcard+`"]
  event_handlers:
    work.ready: {}
    "`+wildcard+`": {}
`)
}
