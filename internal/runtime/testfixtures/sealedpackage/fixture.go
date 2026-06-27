package sealedpackage

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Options toggles the fixture variants used by conformance and runtime tests.
type Options struct {
	OmitConsumerInputBind      bool
	OmitConsumerPolicyBind     bool
	OmitConsumerCredentialBind bool
	ForbiddenSiblingWildcard   bool
	InvalidConnectReceiver     bool
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	repoRoot := repoRoot(t)
	fixtureRoot := Write(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	connectTo := "consumer.shared_done"
	if opts.InvalidConnectReceiver {
		connectTo = "consumer.missing_shared_done"
	}
	consumerSubscription := "**/audit.seen"
	if opts.ForbiddenSiblingWildcard {
		consumerSubscription = "producer/**/audit.seen"
	}

	writeFile(t, filepath.Join(root, "package.yaml"), `
name: sealed-flow-package-fixture
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
    bind:
      inputs:
        source.start: parent.producer_start
      outputs:
        shared.done: parent.producer_done
      policy:
        runtime.profile: parent.policy.producer.runtime.profile
      credentials:
        shared_token: producer_deployment_token
  - id: consumer
    flow: consumer
    mode: static
`+consumerBindYAML(opts)+connectYAML(connectTo)+`
`)
	writeRootFiles(t, root)
	writeConsumer(t, root, consumerSubscription)
	return root
}

func connectYAML(connectTo string) string {
	return `connect:
  - from: producer.shared_done
    to: ` + connectTo + `
    delivery: one
    map:
      flow_instance:
        source: payload.flow_instance
        target: instance.flow_instance
`
}

func writeRootFiles(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: sealed-flow-package-fixture\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), `
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
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), `
parent.producer_start:
  flow_instance: string
parent.producer_done:
  flow_instance: string
parent.consumer_start:
  flow_instance: string
`)
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeProducer(t, root)
}

func consumerBindYAML(opts Options) string {
	var b strings.Builder
	b.WriteString("    bind:\n")
	if !opts.OmitConsumerInputBind {
		b.WriteString("      inputs:\n")
		b.WriteString("        control.start: parent.consumer_start\n")
	}
	if !opts.OmitConsumerPolicyBind {
		b.WriteString("      policy:\n")
		b.WriteString("        runtime.profile: parent.policy.consumer.runtime.profile\n")
	}
	if !opts.OmitConsumerCredentialBind {
		b.WriteString("      credentials:\n")
		b.WriteString("        shared_token: consumer_deployment_token\n")
	}
	return b.String()
}

func writeProducer(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), `
name: sealed-shared-component
version: "1.0.0"
requires:
  inputs: [source.start]
  outputs: [shared.done]
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: sealed-shared-component
mode: static
pins:
  inputs:
    events:
      - name: source_start
        event: source.start
  outputs:
    events:
      - name: shared_done
        event: shared.done
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), `
runtime:
  profile: producer-local
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
source.start:
  flow_instance: string
shared.done:
  flow_instance: string
audit.seen:
  flow_instance: string
`)
	writeFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-handler:
  id: producer-handler
  execution_type: system_node
  subscribes_to: [source.start]
  produces: [shared.done, audit.seen]
  event_handlers:
    source.start: {}
`)
}

func writeConsumer(t testing.TB, root, wildcardSubscription string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "consumer", "package.yaml"), `
name: sealed-shared-component
version: "1.0.0"
requires:
  inputs: [control.start]
  outputs: []
  policy: [runtime.profile]
  credentials: [shared_token]
`)
	writeFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: sealed-shared-component
mode: static
pins:
  inputs:
    events:
      - name: control_start
        event: control.start
      - name: shared_done
        event: shared.done
        address:
          by: flow_instance
          source: payload.flow_instance
          target: instance.flow_instance
          cardinality: one
`)
	writeFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), `
runtime:
  profile: consumer-local
`)
	writeFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "consumer", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
shared.done:
  flow_instance: string
control.start:
  flow_instance: string
audit.seen:
  flow_instance: string
`)
	writeFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-handler:
  id: consumer-handler
  execution_type: system_node
  subscribes_to: [shared.done, "`+wildcardSubscription+`"]
  event_handlers:
    shared.done: {}
    "`+wildcardSubscription+`": {}
`)
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve sealed package fixture path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func writeFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}
