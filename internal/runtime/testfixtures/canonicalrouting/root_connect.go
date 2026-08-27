package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

type RootConnectEmit uint8

const (
	RootConnectNoEmitter RootConnectEmit = iota
	RootConnectCanonicalEmit
)

// WritePublicTemplateInputRoute owns the API publication fixture with one root
// input and one public template input. Callers load the returned contract tree.
func WritePublicTemplateInputRoute(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `name: review
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"schema.yaml": `name: review
pins:
  inputs:
    events:
      - event: bootstrap.requested
        source: external
`,
		"events.yaml": `bootstrap.requested:
  topic: text?
`,
		"nodes.yaml": `bootstrap-node:
  id: bootstrap-node
  execution_type: system_node
  subscribes_to: [bootstrap.requested]
  event_handlers:
    bootstrap.requested: {}
`,
		"flows/operating/schema.yaml": `name: operating
mode: template
instance: operating_id
pins:
  inputs:
    events:
      - event: opco.product_initialization_requested
        source: external
        resolution:
          mode: create
          from: event.id
`,
		"flows/operating/entities.yaml": `operating:
  operating_id:
    type: uuid
    immutable: true
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  topic: text?
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  event_handlers:
    opco.product_initialization_requested:
      guard:
        check: _entity.id != ""
`,
	}
	for relative, source := range files {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

// CopyRootOutputConnect derives the closed root-output-to-child route matrix
// from the checked-in parent-connect artifact.
func CopyRootOutputConnect(t testing.TB, emit RootConnectEmit) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "package.yaml", `name: root-output-connect
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: static
connect:
  - event: root.ready
    from: .
    to: consumer
`)
	rootInput := ""
	rootNodes := ""
	if emit != RootConnectNoEmitter {
		rootInput = "  inputs:\n    events: [root.start]\n"
		emitBody := "      emit:\n        event: root.ready\n        fields:\n          entity_id: payload.entity_id\n"
		if emit != RootConnectCanonicalEmit {
			t.Fatalf("unsupported root connect emitter %d", emit)
		}
		rootNodes = "root-node:\n  id: root-node\n  execution_type: system_node\n  event_handlers:\n    root.start:\n" + emitBody
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-output-connect\npins:\n"+rootInput+"  outputs:\n    events: [root.ready]\n")
	writeClosedVariantFile(t, root, "events.yaml", "root.start:\n  entity_id: text\nroot.ready:\n  entity_id: text\n")
	if rootNodes != "" {
		writeClosedVariantFile(t, root, "nodes.yaml", rootNodes)
	}
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: static
pins:
  inputs:
    events: [root.ready]
`, "", "consumer_state:\n  entity_id: text\n", `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [root.ready]
  event_handlers:
    root.ready:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	return root
}

func CopyRootOutputConnectMissingReceiver(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	applyClosedReplacement(t, root+"/package.yaml", "    to: consumer\n", "    to: missing\n")
	return root
}

// CopyRootOutputSingletonConnect owns the canonical first-delivery
// root-to-singleton target-ownership proof.
func CopyRootOutputSingletonConnect(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "package.yaml", `name: root-output-singleton-connect
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: singleton
connect:
  - event: root.ready
    from: .
    to: consumer
`)
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: singleton
pins:
  inputs:
    events: [root.ready]
`, "", "consumer_state:\n  entity_id: text\n", `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [root.ready]
  event_handlers:
    root.ready:
      create_entity: true
`)
	return root
}

func CopyRootOutputSingletonArc(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputSingletonConnect(t)
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "  - id: consumer\n    flow: consumer\n", "  - id: receiver\n    flow: receiver\n")
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"), "    to: consumer\n", "    to: receiver\n")
	consumerDir := filepath.Join(root, "flows", "consumer")
	receiverDir := filepath.Join(root, "flows", "receiver")
	if err := os.Rename(consumerDir, receiverDir); err != nil {
		t.Fatalf("rename singleton receiver fixture: %v", err)
	}
	applyClosedReplacement(t, filepath.Join(receiverDir, "schema.yaml"), "name: consumer\n", "name: receiver\n")
	applyClosedReplacement(t, filepath.Join(receiverDir, "entities.yaml"), "consumer_state:", "receiver_state:")
	applyClosedReplacement(t, filepath.Join(receiverDir, "nodes.yaml"), "consumer-node", "arc-receiver")
	applyClosedReplacement(t, filepath.Join(receiverDir, "nodes.yaml"), "  id: consumer-node", "  id: arc-receiver")
	return root
}

// CopySingletonOutputRootConnect owns the reverse singleton-child-to-root
// routing proof through the compiler's root project view.
func CopySingletonOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "package.yaml", `name: singleton-output-root-connect
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scout
    flow: scout
    mode: singleton
connect:
  - event: scout.completed
    from: scout
    to: .
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: singleton-output-root-connect
pins:
  inputs:
    events: [scout.completed]
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `root-collector:
  id: root-collector
  execution_type: system_node
  subscribes_to: [scout.completed]
  event_handlers:
    scout.completed:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	writeLegacyInstanceFlow(t, root, "scout", `name: scout
mode: singleton
pins:
  outputs:
    events: [scout.completed]
`, "scout.completed:\n  proof: string\n", "", "")
	return root
}

// CopyRootSingletonBoomerang owns the reentrant root-to-child-to-root proof.
func CopyRootSingletonBoomerang(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "package.yaml", `name: root-singleton-boomerang
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: boomerang
    flow: boomerang
    mode: singleton
connect:
  - event: work.ping
    from: .
    to: boomerang
  - event: work.pong
    from: boomerang
    to: .
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: root-singleton-boomerang
pins:
  inputs:
    events: [work.pong]
  outputs:
    events: [work.ping]
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.ping:\n  turn: integer\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `root-boomerang:
  id: root-boomerang
  execution_type: system_node
  subscribes_to: [work.pong]
  event_handlers:
    work.pong:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	writeLegacyInstanceFlow(t, root, "boomerang", `name: boomerang
mode: singleton
pins:
  inputs:
    events: [work.ping]
  outputs:
    events: [work.pong]
`, "work.pong:\n  turn: integer\n", "boomerang_state:\n  turn: integer\n", `boomerang-worker:
  id: boomerang-worker
  execution_type: system_node
  subscribes_to: [work.ping]
  event_handlers:
    work.ping:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	return root
}
