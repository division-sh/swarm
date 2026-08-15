package canonicalrouting

import "testing"

type RootConnectEmit uint8

const (
	RootConnectNoEmitter RootConnectEmit = iota
	RootConnectCanonicalEmit
)

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
  - from: .root_ready
    to: consumer.ready
`)
	rootInput := ""
	rootNodes := "{}\n"
	if emit != RootConnectNoEmitter {
		rootInput = "  inputs:\n    events: [root.start]\n"
		emitBody := "      emit:\n        event: root.ready\n        fields:\n          entity_id: payload.entity_id\n"
		if emit != RootConnectCanonicalEmit {
			t.Fatalf("unsupported root connect emitter %d", emit)
		}
		rootNodes = "root-node:\n  id: root-node\n  execution_type: system_node\n  event_handlers:\n    root.start:\n" + emitBody
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-output-connect\npins:\n"+rootInput+"  outputs:\n    events:\n      - name: root_ready\n        event: root.ready\n")
	writeClosedVariantFile(t, root, "events.yaml", "root.start:\n  entity_id: text\nroot.ready:\n  entity_id: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", rootNodes)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "entities.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: static
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
`, "root.ready:\n  entity_id: text\n", "{}\n", "{}\n")
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
  - from: .root_ready
    to: consumer.ready
`)
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: singleton
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
`, "root.ready:\n  entity_id: text\n", "consumer_state:\n  entity_id: text\n", `consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [root.ready]
  event_handlers:
    root.ready:
      create_entity: true
`)
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
  - from: scout.completed
    to: .scout_completed
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: singleton-output-root-connect
pins:
  inputs:
    events:
      - name: scout_completed
        event: scout.completed
`)
	writeClosedVariantFile(t, root, "events.yaml", "scout.completed:\n  proof: string\n")
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
    events:
      - name: completed
        event: scout.completed
`, "scout.completed:\n  proof: string\n", "{}\n", "{}\n")
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
  - from: .ping
    to: boomerang.ping
  - from: boomerang.pong
    to: .pong
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: root-singleton-boomerang
pins:
  inputs:
    events:
      - name: pong
        event: work.pong
  outputs:
    events:
      - name: ping
        event: work.ping
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.ping:\n  turn: integer\nwork.pong:\n  turn: integer\n")
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
    events:
      - name: ping
        event: work.ping
  outputs:
    events:
      - name: pong
        event: work.pong
`, "work.ping:\n  turn: integer\nwork.pong:\n  turn: integer\n", "boomerang_state:\n  turn: integer\n", `boomerang-worker:
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

// CopyRootAutoEmitKeyCarries owns the fixed root output key/carries proof used
// by boot verification. It is not an open overlay surface.
func CopyRootAutoEmitKeyCarries(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "package.yaml", `name: root-auto-emit-key-carries
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: .root_ready
    to: consumer.ready
`)
	writeClosedVariantFile(t, root, "schema.yaml", `name: root-auto-emit-key-carries
auto_emit_on_create:
  event: root.ready
pins:
  outputs:
    events:
      - name: root_ready
        event: root.ready
        key: entity_id
        carries: [entity_id]
`)
	writeClosedVariantFile(t, root, "events.yaml", "root.ready:\n  entity_id: string\n")
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: static
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
`, "root.ready:\n  entity_id: string\n", "{}\n", "{}\n")
	return root
}
