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
