package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

type ForkReceiverPolicy uint8

const (
	ForkReceiverOptionalAbsent ForkReceiverPolicy = iota + 1
	ForkReceiverOptionalExisting
	ForkReceiverRequiredExisting
	ForkReceiverRequiredMissing
	ForkReceiverAutoMaterializing
	ForkReceiverExplicitCreate
)

type ForkReceiver struct {
	Path   string
	Policy ForkReceiverPolicy
}

// CopyForkReceiverBusinessMutationOwnership preserves independent owners but
// settles the receiver through authored state, not a later emitted event.
func CopyForkReceiverBusinessMutationOwnership(t testing.TB, entitylessProducer bool) string {
	t.Helper()
	root := CopyForkReceiverOwnership(t, []ForkReceiver{{Path: "consumer", Policy: ForkReceiverRequiredExisting}}, entitylessProducer)
	applyClosedReplacement(t, filepath.Join(root, "consumer/entities.yaml"), "  marker: text\n", "  marker: text\n  processed_token: text\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"),
		"          - {target_field: marker, expression: \"'consumer-owned'\"}\n",
		"          - {target_field: marker, expression: \"'consumer-owned'\"}\n          - {target_field: processed_token, expression: \"'seeded'\"}\n")
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"), `      emit:
        event: receiver.finished
        fields:
          owner: {literal: consumer}
          token: {expression: payload.token}
`, `      data_accumulation:
        writes:
          - {target_field: processed_token, expression: payload.token}
`)
	applyClosedReplacement(t, filepath.Join(root, "consumer/schema.yaml"), "  outputs:\n    events: [receiver.finished]\n", "")
	applyClosedReplacement(t, filepath.Join(root, "consumer/events.yaml"), "receiver.finished:\n  owner: text\n  token: text\n  swarm:\n    consumer: external\n", "")
	return root
}

// CopyReceiverMaterializationWithAgent retains the ordinary connected seed
// handler and adds an independent observer of the same receiving pin.
func CopyReceiverMaterializationWithAgent(t testing.TB, agent string) string {
	t.Helper()
	if agent != "collector" && agent != "renamed-observer" {
		t.Fatalf("unsupported closed receiver observer variant %q", agent)
	}
	root := CopyForkReceiverBusinessMutationOwnership(t, false)
	writeClosedVariantFile(t, root, "consumer/prompts/observer.md", "Observe the admitted item.\n")
	writeClosedVariantFile(t, root, "consumer/agents.yaml", fmt.Sprintf(`%s:
  id: %s
  role: observer
  model: regular
  intent: prompts/observer.md
  subscriptions: [receiver.seeded]
  emit_events: []
`, agent, agent))
	return root
}

func CopyReceiverMaterializationCompetingNodes(t testing.TB) string {
	t.Helper()
	root := CopyReceiverMaterializationWithAgent(t, "collector")
	applyClosedReplacement(t, filepath.Join(root, "consumer/nodes.yaml"), "collector:\n", `competing-materializer:
  execution_type: system_node
  subscribes_to: [receiver.seeded]
  event_handlers:
    receiver.seeded:
      create_entity: true
      advances_to: active
collector:
`)
	return root
}

func CopyReceiverMaterializationGeometry(t testing.TB, nested bool) string {
	t.Helper()
	receivers := []ForkReceiver{{Path: "consumer", Policy: ForkReceiverRequiredExisting}, {Path: "sibling", Policy: ForkReceiverRequiredExisting}}
	root := CopyForkReceiverOwnership(t, receivers, false)
	prefix := ""
	if nested {
		root, prefix = CopyForkReceiverNestedOwnership(t, receivers), "branch/"
	}
	for _, receiver := range receivers {
		path := prefix + receiver.Path
		writeClosedVariantFile(t, root, path+"/prompts/observer.md", "Observe the admitted item.\n")
		writeClosedVariantFile(t, root, path+"/agents.yaml", `collector:
  id: collector
  role: observer
  model: regular
  intent: prompts/observer.md
  subscriptions: [receiver.seeded]
  emit_events: []
`)
	}
	return root
}

func CopyReceiverMaterializationSourceLocalObserver(t testing.TB) string {
	t.Helper()
	root := CopyForkReceiverBusinessMutationOwnership(t, false)
	writeClosedVariantFile(t, root, "prompts/observer.md", "Observe the source input without acquiring a receiver entity.\n")
	writeClosedVariantFile(t, root, "agents.yaml", `observer:
  id: observer
  role: observer
  model: regular
  intent: prompts/observer.md
  subscriptions: [start.seeded]
  emit_events: []
`)
	return root
}

func CopyReceiverMaterializationIntoRoot(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: receiver-root-materialization
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.closed, source: external}
      - child.ready
connect:
  - {event: child.ready, from: child, to: .}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.closed: {}\n")
	writeClosedVariantFile(t, root, "entities.yaml", "receipt:\n  token: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `collector:
  execution_type: system_node
  subscribes_to: [child.ready, work.closed]
  event_handlers:
    work.closed:
      advances_to: done
    child.ready:
      create_entity: true
      advances_to: active
      data_accumulation:
        writes: [{target_field: token, expression: payload.token}]
`)
	writeClosedVariantFile(t, root, "agents.yaml", `collector:
  id: collector
  role: observer
  model: regular
  intent: prompts/observer.md
  subscriptions: [child.ready]
  emit_events: []
`)
	writeClosedVariantFile(t, root, "prompts/observer.md", "Observe the receiving root entity.\n")
	writeClosedVariantFile(t, root, "child/schema.yaml", "name: child\npins:\n  inputs:\n    events: [{event: work.requested, source: external}]\n  outputs:\n    events: [child.ready]\n")
	writeClosedVariantFile(t, root, "child/events.yaml", "work.requested:\n  token: text\nchild.ready:\n  token: text\n")
	writeClosedVariantFile(t, root, "child/nodes.yaml", `producer:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      emit:
        event: child.ready
        fields: {token: {expression: payload.token}}
`)
	return root
}

func CopyForkReceiverRepeatedOwnership(t testing.TB, receivers []ForkReceiver) string {
	t.Helper()
	root := CopyForkReceiverOwnership(t, receivers, false)
	applyClosedReplacement(t, filepath.Join(root, "producer/schema.yaml"), "  active: {terminal: true}", "  active: {}\n  done: {terminal: true}")
	applyClosedReplacement(t, filepath.Join(root, "producer/schema.yaml"), "    events: [work.requested]", "    events:\n      - work.requested\n      - {event: producer.closed, source: external}")
	applyClosedReplacement(t, filepath.Join(root, "producer/events.yaml"), "work.ready:", "producer.closed: {}\nwork.ready:")
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"), "subscribes_to: [work.requested]", "subscribes_to: [work.requested, producer.closed]")
	applyClosedReplacement(t, filepath.Join(root, "producer/nodes.yaml"), "  event_handlers:\n", "  event_handlers:\n    producer.closed:\n      advances_to: done\n")
	return root
}

func CopyForkReceiverStaticAcquisitionRefusal(t testing.TB) string {
	t.Helper()
	root := CopyForkReceiverOwnership(t, []ForkReceiver{{Path: "consumer", Policy: ForkReceiverExplicitCreate}}, false)
	applyClosedReplacement(t, filepath.Join(root, "consumer/schema.yaml"), "name: consumer\n", "name: consumer\nmode: static\n")
	return root
}

// CopyForkReceiverNestedOwnership crosses two explicit package boundaries;
// local collector/work.ready spellings are deliberately reused by siblings.
func CopyForkReceiverNestedOwnership(t testing.TB, receivers []ForkReceiver) string {
	t.Helper()
	inner := CopyForkReceiverOwnership(t, receivers, false)
	applyClosedReplacement(t, filepath.Join(inner, "schema.yaml"), "{event: start.seeded, source: external}", "start.seeded")
	applyClosedReplacement(t, filepath.Join(inner, "schema.yaml"), "{event: start.requested, source: external}", "start.requested")
	applyClosedReplacement(t, filepath.Join(inner, "events.yaml"), "start.seeded:\n  token: text\nstart.requested:\n  token: text\n", "")
	root := t.TempDir()
	copyTree(t, inner, filepath.Join(root, "branch"))
	writeClosedVariantFile(t, root, "schema.yaml", `name: fork-receiver-outer
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: outer.seeded, source: external}
      - {event: outer.requested, source: external}
      - {event: outer.closed, source: external}
  outputs:
    events: [start.seeded, start.requested]
connect:
  - {event: start.seeded, from: ., to: branch}
  - {event: start.requested, from: ., to: branch}
`)
	writeClosedVariantFile(t, root, "entities.yaml", "root:\n  marker: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "outer.seeded:\n  token: text\nouter.requested:\n  token: text\nouter.closed: {}\nstart.seeded:\n  token: text\nstart.requested:\n  token: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  subscribes_to: [outer.seeded, outer.requested, outer.closed]
  event_handlers:
    outer.seeded:
      advances_to: active
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'root-owned'"}
      emit:
        event: start.seeded
        fields: {token: {expression: payload.token}}
    outer.requested:
      emit:
        event: start.requested
        fields: {token: {expression: payload.token}}
    outer.closed:
      advances_to: done
`)
	return root
}

// CopyForkReceiverOwnership keeps producer, seed and receiving effects separate.
// Every existing receiver is created by an ordinary connected source delivery.
func CopyForkReceiverOwnership(t testing.TB, receivers []ForkReceiver, entitylessProducer bool) string {
	t.Helper()
	root := t.TempDir()
	rootEdges := "  - {event: work.requested, from: ., to: producer}\n"
	seedEdges := ""
	seen := map[string]bool{}
	for _, receiver := range receivers {
		if receiver.Path == "" || seen[receiver.Path] || strings.Contains(receiver.Path, "/") {
			t.Fatalf("fork receiver fixture requires distinct direct child paths: %q", receiver.Path)
		}
		seen[receiver.Path] = true
		rootEdges += fmt.Sprintf("  - {event: work.ready, from: producer, to: %s}\n", receiver.Path)
		seeded := receiver.Policy == ForkReceiverOptionalExisting || receiver.Policy == ForkReceiverRequiredExisting
		if seeded {
			seedEdges += fmt.Sprintf("  - {event: receiver.seeded, from: ., to: %s}\n", receiver.Path)
		}
		inputs := "work.ready"
		pinInputs := "      - work.ready\n"
		seedHandler := ""
		seedSchema := ""
		if seeded || receiver.Policy == ForkReceiverRequiredMissing {
			inputs += ", receiver.seeded"
			if seeded {
				pinInputs += "      - receiver.seeded\n"
			} else {
				pinInputs += "      - {event: receiver.seeded, source: external}\n"
				seedSchema = "receiver.seeded:\n  token: text\n"
			}
			seedHandler = fmt.Sprintf(`    receiver.seeded:
      advances_to: active
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'%s-owned'"}
`, receiver.Path)
		}
		stages := ""
		closeEvent := ""
		entities := "receipt: {}\n"
		if receiver.Policy != ForkReceiverOptionalAbsent {
			closeEvent = "receiver.closed: {}\n"
			stages = "stages:\n  waiting: {initial: true}\n  active: {}\n  done: {terminal: true}\n"
			inputs += ", receiver.closed"
			pinInputs += "      - {event: receiver.closed, source: external}\n"
			seedHandler += "    receiver.closed:\n      advances_to: done\n"
			entities = "receipt:\n  marker: text\n"
		}
		body := ""
		switch receiver.Policy {
		case ForkReceiverOptionalAbsent, ForkReceiverOptionalExisting:
		case ForkReceiverRequiredExisting, ForkReceiverRequiredMissing:
			body = fmt.Sprintf("      guard:\n        id: exact_receiver_marker\n        check: \"entity.marker == '%s-owned'\"\n", receiver.Path)
		case ForkReceiverExplicitCreate:
			body = "      create_entity: true\n"
			fallthrough
		case ForkReceiverAutoMaterializing:
			body += fmt.Sprintf("      advances_to: active\n      data_accumulation:\n        writes:\n          - {target_field: marker, expression: \"'%s-created'\"}\n", receiver.Path)
		default:
			t.Fatalf("unknown fork receiver policy %d", receiver.Policy)
		}
		body += fmt.Sprintf(`      emit:
        event: receiver.finished
        fields:
          owner: {literal: %s}
          token: {expression: payload.token}
`, receiver.Path)
		writeClosedVariantFile(t, root, receiver.Path+"/schema.yaml", fmt.Sprintf(`name: %s
%spins:
  inputs:
    events:
%s  outputs:
    events: [receiver.finished]
`, receiver.Path, stages, pinInputs))
		writeClosedVariantFile(t, root, receiver.Path+"/entities.yaml", entities)
		writeClosedVariantFile(t, root, receiver.Path+"/events.yaml", seedSchema+closeEvent+"receiver.finished:\n  owner: text\n  token: text\n  swarm:\n    consumer: external\n")
		writeClosedVariantFile(t, root, receiver.Path+"/nodes.yaml", fmt.Sprintf(`collector:
  execution_type: system_node
  subscribes_to: [%s]
  event_handlers:
%s    work.ready:
%s`, inputs, seedHandler, body))
	}
	seedEmit := ""
	seedEvent := ""
	outputs := "work.requested"
	if seedEdges != "" {
		seedEvent = "receiver.seeded:\n  token: text\n"
		outputs += ", receiver.seeded"
		seedEmit = "      emit:\n        event: receiver.seeded\n        fields: {token: {expression: payload.token}}\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", `name: fork-receiver-ownership
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: start.seeded, source: external}
      - {event: start.requested, source: external}
      - {event: start.closed, source: external}
  outputs:
    events: [`+outputs+`]
connect:
`+rootEdges+seedEdges)
	writeClosedVariantFile(t, root, "entities.yaml", "root:\n  marker: text\n")
	writeClosedVariantFile(t, root, "events.yaml", "start.seeded:\n  token: text\nstart.requested:\n  token: text\nstart.closed: {}\n"+seedEvent+"work.requested:\n  token: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  execution_type: system_node
  subscribes_to: [start.seeded, start.requested, start.closed]
  event_handlers:
    start.seeded:
      advances_to: active
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'root-owned'"}
`+seedEmit+`    start.requested:
      emit:
        event: work.requested
        fields: {token: {expression: payload.token}}
    start.closed:
      advances_to: done
`)
	producerStages, producerBody := "", ""
	if !entitylessProducer {
		producerStages = "stages:\n  waiting: {initial: true}\n  active: {terminal: true}\n"
		producerBody = "      advances_to: active\n      data_accumulation:\n        writes:\n          - {target_field: marker, expression: \"'producer-owned'\"}\n"
		writeClosedVariantFile(t, root, "producer/entities.yaml", "work:\n  marker: text\n")
	}
	writeClosedVariantFile(t, root, "producer/schema.yaml", "name: producer\n"+producerStages+"pins:\n  inputs:\n    events: [work.requested]\n  outputs:\n    events: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n  token: text\n")
	writeClosedVariantFile(t, root, "producer/nodes.yaml", `producer:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
`+producerBody+`      emit:
        event: work.ready
        fields: {token: {expression: payload.token}}
`)
	return root
}
