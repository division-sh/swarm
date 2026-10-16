package canonicalrouting

import "testing"

// CopyCompleteObserverDependencies constructs two independent producer scopes
// feeding one dynamic observer for source-only compiled relation proofs.
func CopyCompleteObserverDependencies(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: '1.0.0'\nplatform_version: '>=0.7.0 <0.8.0'\n")
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  active: {}
  done: {final: true}
connect:
  - {event: work.ready, from: producer, to: observer, resolution: select}
  - {event: other.ready, from: other, to: observer, resolution: select}
`)
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
instance: item_id
stages:
  active: {}
  done: {final: true}
pins:
  outputs:
    - work.ready
`, "work.ready:\n  item_id: text\n", "producer_state:\n  item_id: {type: text}\n", "")
	writeLegacyInstanceFlow(t, root, "other", `name: other
instance: item_id
stages:
  active: {}
  done: {final: true}
pins:
  outputs:
    - other.ready
`, "other.ready:\n  item_id: text\n", "other_state:\n  item_id: {type: text}\n", "")
	writeLegacyInstanceFlow(t, root, "observer", `name: observer
instance: item_id
stages:
  active: {}
  done: {final: true}
pins:
  inputs:
    - work.ready
    - other.ready
`, "", "observer_state:\n  item_id: {type: text}\n", `observe:
  execution_type: system_node
  subscribes_to: [work.ready, other.ready]
`)
	return root
}

// CopyConstructibleObserverDependencies retains both observer edges and adds
// ordinary connected constructor inputs for genuine native setup.
func CopyConstructibleObserverDependencies(t testing.TB) string {
	t.Helper()
	root := CopyCompleteObserverDependencies(t)
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  active: {}
  done: {final: true}
pins:
  inputs: [producer.create, other.create, observer.create]
connect:
  - {event: work.ready, from: producer, to: observer, resolution: select}
  - {event: other.ready, from: other, to: observer, resolution: select}
  - {event: producer.create, from: '.', to: producer, rename: item.created, resolution: create}
  - {event: other.create, from: '.', to: other, rename: item.created, resolution: create}
  - {event: observer.create, from: '.', to: observer, rename: item.created, resolution: create}
`)
	writeClosedVariantFile(t, root, "events.yaml", `producer.create:
  item_id: text
other.create:
  item_id: text
observer.create:
  item_id: text
`)
	for _, flow := range []string{"producer", "other", "observer"} {
		inputs, outputs, local := "[item.created]", "", ""
		if flow == "observer" {
			inputs = "[item.created, work.ready, other.ready]"
		} else {
			local = "work.ready"
			if flow == "other" {
				local = "other.ready"
			}
			outputs = "  outputs: [" + local + "]\n"
		}
		schema := "name: " + flow + "\ninstance: item_id\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs: " + inputs + "\n" + outputs
		events := ""
		if local != "" {
			events += local + ":\n  item_id: text\n"
		}
		nodes := "constructor-receiver:\n  execution_type: system_node\n  subscribes_to: [item.created]\n  event_handlers:\n    item.created:\n      guard: {id: constructed, check: true}\n"
		if flow == "observer" {
			nodes += "observe:\n  execution_type: system_node\n  subscribes_to: [work.ready, other.ready]\n  event_handlers:\n    work.ready:\n      guard: {id: observed, check: true}\n    other.ready:\n      guard: {id: observed, check: true}\n"
		}
		writeLegacyInstanceFlow(t, root, flow, schema, events, flow+"_state:\n  item_id: {type: text}\n", nodes)
	}
	return root
}

// CopyExecutableObserverDependencies drives the same connected observer through
// ordinary public constructor/selection inputs and real node emissions.
func CopyExecutableObserverDependencies(t testing.TB) string {
	t.Helper()
	root := CopyConstructibleObserverDependencies(t)
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  active: {}
pins:
  inputs: [producer.create, other.create, observer.create, producer.fire, other.fire, observer.stop]
connect:
  - {event: work.ready, from: producer, to: observer, resolution: select}
  - {event: other.ready, from: other, to: observer, resolution: select}
  - {event: producer.create, from: '.', to: producer, rename: item.created, resolution: create}
  - {event: other.create, from: '.', to: other, rename: item.created, resolution: create}
  - {event: observer.create, from: '.', to: observer, rename: item.created, resolution: create}
  - {event: producer.fire, from: '.', to: producer, rename: item.fired, resolution: select}
  - {event: other.fire, from: '.', to: other, rename: item.fired, resolution: select}
  - {event: observer.stop, from: '.', to: observer, rename: stop, resolution: select}
  - {event: observation.logged, from: observer, to: '.'}
`)
	writeClosedVariantFile(t, root, "events.yaml", `producer.create:
  item_id: text
other.create:
  item_id: text
observer.create:
  item_id: text
producer.fire:
  item_id: text
  observer_id: text
  marker: text
other.fire:
  item_id: text
  observer_id: text
  marker: text
observer.stop:
  item_id: text
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `observations:
  execution_type: system_node
  subscribes_to: [observation.logged]
  event_handlers:
    observation.logged:
      guard: {id: recorded, check: true}
`)
	for _, flow := range []string{"producer", "other", "observer"} {
		inputs, output, local := "[item.created, item.fired]", "work.ready", ""
		stages := "stages:\n  active: {}\n"
		if flow == "other" {
			output = "other.ready"
		}
		nodes := "constructor-receiver:\n  execution_type: system_node\n  subscribes_to: [item.created]\n  event_handlers:\n    item.created:\n      guard: {id: constructed, check: true}\n"
		if flow == "observer" {
			inputs, output = "[item.created, work.ready, other.ready, stop]", "observation.logged"
			stages += "  done: {final: true}\n"
			nodes += "observe:\n  execution_type: system_node\n  subscribes_to: [work.ready, other.ready]\n  event_handlers:\n"
			for _, event := range []string{"work.ready", "other.ready"} {
				nodes += "    " + event + ":\n      emit: {event: observation.logged, fields: {item_id: payload.item_id, marker: payload.marker}}\n"
			}
			nodes += "stop:\n  execution_type: system_node\n  subscribes_to: [stop]\n  event_handlers:\n    stop:\n      advances_to: done\n"
		} else {
			local = "publisher:\n  execution_type: system_node\n  subscribes_to: [item.fired]\n  event_handlers:\n    item.fired:\n      emit: {event: " + output + ", fields: {item_id: payload.observer_id, marker: payload.marker}}\n"
		}
		schema := "name: " + flow + "\ninstance: item_id\n" + stages + "pins:\n  inputs: " + inputs + "\n  outputs: [" + output + "]\n"
		events := output + ":\n  item_id: text\n  marker: text\n"
		writeLegacyInstanceFlow(t, root, flow, schema, events, flow+"_state:\n  item_id: {type: text}\n", nodes+local)
	}
	return root
}
