package canonicalrouting

import "testing"

// CopyCompleteObserverDependencies constructs two independent producer scopes
// feeding one dynamic observer for exact route-set replacement proofs.
func CopyCompleteObserverDependencies(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "name: complete-observer\nversion: '1.0.0'\nplatform_version: '>=0.7.0 <0.8.0'\n")
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  active: {initial: true}
  done: {terminal: true}
connect:
  - {event: work.ready, from: producer, to: observer, resolution: select}
  - {event: other.ready, from: other, to: observer, resolution: select}
`)
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
instance: item_id
stages:
  active: {initial: true}
  done: {terminal: true}
pins:
  outputs:
    events: [work.ready]
`, "work.ready:\n  item_id: text\n", "producer_state:\n  item_id: {type: text}\n", "")
	writeLegacyInstanceFlow(t, root, "other", `name: other
instance: item_id
stages:
  active: {initial: true}
  done: {terminal: true}
pins:
  outputs:
    events: [other.ready]
`, "other.ready:\n  item_id: text\n", "other_state:\n  item_id: {type: text}\n", "")
	writeLegacyInstanceFlow(t, root, "observer", `name: observer
instance: item_id
stages:
  active: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [work.ready, other.ready]
`, "", "observer_state:\n  item_id: {type: text}\n", `observe:
  execution_type: system_node
  subscribes_to: [work.ready, other.ready]
`)
	return root
}
