package canonicalrouting

import "testing"

// CopyForkDeliveryRouteEvidence preserves the ordinary connected source used
// to reproduce loss of route authority during historical fork planning.
func CopyForkDeliveryRouteEvidence(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "schema.yaml", `name: parent-seed-probe
stages:
  waiting: {initial: true}
  active: {terminal: true}
pins:
  inputs:
    events:
      - {event: parent.seeded, source: external}
  outputs:
    events: [work.requested]
connect:
  - {event: work.requested, from: ., to: producer}
  - {event: work.ready, from: producer, to: consumer}
`)
	writeClosedVariantFile(t, root, "events.yaml", "parent.seeded:\n  work_id: text\nwork.requested:\n  work_id: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "seed: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `parent-node:
  id: parent-node
  execution_type: system_node
  subscribes_to: [parent.seeded]
  event_handlers:
    parent.seeded:
      advances_to: active
      emit:
        event: work.requested
        fields:
          work_id: payload.work_id
`)
	writeClosedVariantFile(t, root, "producer/schema.yaml", `name: producer
stages:
  waiting: {initial: true}
  active: {terminal: true}
pins:
  inputs:
    events:
      - work.requested
  outputs:
    events: [work.ready]
`)
	writeClosedVariantFile(t, root, "producer/entities.yaml", "work:\n  work_id: text\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n  work_id: text\n")
	writeClosedVariantFile(t, root, "producer/nodes.yaml", `producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      advances_to: active
      data_accumulation:
        writes:
          - target_field: work_id
            expression: payload.work_id
      emit:
        event: work.ready
        fields:
          work_id: payload.work_id
`)
	return root
}
