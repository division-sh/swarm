package canonicalrouting

import "testing"

// CopyForkDeliveryRouteEvidence preserves the ordinary connected source used
// to reproduce loss of route authority during historical fork planning.
func CopyForkDeliveryRouteEvidence(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	writeClosedVariantFile(t, root, "schema.yaml", `name: parent-seed-probe
stages:
  waiting: {}
  active: {final: true}
inputs:
  - parent.seeded
outputs:
  - work.requested
connect:
  - {event: work.requested, from: ., to: producer}
  - {event: work.ready, from: producer, to: consumer}
`)
	writeClosedVariantFile(t, root, "events.yaml", "parent.seeded:\n  work_id: text\nwork.requested:\n  work_id: text\n")
	writeClosedVariantFile(t, root, "state.yaml", "")
	writeClosedVariantFile(t, root, "nodes.yaml", `parent-node:
  parent.seeded:
    advances_to: active
    emit:
      event: work.requested
      fields:
        work_id: payload.work_id
`)
	writeClosedVariantFile(t, root, "producer/schema.yaml", `name: producer
stages:
  waiting: {}
  active: {final: true}
inputs:
  - work.requested
outputs:
  - work.ready
`)
	writeClosedVariantFile(t, root, "producer/state.yaml", "work_id: text\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n  work_id: text\n")
	writeClosedVariantFile(t, root, "producer/nodes.yaml", `producer-node:
  work.requested:
    advances_to: active
    data_accumulation:
      writes:
        - target_field: work_id
          value: payload.work_id
    emit:
      event: work.ready
      fields:
        work_id: payload.work_id
`)
	return root
}
