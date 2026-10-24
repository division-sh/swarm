package canonicalrouting

import "testing"

// CopySelectionRetry owns the source for a stateful selection/CAS retry journey.
func CopySelectionRetry(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: selection-retry
stages:
  queued: {}
  done: {final: true}
inputs:
  - seed
  - select
outputs:
  - ack
`)
	writeClosedVariantFile(t, root, "state.yaml", "marker: text\n")
	writeClosedVariantFile(t, root, "events.yaml", `seed:
select:
selected:
  marker: text
ack:
  marker: text
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `seed:
  seed:
    data_accumulation:
      writes: [{target_field: marker, value: "first"}]
select:
  select:
    guard: has(state.marker)
    rules:
      - when: state.marker == 'first'
        emit: {event: selected, fields: {marker: "first"}}
      - when: state.marker == 'second'
        emit: {event: selected, fields: {marker: "second"}}
    data_accumulation:
      writes: [{target_field: marker, value: state.marker}]
final:
  selected:
    emit: {event: ack, fields: {marker: payload.marker}}
    advances_to: done
`)
	return root
}
