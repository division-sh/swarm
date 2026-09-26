package canonicalrouting

import "testing"

// CopySelectionRetry owns the source for a stateful selection/CAS retry journey.
func CopySelectionRetry(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: selection-retry
stages:
  queued: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: seed, source: external}
      - {event: select, source: external}
`)
	writeClosedVariantFile(t, root, "entities.yaml", "work:\n  marker: text\n")
	writeClosedVariantFile(t, root, "events.yaml", `seed: {}
select: {}
selected:
  marker: text
ack:
  marker: text
  swarm: {consumer: external}
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `seed:
  execution_type: system_node
  subscribes_to: [seed]
  event_handlers:
    seed:
      create_entity: true
      data_accumulation:
        writes: [{target_field: marker, value: first}]
select:
  execution_type: system_node
  subscribes_to: [select]
  event_handlers:
    select:
      guard: {check: "has(entity.marker)"}
      rules:
        - id: first
          condition: "entity.marker == 'first'"
          emit: {event: selected, fields: {marker: {literal: first}}}
        - id: second
          condition: "entity.marker == 'second'"
          emit: {event: selected, fields: {marker: {literal: second}}}
      data_accumulation:
        writes: [{target_field: marker, expression: entity.marker}]
final:
  execution_type: system_node
  subscribes_to: [selected]
  event_handlers:
    selected:
      emit: {event: ack, fields: {marker: payload.marker}}
      advances_to: done
`)
	return root
}
