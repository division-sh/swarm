package canonicalrouting

// Closed parser fixtures exercise schema families, not a runnable topology.
const SchemaAdmissionCompleteRoot = `instance: work_id
connect: [{event: work.requested, from: source, to: worker, rename: work.received, resolution: create, key_from: event.id}]
inputs:
  - work.requested
outputs: [work.completed]
auto_emit_on_create: {event: work.started}
stages:
  waiting:
    final: false
    timers: [{after: 1s, emit: work.expired}]
    gate:
      decision: approval
      context: {null_value: null, zero: 0, dynamic: payload.note}
      outcomes:
        approve:
          advances_to: done
          input: {comment: {type: text, required: false}}
          emit: {event: work.completed, fields: {record: {note: payload.note, preserved: null}}}
  done: {final: true}
loops:
  revision:
    revision_field: revision
    max_attempts: 10
    escape: {advances_to: done, emit: work.expired}
`

const SchemaAdmissionAliasProvenance = `auto_emit_on_create: {event: fixture.started, description: &label Example}
stages:
  waiting:
    description: *label
    timers: [{after: 1s, emit: work.expired}]
  done: {final: true}
inputs: [work.requested]
outputs: [work.completed]
`

const (
	SchemaConnectParserEnvelope    = "connect:\n  -"
	SchemaInputPinsParserEnvelope  = "inputs:"
	SchemaOutputPinsParserEnvelope = "outputs:"
)

const SchemaProviderReferenceAdmission = `inputs: [inbound.telegram.message]
outputs: [inbound.telegram.message]
connect:
  - {event: inbound.telegram.message, from: ., to: child, resolution: select}
`
