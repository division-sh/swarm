package canonicalrouting

// Closed parser fixtures exercise schema families, not a runnable topology.
const SchemaAdmissionCompleteRoot = `name: Complete
activation: standing
instance: work_id
ingress:
  alias: hooks
  providers:
    - provider: custom
      signing_secret: TOKEN
      admission:
        kind: raw
        authentication: {kind: token, header: X-Token, prefix: ''}
        event: work.requested
        delivery_id: {source: body_sha256}
        payload: json
imports:
  connector_packs: [{provider: telegram, tool: telegram.send_message}]
  provider_trigger_events: [{provider: telegram, event: inbound.telegram.text_message}]
connect: [{event: work.requested, from: source, to: worker, rename: work.received, resolution: create, key_from: event.id}]
pins:
  inputs:
    - event: work.requested
      initialize: {note: payload.note}
  outputs: [work.completed]
required_agents: [{role: worker, subscribes_to: [], emits: [work.completed], description: ''}]
instance_variables:
  description: Configuration
  variables:
    note: {type: text, default: '', length: {min: 0}}
auto_emit_on_create: {event: work.started}
stages:
  waiting:
    initial: true
    terminal: false
    timers: [{after: 1s, emit: work.expired}]
    gate:
      decision: approval
      context: {null_value: null, zero: 0, dynamic: payload.note}
      outcomes:
        approve:
          advances_to: done
          input: {comment: {type: text, required: false, label: ''}}
          emit: {event: work.completed, fields: {record: {note: payload.note, preserved: null}}}
  done: {terminal: true}
loops:
  revision:
    revision_field: revision
    max_attempts: 10
    escape: {advances_to: done, emit: work.expired}
`

const SchemaAdmissionAliasProvenance = `name: &label Example
stages:
  waiting:
    description: *label
    initial: true
    timers: [{after: 1s, emit: work.expired}]
  done: {terminal: true}
pins:
  inputs: [work.requested]
  outputs: [work.completed]
`

const (
	SchemaConnectParserEnvelope    = "connect:\n  -"
	SchemaPinsParserEnvelope       = "pins:"
	SchemaInputPinsParserEnvelope  = "pins:\n  inputs:"
	SchemaOutputPinsParserEnvelope = "pins:\n  outputs:"
)
