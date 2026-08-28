package canonicalrouting

import "testing"

// UnsupportedResolutionSnippet identifies one closed parser-only failure shape.
type UnsupportedResolutionSnippet string

const (
	UnsupportedResolutionField  UnsupportedResolutionSnippet = "resolution"
	UnsupportedInstanceKeyField UnsupportedResolutionSnippet = "instance-key"
	UnsupportedResolutionCarry  UnsupportedResolutionSnippet = "carry"
	RetiredInstanceKeyCarry     UnsupportedResolutionSnippet = "retired-instance-key-carry"
)

// EventMetadataSnippet identifies one closed event-metadata parser shape.
type EventMetadataSnippet string

const (
	CanonicalExternalEventMetadata EventMetadataSnippet = "canonical-external"
	RetiredExternalEventMetadata   EventMetadataSnippet = "retired-external"
	ConflictingEventMetadata       EventMetadataSnippet = "conflicting"
)

// InputPinSourceSnippet identifies one closed source-enum parser specimen.
type InputPinSourceSnippet string

const (
	InputPinSourceDefault  InputPinSourceSnippet = "default"
	InputPinSourceExternal InputPinSourceSnippet = "external"
	InputPinSourceHarness  InputPinSourceSnippet = "harness"
	InputPinSourceInvalid  InputPinSourceSnippet = "invalid"
)

// W2MappingKeySnippet identifies one closed byte-exact mapping-key failure.
type W2MappingKeySnippet string

const (
	W2FlowPinsSurroundingKey       W2MappingKeySnippet = "flow-pins-surrounding-key"
	W2FlowPinsBlankKey             W2MappingKeySnippet = "flow-pins-blank-key"
	W2InputDirectionSurroundingKey W2MappingKeySnippet = "input-direction-surrounding-key"
	W2InputDirectionBlankKey       W2MappingKeySnippet = "input-direction-blank-key"
	W2InputEventSurroundingKey     W2MappingKeySnippet = "input-event-surrounding-key"
	W2InputEventBlankKey           W2MappingKeySnippet = "input-event-blank-key"
	W2OutputEventSurroundingKey    W2MappingKeySnippet = "output-event-surrounding-key"
	W2OutputEventBlankKey          W2MappingKeySnippet = "output-event-blank-key"
	W2ResolutionSurroundingKey     W2MappingKeySnippet = "resolution-surrounding-key"
	W2ResolutionBlankKey           W2MappingKeySnippet = "resolution-blank-key"
	W2ConnectSurroundingKey        W2MappingKeySnippet = "connect-surrounding-key"
	W2ConnectBlankKey              W2MappingKeySnippet = "connect-blank-key"
)

// RetiredReceiverRoutingSnippet identifies one non-materializing old-form
// parser specimen. These sources cannot create a complete fixture bundle.
type RetiredReceiverRoutingSnippet string

const (
	RetiredInputAddressEmpty             RetiredReceiverRoutingSnippet = "input-address-empty"
	RetiredInputAddressMalformed         RetiredReceiverRoutingSnippet = "input-address-malformed"
	RetiredInputAddressPopulated         RetiredReceiverRoutingSnippet = "input-address-populated"
	RetiredInputAddressMixed             RetiredReceiverRoutingSnippet = "input-address-mixed"
	RetiredInputAddressUnsupportedNested RetiredReceiverRoutingSnippet = "input-address-unsupported-nested"
	RetiredConnectMapEmpty               RetiredReceiverRoutingSnippet = "connect-map-empty"
	RetiredConnectMapMalformed           RetiredReceiverRoutingSnippet = "connect-map-malformed"
	RetiredConnectMapPopulated           RetiredReceiverRoutingSnippet = "connect-map-populated"
	RetiredConnectMapMixed               RetiredReceiverRoutingSnippet = "connect-map-mixed"
	RetiredConnectUsingEmpty             RetiredReceiverRoutingSnippet = "connect-using-empty"
	RetiredConnectUsingMalformed         RetiredReceiverRoutingSnippet = "connect-using-malformed"
	RetiredConnectUsingPopulated         RetiredReceiverRoutingSnippet = "connect-using-populated"
	RetiredConnectUsingComposite         RetiredReceiverRoutingSnippet = "connect-using-composite"
	RetiredConnectUsingMixed             RetiredReceiverRoutingSnippet = "connect-using-mixed"
)

func PackageConnectSourceSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, "name: test\nversion: 1.0.0\nconnect:\n  - event: work.done\n    from: producer\n    to: consumer\n")
}

func InputPinSourceParserSnippet(t testing.TB, id InputPinSourceSnippet) ParserSnippet {
	t.Helper()
	var source string
	switch id {
	case InputPinSourceDefault:
		source = "name: source-enum\npins:\n  inputs:\n    events:\n      - work.requested\n"
	case InputPinSourceExternal:
		source = "name: source-enum\npins:\n  inputs:\n    events:\n      - event: work.requested\n        source: external\n"
	case InputPinSourceHarness:
		source = "name: source-enum\npins:\n  inputs:\n    events:\n      - event: work.requested\n        source: harness\n"
	case InputPinSourceInvalid:
		source = "name: source-enum\npins:\n  inputs:\n    events:\n      - event: work.requested\n        source: fallback\n"
	default:
		t.Fatalf("unsupported input pin source parser snippet %q", id)
	}
	return NewParserSnippet(t, source)
}

func W2OptionPinsParserSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
states:
  - pending
initial_state: pending
terminal_states: []
pins:
  inputs:
    events:
      - event: check.requested
        source: external
    reads:
      - entity.score
  outputs:
    events:
      - event: check.passed
        sink: harness
    writes:
      - entity.status
`)
}

func W2EmptyResolutionParserSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, "pins:\n  inputs:\n    events:\n      - event: work.requested\n        resolution: {}\n")
}

func W2MappingKeyParserSnippet(t testing.TB, id W2MappingKeySnippet) ParserSnippet {
	t.Helper()
	var source string
	switch id {
	case W2FlowPinsSurroundingKey:
		source = "pins:\n  \" inputs \":\n    events: [work.requested]\n"
	case W2FlowPinsBlankKey:
		source = "pins:\n  \" \": {}\n"
	case W2InputDirectionSurroundingKey:
		source = "pins:\n  inputs:\n    \" events \": [work.requested]\n"
	case W2InputDirectionBlankKey:
		source = "pins:\n  inputs:\n    \" \": [work.requested]\n"
	case W2InputEventSurroundingKey:
		source = "pins:\n  inputs:\n    events:\n      - \" event \": work.requested\n        source: external\n"
	case W2InputEventBlankKey:
		source = "pins:\n  inputs:\n    events:\n      - \" \": ignored\n        event: work.requested\n        source: external\n"
	case W2OutputEventSurroundingKey:
		source = "pins:\n  outputs:\n    events:\n      - event: work.completed\n        \" sink \": harness\n"
	case W2OutputEventBlankKey:
		source = "pins:\n  outputs:\n    events:\n      - \" \": ignored\n        event: work.completed\n        sink: harness\n"
	case W2ResolutionSurroundingKey:
		source = "pins:\n  inputs:\n    events:\n      - event: work.requested\n        resolution:\n          \" mode \": create\n"
	case W2ResolutionBlankKey:
		source = "pins:\n  inputs:\n    events:\n      - event: work.requested\n        resolution:\n          \" \": ignored\n          mode: create\n"
	case W2ConnectSurroundingKey:
		source = "name: hostile-connect\nconnect:\n  - \" event \": work.requested\n    from: producer\n    to: consumer\n"
	case W2ConnectBlankKey:
		source = "name: hostile-connect\nconnect:\n  - \" \": ignored\n    event: work.requested\n    from: producer\n    to: consumer\n"
	default:
		t.Fatalf("unsupported W2 mapping-key parser snippet %q", id)
	}
	return NewParserSnippet(t, source)
}

func PackageRequiresBindConnectSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
name: package-boundary
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
  policy: [provider.threshold]
  credentials: [provider_token]
  platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: worker
    flow: worker
    bind:
      inputs:
        work.requested: parent.work_requested
      outputs:
        work.completed: parent.work_completed
      policy:
        provider.threshold: parent.policy.threshold
      credentials:
        provider_token: parent_provider_token
packages:
  - path: packages/child
    bind:
      inputs:
        child.requested: parent.child_requested
      outputs:
        child.completed: parent.child_completed
      policy:
        child.policy: parent.policy.child
      credentials:
        child_token: parent_child_token
connect:
  - event: parent.work_completed
    from: worker
    to: worker
    rename: parent.work_requested
`)
}

func InvalidPackageConnectFieldSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
name: invalid
connect:
  - event: work.ready
    from: producer
    to: consumer
    topic: unsupported
`)
}

func InputPinResolutionModesSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
name: resolution-pins
pins:
  inputs:
    events:
      - event: validation.requested
        resolution:
          mode: create
          from: generated.uuid
      - event: account.selected
        resolution:
          mode: select
      - event: account.requested
        resolution:
          mode: select-or-create
          from: payload.external_account_id
      - event: report.ready
        resolution:
          mode: fan-in
          aggregation: stream
          window: report_period
          dedup_by: [event.id, payload.operating_id]
          singleton: portfolio/default
      - event: operating.requested
        resolution:
          mode: fan-out
      - event: provider.replied
        resolution:
          mode: reply
          replies_to: provider.requested
          correlation_key: payload.provider_request_id
`)
}

func UnsupportedInputPinResolutionSnippet(t testing.TB, id UnsupportedResolutionSnippet) ParserSnippet {
	t.Helper()
	var source string
	switch id {
	case UnsupportedResolutionField:
		source = `
name: invalid-resolution
pins:
  inputs:
    events:
      - event: work.requested
        resolution:
          mode: create
          unsupported: true
`
	case UnsupportedInstanceKeyField:
		source = `
name: invalid-resolution-instance-key
pins:
  inputs:
    events:
      - event: work.requested
        resolution:
          mode: create
          instance_key:
            mint: uuid
            as: work_id
            unsupported: true
`
	case UnsupportedResolutionCarry:
		source = `
name: invalid-resolution-carries
pins:
  inputs:
    events:
      - event: work.requested
        carries:
          work_id:
            from: payload.work_id
            unsupported: true
`
	case RetiredInstanceKeyCarry:
		source = `
name: retired-instance-key-source
mode: template
instance: work_id
pins:
  inputs:
    events:
      - event: work.requested
        resolution:
          mode: create
        carries:
          work_id:
            from: instance.key.work_id
            type: uuid
`
	default:
		t.Fatalf("unsupported resolution parser snippet %q", id)
	}
	return NewParserSnippet(t, source)
}

func RetiredReceiverRoutingParserSnippet(t testing.TB, id RetiredReceiverRoutingSnippet) ParserSnippet {
	t.Helper()
	var source string
	switch id {
	case RetiredInputAddressEmpty:
		source = "name: retired-address\npins: {inputs: {events: [{event: work.requested, address: {}}]}}\n"
	case RetiredInputAddressMalformed:
		source = "name: retired-address\npins: {inputs: {events: [{event: work.requested, address: unsupported}]}}\n"
	case RetiredInputAddressPopulated:
		source = "name: retired-address\npins: {inputs: {events: [{event: work.requested, address: {by: work_id, source: payload.work_id, target: entity.work_id}}]}}\n"
	case RetiredInputAddressMixed:
		source = "name: retired-address\npins: {inputs: {events: [{event: work.requested, address: {by: work_id, resolution: {mode: select}}}]}}\n"
	case RetiredInputAddressUnsupportedNested:
		source = "name: retired-address\npins: {inputs: {events: [{event: work.requested, address: {by: work_id, unsupported: nope}}]}}\n"
	case RetiredConnectMapEmpty:
		source = "name: retired-map\nconnect: [{event: work.done, from: producer, to: consumer, map: {}}]\n"
	case RetiredConnectMapMalformed:
		source = "name: retired-map\nconnect: [{event: work.done, from: producer, to: consumer, map: unsupported}]\n"
	case RetiredConnectMapPopulated:
		source = "name: retired-map\nconnect: [{event: work.done, from: producer, to: consumer, map: {work_id: {source: payload.work_id, target: entity.work_id}}}]\n"
	case RetiredConnectMapMixed:
		source = "name: retired-map\nconnect: [{event: work.done, from: producer, to: consumer, map: {work_id: {source: payload.work_id}, resolution: {mode: select}}}]\n"
	case RetiredConnectUsingEmpty:
		source = "name: retired-using\nconnect: [{event: work.done, from: producer, to: consumer, using: {}}]\n"
	case RetiredConnectUsingMalformed:
		source = "name: retired-using\nconnect: [{event: work.done, from: producer, to: consumer, using: unsupported}]\n"
	case RetiredConnectUsingPopulated:
		source = "name: retired-using\nconnect: [{event: work.done, from: producer, to: consumer, using: {instance: {source: payload.account_id, target: account_id}}}]\n"
	case RetiredConnectUsingComposite:
		source = "name: retired-using\nconnect: [{event: work.done, from: producer, to: consumer, using: {instance: {source: [payload.scope, payload.account_id], target: [scope, account_id]}}}]\n"
	case RetiredConnectUsingMixed:
		source = "name: retired-using\nconnect: [{event: work.done, from: producer, to: consumer, using: {instance: {source: payload.account_id}, map: {account_id: payload.account_id}}}]\n"
	default:
		t.Fatalf("unsupported retired receiver routing parser snippet %q", id)
	}
	return NewParserSnippet(t, source)
}

func EventCatalogMetadataParserSnippet(t testing.TB, id EventMetadataSnippet) ParserSnippet {
	t.Helper()
	var source string
	switch id {
	case CanonicalExternalEventMetadata:
		source = `
swarm:
  source: external (human board interface)
  producer: mailbox_human
  consumer: mailbox_system (external UI, not agent-subscribed)
  status: planned
  note: Human board handoff
consumer_type: external_ui
entity_id: string
`
	case RetiredExternalEventMetadata:
		source = `
_source: external (human board interface)
_producer: mailbox_human
_consumer: mailbox_system (external UI, not agent-subscribed)
_consumer_type: external_ui
_status: planned
_note: Human board handoff
source: text
`
	case ConflictingEventMetadata:
		source = `
swarm:
  source: external (operator)
_source: platform (timer)
entity_id: string
`
	default:
		t.Fatalf("unsupported event metadata parser snippet %q", id)
	}
	return NewParserSnippet(t, source)
}
