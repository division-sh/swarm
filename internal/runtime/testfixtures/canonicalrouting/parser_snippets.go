package canonicalrouting

import "testing"

type ConnectionAdmissionCase struct {
	Name      string
	Source    string
	Mode      string
	WantError string
}

// ConnectionAdmissionCases bounds parser proof to one document, never a bundle.
func ConnectionAdmissionCases() []ConnectionAdmissionCase {
	var out []ConnectionAdmissionCase
	for _, mode := range []string{"create", "select", "select-or-create"} {
		out = append(out,
			ConnectionAdmissionCase{Name: "edge/" + mode, Source: "connect: [{event: work.requested, from: ., to: worker, resolution: " + mode + "}]\n", Mode: mode},
			ConnectionAdmissionCase{Name: "retired-pin/" + mode, Source: "pins: {inputs: {events: [{event: work.requested, resolution: {mode: " + mode + "}}]}}\n", WantError: "each connect row"},
		)
	}
	for _, choice := range []string{
		"resolution: null", "resolution: ''", "resolution: {}", "resolution: {mode: create}",
		"resolution: fan-in", "resolution: reply", "resolution: fan-out", "resolution: unknown",
		"key_from: payload.case_id", "resolution: select, key_from: generated.uuid",
		"resolution: create, key_from: null", "resolution: create, key_from: ''",
		"resolution: create, key_from: payload.case.id", "resolution: create, arbitrary: true",
	} {
		out = append(out, ConnectionAdmissionCase{Name: choice, Source: "connect: [{event: work.requested, from: ., to: worker, " + choice + "}]\n", WantError: "*"})
	}
	for name, source := range map[string]string{
		"scalar":     "work.requested",
		"reply":      "{event: work.requested, resolution: {mode: reply, replies_to: work.sent}}",
		"initialize": "{event: work.requested, initialize: {priority: payload.priority}}",
	} {
		out = append(out, ConnectionAdmissionCase{Name: "retained/" + name, Source: "pins: {inputs: {events: [" + source + "]}}\n"})
	}
	// These are parser-only rejection specimens, never positive fixtures.
	for _, retired := range []string{
		"{mode: fan-in}",
		"{mode: fan-in, aggregation: stream, window: payload.period_id, dedup_by: [payload.operating_id], singleton: portfolio}",
		"{mode: fan-in, aggregation: barrier, window: payload.period_id, dedup_by: [payload.operating_id], singleton: portfolio}",
	} {
		out = append(out, ConnectionAdmissionCase{Name: "retired-pin/" + retired, Source: "pins: {inputs: {events: [{event: work.requested, resolution: " + retired + "}]}}\n", WantError: "*"})
	}
	return out
}

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
	return NewParserSnippet(t, `stages:
  pending: {initial: true}
pins:
  inputs:
    events:
      - event: check.requested
        source: harness
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

func ReceiverInitializeParserSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, "events:\n  - event: work.requested\n    initialize: {count: payload.settings.count}\n")
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

func InputPinResolutionModesSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
name: resolution-pins
connect:
  - {event: validation.requested, from: ., to: worker, resolution: create, key_from: generated.uuid}
  - {event: account.selected, from: ., to: worker, resolution: select}
  - {event: account.requested, from: ., to: worker, resolution: select-or-create, key_from: payload.external_account_id}
pins:
  inputs:
    events:
      - validation.requested
      - account.selected
      - account.requested
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
