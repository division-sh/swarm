package canonicalrouting

import "testing"

// UnsupportedResolutionSnippet identifies one closed parser-only failure shape.
type UnsupportedResolutionSnippet string

const (
	UnsupportedResolutionField  UnsupportedResolutionSnippet = "resolution"
	UnsupportedInstanceKeyField UnsupportedResolutionSnippet = "instance-key"
	UnsupportedResolutionCarry  UnsupportedResolutionSnippet = "carry"
)

// EventMetadataSnippet identifies one closed event-metadata parser shape.
type EventMetadataSnippet string

const (
	CanonicalExternalEventMetadata EventMetadataSnippet = "canonical-external"
	RetiredExternalEventMetadata   EventMetadataSnippet = "retired-external"
	ConflictingEventMetadata       EventMetadataSnippet = "conflicting"
)

func PackageConnectSourceSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, "name: test\nversion: 1.0.0\nconnect:\n  - from: producer.done\n    to: consumer.done\n")
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
  - from: worker.work.completed
    to: worker.work.requested
    delivery: one
    map:
      work_id:
        source: payload.work_id
        target: entity.work_id
`)
}

func InvalidPackageConnectFieldSnippet(t testing.TB) ParserSnippet {
	t.Helper()
	return NewParserSnippet(t, `
name: invalid
connect:
  - from: producer.ready
    to: consumer.ready
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
      - name: create_requested
        event: validation.requested
        resolution:
          mode: create
          instance_key:
            mint: uuid
            as: validation_case_id
        carries:
          validation_case_id:
            from: instance.key.validation_case_id
            type: uuid
      - name: select_requested
        event: account.selected
        resolution:
          mode: select
          instance_key: account_id
      - name: select_or_create_requested
        event: account.requested
        resolution:
          mode: select-or-create
          instance_key:
            from: payload.account_id
      - name: fan_in_requested
        event: report.ready
        resolution:
          mode: fan-in
          aggregation: stream
          window: report_period
          dedup_by: [event.id, payload.operating_id]
          singleton: portfolio/default
      - name: fan_out_requested
        event: operating.requested
        resolution:
          mode: fan-out
          instance_key: operating_id
      - name: reply_received
        event: provider.replied
        resolution:
          mode: reply
          replies_to: provider_requested
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
      - name: requested
        event: work.requested
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
      - name: requested
        event: work.requested
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
      - name: requested
        event: work.requested
        carries:
          work_id:
            from: instance.key.work_id
            unsupported: true
`
	default:
		t.Fatalf("unsupported resolution parser snippet %q", id)
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
