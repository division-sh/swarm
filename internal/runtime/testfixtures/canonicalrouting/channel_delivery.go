package canonicalrouting

import "testing"

// CopyChannelLearnedObjectJourney owns the finite routing source for the
// learned-provider delivery proof; provider pack declarations remain separate.
func CopyChannelLearnedObjectJourney(t testing.TB) string {
	return copyChannelLearnedObjectInputJourney(t, "text")
}

func CopyChannelLearnedObjectIntegerInputJourney(t testing.TB) string {
	return copyChannelLearnedObjectInputJourney(t, "integer")
}

func copyChannelLearnedObjectInputJourney(t testing.TB, inputType string) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: object-channel
pins:
  inputs: {events: [work.requested]}
  outputs: {events: [work.requested]}
connect:
  - {event: work.requested, from: ., to: reviews}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  detail: text\n")
	writeClosedVariantFile(t, root, "reviews/schema.yaml", `name: reviews
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: review_decision
      context: {detail: '${entity.detail}'}
      outcomes:
        approve: {advances_to: done}
        reject:
          input: {reason: {type: `+inputType+`, required: true}}
          advances_to: done
  done: {terminal: true}
pins:
  inputs: {events: [work.requested]}
`)
	writeClosedVariantFile(t, root, "reviews/entities.yaml", "work:\n  detail: text\n")
	writeClosedVariantFile(t, root, "reviews/nodes.yaml", `requester:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      create_entity: true
      data_accumulation:
        writes:
          - {target_field: detail, value: '${payload.detail}'}
      advances_to: review
`)
	writeClosedVariantFile(t, root, "ingress/schema.yaml", `name: ingress
mode: singleton
activation: standing
stages:
  active: {initial: true, gate: {decision: retire_service, outcomes: {retire: {advances_to: done}}}}
  done: {terminal: true}
pins:
  inputs: {events: [inbound.mock]}
ingress:
  alias: objects
  providers: [{provider: mock, signing_secret: webhook_signing.mock}]
`)
	writeClosedVariantFile(t, root, "ingress/entities.yaml", "object_service: {}\n")
	return root
}
