package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyChannelLearnedObjectJourney owns the finite routing source for the
// learned-provider delivery proof; provider pack declarations remain separate.
func CopyChannelLearnedObjectJourney(t testing.TB) string {
	return copyChannelLearnedObjectInputJourney(t, "text")
}

func CopyChannelLearnedObjectIntegerInputJourney(t testing.TB) string {
	return copyChannelLearnedObjectInputJourney(t, "integer")
}

func CopyChannelLearnedObjectOptionalInputJourney(t testing.TB, partial bool) string {
	t.Helper()
	root := CopyChannelLearnedObjectJourney(t)
	input := "input: {reason: {type: text, required: false}}"
	if partial {
		input = "input:\n            reason: {type: text, required: false}\n            later: {type: text, required: true}"
	}
	applyClosedReplacement(t, filepath.Join(root, "reviews/schema.yaml"),
		"input: {reason: {type: text, required: true}}", input)
	return root
}

// CopyChannelLearnedObjectAnchorJourney retains the real gate, ask_human and
// approved connector producers, using the independently declared mock protocol.
func CopyChannelLearnedObjectAnchorJourney(t testing.TB) string {
	t.Helper()
	root := CopyChannelLearnedObjectJourney(t)
	mailbox := CopyMailboxCompletionMatrix(t)
	copyTree(t, mailbox, filepath.Join(root, "reviews"))
	copyTree(t, filepath.Join(mailbox, "observers"), filepath.Join(root, "observers"))
	removeClosedVariantFiles(t, root, "reviews/observers/mocks/observer.py", "reviews/observers/mocks", "reviews/observers/agents.yaml", "reviews/observers/entities.yaml", "reviews/observers/nodes.yaml", "reviews/observers/schema.yaml", "reviews/observers")
	applyClosedReplacement(t, filepath.Join(root, "reviews/schema.yaml"), "provider: telegram\n      tool: telegram.send_message", "provider: mock\n      tool: mock.deliver")
	applyClosedReplacement(t, filepath.Join(root, "reviews/schema.yaml"), "      - observer.requested\n  outputs:\n    events: [observer.requested]\nconnect:\n  - {event: observer.requested, from: ., to: observers}\n", "")
	applyClosedReplacement(t, filepath.Join(root, "reviews/nodes.yaml"), "tool: telegram.send_message", "tool: mock.deliver")
	applyClosedReplacement(t, filepath.Join(root, "reviews/nodes.yaml"), "          chat_id: {literal: \"42\"}\n          text: {literal: review}", "          queue: {literal: queue-b}\n          body: {literal: approved-effect}\n          controls: {literal: []}")
	writeClosedVariantFile(t, root, "reviews/events.yaml", "work.completed:\n  result: text\n")
	writeClosedVariantFile(t, root, "schema.yaml", `name: object-channel
pins:
  inputs: {events: [work.requested, observer.requested, effect.requested]}
  outputs: {events: [work.requested, observer.requested, effect.requested]}
connect:
  - {event: work.requested, from: ., to: reviews}
  - {event: effect.requested, from: ., to: reviews}
  - {event: observer.requested, from: ., to: observers}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\nobserver.requested:\n  seed: boolean\n  deadline_at: text?\neffect.requested:\n  seed: boolean\n")
	return root
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
