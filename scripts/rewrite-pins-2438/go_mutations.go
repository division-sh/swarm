package main

import (
	"path/filepath"
	"strings"
)

// These literals are closed fixture mutations, not complete YAML documents.
// Their before/after spellings must move together with the sources they edit.
var pinMutationEvents = map[string][]string{
	"notify_all_children.go":                {"account.notify.requested", "account.created", "account.registered", "portfolio.notify.requested", "portfolio.notify.completed"},
	"notify_all_children_nested_serving.go": {"portfolio.notify.requested", "account.tasks.completed", "account.task.completed"},
	"served_variants.go":                    {"item.received", "item.processed", "external.observed", "fork.source_message", "item.agent_hold", "opco.spinup_requested", "opco.product_review_requested"},
	"fan_in.go":                             {"operating.reported", "event: operating.reported"},
	"fork_loop_retained_join.go":            {"work.requested", "checkpoint.requested"},
	"mailbox_completion.go":                 {"work.requested", "effect.requested", "observer.requested", "notice.requested", "observer.seed"},
	"parent_connect.go":                     {"work.ready", "deploy.done", "deploy.completed"},
	"receiver_composition.go":               {"work.requested", "child.closed"},
	"selected_fork_pending_input.go":        {"work.first", "work.marked"},
	"fork_receiver_ownership.go":            {"start.closed", "receiver.closed", "receiver.close.requested", "producer.closed", "%s.finished"},
	"lifecycle_emitter_followthrough.go":    {"work.cancelled"},
}

var pinMutationSpellings = map[string][]string{
	"notify_all_children.go": {"  outputs:\n    events:\n", "  outputs:\n"},
	"served_variants.go": {
		"events: [external.observed", "outputs: [external.observed",
		"outputs:\n    events: [opco.create_requested]", "outputs: [opco.create_requested]",
		"inputs:\n    events: [opco.create_requested]", "inputs: [opco.create_requested]",
		"outputs:\n    events: [opco.create_requested, opco.product_review_requested]", "outputs: [opco.create_requested, opco.product_review_requested]",
	},
	"fork_receiver_ownership.go": {
		"    events: [work.requested]", "    - work.requested",
		"    events:\n      - work.requested\n      - producer.closed", "    - work.requested\n    - producer.closed",
		"events: [work.requested", "outputs: [work.requested",
		"events: [producer.closed, work.requested", "outputs: [producer.closed, work.requested",
	},
	"receiver_composition.go": {"events: [work.completed", "outputs: [work.completed"},
}

func rewritePinMutationLiteral(name, text string) string {
	if !strings.Contains(filepath.ToSlash(name), "internal/runtime/testfixtures/canonicalrouting/") {
		return text
	}
	base := filepath.Base(name)
	for _, event := range pinMutationEvents[base] {
		text = strings.ReplaceAll(text, "      - "+event, "    - "+event)
	}
	if pairs := pinMutationSpellings[base]; len(pairs) != 0 {
		text = strings.NewReplacer(pairs...).Replace(text)
	}
	return text
}
