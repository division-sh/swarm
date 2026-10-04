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
		"  outputs:\n    [", "  outputs: [",
	},
	"fork_receiver_acquisition_effect_only.go": {"  outputs: [receiver.finished]\n", "  outputs:\n    - receiver.finished\n"},
	"fork_receiver_notice_effect.go":           {"  outputs: [receiver.finished]\n", "  outputs:\n    - receiver.finished\n"},
	"receiver_agent_collision.go": {
		"    - event: work.ready\n        initialize:\n", "    - event: work.ready\n      initialize:\n",
		"          %s: payload.values.%s\n", "        %s: payload.values.%s\n",
	},
	"lifecycle_emitters.go":   {"inputs: [loop.escaped]", "    - loop.escaped\n", "inputs: [loop.escaped, ordinary.repeated]", "    - loop.escaped\n    - ordinary.repeated\n"},
	"receiver_composition.go": {"events: [work.completed", "outputs: [work.completed"},
	"channel_delivery.go":     {"  inputs: {events: [work.requested]}", "  inputs: [work.requested]", "      - observer.requested\n", "    - observer.requested\n"},
}

var inlinePinSpellings = map[string][]string{
	"internal/runtime/bus/routing_derivation_test.go":          {"  inputs:\n    events: [root.start]", "  inputs: [root.start]"},
	"internal/runtime/workflow_timer_startup_recovery_test.go": {"pins:\n  inputs:\n    events: [generic.tick]", "pins:\n  inputs: [generic.tick]"},
	"internal/serveapp/provider_alias_authority_test.go": {
		"  inputs:\n    events: [", "  inputs: [",
		"  outputs:\n    events: [", "  outputs: [",
	},
	"internal/serveapp/source_admission_fork_proof_test.go": {"  outputs:\n    events: [work.requested]", "  outputs: [work.requested]"},
	"internal/runtime/conformance/fan_out_resource_selected_fork_external_test.go": {
		"  outputs:\n    events:\n", "  outputs:\n",
		"      - event: task.assigned\n        sink: harness\n", "    - task.assigned\n",
	},
	"internal/store/internal/runtimepersistence/mutation_protocol_composed_journey_test.go": {"      - start.closed\n", "    - start.closed\n", "      - fanout.requested\n", "    - fanout.requested\n", "events: [work.requested", "outputs: [work.requested", "events: [fanout.child, work.requested", "outputs: [fanout.child, work.requested"},
	"internal/cliapp/main_test.go":                                        {"pins:\n  inputs:\n    events: [item.arrived]", "pins:\n  inputs: [item.arrived]"},
	"internal/runtime/connector_schema_binding_test.go":                   {"      - activity.requested", "    - activity.requested", "      - inbound.telegram.text_message", "    - inbound.telegram.text_message"},
	"internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go": {"      - %s\n", "    - %s\n"},
	"internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go":   {"      - account.task.completed", "    - account.task.completed", "      - account.tasks.completed", "    - account.tasks.completed", "      - account.notification.completed", "    - account.notification.completed"},
	"internal/runtime/conformance/data_text_file_journey_2456_test.go":    {"events: [root.ready]", "    - root.ready\n", "events: [root.ready, root.ready.body]", "    - root.ready\n    - root.ready.body\n"},
}

func rewritePinMutationLiteral(name, text string) string {
	if pairs := inlinePinSpellings[filepath.ToSlash(name)]; len(pairs) != 0 {
		return strings.NewReplacer(pairs...).Replace(text)
	}
	if filepath.ToSlash(name) == "internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go" {
		return strings.NewReplacer("events: [item.ready]", "    - item.ready\n", "events: [item.ready, leaf.ready]", "    - item.ready\n    - leaf.ready\n").Replace(text)
	}
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
