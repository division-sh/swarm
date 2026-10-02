package canonicalrouting

import (
	"path/filepath"
	"strings"
	"testing"
)

type SelectedForkPendingInputVariant uint8

const (
	PendingInputOriginal SelectedForkPendingInputVariant = iota
	PendingInputCompatible
	PendingInputIncompatible
	PendingInputMissingEndpoint
	PendingInputOrdinaryRoot
	PendingInputDuplicateEndpoint
	PendingInputMixedCompletion
	PendingInputConnectedSiblings
)

// CopySelectedForkPendingInput owns the two-flow routing bundle and its bounded
// positive/negative variants. Delivery scheduling remains with the test caller.
func CopySelectedForkPendingInput(t testing.TB, variant SelectedForkPendingInputVariant) string {
	t.Helper()
	tokenType, eventName := "text", "work.first"
	switch variant {
	case PendingInputOriginal, PendingInputOrdinaryRoot, PendingInputDuplicateEndpoint, PendingInputMixedCompletion, PendingInputConnectedSiblings:
	case PendingInputCompatible:
		tokenType = "text?"
	case PendingInputIncompatible:
		tokenType = "boolean"
	case PendingInputMissingEndpoint:
		eventName = "work.other"
	default:
		t.Fatalf("unsupported selected pending-input variant %d", variant)
	}
	root := t.TempDir()
	scopes := []string{"", "child"}
	if variant == PendingInputConnectedSiblings {
		scopes = append(scopes, "sibling")
	}
	for _, scope := range scopes {
		files := map[string]string{
			"schema.yaml":   "name: fork-input\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events:\n      - work.seeded\n      - " + eventName + "\n",
			"events.yaml":   "work.seeded:\n  seed: boolean\n" + eventName + ":\n  token: " + tokenType + "\n",
			"entities.yaml": "work: {}\n",
			"nodes.yaml":    "controller:\n  execution_type: system_node\n  subscribes_to: [work.seeded, " + eventName + "]\n  event_handlers:\n    work.seeded:\n      advances_to: ready\n    " + eventName + ":\n      advances_to: done\n",
		}
		if scope == "" {
			switch variant {
			case PendingInputOrdinaryRoot:
				files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "- work.first", "- work.requested")
				files["events.yaml"] += "work.requested:\n  token: text\n"
				files["nodes.yaml"] += "emitter:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  produces: [work.first]\n  event_handlers:\n    work.requested:\n      emit:\n        event: work.first\n        fields:\n          token: ${payload.token}\n"
			case PendingInputDuplicateEndpoint:
				files["schema.yaml"] += "      - work.first\n"
			case PendingInputMixedCompletion:
				files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "done: {terminal: true}", "archived: {terminal: true}") + "      - work.marked\n"
				files["events.yaml"] += "work.marked:\n  token: text\n"
				files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "subscribes_to: [work.seeded, work.first]", "subscribes_to: [work.seeded]", 1)
				files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "    work.first:\n      advances_to: done\n", "", 1)
				files["nodes.yaml"] += "marker:\n  execution_type: system_node\n  subscribes_to: [work.marked]\n  event_handlers:\n    work.marked:\n      advances_to: archived\n"
			}
			files["schema.yaml"] += "  outputs:\n    events: [work.seeded, " + eventName + "]\nconnect:\n  - {event: work.seeded, from: ., to: child}\n  - {event: " + eventName + ", from: ., to: child}\n"
			if variant == PendingInputMixedCompletion {
				files["schema.yaml"] = strings.Replace(files["schema.yaml"], "connect:\n", "connect:\n  - {event: work.seeded, from: ., to: a_finished}\n  - {event: work.first, from: ., to: a_finished}\n", 1)
			}
			if variant == PendingInputConnectedSiblings {
				files["schema.yaml"] += "  - {event: work.seeded, from: ., to: sibling}\n  - {event: " + eventName + ", from: ., to: sibling}\n  - {event: " + eventName + ", from: ., to: on_demand, resolution: select-or-create}\n"
			}
		} else {
			delete(files, "events.yaml")
		}
		for name, body := range files {
			writeClosedVariantFile(t, root, filepath.Join(scope, name), body)
		}
	}
	if variant == PendingInputMixedCompletion {
		writeClosedVariantFile(t, root, "a_finished/schema.yaml", "name: finished\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [work.seeded, work.first]\n")
		writeClosedVariantFile(t, root, "a_finished/entities.yaml", "work: {}\n")
		writeClosedVariantFile(t, root, "a_finished/nodes.yaml", "controller:\n  execution_type: system_node\n  subscribes_to: [work.seeded, work.first]\n  event_handlers:\n    work.seeded:\n      advances_to: ready\n    work.first:\n      advances_to: done\n")
	}
	if variant == PendingInputConnectedSiblings {
		writeClosedVariantFile(t, root, "on_demand/schema.yaml", "name: on_demand\ninstance: token\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [work.first]\n")
		writeClosedVariantFile(t, root, "on_demand/entities.yaml", "work:\n  token: {type: text, indexed: true, _unused_reason: receiver instance identity}\n")
		writeClosedVariantFile(t, root, "on_demand/nodes.yaml", "controller:\n  execution_type: system_node\n  subscribes_to: [work.first]\n  event_handlers:\n    work.first:\n      advances_to: done\n")
	}
	return root
}

func CopySelectedInputValidationProbe(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml":       "name: selected-input\npins:\n  inputs:\n    events:\n      - thing.created\n",
		"events.yaml":       "thing.created:\n",
		"nodes.yaml":        "worker:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard:\n        id: selected_owner\n        check: '_entity.id != \"\"'\n",
		"child/schema.yaml": "name: child\npins:\n  inputs:\n    events:\n      - thing.created\n",
		"child/events.yaml": "thing.created:\n",
		"child/nodes.yaml":  "worker:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard:\n        id: selected_owner\n        check: '_entity.id != \"\"'\n",
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
