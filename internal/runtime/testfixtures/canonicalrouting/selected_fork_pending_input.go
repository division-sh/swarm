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
)

// CopySelectedForkPendingInput owns the two-flow routing bundle and its bounded
// positive/negative variants. Delivery scheduling remains with the test caller.
func CopySelectedForkPendingInput(t testing.TB, variant SelectedForkPendingInputVariant) string {
	t.Helper()
	tokenType, eventName := "text", "work.first"
	switch variant {
	case PendingInputOriginal, PendingInputOrdinaryRoot, PendingInputDuplicateEndpoint, PendingInputMixedCompletion:
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
	for _, scope := range []string{"", "child"} {
		files := map[string]string{
			"schema.yaml":   "name: fork-input\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events:\n      - {event: work.seeded, source: external}\n      - {event: " + eventName + ", source: external}\n",
			"events.yaml":   "work.seeded:\n  seed: boolean\n" + eventName + ":\n  token: " + tokenType + "\n",
			"entities.yaml": "work: {}\n",
			"nodes.yaml":    "controller:\n  execution_type: system_node\n  subscribes_to: [work.seeded, " + eventName + "]\n  event_handlers:\n    work.seeded:\n      create_entity: true\n      advances_to: ready\n    " + eventName + ":\n      advances_to: done\n",
		}
		if scope == "" {
			switch variant {
			case PendingInputOrdinaryRoot:
				files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "event: work.first", "event: work.requested")
				files["events.yaml"] += "work.requested:\n  token: text\n"
				files["nodes.yaml"] += "emitter:\n  execution_type: system_node\n  subscribes_to: [work.requested]\n  produces: [work.first]\n  event_handlers:\n    work.requested:\n      emit:\n        event: work.first\n        fields:\n          token: 'payload.token'\n"
			case PendingInputDuplicateEndpoint:
				files["schema.yaml"] += "      - {event: work.first, source: external}\n"
			case PendingInputMixedCompletion:
				files["schema.yaml"] = strings.ReplaceAll(files["schema.yaml"], "done: {terminal: true}", "done: {}\n  archived: {terminal: true}") + "      - {event: work.marked, source: external}\n"
				files["events.yaml"] += "work.marked:\n  token: text\n"
				files["nodes.yaml"] += "marker:\n  execution_type: system_node\n  subscribes_to: [work.marked]\n  event_handlers:\n    work.marked:\n      advances_to: archived\n"
			}
		}
		for name, body := range files {
			writeClosedVariantFile(t, root, filepath.Join(scope, name), body)
		}
	}
	return root
}

func CopySelectedInputValidationProbe(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml":       "name: selected-input\nmode: static\npins:\n  inputs:\n    events:\n      - {event: thing.created, source: external}\n",
		"events.yaml":       "thing.created: {}\n",
		"child/schema.yaml": "name: child\nmode: static\npins:\n  inputs:\n    events:\n      - {event: thing.created, source: external}\n",
		"child/events.yaml": "thing.created: {}\n",
		"child/nodes.yaml":  "worker:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard:\n        id: selected_owner\n        check: '_entity.id != \"\"'\n",
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
