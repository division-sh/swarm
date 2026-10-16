package canonicalrouting

import (
	"fmt"
	"strings"
	"testing"
)

// CopyUnavailableConnectionScan owns the finite terminal-before-later-read
// geometries. All construction observations must use this actual compiled tree.
func CopyUnavailableConnectionScan(t testing.TB, variant string) string {
	t.Helper()
	switch variant {
	case "none", "collision", "missing_key", "create", "pin_collision":
	default:
		t.Fatalf("unsupported unavailable-connection scan variant %q", variant)
	}
	root := t.TempDir()
	producerFields := ""
	if variant == "missing_key" || variant == "create" {
		producerFields = "  instance_key: text\n"
	}
	writeClosedVariantFile(t, root, "producer/schema.yaml", "pins:\n  outputs: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n"+producerFields)
	receivers := []string{"aterminal", "bintermediate", "yerror", "zerror"}
	if variant == "pin_collision" {
		receivers = []string{"consumer"}
	}
	var connects strings.Builder
	connects.WriteString("connect:\n")
	for _, receiver := range receivers {
		inputs := []string{"work.accepted"}
		if variant == "pin_collision" || receiver == "bintermediate" && variant == "collision" {
			inputs = append(inputs, "work.audited")
		}
		instance, resolution := "", ""
		if receiver == "bintermediate" && (variant == "missing_key" || variant == "create") {
			instance, resolution = "instance: instance_key\n", ", resolution: select"
			if variant == "create" {
				resolution = ", resolution: select-or-create"
			}
			writeClosedVariantFile(t, root, receiver+"/entities.yaml", "work:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n")
		}
		writeClosedVariantFile(t, root, receiver+"/schema.yaml", instance+"pins:\n  inputs: ["+strings.Join(inputs, ", ")+"]\n")
		var handlers strings.Builder
		handlers.WriteString("receiver:\n  execution_type: system_node\n  event_handlers:\n")
		for _, input := range inputs {
			fmt.Fprintf(&connects, "  - {event: work.ready, from: producer, to: %s, rename: %s%s}\n", receiver, input, resolution)
			fmt.Fprintf(&handlers, "    %s:\n      guard:\n        id: selected_owner\n        check: _entity.id != \"\"\n", input)
		}
		writeClosedVariantFile(t, root, receiver+"/nodes.yaml", handlers.String())
	}
	writeClosedVariantFile(t, root, "schema.yaml", connects.String())
	return root
}
