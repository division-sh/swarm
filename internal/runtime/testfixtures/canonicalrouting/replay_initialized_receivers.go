package canonicalrouting

import (
	"fmt"
	"testing"
)

// CopyReplayInitializedReceivers declares distinct same-slug agent receivers
// whose initializer deliveries must complete before agent-only replay.
func CopyReplayInitializedReceivers(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: replay-initialized-receivers
pins:
  inputs:
    events: [{event: start, source: external}]
  outputs:
    events: [work.ready]
connect:
  - {event: work.ready, from: ., to: left}
  - {event: work.ready, from: ., to: right}
`)
	writeClosedVariantFile(t, root, "events.yaml", "start: {}\nwork.ready: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "source:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      emit: {event: work.ready}\n")
	for _, flow := range []string{"left", "right"} {
		writeClosedVariantFile(t, root, flow+"/schema.yaml", fmt.Sprintf("name: %s\nmode: static\nstages:\n  active: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [work.ready]\n", flow))
		writeClosedVariantFile(t, root, flow+"/entities.yaml", "receipt: {}\n")
		writeClosedVariantFile(t, root, flow+"/nodes.yaml", "initialize:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      create_entity: true\n")
		writeClosedVariantFile(t, root, flow+"/agents.yaml", "worker:\n  id: worker\n  model: regular\n  intent: {inline: Observe initialized work.}\n  subscriptions: [work.ready]\n")
	}
	return root
}
