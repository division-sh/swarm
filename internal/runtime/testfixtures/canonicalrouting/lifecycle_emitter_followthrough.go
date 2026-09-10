package canonicalrouting

import (
	"strings"
	"testing"
)

type LifecycleLoopScopeTopologyVariant uint8

const (
	LifecycleLoopScopeLocal LifecycleLoopScopeTopologyVariant = iota
	LifecycleLoopScopeWildcard
	LifecycleLoopScopeForeignOnly
)

func CopyLifecycleEmitterLoopScopeTopology(t testing.TB, variant LifecycleLoopScopeTopologyVariant) string {
	t.Helper()
	root := CopyLifecycleEmitterStatic(t, LifecycleStaticLoopLocal)
	if variant == LifecycleLoopScopeWildcard {
		agents := strings.ReplaceAll(lifecycleStaticRead(t, root, "agents.yaml"), "[loop.escaped]", "[loop.*]")
		writeClosedVariantFile(t, root, "agents.yaml", agents)
	} else if variant != LifecycleLoopScopeLocal && variant != LifecycleLoopScopeForeignOnly {
		t.Fatalf("unsupported loop scope variant %d", variant)
	}
	for _, scope := range []string{"left", "left/deep", "right"} {
		for _, file := range []string{"schema.yaml", "events.yaml", "nodes.yaml", "entities.yaml", "agents.yaml", "prompts/collector.md"} {
			writeClosedVariantFile(t, root, scope+"/"+file, lifecycleStaticRead(t, root, file))
		}
	}
	if variant == LifecycleLoopScopeForeignOnly {
		removeClosedVariantFiles(t, root, "agents.yaml")
	}
	return root
}

func CopyLifecycleEmitterCompetingExit(t testing.TB, timer bool) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateLocal)
	schema := lifecycleStaticRead(t, root, "schema.yaml")
	schema = strings.Replace(schema, "  done: {terminal: true}", "  done: {terminal: true}\n  cancelled: {terminal: true}", 1)
	nodes := lifecycleStaticRead(t, root, "nodes.yaml")
	nodes = strings.Replace(nodes, "    work.completed:\n", "    work.completed:\n      guard: {id: approved_stage, check: \"_entity.current_state == 'approved'\"}\n", 1)
	if timer {
		schema = strings.Replace(schema, "  review:\n", "  review:\n    timers:\n      - {id: review_expired, after: 2s, advances_to: cancelled}\n", 1)
	} else {
		schema += "      - {event: work.cancelled, source: external}\n"
		writeClosedVariantFile(t, root, "events.yaml", lifecycleStaticRead(t, root, "events.yaml")+"work.cancelled:\n  seed: boolean\n")
		nodes += "canceller:\n  execution_type: system_node\n  subscribes_to: [work.cancelled]\n  event_handlers:\n    work.cancelled:\n      guard: {id: review_stage, check: \"_entity.current_state == 'review'\"}\n      advances_to: cancelled\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", schema)
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	return root
}
