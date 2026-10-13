package bootverify

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestExecutableReaderPresentationKeepsPositionalSourceSlots(t *testing.T) {
	for _, flow := range []string{".", "research"} {
		for _, tc := range []struct {
			name, raw, expression, display, slot string
			context                              pipeline.WorkflowConditionContext
			phase                                pipeline.WorkflowEntityFieldLifecyclePhase
		}{
			{
				name: "rules", raw: "rules:\n  - when: false\n  - case: {selector: payload.mode, equals: 'gold'}\n  - else: true\n",
				expression: `payload.mode == "gold"`, display: "rules[1]: case {selector: payload.mode, equals: 'gold'}.condition", slot: "rules[1].when",
				context: pipeline.WorkflowConditionContextRule, phase: pipeline.WorkflowEntityFieldLifecycleRule,
			},
			{
				name: "on_complete", raw: "on_complete:\n  - condition: false\n  - condition: payload.ready\n",
				expression: "payload.ready", display: "on_complete[1]: condition payload.ready.condition", slot: "on_complete[1].condition",
				context: pipeline.WorkflowConditionContextOnComplete, phase: pipeline.WorkflowEntityFieldLifecycleOnComplete,
			},
		} {
			t.Run(flow+"/"+tc.name, func(t *testing.T) {
				handler := mustBootHandlerFixture(t, tc.raw)
				node := identitytest.ExecutableNode(t, flow, "worker")
				readers := pipeline.WorkflowHandlerExecutableReaders(nil, node, "request", handler)
				found := 0
				for _, reader := range readers {
					if reader.Expression != tc.expression {
						continue
					}
					found++
					if reader.Kind != tc.display || reader.SourceSlot != tc.slot || reader.RuleCollection != tc.name || reader.RuleField != "Condition" || !reader.HasRuleIndex || reader.RuleIndex != 1 || !reader.HasConditionContext || reader.ConditionContext != tc.context || reader.Phase != tc.phase {
						t.Fatalf("display changed positional source/compiler ownership: %#v", reader)
					}
				}
				if found != 1 {
					t.Fatalf("got %d exact predicate readers, want 1: %#v", found, readers)
				}
			})
		}
	}
}
