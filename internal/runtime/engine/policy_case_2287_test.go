package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestExecutorCanonicalPolicyCaseShapes2287(t *testing.T) {
	for _, row := range []string{
		"selector: payload.mode, equals: gold",
		"selectors: [payload.mode, payload.ready, payload.count], equals: [gold, true, 1]",
	} {
		t.Run(row, func(t *testing.T) {
			handler, err := loadNodeHandlerFixture("rules:\n  - case: {" + row + "}\n    advances_to: selected\n  - advances_to: fallback\n")
			if err != nil {
				t.Fatal(err)
			}
			bundle := &rc.WorkflowContractBundle{
				Events: map[string]rc.EventCatalogEntry{"requested": requiredEventPayload(map[string]rc.EventFieldSpec{
					"mode": {Type: "text"}, "ready": {Type: "boolean"}, "count": {Type: "integer"},
				})},
				Nodes: map[string]rc.SystemNodeContract{"router": {EventHandlers: map[string]rc.SystemNodeEventHandler{"requested": handler}}},
			}
			executor, err := NewExecutor(RuntimeDependencies{
				Source:    sourceWithFixtureStages(mustCompileEngineSource(bundle), ".", "pending", "pending", "selected", "fallback"),
				StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
			}, schemaBoundWildcardEvaluator{})
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct{ payload, stage string }{
				{`{"mode":"gold","ready":true,"count":1}`, "selected"},
				{`{"mode":"blue","ready":true,"count":1}`, "fallback"},
			} {
				result, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
					EntityID: "case-1", Node: identitytest.RootNode(t, "router"), HandlerEventKey: "requested",
					Event:   eventtest.RunCreatingRootIngress("case-proof", "requested", "", "", json.RawMessage(tc.payload), 0, "", "", events.EventEnvelope{}, time.Time{}),
					Handler: handler, State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
				})
				if err != nil || result.NextState != tc.stage {
					t.Fatalf("real CEL selection stage=%q want %q, error=%v", result.NextState, tc.stage, err)
				}
				label := requireResolvedSelection(t, result.HandlerRuleSelection).DisplayLabel()
				if tc.stage == "selected" && label != "rules[0]: case {"+row+"}" || tc.stage == "fallback" && !strings.HasPrefix(label, "rules[1]: default") {
					t.Fatalf("positional authored selection changed: %s", label)
				}
			}
		})
	}
}
