package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestRuleDefaultRuntimeDoesNotInterpretElse(t *testing.T) {
	for _, expression := range []string{"else", "ELSE", "Else"} {
		t.Run(expression, func(t *testing.T) {
			handler, err := loadNodeHandlerFixture("rules: [{when: " + expression + ", emit: forbidden}]\n")
			if err != nil {
				t.Fatal(err)
			}
			node := testRootExecutableNode(t, "scoring-node")
			handler, err = c.QualifySystemNodeHandlerRuleRefsForEvent(node, "score.dimension_complete", handler)
			if err != nil {
				t.Fatal(err)
			}
			if handlerRuleUnconditional(handler.Rules[0]) {
				t.Fatal("assignment analysis still treats an ordinary CEL reference as unconditional")
			}
			wantErr := errors.New("ordinary predicate evaluation refused")
			exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{errs: map[string]error{expression: wantErr}})
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				ExecutionFlowID: identity.NormalizeFlowID("."), EntityID: "entity-1", Node: node,
				Event:   eventtest.RunCreatingRootIngress("default-refusal", "score.dimension_complete", "", "", json.RawMessage(`{"score":87}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler: handler, State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if !errors.Is(err, wantErr) || result.Committed || len(result.EmitIntents) != 0 || len(result.ActivityIntents) != 0 {
				t.Fatalf("ordinary predicate bypassed evaluation or leaked effects: %+v, %v", result, err)
			}
			fact := requireResolvedSelection(t, result.HandlerRuleSelection)
			if fact.Disposition() != handlerselection.DispositionEvaluationFailed || fact.Context() != handlerselection.ContextRules || fact.DisplayLabel() != "rules[0]: when "+expression || fact.Ref().SemanticPath() != `nodes["scoring-node"].handlers["score.dimension_complete"].rules[0]` {
				t.Fatalf("refusal lost exact attempted identity: %#v", fact)
			}
		})
	}
}
