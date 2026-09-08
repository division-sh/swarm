package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type schemaBoundWildcardEvaluator struct{ NoopEvaluator }

func (schemaBoundWildcardEvaluator) EvalBool(expression string, base BaseContext, payloadType *runtimecontracts.ResolvedCatalogType) (bool, error) {
	result, err := workflowexpr.EvalValueExpressionWithOptions(expression,
		workflowexpr.ValueContext{Payload: base.Payload.Raw()}, workflowexpr.ValueExpressionOptions{PayloadType: payloadType})
	if err != nil {
		return false, err
	}
	value, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("condition returned %T, not boolean", result)
	}
	return value, nil
}

func TestExecutorLocalWildcardPayloadReaderUsesConcreteProducerSchema(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyLocalWildcardPayload(t, canonicalrouting.LocalWildcardPayloadValid)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	executor, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, schemaBoundWildcardEvaluator{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		eventType string
		wantError bool
	}{
		{eventType: "worker/task.done"},
		{eventType: "worker/task.failed"},
		{eventType: "worker/task.undeclared", wantError: true},
		{eventType: "sibling/task.done", wantError: true},
		{eventType: "worker/start", wantError: true},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			_, err := executor.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", ExecutionFlowID: identity.NormalizeFlowID("worker"), Node: identitytest.FlowNode(t, "worker", "observer"),
				HandlerEventKey: "task.*",
				Event: eventtest.ExistingRunRootIngress("wildcard-proof", events.EventType(tc.eventType), "", "", json.RawMessage(`{"work_id":"work-1"}`), 0,
					eventtest.UUID("wildcard-proof-run"), events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{ID: "accept", Condition: `payload.work_id != ""`}}},
				State:   testStateSnapshot("active", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "without an exact structural schema") {
					t.Fatalf("error = %v, want missing structural schema", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
