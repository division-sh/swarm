package pipeline

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeactivityidentity "github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyS1ScopedActivityDispatchUsesExactDeclarationForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowActivityNativeFixtureForTest) {
	t.Helper()
	var firstCalls, secondCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		origin := "research"
		switch request.URL.Path {
		case "/research":
			firstCalls.Add(1)
		case "/research2":
			origin = "research2"
			secondCalls.Add(1)
		default:
			t.Errorf("unexpected scoped endpoint %q", request.URL.Path)
			http.Error(w, "wrong endpoint", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"origin": origin})
	}))
	defer server.Close()
	files := map[string]string{"schema.yaml": "stages: []\n"}
	for _, flow := range []string{"research", "research2"} {
		files[flow+"/schema.yaml"] = "stages: []\n"
		files[flow+"/events.yaml"] = "request:\n"
		files[flow+"/nodes.yaml"] = "scanner:\n  execution_type: system_node\n  subscribes_to: [request]\n  event_handlers:\n    request:\n      activity: {id: send, tool: provider_write, input: {}}\n"
		files[flow+"/tools.yaml"] = fmt.Sprintf("provider_write:\n  handler_type: http\n  effect_class: non_idempotent_write\n  input_schema: {type: object}\n  output_schema:\n    type: object\n    required: [origin]\n    properties: {origin: {type: string, enum: [%s]}}\n  http: {method: POST, url: %q}\n", flow, server.URL+"/"+flow)
	}
	source := semanticview.Wrap(loadWorkflowTempBundle(t, files))
	if _, present := source.ToolEntries()["provider_write"]; present {
		t.Fatal("fixture did not remove the ambiguous global tool alias")
	}
	fixture := open(t, source)
	ctx := fixture.Context
	runID := uuid.NewString()
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	bus := &recordingPipelineBus{}
	pc := fixture.NewCoordinator(bus, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}})
	for _, flow := range []string{"research", "research2"} {
		intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
		intent.ActivityID = "send"
		intent.Owner = runtimeactivityidentity.MustNodeOwner(mustPipelineNode(flow, "scanner"))
		intent.ExecutionFlowID = identity.NormalizeFlowID(flow)
		intent.FlowInstance = flow + "/entity-1"
		intent.HandlerEventKey = "request"
		intent.SuccessEvent = flow + "/send.succeeded"
		intent.FailureEvent = flow + "/send.failed"
		routing, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
			FlowID: flow, FlowInstance: intent.FlowInstance, EntityID: intent.EntityID.String(),
		})
		if err != nil {
			t.Fatal(err)
		}
		intent.RoutingSource = routing
		request, err := activityRequestEmitIntent(intent)
		if err != nil {
			t.Fatal(err)
		}
		for replay := 0; replay < 2; replay++ {
			handled, _, err := pc.handleEventResult(ctx, request.Event)
			if err != nil || !handled {
				t.Fatalf("scoped dispatch/replay %s/%d: handled=%t err=%v", flow, replay, handled, err)
			}
			if len(bus.publishes) == 0 {
				t.Fatal("scoped activity produced no result")
			}
			last := bus.publishes[len(bus.publishes)-1]
			if last.Type() != events.EventType(flow+"/entity-1/send.succeeded") {
				t.Fatalf("scoped call did not produce its success: %s %s", last.Type(), last.Payload())
			}
			var payload struct{ Result struct{ Origin string } }
			if err := json.Unmarshal(last.Payload(), &payload); err != nil || payload.Result.Origin != flow {
				t.Fatalf("scoped call used another endpoint/schema: %+v %v", payload, err)
			}
		}
		if bus.publishes[len(bus.publishes)-1].ID() != bus.publishes[len(bus.publishes)-2].ID() {
			t.Fatal("duplicate activity changed the canonical journaled result identity")
		}
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || len(bus.publishes) != 4 {
		t.Fatalf("scoped replay redispatched or lost a result: calls=%d/%d results=%d", firstCalls.Load(), secondCalls.Load(), len(bus.publishes))
	}
	journal, err := fixture.ReadJournal(ctx, runID)
	if err != nil || journal.Attempts != 2 {
		t.Fatalf("scoped journal ownership differs: %+v %v", journal, err)
	}
	statuses, err := fixture.ReadAttemptStatuses(ctx)
	if err != nil || len(statuses) != 2 || statuses[0] != "succeeded" || statuses[1] != "succeeded" {
		t.Fatalf("scoped journal did not settle both exact effects: %+v %v", statuses, err)
	}
}
