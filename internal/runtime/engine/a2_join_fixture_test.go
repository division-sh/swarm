package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

// These fixtures explicitly supply admitted lifecycle evidence. They do not
// infer an arm or an admission receipt from an arrival's payload or state.
func a2EngineJoinEntry(runID, entityID, stage string, route flowidentity.Route) timeridentity.StageEntryRef {
	return timeridentity.StageEntryRef{RunID: runID, FlowScope: route.ScopeKey, InstanceID: route.InstanceID,
		InstancePath: route.InstancePath, EntityID: entityID, Stage: stage, Cause: "construction"}
}

func a2EngineJoinFixtureEntry(entityID, stage string) timeridentity.StageEntryRef {
	return a2EngineJoinEntry(semanticExecutionFixtureRunID, entityID, stage,
		flowidentity.StoredRoute(".", semanticExecutionFixtureRunID, semanticExecutionFixtureRunID))
}

func a2BoundJoinContext(t *testing.T, activation joinruntime.Activation) context.Context {
	t.Helper()
	delivery := events.DeliveryContext{Joins: []events.JoinAdmissionReceipt{{Ref: activation.JoinRef(), Disposition: events.JoinAdmissionBound}}}
	if err := delivery.Validate(); err != nil {
		t.Fatal(err)
	}
	return events.WithDeliveryContext(context.Background(), delivery)
}

func a2JoinFixtureRoute(entry timeridentity.StageEntryRef) flowidentity.Route {
	return flowidentity.StoredRoute(entry.FlowScope, entry.InstanceID, entry.InstancePath)
}

func a2JoinFixtureSnapshot(t *testing.T, activation joinruntime.Activation, fields map[string]any, buckets map[string]map[string]any) StateSnapshot {
	t.Helper()
	state := testStateSnapshot(activation.JoinRef().Stage(), fields, nil, buckets)
	state.StateCarrier.Bookkeeping = map[string]any{}
	if err := workflowlifecycle.StoreStageEntry(state.StateCarrier.Bookkeeping, activation.JoinRef().StageEntry()); err != nil {
		t.Fatal(err)
	}
	return state
}

func a2JoinContinuationRequest(t *testing.T, req ExecutionRequest, activation joinruntime.Activation, at time.Time) ExecutionRequest {
	t.Helper()
	if activation.Status != joinruntime.StatusClosed || !activation.OutcomePending || activation.OutcomeFired {
		t.Fatalf("completion requires retained pending closure: %#v", activation)
	}
	handle := activation.TimerHandle()
	if handle.Kind() != timeridentity.TimerHandleJoinComplete {
		t.Fatal("pending closure does not own the exact completion handle")
	}
	payload, err := json.Marshal(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	req.Event = eventtest.RuntimeControl("completion-"+req.Event.ID(), events.EventType(handle.EventType()), "runtime", handle.TaskID(), payload, 0,
		activation.JoinRef().StageEntry().RunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, req.EntityID.String()), at)
	req.Route = a2JoinFixtureRoute(activation.JoinRef().StageEntry())
	return req
}

func a2TypedJoinFixtureSource(t *testing.T, node identity.ExecutableNode, eventType string, spec rc.JoinSpec, resultType rc.CatalogTypeReference, fields map[string]rc.EntityFieldDecl) (semanticview.Source, rc.SystemNodeEventHandler) {
	t.Helper()
	handler, err := completeSemanticFixtureHandlerRuleIdentity(node, eventType, rc.SystemNodeEventHandler{Join: &spec})
	if err != nil {
		t.Fatal(err)
	}
	bundle := &rc.WorkflowContractBundle{
		RootSchema: &rc.FlowSchemaDocument{}, RootTypes: resultType.Catalog,
		RootEntities: rc.EntityContractsDocument{"work": {Fields: fields}},
		Nodes:        map[string]rc.SystemNodeContract{node.NodeID(): {EventHandlers: map[string]rc.SystemNodeEventHandler{eventType: handler}}},
		Events: map[string]rc.EventCatalogEntry{eventType: {Payload: rc.EventPayloadSpec{Properties: map[string]rc.EventFieldSpec{
			strings.TrimPrefix(spec.Members.By, "payload."): {Type: "text"}, strings.TrimPrefix(spec.Output, "payload."): {Type: resultType.Type},
		}}}},
	}
	root := &rc.FlowContractView{Path: ".", Paths: rc.FlowContractPaths{FlowPath: "."}, Schema: *bundle.RootSchema, Nodes: bundle.Nodes, Events: bundle.Events}
	bundle.FlowTree = rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{".": root}, ByPath: map[string]*rc.FlowContractView{".": root}}
	if err := rc.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	plan, ok := semanticview.WorkflowJoinPlanForHandler(semanticview.Wrap(bundle), node, eventType)
	if !ok {
		t.Fatal("typed fixture lacks compiled join")
	}
	handler.Join = &plan.Spec
	return sourceWithFixtureStages(semanticview.Wrap(bundle), ".", "awaiting", "awaiting", "ready", "attention"), handler
}

func a2MigrateJoinFixtureFile(t *testing.T, path string, replacements ...string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for i := 0; i < len(replacements); i += 2 {
		if !strings.Contains(text, replacements[i]) {
			t.Fatalf("fixture no longer contains migration source %q", replacements[i])
		}
		text = strings.ReplaceAll(text, replacements[i], replacements[i+1])
	}
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}

func a2CopyArrivalBarrier(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.FanInBarrier)
	a2MigrateJoinFixtureFile(t, filepath.Join(root, "portfolio", "nodes.yaml"),
		"          from: entity.expected_operating_ids\n        window:\n          from: entity.period_id\n", "          from: state.expected_operating_ids\n          by: payload.operating_id\n",
		"        timeout:\n          after: 5m\n          advances_to: failed\n", "        deadline: {after: 5m, from: stage_entry}\n        on_deadline:\n          advances_to: failed\n")
	return root
}
