package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func a2FanOutCollectionFixture(t *testing.T, sourcePath, typ string, payload, state map[string]any, writes []rc.WorkflowDataWrite, bound int) (*Executor, events.Event, ExecutionResult, error) {
	t.Helper()
	node := testRootExecutableNode(t, "scatter")
	handler, err := rc.QualifySystemNodeHandlerRuleRefsForEvent(node, "batch.ready", rc.SystemNodeEventHandler{
		FanOut: &rc.FanOutSpec{ItemsFrom: sourcePath, As: "key", MaxItems: bound, MaxItemsSet: bound > 0,
			Emit: rc.EmitSpec{Event: "item.ready", Fields: map[string]rc.ExpressionValue{
				"key": rc.CELExpression("key"), "index": rc.CELExpression("fan_out.index"), "count": rc.CELExpression("fan_out.count"),
			}}},
		DataAccumulation: rc.WorkflowDataAccumulation{Writes: writes},
	})
	if err != nil {
		t.Fatal(err)
	}
	schema := rc.FlowSchemaDocument{Pins: rc.FlowPins{Outputs: rc.FlowOutputPins{EventPins: []rc.FlowOutputEventPin{{Event: "item.ready"}}}}}
	catalog := map[string]rc.EventCatalogEntry{
		"batch.ready": requiredEventPayload(map[string]rc.EventFieldSpec{"items": {Type: typ}}),
		"item.ready":  requiredEventPayload(map[string]rc.EventFieldSpec{"key": {Type: "text"}, "index": {Type: "integer"}, "count": {Type: "integer"}}),
	}
	root := &rc.FlowContractView{Path: ".", Paths: rc.FlowContractPaths{FlowPath: ".", SchemaFile: "schema.yaml"}, Schema: schema, Events: catalog}
	bundle := &rc.WorkflowContractBundle{
		RootSchema: &schema, Events: catalog,
		FlowSources:  map[string]rc.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml"}},
		FlowTree:     rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{".": root}, ByPath: map[string]*rc.FlowContractView{".": root}},
		Nodes:        map[string]rc.SystemNodeContract{"scatter": {EventHandlers: map[string]rc.SystemNodeEventHandler{"batch.ready": handler}}},
		Semantics:    rc.WorkflowSemanticView{Name: "root", Version: "v-test", NodeHandlers: map[string]map[string]rc.SystemNodeEventHandler{"scatter": {"batch.ready": handler}}},
		RootEntities: rc.EntityContractsDocument{"subject": {Fields: map[string]rc.EntityFieldDecl{"items": {Type: typ}}}},
	}
	exec, err := NewExecutor(RuntimeDependencies{Source: fanOutSourceWithBundleIdentity(t, bundle), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, PayloadShaper: stubPayloadShaper{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	trigger := eventtest.RunCreatingRootIngress(eventtest.UUID("a2-collection"), "batch.ready", "", "", raw, 0, semanticExecutionFixtureRunID, "", events.EventEnvelope{}, time.Now().UTC())
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{EntityID: identity.NormalizeEntityID(semanticExecutionFixtureRunID), Node: node, Event: trigger, Handler: handler, State: testStateSnapshot("pending", state, nil, nil)})
	return exec, trigger, result, err
}

func TestA2FanOutMapProjectionAndTypedOrdinalAlias(t *testing.T) {
	for _, source := range []string{"payload.items", "entity.items"} {
		t.Run(source, func(t *testing.T) {
			value := map[string]any{"z": []any{4}, " a ": []any{1, 1}, "a": []any{2}}
			exec, trigger, result, err := a2FanOutCollectionFixture(t, source, "map[text][integer]", map[string]any{"items": value}, map[string]any{"items": value}, nil, 0)
			if err != nil || result.FanOutIntent == nil {
				t.Fatalf("capture: %v", err)
			}
			request := *result.FanOutIntent
			if request.Cardinality != 3 || request.Capsule.SourceProjection.ItemType.Kind != rc.CatalogTypeText {
				t.Fatalf("wrong projection: %#v", request)
			}
			if source == "entity.items" {
				if _, ok := request.Capsule.StateFields["items"]; ok {
					t.Fatal("source duplicated into capsule")
				}
			}
			now := time.Now().UTC()
			intent := fanoutobligation.Intent{Request: request, Source: request.Source, Status: fanoutobligation.StatusOpen, NextChunkSize: 2, CreatedAt: now, UpdatedAt: now, Cursor: 1}
			if source == "entity.items" {
				intent.Source.MutationID = eventtest.UUID("immutable-source")
			}
			prepared, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := request.ProjectSource(value)
			if err != nil {
				t.Fatal(err)
			}
			for ordinal := 1; ordinal < 3; ordinal++ {
				emit, err := prepared.EvaluateOrdinal(context.Background(), keys[ordinal], ordinal)
				if err != nil {
					t.Fatal(err)
				}
				var payload map[string]any
				if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
					t.Fatal(err)
				}
				if payload["key"] != keys[ordinal] || payload["index"] != float64(ordinal) || payload["count"] != float64(3) {
					t.Fatalf("wrong deferred alias/index/count: %#v", payload)
				}
			}
			bad := intent
			bad.Request.Capsule.SourceProjection = request.Capsule.SourceProjection.Clone()
			bad.Request.Capsule.SourceProjection.CollectionProjection.Kind = rc.CollectionListItems
			if _, err := exec.PrepareFanOutEvaluation(context.Background(), bad, trigger); err == nil {
				t.Fatal("evaluator accepted contradictory projection")
			}
		})
	}
}

func TestA2FanOutSelectedPostWriteMapAndFailClosedSources(t *testing.T) {
	replacement := map[string]any{"b": 2, "a": 1}
	_, _, result, err := a2FanOutCollectionFixture(t, "entity.items", "map[text]integer", map[string]any{"items": replacement}, map[string]any{"items": map[string]any{"old": 9}}, []rc.WorkflowDataWrite{{TargetRef: "entity.items", Value: rc.LiteralExpression(replacement)}}, 0)
	if err != nil || result.FanOutIntent == nil {
		t.Fatalf("postwrite capture: %v", err)
	}
	if !result.FanOutIntent.Capsule.SourceProjection.SourceAfterWrites || result.FanOutIntent.Cardinality != 2 {
		t.Fatal("captured prewrite map")
	}
	actual, marshalErr := json.Marshal(result.StateMutation.StateCarrier.Fields["items"])
	expected, _ := json.Marshal(replacement)
	if marshalErr != nil || string(actual) != string(expected) {
		t.Fatalf("selected mutation source = %#v", result.StateMutation.StateCarrier.Fields)
	}
	for _, payload := range []map[string]any{{}, {"items": nil}, {"items": []any{"not-map"}}, {"items": 7}} {
		_, _, got, err := a2FanOutCollectionFixture(t, "payload.items", "map[text]integer", payload, nil, nil, 0)
		if err == nil || got.FanOutIntent != nil {
			t.Fatalf("accepted missing/null/wrong-kind source %#v: %v", payload, err)
		}
	}
	_, _, got, err := a2FanOutCollectionFixture(t, "payload.items", "map[text]integer", map[string]any{"items": replacement}, nil, nil, 1)
	if err == nil || got.FanOutIntent != nil {
		t.Fatal("bound violation retained an intent")
	}
}

func TestA2FanOutListMultiplicityAndEmptyCollections(t *testing.T) {
	for _, tc := range []struct {
		name, typ string
		value     any
		count     int
	}{
		{"list_duplicates", "[text]", []any{"z", "a", "z"}, 3},
		{"empty_list", "[text]", []any{}, 0},
		{"empty_map", "map[text]integer", map[string]any{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, trigger, result, err := a2FanOutCollectionFixture(t, "payload.items", tc.typ, map[string]any{"items": tc.value}, nil, nil, 0)
			if err != nil || result.FanOutIntent == nil || result.FanOutIntent.Cardinality != tc.count {
				t.Fatalf("capture count = %#v, %v", result.FanOutIntent, err)
			}
			if tc.count == 0 {
				return
			}
			request := *result.FanOutIntent
			now := time.Now().UTC()
			intent := fanoutobligation.Intent{Request: request, Source: request.Source, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
			prepared, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
			if err != nil {
				t.Fatal(err)
			}
			items, err := request.ProjectSource(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			for ordinal, item := range items {
				emit, err := prepared.EvaluateOrdinal(context.Background(), item, ordinal)
				if err != nil {
					t.Fatal(err)
				}
				var payload map[string]any
				if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
					t.Fatal(err)
				}
				if payload["key"] != []string{"z", "a", "z"}[ordinal] || payload["index"] != float64(ordinal) || payload["count"] != float64(3) {
					t.Fatalf("list multiplicity/order lost: %#v", payload)
				}
			}
		})
	}
}
