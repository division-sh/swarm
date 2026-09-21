package manager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestActivateFlowInstanceAdmitsReceiverConfigBeforeAllConsumers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  map[string]any
		want    map[string]any
		wantErr string
	}{
		{"defaults", map[string]any{"name": "business"}, map[string]any{"name": "business", "count": int64(3), "flag": true}, ""},
		{"zero and false", map[string]any{"name": "business", "count": 0, "flag": false}, map[string]any{"name": "business", "count": int64(0), "flag": false}, ""},
		{"null is not absent", map[string]any{"name": "business", "count": nil}, nil, "receiver variable count"},
		{"wrong type", map[string]any{"name": "business", "count": "3"}, nil, "receiver variable count"},
		{"missing required", nil, nil, "receiver variable name is missing"},
		{"undeclared", map[string]any{"name": "business", "template_instance_key": "not-business"}, nil, "undeclared variable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			bus := &flowActivationTestBus{}
			manager := newFlowActivationManager(t, bus, instances, agents)
			bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"name": {Type: "string"}, "count": {Type: "integer"}, "flag": {Type: "boolean"},
				}, Required: []string{"name", "count", "flag"}},
			})
			declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{
				"name":  {Type: "string"},
				"count": {Type: "integer", HasDefault: true, Default: 3},
				"flag":  {Type: "boolean", HasDefault: true, Default: true},
			})
			setFlowActivationManagerSemanticSource(manager, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
			req.Config = tc.config
			err := manager.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("activation error = %v, want %s", err, tc.wantErr)
				}
				if len(instances.creates) != 0 || len(instances.readiness) != 0 || len(agents.upserts) != 0 || len(bus.addedRouteRequests) != 0 || len(bus.published) != 0 {
					t.Fatal("invalid receiver config reached activation consumers")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(instances.creates) != 1 || !reflect.DeepEqual(instances.creates[0].Config, tc.want) {
				t.Fatalf("persisted config = %#v, want %#v", instances.creates, tc.want)
			}
			wire, err := canonicaljson.MarshalPreservingNumberKinds(tc.want)
			if err != nil {
				t.Fatal(err)
			}
			cfg, found := testFlowActivationAgentConfig(t, manager, "reviewer", "review/one")
			if !found || string(cfg.Config) != string(wire) || len(bus.published) != 1 || string(bus.published[0].Payload()) != string(wire) {
				t.Fatalf("agent/autoemit did not consume admitted config: agent=%s events=%#v want=%s", cfg.Config, bus.published, wire)
			}
			if len(bus.addedRouteRequests) != 1 {
				t.Fatal("missing route consumer")
			}
			for key, value := range tc.want {
				if bus.addedRouteRequests[0].ActivationVariables[key] != stringifyPromptTemplateValue(value) {
					t.Fatalf("route variable %s did not consume admitted value", key)
				}
			}
			if tc.name == "defaults" && len(tc.config) != 1 {
				t.Fatal("default admission mutated caller config")
			}
		})
	}
}

func TestEnsureFlowInstanceReuseCannotReplaceCommittedConfigOrPendingAutoEmit(t *testing.T) {
	for _, entry := range []string{"ensure", "standing"} {
		for _, incoming := range []string{"changed", "missing"} {
			t.Run(entry+"/"+incoming, func(t *testing.T) {
				instances := &flowActivationTestInstanceStore{}
				agents := &flowActivationTestStore{}
				firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}, publishErr: errors.New("stop before creation publication")}
				first := newFlowActivationManager(t, firstBus, instances, agents)
				bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
					Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
						"name": {Type: "string"}, "priority": {Type: "integer"},
						"status": {Type: "boolean"}, "flow_path": {Type: "json"}, "nested": {Type: "json"},
					}, Required: []string{"name", "priority"}},
				})
				declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{
					"name": {Type: "string"}, "priority": {Type: "integer"},
					"status": {Type: "boolean"}, "flow_path": {Type: "json"}, "nested": {Type: "json"},
				})
				ctx := testAuthorActivityContext(context.Background())
				setFlowActivationManagerSemanticSource(first, semanticview.Wrap(bundle))
				req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
				committedConfig := map[string]any{
					"name": "committed", "priority": int64(7), "status": false, "flow_path": []any{"business", "path"},
					"nested": map[string]any{"values": []any{int64(7), float64(7), nil}},
				}
				req.Config = cloneFlowConfig(committedConfig)
				if err := first.ActivateFlowInstance(ctx, req); err == nil {
					t.Fatal("expected creation publication failure")
				}
				before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found || before.Plan.CreationEvent == nil || !before.CreationEventEmittedAt.IsZero() || len(instances.creates) != 1 {
					t.Fatalf("missing committed pending activation: %#v %v", before, err)
				}
				restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
				restarted := newFlowActivationManager(t, restartBus, instances, agents)
				setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
				if incoming == "changed" {
					req.Config = map[string]any{"name": "replacement", "priority": "invalid-new-value", "extra": true}
				} else {
					req.Config = nil
				}
				if entry == "standing" {
					created, finish, err := restarted.PrepareStandingFlowInstance(ctx, req)
					if err != nil || created || finish == nil {
						t.Fatalf("standing reuse: created=%v err=%v", created, err)
					}
					if err := finish(); err != nil {
						t.Fatal(err)
					}
				} else if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil || created {
					t.Fatalf("ensure reuse: created=%v err=%v", created, err)
				}
				after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found || !reflect.DeepEqual(before.Plan.CreationEvent, after.Plan.CreationEvent) || !reflect.DeepEqual(before.Plan.Agents, after.Plan.Agents) {
					t.Fatalf("reuse replaced pending creation or agent revisions: before=%#v after=%#v err=%v", before.Plan, after.Plan, err)
				}
				if len(restartBus.published) != 1 || string(restartBus.published[0].Payload()) != string(before.Plan.CreationEvent.Payload) {
					t.Fatalf("recovery did not consume committed payload: %#v", restartBus.published)
				}
				stored, found, err := instances.Load(ctx, testActivationFlowIdentity(req))
				if err != nil || !found || !reflect.DeepEqual(stored.Config, committedConfig) {
					t.Fatalf("reuse changed persisted business config: %#v %v", stored.Config, err)
				}
				for _, route := range restartBus.addedRouteRequests {
					if route.ActivationVariables["name"] != "committed" || route.ActivationVariables["priority"] != "7" {
						t.Fatalf("route consumed incoming config: %#v", route)
					}
				}
				cfg, found := testFlowActivationAgentConfig(t, restarted, "reviewer", "review/one")
				if !found {
					t.Fatal("recovered agent missing")
				}
				var config map[string]any
				if err := canonicaljson.DecodePreservingNumberLexemes(cfg.Config, &config); err != nil || config["name"] != "committed" || config["priority"] != json.Number("7") {
					t.Fatalf("agent consumed incoming config: %#v %v", config, err)
				}
				wantWire, err := canonicaljson.MarshalPreservingNumberKinds(committedConfig)
				if err != nil || string(cfg.Config) != string(wantWire) || string(restartBus.published[0].Payload()) != string(wantWire) {
					t.Fatalf("recovery changed business controls or nested numeric kinds: agent=%s event=%s want=%s err=%v", cfg.Config, restartBus.published[0].Payload(), wantWire, err)
				}
				if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil || created || len(instances.creates) != 1 || len(restartBus.published) != 1 {
					t.Fatalf("repeat ensure changed lifecycle: created=%v err=%v creates=%d emits=%d", created, err, len(instances.creates), len(restartBus.published))
				}
			})
		}
	}
}

func TestFlowActivationPreparedConfigDoesNotAliasNestedCallerValues(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	manager := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
	bundle := testFlowBundle(t, "")
	declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{"nested": {Type: "json"}})
	setFlowActivationManagerSemanticSource(manager, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
	req.Config = map[string]any{"nested": []any{map[string]any{"integer": int64(5), "double": float64(5), "number": json.Number("5.0"), "null": nil}}}
	plan, err := manager.PrepareFlowInstanceActivation(testAuthorActivityContext(context.Background()), req)
	if err != nil {
		t.Fatal(err)
	}
	req.Config["nested"].([]any)[0].(map[string]any)["integer"] = int64(99)
	got := plan.Instance.Config["nested"].([]any)[0].(map[string]any)
	if !reflect.DeepEqual(got, map[string]any{"integer": int64(5), "double": float64(5), "number": float64(5), "null": nil}) {
		t.Fatalf("prepared config aliased caller or changed number kinds: %#v", got)
	}
	if len(instances.creates) != 0 {
		t.Fatal("preparation mutated persistence")
	}
}

func TestReceiverConfigRecoverySourceRevisionDoesNotReadmitDefaults(t *testing.T) {
	for _, entry := range []string{"ensure", "startup"} {
		t.Run(entry, func(t *testing.T) {
			makeBundle := func(event string, priority int, revised bool) *runtimecontracts.WorkflowContractBundle {
				bundle := testFlowBundleWithAutoEmitEntry(t, event, runtimecontracts.EventCatalogEntry{
					Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
						"name": {Type: "string"}, "priority": {Type: "integer"},
					}, Required: []string{"name", "priority"}},
				})
				variables := map[string]runtimecontracts.FlowVariable{
					"name":     {Type: "string", HasDefault: true, Default: "committed"},
					"priority": {Type: "integer", HasDefault: true, Default: priority},
				}
				if revised {
					variables["new_default"] = runtimecontracts.FlowVariable{Type: "string", IsOptional: true, HasDefault: true, Default: "must-not-appear"}
				}
				declareReceiverConfig(t, bundle, variables)
				return bundle
			}
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}, publishErr: errors.New("stop before creation publication")}
			first := newFlowActivationManager(t, firstBus, instances, agents)
			bundle := makeBundle("task.started", 7, false)
			setFlowActivationManagerSemanticSource(first, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
			ctx := testAuthorActivityContext(context.Background())
			if err := first.ActivateFlowInstance(ctx, req); err == nil || len(instances.creates) != 1 {
				t.Fatalf("missing pending committed activation: %v", err)
			}
			before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || before.Plan.CreationEvent == nil {
				t.Fatalf("missing pending creation: %#v %v", before, err)
			}
			revised := makeBundle("task.revised", 99, true)
			restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
			restarted := newFlowActivationManager(t, restartBus, instances, agents)
			revisedFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("d", 64))
			if err != nil {
				t.Fatal(err)
			}
			setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(revised), revisedFact)
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, revisedFact)
			if entry == "startup" {
				if err := reconcileDynamicFlowRuntimeStartupForTest(restarted, ctx, revisedFact, true); err != nil {
					t.Fatal(err)
				}
			} else {
				revisedReq := testActivationRequest(revised, "review", "one", "parent", "review/one")
				revisedReq.Config = map[string]any{"priority": "incoming-must-not-be-read"}
				if created, err := restarted.EnsureFlowInstance(ctx, revisedReq); err != nil || created {
					t.Fatalf("ensure revised source: created=%v err=%v", created, err)
				}
			}
			after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || after.Plan.BundleHash != revisedFact.BundleHash() || after.Plan.BundleHash == before.Plan.BundleHash || after.Plan.CreationEvent == nil || string(after.Plan.CreationEvent.Payload) != string(before.Plan.CreationEvent.Payload) {
				t.Fatalf("revised creation lost stored config: %#v %v", after, err)
			}
			if len(restartBus.published) != 1 || string(restartBus.published[0].Type()) != "review/one/task.revised" || string(restartBus.published[0].Payload()) != `{"name":"committed","priority":7}` {
				t.Fatalf("revised autoemit did not consume stored config: %#v", restartBus.published)
			}
			stored, found, err := instances.Load(ctx, testActivationFlowIdentity(req))
			if err != nil || !found || !reflect.DeepEqual(stored.Config, map[string]any{"name": "committed", "priority": int64(7)}) {
				t.Fatalf("recovery reapplied defaults: %#v %v", stored.Config, err)
			}
			cfg, found := testFlowActivationAgentConfig(t, restarted, "reviewer", "review/one")
			if !found || string(cfg.Config) != `{"name":"committed","priority":7}` {
				t.Fatalf("agent recovery reapplied defaults: %s", cfg.Config)
			}
		})
	}
}

func declareReceiverConfig(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, variables map[string]runtimecontracts.FlowVariable) {
	t.Helper()
	schema := bundle.FlowSchemas["review"]
	schema.InstanceVariables = runtimecontracts.FlowInstanceVariables{Variables: variables}
	bundle.FlowSchemas["review"] = schema
	bundle.FlowTree.ByID["review"].Schema = schema
	compileFlowActivationFixture(t, bundle)
}

func TestTemplateFlowMaterializationRequiresExactCommittedConfig(t *testing.T) {
	bundle := testFlowBundle(t, "")
	declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{
		"key": {Type: "string"}, "nested": {Type: "json"},
		"new_default": {Type: "string", IsOptional: true, HasDefault: true, Default: "must-not-be-applied"},
	})
	source := semanticview.Wrap(bundle)
	const path = "review/ti-not-the-business-key"
	if plan, err := TemplateFlowMaterialization(source, "review", path, "entity-1", nil); err == nil || !strings.Contains(err.Error(), "exact committed receiver configuration") || len(plan.Agents) != 0 {
		t.Fatalf("missing evidence acquired materialization: %#v %v", plan, err)
	}
	config := map[string]any{"key": "original-business-key", "nested": []any{map[string]any{"integer": int64(7), "double": float64(7), "null": nil}}}
	plan, err := TemplateFlowMaterialization(source, "review", path, "entity-1", config)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := canonicaljson.MarshalPreservingNumberKinds(config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.Config, config) || len(plan.Agents) != 1 || string(plan.Agents[0].Config.Config) != string(wire) || plan.ActivationVariables["key"] != "original-business-key" {
		t.Fatalf("materialization lost committed config or reapplied defaults: %#v", plan)
	}
	config["nested"].([]any)[0].(map[string]any)["integer"] = int64(99)
	if got := plan.Config["nested"].([]any)[0].(map[string]any)["integer"]; got != int64(7) {
		t.Fatalf("materialized config aliased input: %#v", got)
	}
}
