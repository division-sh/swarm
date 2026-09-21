package bus

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverInitializationEventBusAdmissionAndReuse(t *testing.T) {
	for _, tc := range []struct {
		name, payload              string
		reuse, descriptor, invalid bool
	}{
		{"typed zero false", `{"account_id":"acct-1","count":0,"ratio":2.0,"active":false,"label":"kept","attributes":{"nested":[1,2.0]}}`, false, false, false},
		{"defaults", `{"account_id":"acct-1","active":true,"label":"kept","attributes":[]}`, false, false, false},
		{"missing required", `{"account_id":"acct-1","active":true,"attributes":{}}`, false, false, true},
		{"null is not default", `{"account_id":"acct-1","count":null,"active":true,"label":"kept","attributes":{}}`, false, false, true},
		{"wrong integer", `{"account_id":"acct-1","count":"3","active":true,"label":"kept","attributes":{}}`, false, false, true},
		{"descriptor reuse missing initialization", `{"account_id":"acct-1"}`, true, true, false},
		{"route reuse missing initialization", `{"account_id":"acct-1"}`, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyReceiverInitialization(t))
			plan := mustInstanceKeyConnectRoutePlan(t, source)
			// The fixture also has a create pin; choose the exact initializing input.
			plans, issues := compiledConnectPlans(source)
			if len(issues) != 0 {
				t.Fatal(issues)
			}
			for _, candidate := range plans {
				if candidate.InstanceKey() != nil && candidate.InstanceKey().Mode() == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
					plan = candidate
				}
			}
			keys := []runtimecontracts.TemplateInstanceKeyValue{{Field: mustBusTemplateInstanceField(t, "account_id"), Value: "acct-1"}}
			instance := plan.DeriveReceiverIdentity(source, templateInstanceLifecycleInstanceID(plan, keys))
			store := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}}
			if tc.descriptor {
				store.flowInstances = []ActiveFlowInstanceDescriptor{{InstanceID: instance.InstanceID, EntityID: instance.EntityID, FlowInstance: instance.InstancePath, FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-1"}}}
			}
			owner := newTestFlowInstanceActivationOwner(store.Activate)
			bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstancePlanner: owner})
			if err != nil {
				t.Fatal(err)
			}
			store.bus = bus
			if tc.reuse {
				store.setTargetOwnerRoutes(plan.ReceiverRoute(instance.InstancePath, instance.EntityID))
				store.workflowInstances = []runtimepipeline.WorkflowInstance{{
					EntityID: instance.EntityID, WorkflowName: "account", InstanceID: instance.InstanceID,
					StorageRef: instance.InstancePath, EntityType: "account_state", CurrentState: "active", Status: "active",
					Fields: map[string]any{"account_id": "acct-1"},
				}}
				if err := bus.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: testRunScopedFlowRoute(instance.Route())}); err != nil {
					t.Fatal(err)
				}
			}
			eventID := eventtest.UUID("initialize-" + tc.name)
			event := connectRoutePlanStaticProducerEvent(eventID, events.EventType("producer/account.ready"), "", "", json.RawMessage(tc.payload), 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
			preview, err := bus.CheckPublishRecipientPlan(context.Background(), event)
			if tc.invalid {
				if err == nil && preview.TargetFailure == "" {
					t.Fatal("invalid initializer passed preparation")
				}
				if len(store.activations) != 0 || len(store.routes[eventID]) != 0 {
					t.Fatal("invalid preparation mutated receiver or delivery")
				}
				if err := bus.Publish(context.Background(), event); err == nil {
					t.Fatal("invalid initializer passed publication")
				}
				if len(store.activations) != 0 || len(store.routes[eventID]) != 0 {
					t.Fatal("invalid publication mutated receiver or delivery")
				}
				return
			}
			if err != nil || preview.TargetFailure != "" {
				t.Fatalf("prepare: %v %s", err, preview.TargetFailure)
			}
			if len(store.activations) != 0 {
				t.Fatal("preflight performed creation")
			}
			if err := bus.Publish(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if tc.reuse {
				if len(store.activations) != 0 {
					t.Fatal("reuse reinitialized receiver")
				}
			} else {
				if len(store.activations) != 1 {
					t.Fatalf("activations=%d", len(store.activations))
				}
				config := store.activations[0].Config
				if config["account_id"] != "acct-1" || config["label"] != "kept" {
					t.Fatalf("config=%#v", config)
				}
				if tc.name == "typed zero false" && (config["count"] != int64(0) || config["ratio"] != float64(2) || config["active"] != false || !reflect.DeepEqual(config["attributes"], map[string]any{"nested": []any{int64(1), float64(2)}})) {
					t.Fatalf("typed values drifted: %#v", config)
				}
				if tc.name == "defaults" && (config["count"] != int64(3) || config["ratio"] != float64(2)) {
					t.Fatalf("defaults=%#v", config)
				}
			}
			if len(store.routes[eventID]) != 1 {
				t.Fatalf("delivery count=%d", len(store.routes[eventID]))
			}
			if got := store.routes[eventID][0].Target.Route(); got != plan.ReceiverRoute(instance.InstancePath, instance.EntityID) {
				t.Fatalf("receiver ownership=%#v", got)
			}
		})
	}
}
