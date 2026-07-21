package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestTemplateFlowPilotRuntime_ParentConnectCreatesTemplateInstanceAndPersistedDeliveryRoute(t *testing.T) {
	bundle := templateflowpilot.LoadBundle(t, templateflowpilot.Options{})
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("template-flow pilot hard invalidities = %#v, want none", got)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	var manager *runtimemanager.AgentManager
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle: source,
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				t.Fatal("agent manager not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		WorkOwner:         runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances: workflowStore,
	}))

	evt := eventtest.RunCreatingRootIngress(
		"99999999-9999-4999-8999-999999999952",
		events.EventType("producer/account.ready"),
		"producer",
		"",
		json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
		0,
		templateInstanceDeliveryRunID,
		"",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: "producer", FlowInstance: "producer", EntityID: "88888888-8888-4888-8888-888888888888",
		}),
		time.Now().UTC(),
	)
	preflight, err := bus.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one deterministic template route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if target := preflight.DeliveryRoutes[0].Target; target.FlowID != "account" || !strings.HasPrefix(target.FlowInstance, "account/") || target.EntityID == "" {
		t.Fatalf("preflight target = %#v, want account template flow instance", target)
	}
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM flow_instances
		WHERE flow_template = 'account'
	`, 0)

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish validation request: %v", err)
	}
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'account-node'
	`, 1, evt.ID())
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id IN ('workflow-runtime', 'raw-source-listener')
	`, 0, evt.ID())

	flowInstance, entityID := loadTemplateFlowPilotInstanceIdentity(t, ctx, db)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'account-node'
		  AND delivery_target_route @> $2::jsonb
	`, 1, evt.ID(), templateFlowPilotDeliveryTargetRouteJSON(t, events.RouteIdentity{
		FlowID:       "account",
		FlowInstance: flowInstance,
		EntityID:     entityID,
	}))
	loaded, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.StorageRef != flowInstance || loaded.WorkflowName != "account" || loaded.CurrentState != "pending" {
		t.Fatalf("loaded account instance = storage:%q workflow:%q state:%q, want %s/account/pending", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState, flowInstance)
	}
	if loaded.Metadata["account_id"] != "acct-1" {
		t.Fatalf("loaded account metadata = %#v, want account_id from route activation", loaded.Metadata)
	}
}

func TestTemplateFlowPilotRuntime_FailsClosedForMissingAndAmbiguousKeys(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	tests := []struct {
		name          string
		payload       json.RawMessage
		flowInstances []runtimebus.ActiveFlowInstanceDescriptor
		wantFailure   string
	}{
		{
			name:        "missing producer key",
			payload:     json.RawMessage(`{"score":"91","decision":"approved"}`),
			wantFailure: string(runtimepinrouting.ConnectFailureAddressValueMissing),
		},
		{
			name:    "ambiguous receiver key",
			payload: json.RawMessage(`{"account_id":"acct-1","score":"91","decision":"approved"}`),
			flowInstances: []runtimebus.ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: "11111111-1111-4111-8111-111111111111", FlowInstance: "account/one", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
				{InstanceID: "two", EntityID: "22222222-2222-4222-8222-222222222222", FlowInstance: "account/two", FlowTemplate: "account", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
			},
			wantFailure: string(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &templateFlowPilotMemoryStore{flowInstances: tc.flowInstances}
			bus, err := newScopedTestEventBus(t, store, runtimebus.EventBusOptions{
				ContractBundle: source,
				TemplateInstanceActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error {
					t.Fatal("fail-closed route must not activate a template instance")
					return nil
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			evt := eventtest.RunCreatingRootIngress(
				"99999999-9999-4999-8999-999999999953",
				events.EventType("producer/account.ready"),
				"producer",
				"",
				tc.payload,
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID: "producer", FlowInstance: "producer", EntityID: "88888888-8888-4888-8888-888888888888",
				}),
				time.Now().UTC(),
			)
			plan, err := bus.CheckPublishRecipientPlan(testAuthorActivityContext(context.Background()), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if plan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", plan.TargetFailure, tc.wantFailure)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("fail-closed route exposed executable plan: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			if err := bus.Publish(testAuthorActivityContext(context.Background()), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if routes := store.deliveryRoutes[evt.ID()]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none", routes)
			}
		})
	}
}

type templateFlowPilotMemoryStore struct {
	runtimebus.InMemoryEventStore
	flowInstances  []runtimebus.ActiveFlowInstanceDescriptor
	deliveryRoutes map[string][]events.DeliveryRoute
}

func (s *templateFlowPilotMemoryStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return append([]runtimebus.ActiveFlowInstanceDescriptor(nil), s.flowInstances...), nil
}

func (s *templateFlowPilotMemoryStore) InsertEventDeliveryRoutes(_ context.Context, eventID string, routes []events.DeliveryRoute) error {
	if s.deliveryRoutes == nil {
		s.deliveryRoutes = map[string][]events.DeliveryRoute{}
	}
	s.deliveryRoutes[eventID] = events.NormalizeDeliveryRoutes(routes)
	return nil
}

func loadTemplateFlowPilotInstanceIdentity(t *testing.T, ctx context.Context, db *sql.DB) (string, string) {
	t.Helper()
	var flowInstance string
	var entityID string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_id::text
		FROM entity_state
		WHERE flow_instance LIKE 'account/%'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&flowInstance, &entityID); err != nil {
		t.Fatalf("load account instance identity: %v", err)
	}
	return flowInstance, entityID
}

func templateFlowPilotDeliveryTargetRouteJSON(t *testing.T, target events.RouteIdentity) string {
	t.Helper()
	encoded, err := json.Marshal(target.Normalized())
	if err != nil {
		t.Fatalf("marshal template-flow pilot delivery target: %v", err)
	}
	return string(encoded)
}
