package runtime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestRuleResultEventsFlowThroughDurableCallbackDelivery(t *testing.T) {
	tests := []struct {
		name            string
		requestEventID  string
		requestID       string
		mvpYAML         string
		resultEventName string
		resultKind      string
	}{
		{
			name:            "success",
			requestEventID:  "99999999-9999-4999-8999-999999999941",
			requestID:       "99999999-9999-4999-8999-999999999951",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
		},
		{
			name:            "failure",
			requestEventID:  "99999999-9999-4999-8999-999999999942",
			requestID:       "99999999-9999-4999-8999-999999999952",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repoNodeID := artifactActionResultNodeID(t)
			resultEventType := "repo-scaffold/inst-1/" + tc.resultEventName
			bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyArtifactActionResultDelivery(t, "template", false))
			source := semanticview.Wrap(bundle)
			pg := storetest.StartPostgresRuntimeStore(t)
			ctx := seedRuntimeTestRunForSource(t, pg, source)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
				SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
				ContractBundle:     source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			module := newRuntimeTestWorkflowModule(t, source)
			resultHandlerStarted := make(chan string, 4)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
				SourceArtifactFact:  runtimeTestSourceArtifactFact(t, source),
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              module,
				Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
				RunLifecycle:        pg,
				PipelineObligations: pg.PipelineObligations(),
				DeliveryStore:       pg,
				TestWorkflowNodeHandlerStartHook: func(_ context.Context, nodeID string, evt events.Event) error {
					if strings.TrimSpace(nodeID) == repoNodeID && strings.TrimSpace(string(evt.Type())) == resultEventType {
						select {
						case resultHandlerStarted <- evt.ID():
						default:
						}
					}
					return nil
				},
			})

			instance := artifactActionResultWorkflowInstance()
			instance.InstanceKey = "inst-1"
			instance.WorkflowVersion = source.WorkflowVersion()
			seedRuntimeTestKeylessSource(t, ctx, pg, pc, semanticview.RootExecutionFlowID(source))
			constructed := seedRuntimeTestPreparedInstance(t, ctx, pg, pc, instance)
			assertRuntimeTestConstructedInstance(t, ctx, pg, source, constructed.Plan.Identity)

			requestPayload, err := json.Marshal(map[string]any{
				"request_id": tc.requestID,
				"mvp_yaml":   tc.mvpYAML,
			})
			if err != nil {
				t.Fatalf("marshal request payload: %v", err)
			}
			requestEvent := eventtest.ExistingRunRootIngressWithRoutingSource(
				tc.requestEventID,
				events.EventType("repo-scaffold/inst-1/repo_scaffold.repo_commit_requested"),
				"test",
				"",
				requestPayload,
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForSourceRoute(
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), "repo-scaffold/inst-1"),
					events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID},
				),
				eventtest.ConcreteTemplateRoutingSource("repo-scaffold", "repo-scaffold/inst-1", artifactActionResultEntityID),
				time.Now().UTC(),
			)

			if err := bus.Publish(ctx, requestEvent); err != nil {
				t.Fatalf("Publish request event: %v", err)
			}
			resultEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateSourceNamedEventIdentity", func() (string, error) {
				return storetest.ReadTemplateSourceNamedEventIdentity(ctx, pg, resultEventType, tc.requestEventID)
			})
			assertArtifactActionResultEventContext(t, ctx, pg, resultEventID, tc.resultKind, "repo-scaffold/inst-1", artifactActionResultEntityID)
			assertArtifactActionResultNodeRoute(t, ctx, pg, resultEventID, "repo-scaffold/inst-1", artifactActionResultEntityID)
			waitArtifactActionResultHandlerStarted(t, ctx, pg, resultHandlerStarted, resultEventID)
			waitArtifactActionResultSettledCount(t, ctx, pg, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON("repo-scaffold/inst-1", artifactActionResultEntityID), repoNodeID)
			waitRuntimeNodeDeliveryOutcome(t, ctx, pg, resultEventID, repoNodeID)
			assertArtifactActionResultDiagnosticReadback(t, ctx, pg, resultEventID, repoNodeID)
		})
	}
}

func TestRuleResultEventsFlowThroughStaticServiceCallbackDelivery(t *testing.T) {
	tests := []struct {
		name            string
		requestEventID  string
		requestID       string
		mvpYAML         string
		resultEventName string
		resultKind      string
		requestFlowPath string
		wantFlowPath    string
	}{
		{
			name:            "success",
			requestEventID:  "99999999-9999-4999-8999-999999999961",
			requestID:       "99999999-9999-4999-8999-999999999971",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
			requestFlowPath: "repo-scaffold",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "failure",
			requestEventID:  "99999999-9999-4999-8999-999999999962",
			requestID:       "99999999-9999-4999-8999-999999999972",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
			requestFlowPath: "repo-scaffold",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "connected_child_inbound_success",
			requestEventID:  "99999999-9999-4999-8999-999999999963",
			requestID:       "99999999-9999-4999-8999-999999999973",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
			requestFlowPath: "repo-scaffold/child-1",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "connected_child_inbound_failure",
			requestEventID:  "99999999-9999-4999-8999-999999999964",
			requestID:       "99999999-9999-4999-8999-999999999974",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
			requestFlowPath: "repo-scaffold/child-1",
			wantFlowPath:    "repo-scaffold",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repoNodeID := artifactActionResultNodeID(t)
			resultEventType := "repo-scaffold/" + tc.resultEventName
			childRequest := tc.requestFlowPath != tc.wantFlowPath
			bundle := loadRuntimeBundleRoot(t, canonicalrouting.CopyArtifactActionResultDelivery(t, "static", childRequest))
			source := semanticview.Wrap(bundle)
			pg := storetest.StartPostgresRuntimeStore(t)
			ctx := seedRuntimeTestRunForSource(t, pg, source)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
				ContractBundle:     source,
				SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			module := newRuntimeTestWorkflowModule(t, source)
			resultHandlerStarted := make(chan string, 4)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
				SourceArtifactFact:  runtimeTestSourceArtifactFact(t, source),
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              module,
				Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
				RunLifecycle:        pg,
				PipelineObligations: pg.PipelineObligations(),
				DeliveryStore:       pg,
				TestWorkflowNodeHandlerStartHook: func(_ context.Context, nodeID string, evt events.Event) error {
					if strings.TrimSpace(nodeID) == repoNodeID && strings.TrimSpace(string(evt.Type())) == resultEventType {
						select {
						case resultHandlerStarted <- evt.ID():
						default:
						}
					}
					return nil
				},
			})

			root := runtimeflowidentity.Stored(source, ".", templateInstanceDeliveryRunID, templateInstanceDeliveryRunID, templateInstanceDeliveryRunID, "")
			parent, err := runtimeflowidentity.KeylessChild(source, root, "repo-scaffold")
			if err != nil {
				t.Fatal(err)
			}
			instances := []runtimeflowidentity.Instance{root, parent}
			if childRequest {
				child, err := runtimeflowidentity.KeylessChild(source, parent, tc.requestFlowPath)
				if err != nil {
					t.Fatal(err)
				}
				instances = append(instances, child)
			}
			for _, coordinate := range instances {
				if err := coordinate.ValidateConstruction(source, templateInstanceDeliveryRunID); err != nil {
					t.Fatal(err)
				}
				topology, found := semanticview.WorkflowStageTopology(source, coordinate.TemplateID)
				if !found {
					t.Fatal("component receiver topology is missing")
				}
				initial, err := topology.InitialStoredStage()
				if err != nil {
					t.Fatal(err)
				}
				fields, _ := entityruntime.ResolveForFlow(source, coordinate.TemplateID)
				seedRuntimeTestPreparedInstance(t, ctx, pg, pc, runtimepipeline.WorkflowInstance{
					InstanceID: coordinate.InstanceID, StorageRef: coordinate.InstancePath, EntityID: coordinate.EntityID,
					EntityType: fields.EntityType, Fields: map[string]any{},
					ParentFlowID: coordinate.ParentRoute.FlowID, ParentFlowInstance: coordinate.ParentRoute.FlowInstance, ParentEntityID: coordinate.ParentEntityID,
					WorkflowName: coordinate.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: "static",
					CurrentState: initial.ID(), StageDefined: topology.StageCount() != 0,
				})
			}

			requestPayload, err := json.Marshal(map[string]any{
				"request_id": tc.requestID,
				"mvp_yaml":   tc.mvpYAML,
			})
			if err != nil {
				t.Fatalf("marshal request payload: %v", err)
			}
			requestEvent := eventtest.ExistingRunRootIngressWithRoutingSource(
				tc.requestEventID,
				events.EventType("repo-scaffold/repo_scaffold.repo_commit_requested"),
				"test",
				"",
				requestPayload,
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForSourceRoute(
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, parent.EntityID), tc.requestFlowPath),
					events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: tc.requestFlowPath, EntityID: parent.EntityID},
				),
				eventtest.StaticFlowRoutingSource("repo-scaffold", tc.requestFlowPath, parent.EntityID), time.Now().UTC(),
			)
			if childRequest {
				requestEvent = eventtest.ExistingRunRootIngressWithRoutingSource(tc.requestEventID,
					events.EventType("start.requested"), "test", "", requestPayload, 0, templateInstanceDeliveryRunID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, root.EntityID),
					eventtest.RootRoutingSource(root.EntityID), time.Now().UTC())
			}

			if err := bus.Publish(ctx, requestEvent); err != nil {
				t.Fatalf("Publish request event: %v", err)
			}

			requestEventID := tc.requestEventID
			if childRequest {
				requestEventID = waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateSourceNamedEventIdentity", func() (string, error) {
					return storetest.ReadTemplateSourceNamedEventIdentity(ctx, pg, "repo-scaffold/child-1/repo_scaffold.repo_commit_requested", tc.requestEventID)
				})
				rawSource, err := storetest.ReadPostgresArtifactCallbackSourceRoute(ctx, pg, requestEventID)
				if err != nil {
					t.Fatal(err)
				}
				var sourceRoute events.RouteIdentity
				if err := json.Unmarshal([]byte(rawSource), &sourceRoute); err != nil {
					t.Fatal(err)
				}
				if sourceRoute.FlowID != tc.requestFlowPath || sourceRoute.FlowInstance != tc.requestFlowPath || sourceRoute.EntityID != runtimeflowidentity.EntityID(tc.requestFlowPath) || sourceRoute.EntityID == parent.EntityID {
					t.Fatalf("child request borrowed parent state ownership: %s", rawSource)
				}
			}
			resultEventID := waitRuntimeStoredEventID(t, ctx, pg, "ReadTemplateSourceNamedEventIdentity", func() (string, error) {
				return storetest.ReadTemplateSourceNamedEventIdentity(ctx, pg, resultEventType, requestEventID)
			})
			assertArtifactActionResultEventContext(t, ctx, pg, resultEventID, tc.resultKind, tc.wantFlowPath, parent.EntityID)
			assertArtifactActionResultNodeRoute(t, ctx, pg, resultEventID, tc.wantFlowPath, parent.EntityID)
			waitArtifactActionResultHandlerStarted(t, ctx, pg, resultHandlerStarted, resultEventID)
			waitArtifactActionResultSettledCount(t, ctx, pg, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON(tc.wantFlowPath, parent.EntityID), repoNodeID)
			waitRuntimeNodeDeliveryOutcome(t, ctx, pg, resultEventID, repoNodeID)
			assertArtifactActionResultDiagnosticReadback(t, ctx, pg, resultEventID, repoNodeID)
		})
	}
}

const artifactActionResultEntityID = "22222222-2222-4222-8222-222222222222"

func artifactActionResultNodeID(t testing.TB) string {
	t.Helper()
	return identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node").Key()
}

func artifactActionResultWorkflowInstance() runtimepipeline.WorkflowInstance {
	enteredAt := time.Now().UTC()
	return runtimepipeline.WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      "repo-scaffold/inst-1",
		EntityID:        artifactActionResultEntityID,
		WorkflowName:    "repo-scaffold",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		EnteredStageAt:  enteredAt,
		CreatedAt:       enteredAt,
		Fields:          map[string]any{},
		EntityType:      "test_entity",
	}
}

func assertArtifactActionResultEventContext(t *testing.T, ctx context.Context, selected any, eventID, resultKind, wantFlowInstance, wantEntityID string) {
	t.Helper()
	observed, err := storetest.ReadPostgresArtifactCallbackEventContext(ctx, selected, eventID)
	if err != nil {
		t.Fatalf("query result event context: %v", err)
	}
	entityID, flowInstance, sourceRouteJSON, payloadJSON := observed.EntityID, observed.FlowInstance, observed.SourceRoute, observed.Payload
	if entityID != wantEntityID {
		t.Fatalf("result event entity_id = %q, want %q", entityID, wantEntityID)
	}
	if flowInstance != wantFlowInstance {
		t.Fatalf("result event flow_instance = %q, want %s", flowInstance, wantFlowInstance)
	}
	var sourceRoute map[string]any
	if err := json.Unmarshal([]byte(sourceRouteJSON), &sourceRoute); err != nil {
		t.Fatalf("decode source route %q: %v", sourceRouteJSON, err)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["flow_id"])); got != "repo-scaffold" {
		t.Fatalf("source route flow_id = %q, want repo-scaffold: %#v", got, sourceRoute)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["flow_instance"])); got != wantFlowInstance {
		t.Fatalf("source route flow_instance = %q, want %s: %#v", got, wantFlowInstance, sourceRoute)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["entity_id"])); got != wantEntityID {
		t.Fatalf("source route entity_id = %q, want %s: %#v", got, wantEntityID, sourceRoute)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	if got := strings.TrimSpace(asRuntimeTestString(payload["result_kind"])); got != resultKind {
		t.Fatalf("result payload result_kind = %q, want %q: %#v", got, resultKind, payload)
	}
}

func assertArtifactActionResultNodeRoute(t *testing.T, ctx context.Context, selected any, eventID, wantFlowInstance, wantEntityID string) {
	t.Helper()
	wantRoute := artifactActionResultDeliveryTargetRouteJSON(wantFlowInstance, wantEntityID)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := storetest.CountPostgresTemplateNodeTarget(ctx, selected, eventID, wantRoute, artifactActionResultNodeID(t))
		if err != nil {
			t.Fatalf("query result delivery route: %v", err)
		}
		if got == 1 {
			return
		}
		if time.Now().After(deadline) {
			rows, err := storetest.ReadPostgresArtifactCallbackTargetDiagnostics(ctx, selected, eventID)
			if err != nil {
				t.Fatalf("query result delivery route debug rows: %v", err)
			}
			t.Fatalf("delivery route for event %s missing route %s; rows=%s", eventID, wantRoute, rows)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitArtifactActionResultHandlerStarted(t *testing.T, ctx context.Context, selected any, started <-chan string, eventID string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-started:
			if got == eventID {
				return
			}
		case <-deadline:
			deliveries, err := storetest.ReadPostgresArtifactCallbackDeliveries(ctx, selected, eventID)
			if err != nil {
				t.Fatalf("query callback delivery diagnostics: %v", err)
			}
			receipts, err := storetest.ReadPostgresArtifactCallbackReceipts(ctx, selected, eventID)
			if err != nil {
				t.Fatalf("query callback receipt diagnostics: %v", err)
			}
			t.Fatalf("callback handler did not start for result event %s; deliveries=%s receipts=%s", eventID, deliveries, receipts)
		}
	}
}

func waitArtifactActionResultSettledCount(t *testing.T, ctx context.Context, selected any, want int, eventID, target, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := storetest.CountPostgresArtifactCallbackSettledTarget(ctx, selected, eventID, target, nodeID)
		if err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			diagnostics, _ := storetest.ReadPostgresArtifactCallbackSettlementDiagnostics(ctx, selected, eventID)
			t.Fatalf("count = %d, want %d for settled callback event %s node %s target %s; deliveries=%s", got, want, eventID, nodeID, target, diagnostics)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func artifactActionResultDeliveryTargetRouteJSON(flowInstance, entityID string) string {
	return `{"kind":"existing_entity","route":{"flow_instance":"` + flowInstance + `","entity_id":"` + entityID + `"}}`
}

func assertArtifactActionResultDiagnosticReadback(t *testing.T, ctx context.Context, selected any, eventID, nodeID string) {
	t.Helper()
	for _, read := range []func(context.Context, any, string) (string, error){storetest.ReadPostgresArtifactCallbackTargetDiagnostics, storetest.ReadPostgresArtifactCallbackDeliveries} {
		raw, err := read(ctx, selected, eventID)
		var rows []struct {
			SubscriberID string `json:"subscriber_id"`
		}
		if err != nil || json.Unmarshal([]byte(raw), &rows) != nil {
			t.Fatalf("decode native callback diagnostic: %s/%v", raw, err)
		}
		found := false
		for _, row := range rows {
			found = found || row.SubscriberID == nodeID
		}
		if !found {
			t.Fatalf("callback diagnostic lost real node: %s", raw)
		}
	}
	receipts, err := storetest.ReadPostgresArtifactCallbackReceipts(ctx, selected, eventID)
	var receiptRows []struct {
		EventID      string `json:"event_id"`
		SubscriberID string `json:"subscriber_id"`
		Outcome      string `json:"outcome"`
	}
	if err != nil || json.Unmarshal([]byte(receipts), &receiptRows) != nil || len(receiptRows) != 1 || receiptRows[0].EventID != eventID || receiptRows[0].SubscriberID != "pipeline" || receiptRows[0].Outcome != "success" {
		t.Fatalf("callback receipt lost actual pipeline identity/outcome: %s/%v", receipts, err)
	}
	raw, err := storetest.ReadPostgresArtifactCallbackSettlementDiagnostics(ctx, selected, eventID)
	var combined struct {
		Deliveries []struct {
			Status   string
			Outcomes []struct{ Outcome string }
		}
		Receipts []json.RawMessage
	}
	if err != nil || json.Unmarshal([]byte(raw), &combined) != nil || len(combined.Deliveries) == 0 || len(combined.Receipts) == 0 {
		t.Fatalf("combined callback diagnostic lost actual outcomes/receipts: %s/%v", raw, err)
	}
	found := false
	for _, row := range combined.Deliveries {
		for _, outcome := range row.Outcomes {
			found = found || (row.Status == "delivered" && outcome.Outcome == "delivered")
		}
	}
	if !found {
		t.Fatalf("callback diagnostic lost actual settled attempt: %s", raw)
	}
}

func asRuntimeTestString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}
