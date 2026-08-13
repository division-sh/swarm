package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

type connectRoutePlanTestFlow struct {
	id           string
	mode         string
	inputs       []runtimecontracts.FlowInputEventPin
	outputs      []runtimecontracts.FlowOutputEventPin
	agents       map[string]runtimecontracts.AgentRegistryEntry
	nodes        map[string]runtimecontracts.SystemNodeContract
	entityFields map[string]runtimecontracts.EntityFieldDecl
}

type providerOutputAuthorizedTestSource struct {
	semanticview.Source
	generation     triggergeneration.Generation
	authorizations []runtimeprovideroutput.Authorization
}

func (s providerOutputAuthorizedTestSource) SemanticCapabilities() semanticview.Capabilities {
	return s.Source.SemanticCapabilities().WithProviderTriggerEvents(s.Source, s.generation, s.authorizations)
}

type connectRoutePlanDescriptorStore struct {
	*targetRouteMemoryStore
	flowInstances               []ActiveFlowInstanceDescriptor
	flowInstanceDescriptorCalls int
	flowInstanceDescriptorErr   error
}

type connectRoutePlanMutationStore struct {
	*targetRouteMemoryStore
}

type connectRoutePlanLifecycleStore struct {
	*connectRoutePlanDescriptorStore
	bus                             *EventBus
	activations                     []runtimepipeline.FlowInstanceActivationRequest
	failAfterDescriptorWithoutRoute error
}

type connectRoutePlanConcurrentLifecycleStore struct {
	*connectRoutePlanLifecycleStore
	mu sync.Mutex
}

type connectRoutePlanStaleSnapshotStore struct {
	*connectRoutePlanLifecycleStore
	mutations int
	mutating  bool
}

type apiEventPublicationMemoryStore struct {
	*connectRoutePlanLifecycleStore
	completion apiidempotency.Completion
}

func (s *apiEventPublicationMemoryStore) LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	if len(s.completion.Response) == 0 {
		return apiidempotency.Completion{}, false, nil
	}
	return s.completion, true, nil
}

func (s *apiEventPublicationMemoryStore) CommitAPIEventPublication(ctx context.Context, command APIEventPublicationCommand) (CommittedAPIEventPublication, error) {
	if len(s.completion.Response) > 0 {
		return CommittedAPIEventPublication{Completion: s.completion, Replay: true}, nil
	}
	publication, err := s.CommitPublication(ctx, command.Publication)
	if err != nil {
		return CommittedAPIEventPublication{}, err
	}
	s.completion = command.Completion
	return CommittedAPIEventPublication{Publication: publication, Completion: command.Completion}, nil
}

func (s *connectRoutePlanDescriptorStore) ReplaceFlowInstanceRouteTopology(_ context.Context, sets []FlowInstanceRouteRecordSet) error {
	return validateFlowInstanceRouteTopology(sets)
}

type connectRoutePlanNodeInterceptor struct {
	mu    sync.Mutex
	count int
}

type connectRoutePlanReplyMutationStore struct {
	creates int
	claims  int
}

func (s *connectRoutePlanReplyMutationStore) CreateReplyContext(context.Context, runtimereplycontext.Record) error {
	s.creates++
	return nil
}

func (*connectRoutePlanReplyMutationStore) LoadReplyContext(context.Context, string) (runtimereplycontext.Record, error) {
	return runtimereplycontext.Record{}, runtimereplycontext.ErrNotFound
}

func (s *connectRoutePlanReplyMutationStore) ClaimReplyContext(context.Context, string, string) (runtimereplycontext.Record, runtimereplycontext.ClaimOutcome, error) {
	s.claims++
	return runtimereplycontext.Record{}, runtimereplycontext.ClaimAccepted, nil
}

func (*connectRoutePlanNodeInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return true, nil, runtimepipelineobligation.Continue(), nil
}

func (i *connectRoutePlanNodeInterceptor) InterceptDeliveryRoute(context.Context, events.DeliveryEvent, events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.count++
	return false, nil, runtimepipelineobligation.Continue(), nil
}

func (i *connectRoutePlanNodeInterceptor) Count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.count
}

func TestConnectRoutePlanEventConsumersEnforceProducerMode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		eventType string
		source    events.RouteIdentity
		want      bool
	}{
		{name: "static exact scope", mode: "static", eventType: "producer/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"}, want: true},
		{name: "static descendant", mode: "static", eventType: "producer/inst-1/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}},
		{name: "template concrete instance", mode: "template", eventType: "producer/inst-1/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}, want: true},
		{name: "template base scope", mode: "template", eventType: "producer/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"}},
	} {
		t.Run("issue/"+tc.name, func(t *testing.T) {
			if !tc.source.Empty() {
				tc.source.EntityID = eventtest.UUID("entity-1")
			}
			source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
				id: "producer", mode: tc.mode,
				outputs: []runtimecontracts.FlowOutputEventPin{{Name: "deploy_done", Event: "deploy.done"}},
			}}, []runtimecontracts.FlowPackageConnect{{From: "producer.deploy_done", To: "missing.ready"}}))
			graph := runtimepinrouting.CompileConnectGraph(source)
			issues := graph.Issues()
			if len(issues) != 1 || issues[0].Failure != runtimepinrouting.ConnectFailureReceiverFlowMissing {
				t.Fatalf("compiled issues = %#v, want receiver-flow failure", issues)
			}
			evt := connectRoutePlanStaticProducerEvent("", events.EventType(tc.eventType), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{Source: tc.source}, time.Unix(1, 0).UTC())
			if got := graph.IssueMatchesEvent(issues[0], evt); got != tc.want {
				t.Fatalf("connectIssueMatchesEvent = %v, want %v", got, tc.want)
			}
		})
	}
	diagnosticOnlyIssue := runtimepinrouting.ConnectRoutePlanIssue{
		Connect: runtimecontracts.FlowPackageConnect{From: "producer.deploy_done", To: "missing.ready"},
		Failure: runtimepinrouting.ConnectFailureReceiverFlowMissing,
	}
	diagnosticEvent := connectRoutePlanStaticProducerEvent("", "producer/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
		Source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("entity-1")},
	}, time.Unix(1, 0).UTC())
	diagnosticSource := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
		id: "producer", mode: "static",
		outputs: []runtimecontracts.FlowOutputEventPin{{Name: "deploy_done", Event: "deploy.done"}},
	}}, nil))
	if runtimepinrouting.CompileConnectGraph(diagnosticSource).IssueMatchesEvent(diagnosticOnlyIssue, diagnosticEvent) {
		t.Fatal("diagnostic-only connect issue re-entered event matching")
	}

	rootSource := connectRoutePlanRootProducerStaticSource()
	rootBundle, ok := semanticview.Bundle(rootSource)
	if !ok {
		t.Fatal("root source has no contract bundle")
	}
	invalidRootConnect := runtimecontracts.FlowPackageConnect{From: ".root_ready", To: "missing.ready", SourceFile: "package.yaml", SourceLine: 1}
	rootBundle.Package.Connect = []runtimecontracts.FlowPackageConnect{invalidRootConnect}
	rootBundle.Semantics.CompositionConnects = []runtimecontracts.FlowPackageConnect{invalidRootConnect}
	rootGraph := runtimepinrouting.CompileConnectGraph(rootSource)
	rootIssues := rootGraph.Issues()
	if len(rootIssues) != 1 {
		t.Fatalf("root issues = %#v, want one", rootIssues)
	}
	rootIssue := rootIssues[0]
	for _, tc := range []struct {
		name         string
		flowInstance string
		want         bool
	}{
		{name: "empty-source child context", flowInstance: "child/inst-1"},
		{name: "UUID context is not source authority", flowInstance: "11111111-1111-4111-8111-111111111111"},
	} {
		t.Run("issue/root/"+tc.name, func(t *testing.T) {
			evt := connectRoutePlanStaticProducerEvent("", "root.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
				FlowInstance: tc.flowInstance,
			}, time.Unix(1, 0).UTC())
			if got := rootGraph.IssueMatchesEvent(rootIssue, evt); got != tc.want {
				t.Fatalf("connectIssueMatchesEvent = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConnectRoutePlanReceiverPinCollisionFailsClosedAcrossSupportedSurfaces(t *testing.T) {
	for _, producerMode := range []string{"static", "template"} {
		for _, rootReceiver := range []bool{false, true} {
			for _, subscriberType := range []string{"node", "agent"} {
				name := strings.Join([]string{producerMode, map[bool]string{false: "flow", true: "root"}[rootReceiver], subscriberType}, "/")
				t.Run(name, func(t *testing.T) {
					runID := uuid.NewString()
					ctx := runtimecorrelation.WithRunID(context.Background(), runID)
					source := connectReceiverPinCollisionSource(producerMode, rootReceiver, subscriberType, false)
					store := newTargetRouteMemoryStore()
					producerEntityID := eventtest.UUID("producer-entity")
					receiverEntityID := eventtest.UUID("receiver-entity")
					receiverFlowInstance := "consumer"
					if rootReceiver {
						receiverEntityID = eventtest.UUID("root-entity")
						receiverFlowInstance = runID
					}
					store.setTargetOwners(ActiveTargetDescriptor{
						ID: "receiver-owner", FlowInstance: receiverFlowInstance, EntityID: receiverEntityID,
					})
					routeTable, err := DeriveRouteTable(source)
					if err != nil {
						t.Fatalf("DeriveRouteTable: %v", err)
					}
					if rootReceiver {
						for _, localEvent := range []string{"work.accepted", "work.audited"} {
							identity := agentidentity.Identity{}
							recipient := events.MustNodeDeliveryRecipient("receiver")
							if subscriberType == "agent" {
								identity = connectRoutePlanTestDeclaredAgentIdentity(t, source, "", "receiver", "")
								recipient = events.MustAgentDeliveryRecipient("receiver")
							}
							subscriber := Subscriber{
								Recipient:     recipient,
								MatchPattern:  localEvent,
								routeSource:   subscriberRouteSourceRootInputProject,
								AgentIdentity: identity,
							}
							if subscriber.Recipient.IsNode() {
								subscriber.handlerFlowID = source.WorkflowName()
								subscriber.handlerNodeID = "receiver"
								subscriber.targetHandler, err = runtimepipeline.AdmitDeliveryTargetHandler(source, subscriber.handlerFlowID, subscriber.handlerNodeID)
								if err != nil {
									t.Fatalf("admit root target handler: %v", err)
								}
							}
							routeTable.rootInputRoutes[localEvent] = appendUniqueRootInputSubscriber(routeTable.rootInputRoutes[localEvent], subscriber)
							if err := routeTable.addConnectRecipientLocked("", nil, localEvent, subscriber, ""); err != nil {
								t.Fatalf("admit root receiver: %v", err)
							}
						}
						routeTable.rebuildLocked()
					}
					eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routeTable})
					if err != nil {
						t.Fatalf("NewEventBusWithOptions: %v", err)
					}
					eventType := events.EventType("producer/work.ready")
					sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}
					if producerMode == "template" {
						eventType = "producer/inst-1/work.ready"
						sourceRoute.FlowInstance = "producer/inst-1"
					}
					envelope := events.EventEnvelope{Source: sourceRoute}
					if rootReceiver {
						envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{EntityID: receiverEntityID})
					}
					eventID := uuid.NewString()
					evt := connectRoutePlanStaticProducerEvent(eventID, eventType, "", "", []byte(`{}`), 0, runID, "", envelope, time.Now().UTC())
					if subscriberType == "agent" {
						flowPath := "consumer"
						if rootReceiver {
							flowPath = ""
						}
						identity := connectRoutePlanTestDeclaredAgentIdentity(t, source, map[bool]string{false: "consumer", true: ""}[rootReceiver], "receiver", flowPath)
						admission := testAgentSubscriptionAdmissionForFlow(t, "receiver", flowPath, "work.accepted", "work.audited")
						if ch := subscribeTestAgentAdmissionWithIdentity(t, eb, admission, identity, receiverEntityID); ch == nil {
							t.Fatal("install typed root-agent subscription")
						}
					}

					plan, err := eb.CheckPublishRecipientPlan(ctx, evt)
					if err != nil {
						t.Fatalf("CheckPublishRecipientPlan: %v", err)
					}
					if got, want := plan.TargetFailure, runtimepinrouting.ConnectFailureDeliveryTopologyInvalid.Code(); got != want {
						t.Fatalf("target failure = %q, want %q; plan=%#v", got, want, plan)
					}
					if len(plan.DeliveryRoutes) != 0 {
						t.Fatalf("delivery routes = %#v, want none", plan.DeliveryRoutes)
					}
					if err := eb.Publish(ctx, evt); err != nil {
						t.Fatalf("Publish classified target failure: %v", err)
					}
					if routes := store.routes[eventID]; len(routes) != 0 {
						t.Fatalf("persisted delivery routes = %#v, want none", routes)
					}
				})
			}
		}
	}
}

func TestConnectRoutePlanReceiverPinCollisionGuardPreservesLegalFanoutAndDuplicateEdges(t *testing.T) {
	source := connectReceiverPinCollisionSource("static", false, "node", true)
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(), "producer/work.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{Source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("producer-entity")}}, time.Now().UTC())
	identity := connectRoutePlanTestDeclaredAgentIdentity(t, source, "consumer", "legal-agent", "consumer")
	subscribeTestAgentAdmissionWithIdentity(
		t,
		eb,
		testAgentSubscriptionAdmissionForFlow(t, "legal-agent", "consumer", "work.accepted"),
		identity,
		connectRoutePlanStaticOwner().EntityID,
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, runtimepinrouting.ConnectFailureDeliveryTopologyInvalid.Code(); got != want {
		t.Fatalf("mixed fanout target failure = %q, want %q", got, want)
	}

	for _, tc := range []struct {
		name       string
		shape      string
		wantRoutes int
	}{
		{name: "distinct subscribers", shape: "distinct_subscribers", wantRoutes: 2},
		{name: "duplicate same edge", shape: "duplicate_edge", wantRoutes: 1},
	} {
		t.Run("public "+tc.name, func(t *testing.T) {
			store := newConnectRoutePlanStaticStore()
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: connectReceiverPinLegalSource(tc.shape)})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			eventID := uuid.NewString()
			evt := connectRoutePlanStaticProducerEvent(eventID, "producer/work.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{Source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("producer-entity")}}, time.Now().UTC())
			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != tc.wantRoutes {
				t.Fatalf("preflight = failure:%q routes:%#v, want %d legal routes", plan.TargetFailure, plan.DeliveryRoutes, tc.wantRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if routes := store.routes[eventID]; len(routes) != tc.wantRoutes {
				t.Fatalf("persisted routes = %#v, want %d", routes, tc.wantRoutes)
			}
		})
	}
}

func TestConnectRoutePlanReceiverPinCollisionFailsBeforeReplyContextMutation(t *testing.T) {
	source := connectReceiverPinCollisionSource("static", false, "node", false)
	routeTable, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	replyStore := &connectRoutePlanReplyMutationStore{}
	resolver := newConnectRoutePlanResolver(source, routeTable, nil, nil, nil, replyStore)
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(), "producer/work.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
		Source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("producer-entity")},
	}, time.Now().UTC())

	dispatch, err := resolver.Plan(context.Background(), evt)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got, want := dispatch.Failure, connectRoutePlanTargetFailure(runtimepinrouting.ConnectFailureDeliveryTopologyInvalid); got != want {
		t.Fatalf("dispatch failure = %q, want %q", got, want)
	}
	if replyStore.creates != 0 || replyStore.claims != 0 || dispatch.ReplyContextConsumed {
		t.Fatalf("reply mutations = create:%d claim:%d consumed:%v, want none", replyStore.creates, replyStore.claims, dispatch.ReplyContextConsumed)
	}
}

func connectRoutePlanStaticProducerEvent(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	route := envelope.Source.Normalized()
	if route.Empty() {
		route = events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("producer-entity")}
	}
	var (
		source events.RoutingSource
		err    error
	)
	if route.FlowInstance == route.FlowID {
		source, err = events.NewStaticFlowRoutingSource(route)
	} else {
		source, err = events.NewConcreteTemplateInstanceRoutingSource(route)
	}
	if err != nil {
		panic(err)
	}
	return eventtest.RunCreatingRootIngressWithRoutingSource(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, source, createdAt)
}

func connectRoutePlanRootProducerEvent(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	source, err := events.NewRootRoutingSource(eventtest.UUID("root-source-entity"))
	if err != nil {
		panic(err)
	}
	return eventtest.RunCreatingRootIngressWithRoutingSource(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, source, createdAt)
}

func connectRoutePlanConcreteProducerEvent(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: "child", FlowInstance: "child/inst-9", EntityID: eventtest.UUID("child-source-entity"),
	})
	if err != nil {
		panic(err)
	}
	return eventtest.RunCreatingRootIngressWithRoutingSource(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, source, createdAt)
}

func connectReceiverPinCollisionSource(producerMode string, rootReceiver bool, subscriberType string, mixed bool) semanticview.Source {
	inputs := []runtimecontracts.FlowInputEventPin{
		{Name: "work_accepted", Event: "work.accepted"},
		{Name: "work_audited", Event: "work.audited"},
	}
	producer := connectRoutePlanTestFlow{
		id: "producer", mode: producerMode,
		outputs: []runtimecontracts.FlowOutputEventPin{{Name: "work_ready", Event: "work.ready"}},
	}
	connects := []runtimecontracts.FlowPackageConnect{
		{From: "producer.work_ready", To: "consumer.work_accepted", Adapter: "work_ready_to_accepted"},
		{From: "producer.work_ready", To: "consumer.work_audited", Adapter: "work_ready_to_audited"},
	}
	consumer := connectRoutePlanTestFlow{id: "consumer", mode: "static", inputs: inputs}
	if subscriberType == "agent" {
		consumer.agents = map[string]runtimecontracts.AgentRegistryEntry{
			"receiver": {ID: "receiver", Subscriptions: []string{"work.accepted", "work.audited"}},
		}
	} else {
		consumer.nodes = map[string]runtimecontracts.SystemNodeContract{
			"receiver": {ID: "receiver", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"work.accepted": existingOwnerHandlerFixture(),
				"work.audited":  existingOwnerHandlerFixture(),
			}},
		}
		if mixed {
			consumer.agents = map[string]runtimecontracts.AgentRegistryEntry{
				"legal-agent": {ID: "legal-agent", Subscriptions: []string{"work.accepted"}},
			}
		}
	}
	bundle := connectRoutePlanTestBundle([]connectRoutePlanTestFlow{producer, consumer}, connects)
	if !rootReceiver {
		return semanticview.Wrap(bundle)
	}
	bundle.Semantics.Name = "root"
	bundle.Semantics.CompositionConnects[0].To = ".work_accepted"
	bundle.Semantics.CompositionConnects[1].To = ".work_audited"
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{Events: connectRoutePlanInputEvents(inputs), EventPins: inputs}}}
	bundle.Semantics.FlowInputs[""] = connectRoutePlanInputEvents(inputs)
	bundle.Semantics.FlowInputEventPins[""] = inputs
	bundle.Nodes = consumer.nodes
	bundle.Agents = consumer.agents
	for logicalID := range consumer.agents {
		bundle.URIRegistry.Agents["root/"+logicalID] = runtimecontracts.ContractURIRef{
			Kind: "agent", LocalID: logicalID, Full: "test://root/" + logicalID,
		}
	}
	bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	for _, node := range consumer.nodes {
		bundle.Semantics.NodeHandlers[node.ID] = node.EventHandlers
	}
	return semanticview.Wrap(bundle)
}

func connectReceiverPinLegalSource(shape string) semanticview.Source {
	inputs := []runtimecontracts.FlowInputEventPin{
		{Name: "work_accepted", Event: "work.accepted"},
		{Name: "work_audited", Event: "work.audited"},
	}
	connects := []runtimecontracts.FlowPackageConnect{
		{From: "producer.work_ready", To: "consumer.work_accepted", Adapter: "work_ready_to_accepted"},
		{From: "producer.work_ready", To: "consumer.work_audited", Adapter: "work_ready_to_audited"},
	}
	nodes := map[string]runtimecontracts.SystemNodeContract{
		"accept-node": {ID: "accept-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.accepted": existingOwnerHandlerFixture()}},
		"audit-node":  {ID: "audit-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.audited": existingOwnerHandlerFixture()}},
	}
	if shape == "duplicate_edge" {
		inputs = inputs[:1]
		connects[1] = connects[0]
		nodes = map[string]runtimecontracts.SystemNodeContract{
			"accept-node": {ID: "accept-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.accepted": existingOwnerHandlerFixture()}},
		}
	}
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Name: "work_ready", Event: "work.ready"}}},
		{id: "consumer", mode: "static", inputs: inputs, nodes: nodes},
	}, connects))
}

func runConnectRoutePlanCommitScope(ctx context.Context, _ any, fn func(context.Context) error) error {
	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 2)
	rollback := make([]runtimepipelinefixture.OwnerAction, 0, 2)
	ctx = runtimepipelinefixture.WithPostCommitActions(ctx, &postCommit)
	ctx = runtimepipelinefixture.WithRollbackActions(ctx, &rollback)
	if err := fn(ctx); err != nil {
		runtimepipelinefixture.FlushRollbackActions(rollback)
		return err
	}
	runtimepipelinefixture.FlushPostCommitActions(postCommit)
	return nil
}

func (s *connectRoutePlanDescriptorStore) ListActiveFlowInstanceDescriptors(context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	s.flowInstanceDescriptorCalls++
	if s.flowInstanceDescriptorErr != nil {
		return nil, s.flowInstanceDescriptorErr
	}
	return exactAuthorActivityFlowInstanceDescriptors(s.flowInstances, "1.0.0"), nil
}

func (s *connectRoutePlanLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	for _, descriptor := range s.flowInstances {
		descriptor = descriptor.Normalized()
		if descriptor.InstanceID == req.Instance.InstanceID || descriptor.FlowInstance == req.Instance.InstancePath {
			return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "connect-route-plan-test", "activate", map[string]any{"flow_instance": req.Instance.InstancePath})
		}
	}
	s.activations = append(s.activations, req)
	s.flowInstances = append(s.flowInstances, ActiveFlowInstanceDescriptor{
		InstanceID:    req.Instance.InstanceID,
		EntityID:      req.Instance.EntityID,
		FlowInstance:  req.Instance.InstancePath,
		FlowTemplate:  req.Instance.TemplateID,
		AddressFields: connectRoutePlanActivationAddressFields(req.Metadata),
	})
	if s.failAfterDescriptorWithoutRoute != nil {
		return s.failAfterDescriptorWithoutRoute
	}
	if s.bus == nil {
		return nil
	}
	return s.bus.AddFlowInstanceRouteContext(ctx, FlowInstanceRouteMaterializationRequest{
		Identity:            req.Instance.Route(),
		ActivationVariables: connectRoutePlanActivationVariables(req),
	})
}

func (s *connectRoutePlanConcurrentLifecycleStore) ListActiveFlowInstanceDescriptors(ctx context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flowInstanceDescriptorCalls++
	return exactAuthorActivityFlowInstanceDescriptors(s.flowInstances, "1.0.0"), nil
}

func (s *connectRoutePlanStaleSnapshotStore) ListActiveFlowInstanceDescriptors(ctx context.Context) ([]ActiveFlowInstanceDescriptor, error) {
	descriptors, err := s.connectRoutePlanLifecycleStore.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil || s.mutations <= 0 || s.bus == nil || s.mutating {
		return descriptors, err
	}
	ordinal := s.mutations
	s.mutations--
	s.mutating = true
	defer func() { s.mutating = false }()
	if err := s.bus.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("consumer", fmt.Sprintf("stale-%d", ordinal)),
	}); err != nil {
		return nil, err
	}
	return descriptors, nil
}

func (s *connectRoutePlanConcurrentLifecycleStore) Activate(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	s.mu.Lock()
	for _, descriptor := range s.flowInstances {
		descriptor = descriptor.Normalized()
		if descriptor.InstanceID == req.Instance.InstanceID || descriptor.FlowInstance == req.Instance.InstancePath {
			s.mu.Unlock()
			return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "connect-route-plan-test", "activate", map[string]any{"flow_instance": req.Instance.InstancePath})
		}
	}
	s.activations = append(s.activations, req)
	s.flowInstances = append(s.flowInstances, ActiveFlowInstanceDescriptor{
		InstanceID:    req.Instance.InstanceID,
		EntityID:      req.Instance.EntityID,
		FlowInstance:  req.Instance.InstancePath,
		FlowTemplate:  req.Instance.TemplateID,
		AddressFields: connectRoutePlanActivationAddressFields(req.Metadata),
	})
	bus := s.bus
	s.mu.Unlock()
	if bus == nil {
		return nil
	}
	return bus.AddFlowInstanceRouteContext(ctx, FlowInstanceRouteMaterializationRequest{
		Identity:            req.Instance.Route(),
		ActivationVariables: connectRoutePlanActivationVariables(req),
	})
}

func (s *connectRoutePlanConcurrentLifecycleStore) ActivationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activations)
}

func (s *connectRoutePlanConcurrentLifecycleStore) FlowInstanceDescriptors() []ActiveFlowInstanceDescriptor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ActiveFlowInstanceDescriptor(nil), s.flowInstances...)
}

func connectRoutePlanActivationAddressFields(metadata map[string]any) map[string]string {
	out := map[string]string{}
	for key, raw := range metadata {
		key = strings.TrimSpace(key)
		if key == "" || key == "entity_type" || key == "instance_kind" || key == "last_source_event" {
			continue
		}
		value := strings.TrimSpace(fmt.Sprint(raw))
		if value != "" {
			out["entity."+key] = value
		}
	}
	return out
}

func connectRoutePlanActivationVariables(req runtimepipeline.FlowInstanceActivationRequest) map[string]string {
	out := map[string]string{}
	for _, values := range []map[string]any{req.Config, req.Metadata} {
		for key, raw := range values {
			key = strings.TrimSpace(key)
			value := strings.TrimSpace(fmt.Sprint(raw))
			if key != "" && value != "" {
				out[key] = value
			}
		}
	}
	return out
}

func (s *targetRouteMemoryStore) UpsertCommittedReplayScope(_ context.Context, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	if s.scopes == nil {
		s.scopes = map[string]runtimepipelineobligation.CommittedScope{}
	}
	s.scopes[eventID] = scope
	return nil
}

func TestStaticConnectRouteUsesExactPersistedTargetOwner(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"ignored":"yes"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
		EntityID:     connectRoutePlanStaticOwner().EntityID,
	}),
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) {
		t.Fatalf("route plan delivery routes = %#v, want %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("preflight delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target)
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node") || !containsString(internal, "consumer-node") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_ConnectRoutePlanRejectsConflictingAdmittedTargetBeforePersistence(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(
		eventID,
		events.EventType("producer/deploy.done"),
		"",
		"",
		json.RawMessage(`{"ignored":"yes"}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID:       "unrelated",
			FlowInstance: "unrelated/one",
			EntityID:     eventtest.UUID("unrelated-one"),
		}),
		time.Now().UTC(),
	)

	err = eb.Publish(context.Background(), evt)
	if err == nil || !strings.Contains(err.Error(), "connect route facts conflict with the admitted event target") {
		t.Fatalf("Publish error = %v, want connect route fact conflict", err)
	}
	if _, persisted := store.events[eventID]; persisted {
		t.Fatal("conflicting event was persisted")
	}
	if len(store.routes[eventID]) != 0 {
		t.Fatalf("conflicting event delivery routes = %#v, want none", store.routes[eventID])
	}
}

func TestEventBusConnectRouteDeliversToLiveAgentCarrier(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"consumer-agent": {
					ID:            "consumer-agent",
					Subscriptions: []string{"deploy.completed"},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:    "producer.deploy_done",
		To:      "consumer.deploy_completed",
		Adapter: "deploy_done_to_completed",
	}}))
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	admission := testAgentSubscriptionAdmissionForFlow(t, "consumer-agent", "consumer", events.EventType("deploy.completed")).CarrierOnly()
	identity := connectRoutePlanTestDeclaredAgentIdentity(t, source, "consumer", "consumer-agent", "consumer")
	ch := subscribeTestAgentAdmissionWithIdentity(t, eb, admission, identity, connectRoutePlanStaticOwner().EntityID)
	if ch == nil {
		t.Fatal("typed carrier-only agent admission returned no channel")
	}
	defer unsubscribeTestAgent(eb, "consumer-agent")

	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(), events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.SubscriptionRecipients) != 0 {
		t.Fatalf("subscription recipients = %#v, want none for carrier-only agent", plan.SubscriptionRecipients)
	}
	if len(plan.DeliveryRoutes) != 1 || !plan.DeliveryRoutes[0].Recipient.IsAgent() || plan.DeliveryRoutes[0].Recipient.ID() != "consumer-agent" {
		t.Fatalf("delivery routes = %#v, want canonical connect route to agent/consumer-agent", plan.DeliveryRoutes)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.ID() != evt.ID() {
			t.Fatalf("delivered event = %q, want %q", got.ID(), evt.ID())
		}
		if err := got.Complete(); err != nil {
			t.Fatalf("complete canonical connect delivery: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canonical connect did not wake live agent carrier")
	}
}

func connectRoutePlanTestDeclaredAgentIdentity(
	t testing.TB,
	source semanticview.Source,
	flowID string,
	logicalID string,
	flowPath string,
) agentidentity.Identity {
	t.Helper()
	owner, ok := semanticview.AgentDeclarationOwner(source, flowID, logicalID)
	if !ok {
		t.Fatalf("resolve declaration owner for %s/%s", flowID, logicalID)
	}
	name, err := agentidentity.DeclaredName(logicalID, owner)
	if err != nil {
		t.Fatalf("build declared agent name: %v", err)
	}
	route := agentidentity.RootRoute()
	if strings.TrimSpace(flowPath) != "" {
		route, err = runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
		if err != nil {
			t.Fatalf("build declared agent route: %v", err)
		}
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("build declared agent identity: %v", err)
	}
	return identity
}

func connectRoutePlanLifecycleAgentRoute(
	t *testing.T,
	source semanticview.Source,
	secondPin canonicalrouting.TemplateInstanceSecondPin,
) (agentidentity.Identity, semanticview.FlowOwnedAgentSubscriptionAdmission, string) {
	t.Helper()
	plan := mustInstanceKeyConnectRoutePlan(t, source)
	instanceID := templateInstanceLifecycleInstanceID(plan, []runtimecontracts.TemplateInstanceKeyValue{{
		Field: mustBusTemplateInstanceField(t, "vertical_id"), Value: "v-1",
	}})
	instance := runtimeflowidentity.Derive(source, "consumer", instanceID)
	identity := connectRoutePlanTestDeclaredAgentIdentity(t, source, "consumer", "consumer-agent", instance.InstancePath)
	subscriptions := []events.EventType{"deploy.done"}
	if secondPin == canonicalrouting.TemplateInstanceSecondPinDistinctEvent {
		subscriptions = append(subscriptions, "deploy.audited")
	}
	return identity, testAgentSubscriptionAdmissionForFlow(t, "consumer-agent", instance.InstancePath, subscriptions...), instance.EntityID
}

func TestEventBusPublish_RootConnectRoutePlanPersistsSingularTarget(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	rootInstanceID := uuid.NewString()
	evt := connectRoutePlanRootProducerEvent(eventID,
		events.EventType("root.ready"), "", "", json.RawMessage(`{"entity_id":"entity-1"}`), 0, "", "",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, rootInstanceID), time.Now().UTC())

	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
		EntityID:     connectRoutePlanStaticOwner().EntityID,
	}),
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched root connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) {
		t.Fatalf("route plan delivery routes = %#v, want %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("preflight delivery routes = %#v, want %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target)
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
}

func TestEventBusPublish_RootConnectToNestedStaticPersistsExactCurrentOwner(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	rootTarget := events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: uuid.NewString(), EntityID: eventtest.UUID("root-source-entity"),
	}.Normalized()
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("root-dispatcher"),
		Target:    events.MustExistingEntityTarget(rootTarget),
	})
	eventID := uuid.NewString()
	evt := connectRoutePlanRootProducerEvent(
		eventID, events.EventType("root.ready"), "root-dispatcher", "", json.RawMessage(`{"request_id":"r-1"}`), 0,
		rootTarget.FlowInstance, "", events.EnvelopeForEntityID(events.EnvelopeForFlowInstance(events.EventEnvelope{}, rootTarget.FlowInstance), rootTarget.EntityID), time.Now().UTC(),
	)
	want := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("consumer-node"),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: "consumer", FlowInstance: "consumer", EntityID: rootTarget.EntityID,
		}),
	}

	preflight, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || !deliveryRoutesContain(preflight.DeliveryRoutes, want) {
		t.Fatalf("preflight failure/routes = %q/%#v, want exact structurally proved route %#v", preflight.TargetFailure, preflight.DeliveryRoutes, want)
	}
	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 1 || !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target.Route())
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(ctx, evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node") || !containsString(internal, "consumer-node") || len(replayRoutes) != 1 || !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay live/internal/routes = %#v/%#v/%#v, want exact persisted structural owner", live, internal, replayRoutes)
	}
}

func TestEventBusPublish_RootConnectStructuralOwnerSourceDisagreementFailsBeforePersistence(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	currentTarget := events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: uuid.NewString(), EntityID: eventtest.UUID("unrelated-current-owner"),
	}.Normalized()
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("root-dispatcher"),
		Target:    events.MustExistingEntityTarget(currentTarget),
	})
	evt := connectRoutePlanRootProducerEvent(
		uuid.NewString(), events.EventType("root.ready"), "root-dispatcher", "", json.RawMessage(`{"request_id":"hostile"}`), 0,
		currentTarget.FlowInstance, "", events.EnvelopeForEntityID(events.EnvelopeForFlowInstance(events.EventEnvelope{}, currentTarget.FlowInstance), currentTarget.EntityID), time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(ctx, evt); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want unproved structural owner rejection", err)
	}
	if err := eb.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
		t.Fatalf("Publish error = %v, want unproved structural owner rejection", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 || len(store.receipts) != 0 || len(store.flowRoutes) != 0 {
		t.Fatalf("rejected publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes)
	}
}

func TestEventBusPublish_RootConnectToSingletonUsesReceiverOwnedMaterializingTarget(t *testing.T) {
	source := connectRoutePlanRootProducerSingletonSource()
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	rootTarget := events.RouteIdentity{
		FlowID: source.WorkflowName(), FlowInstance: uuid.NewString(), EntityID: eventtest.UUID("root-source-entity"),
	}.Normalized()
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("root-dispatcher"),
		Target:    events.MustExistingEntityTarget(rootTarget),
	})
	eventID := uuid.NewString()
	evt := connectRoutePlanRootProducerEvent(
		eventID, events.EventType("root.ready"), "root-dispatcher", "", json.RawMessage(`{"request_id":"r-1"}`), 0,
		rootTarget.FlowInstance, "", events.EnvelopeForEntityID(events.EnvelopeForFlowInstance(events.EventEnvelope{}, rootTarget.FlowInstance), rootTarget.EntityID), time.Now().UTC(),
	)
	want := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("scout-node"),
		Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
			FlowID: "scout", FlowInstance: "scout", EntityID: runtimeflowidentity.EntityID("scout"),
		}),
	}
	if want.Target.Route().EntityID == rootTarget.EntityID {
		t.Fatal("test identities must distinguish root causal/current owner from singleton receiver owner")
	}

	preflight, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || !deliveryRoutesContain(preflight.DeliveryRoutes, want) {
		t.Fatalf("preflight failure/routes = %q/%#v, want exact receiver-owned route %#v", preflight.TargetFailure, preflight.DeliveryRoutes, want)
	}
	if got := preflight.DeliveryRoutes[0].Target.Route(); got.EntityID == rootTarget.EntityID {
		t.Fatalf("preflight receiver target reused current root owner: %#v", got)
	}

	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 1 || !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target.Route())
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(ctx, evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "scout-node") || !containsString(internal, "scout-node") || len(replayRoutes) != 1 || !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay live/internal/routes = %#v/%#v/%#v, want persisted singleton receiver owner", live, internal, replayRoutes)
	}
}

func TestEventBusPublish_SingletonConnectToRootUsesExactSelectedRootOwner(t *testing.T) {
	source := connectRoutePlanSingletonProducerRootReceiverSource()
	store := newTargetRouteMemoryStore()
	runID := uuid.NewString()
	rootEntityID := eventtest.UUID("selected-root-owner")
	store.setTargetOwners(ActiveTargetDescriptor{
		ID: "selected-root-owner", FlowInstance: runID, EntityID: rootEntityID,
	})
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	singletonEntityID := runtimeflowidentity.EntityID("scout")
	if singletonEntityID == rootEntityID {
		t.Fatal("test identities must distinguish singleton source from root receiver owner")
	}
	sourceRoute := events.RouteIdentity{
		FlowID: "scout", FlowInstance: "scout", EntityID: singletonEntityID,
	}
	routingSource, err := events.NewStaticFlowRoutingSource(sourceRoute)
	if err != nil {
		t.Fatalf("singleton routing source: %v", err)
	}
	eventID := uuid.NewString()
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{
		EntityID: singletonEntityID, FlowInstance: "scout",
	}, sourceRoute)
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID, events.EventType("scout/scout.completed"), "scout-worker", "", []byte(`{}`), 0,
		runID, envelope, routingSource, time.Now().UTC(),
	)
	want := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("root-collector"),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: rootEntityID,
		}),
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	preflight, err := eb.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || !deliveryRoutesContain(preflight.DeliveryRoutes, want) {
		t.Fatalf("preflight failure/routes = %q/%#v, want exact selected root route %#v", preflight.TargetFailure, preflight.DeliveryRoutes, want)
	}
	if got := preflight.DeliveryRoutes[0].Target.Route(); got.EntityID == singletonEntityID {
		t.Fatalf("root receiver reused singleton source entity: %#v", got)
	}

	if err := eb.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 1 || !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target.Route())
	}
}

func TestEventBusPublish_SingletonConnectToRootRejectsMissingOrAmbiguousSelectedOwnerBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		owners    func(string) []ActiveTargetDescriptor
		wantError string
	}{
		{name: "missing", owners: func(string) []ActiveTargetDescriptor { return nil }, wantError: "target owner is missing"},
		{name: "ambiguous", owners: func(runID string) []ActiveTargetDescriptor {
			return []ActiveTargetDescriptor{
				{ID: "root-owner-a", FlowInstance: runID, EntityID: eventtest.UUID("root-owner-a")},
				{ID: "root-owner-b", FlowInstance: runID, EntityID: eventtest.UUID("root-owner-b")},
			}
		}, wantError: "target owner is ambiguous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanSingletonProducerRootReceiverSource()
			store := newTargetRouteMemoryStore()
			runID := uuid.NewString()
			store.setTargetOwners(tc.owners(runID)...)
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			singletonEntityID := runtimeflowidentity.EntityID("scout")
			sourceRoute := events.RouteIdentity{FlowID: "scout", FlowInstance: "scout", EntityID: singletonEntityID}
			routingSource, err := events.NewStaticFlowRoutingSource(sourceRoute)
			if err != nil {
				t.Fatalf("singleton routing source: %v", err)
			}
			eventID := uuid.NewString()
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, events.EventType("scout/scout.completed"), "scout-worker", "", []byte(`{}`), 0,
				runID, events.EnvelopeForSourceRoute(events.EventEnvelope{EntityID: singletonEntityID, FlowInstance: "scout"}, sourceRoute),
				routingSource, time.Now().UTC(),
			)
			ctx := runtimecorrelation.WithRunID(context.Background(), runID)
			if _, err := eb.CheckPublishRecipientPlan(ctx, evt); err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("CheckPublishRecipientPlan error = %v, want %q", err, tc.wantError)
			}
			if err := eb.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Publish error = %v, want %q", err, tc.wantError)
			}
			if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 || len(store.receipts) != 0 || len(store.flowRoutes) != 0 || len(store.claims) != 0 || len(store.scans) != 0 || len(store.active) != 0 {
				t.Fatalf("rejected publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v claims=%#v scans=%#v active=%#v",
					store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes, store.claims, store.scans, store.active)
			}
		})
	}
}

func TestEventBusPublish_RootConnectRoutePlanDoesNotCaptureChildScopedSameNameEvent(t *testing.T) {
	source := connectRoutePlanRootProducerStaticSource()
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(
		testSelectedRunTargetOwner("consumer-a-owner", "consumer-a", "consumer-a-owner"),
		testSelectedRunTargetOwner("consumer-b-owner", "consumer-b", "consumer-b-owner"),
	)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanConcreteProducerEvent(
		eventID,
		events.EventType("root.ready"),
		"",
		"",
		json.RawMessage(`{"entity_id":"entity-1"}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, "child/inst-9"),
		time.Now().UTC(),
	)

	forbidden := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
		EntityID:     connectRoutePlanStaticOwner().EntityID,
	}),
	}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityOwner == routePlanSourceConnectRoutePlan || routePlan.AuthorityState == RoutePlanAuthorityCanonicalMatched {
		t.Fatalf("route plan authority = %q/%q, root connect must not match child-scoped same-name event", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if deliveryRoutesContain(routePlan.DeliveryRoutes(), forbidden) {
		t.Fatalf("route plan delivery routes = %#v, must not include root-connect receiver for child-scoped same-name event", routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if deliveryRoutesContain(plan.DeliveryRoutes, forbidden) {
		t.Fatalf("preflight delivery routes = %#v, must not include root-connect receiver for child-scoped same-name event", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; deliveryRoutesContain(routes, forbidden) {
		t.Fatalf("persisted delivery routes = %#v, must not include root-connect receiver for child-scoped same-name event", routes)
	}
}

func TestEventBusCheckPublishRecipientPlan_ConnectRoutePlanUsesSelectedOwner(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("preflight delivery routes = %#v, want exact selected-owner static connect route", plan.DeliveryRoutes)
	}
}

func TestConnectRecipientEvaluationUsesCompiledReceiverPin(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{From: "producer.deploy_done", To: "consumer.deploy_completed"})
	graph := runtimepinrouting.CompileConnectGraph(source)
	plans := graph.Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled plans = %d, want 1", len(plans))
	}
	plan := plans[0]
	recipient, err := runtimepinrouting.NewConnectNodeRecipient("consumer-node", "consumer", "consumer", "consumer-node")
	if err != nil {
		t.Fatalf("recipient: %v", err)
	}
	registrations := graph.AdmitReceiverRecipient("consumer", "deploy.completed", recipient)
	if len(registrations) != 1 {
		t.Fatalf("registrations = %d, want 1", len(registrations))
	}
	evaluation := graph.EvaluateMaterializedRecipients(plan, nil, registrations)
	recipients := evaluation.Recipients()
	if len(recipients) != 1 || recipients[0].ID() != "consumer-node" {
		t.Fatalf("recipients = %#v, want exact compiled receiver", recipients)
	}
	if got := recipients[0].HandlerEvent(); got != "deploy.completed" {
		t.Fatalf("handler event = %q, want deploy.completed", got)
	}
}

func TestConnectRecipientEvaluationRejectsUnrelatedTemplateSameLeaf(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	graph := runtimepinrouting.CompileConnectGraph(source)
	plans := graph.Plans()
	if len(plans) != 1 {
		t.Fatalf("compiled plans = %d, want 1", len(plans))
	}
	plan := plans[0]
	receiver := plan.ReceiverEndpoint().Readback()
	accepted, err := runtimepinrouting.NewConnectNodeRecipient("consumer-node-v1", "consumer/v-1", "consumer", "consumer-node")
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := runtimepinrouting.NewConnectNodeRecipient("unrelated-node-v1", "validation/v-1", "validation", "unrelated-node")
	if err != nil {
		t.Fatal(err)
	}
	registrations := append(
		graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), accepted),
		graph.AdmitReceiverRecipient(receiver.FlowID, events.EventType(receiver.ResolvedEvent), unrelated)...,
	)
	evaluation := graph.EvaluateMaterializedRecipients(plan, []events.RouteIdentity{{
		FlowID: receiver.FlowID, FlowInstance: "consumer/v-1", EntityID: eventtest.UUID("consumer-v1"),
	}}, registrations)
	if got := evaluation.Recipients(); len(got) != 1 || got[0].ID() != "consumer-node-v1" {
		t.Fatalf("recipients = %#v, want only consumer-node-v1", got)
	}
	ledger, err := evaluation.Ledger()
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Plans()) != 1 || len(ledger.Plans()[0].Candidates()) != 2 {
		t.Fatalf("evaluation ledger = %#v, want both considered registrations", ledger)
	}
	outcomes := map[string]events.ConnectCandidateOutcome{}
	for _, candidate := range ledger.Plans()[0].Candidates() {
		outcomes[candidate.Recipient().ID()] = candidate.Outcome()
	}
	if outcomes["consumer-node-v1"] != events.ConnectCandidateAccepted || outcomes["unrelated-node-v1"] != events.ConnectCandidatePathMismatch {
		t.Fatalf("candidate outcomes = %#v", outcomes)
	}
}

func TestCompiledRoutingProducerKindMatrix(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{From: "producer.deploy_done", To: "consumer.deploy_completed"})
	route := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: eventtest.UUID("producer-kind-matrix")}
	staticSource, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	parentID := uuid.NewString()
	at := time.Now().UTC()
	child := func(producerType events.EventProducerType, producerID string) events.Event {
		producer, err := events.NewProducerIdentity(producerType, producerID)
		if err != nil {
			t.Fatalf("construct %s producer: %v", producerType, err)
		}
		return eventtest.ChildForProducerWithRoutingSource(
			uuid.NewString(), "producer/deploy.done", producer, "", json.RawMessage(`{}`), 0,
			events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live},
			events.EventEnvelope{}, staticSource, at,
		)
	}
	control := func(producerID string) events.Event {
		return eventtest.RuntimeControl(
			uuid.NewString(), "producer/deploy.done", producerID, "", json.RawMessage(`{}`), 0,
			runID, parentID, events.EventEnvelope{Source: route}, at,
		)
	}

	for _, tc := range []struct {
		name  string
		event func() events.Event
	}{
		{
			name: "operator ingress",
			event: func() events.Event {
				return eventtest.ExistingRunRootIngressWithRoutingSource(
					uuid.NewString(), "producer/deploy.done", "operator-api", "", json.RawMessage(`{}`), 0,
					runID, events.EventEnvelope{}, staticSource, at,
				)
			},
		},
		{name: "agent emit", event: func() events.Event { return child(events.EventProducerAgent, "producer-agent") }},
		{name: "node emit", event: func() events.Event { return child(events.EventProducerNode, "producer-node") }},
		{name: "schedule", event: func() events.Event { return control("scheduler") }},
		{name: "workflow timer", event: func() events.Event { return control("workflow-timer") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newConnectRoutePlanStaticStore()
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			event := tc.event()
			if err := eb.Publish(context.Background(), event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			settlement := store.settlements[event.ID()]
			if settlement.NoDelivery() || settlement.WriteClass() != events.EventWriteNormalPublication {
				t.Fatalf("settlement = %#v, want normal delivery", settlement)
			}
			plans := settlement.Ledger().Plans()
			if len(plans) != 1 || plans[0].Resolution() != events.ConnectPlanResolved {
				t.Fatalf("compiled plan ledger = %#v, want one resolved plan", plans)
			}
			if routes := store.routes[event.ID()]; !deliveryRoutesContain(routes, connectRoutePlanStaticDeliveryRoute()) {
				t.Fatalf("persisted routes = %#v, want canonical consumer route", routes)
			}
		})
	}
}

func TestEventBusMultiPlanMatchedEmptyPersistsEveryPlanOutcome(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Name: "done", Event: "work.done"}}},
		{id: "consumer-a", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Name: "completed", Event: "work.done"}}, nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-a-node": {ID: "consumer-a-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.done": {}}},
		}},
		{id: "consumer-b", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Name: "completed", Event: "work.done"}}, nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-b-node": {ID: "consumer-b-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.done": {}}},
		}},
	}, []runtimecontracts.FlowPackageConnect{
		{From: "producer.done", To: "consumer-a.completed"},
		{From: "producer.done", To: "consumer-b.completed"},
	}))
	graph := runtimepinrouting.CompileConnectGraph(source)
	if got := len(graph.Plans()); got != 2 {
		t.Fatalf("compiled plans = %d, want 2; issues=%#v", got, graph.Issues())
	}
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(
		testSelectedRunTargetOwner("consumer-a-owner", "consumer-a", "consumer-a-owner"),
		testSelectedRunTargetOwner("consumer-b-owner", "consumer-b", "consumer-b-owner"),
	)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: newRouteTable(source)})
	if err != nil {
		t.Fatal(err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(), "producer/work.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if got := len(graph.MatchingPlans(evt)); got != 2 {
		t.Fatalf("matching plans = %d, want 2", got)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	settlement := store.settlements[evt.ID()]
	if !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryMatchedNoRecipient || len(store.routes[evt.ID()]) != 0 {
		t.Fatalf("settlement = %#v routes=%#v", settlement, store.routes[evt.ID()])
	}
	plans := settlement.Ledger().Plans()
	if len(plans) != 2 {
		t.Fatalf("plan evidence = %#v, want 2", plans)
	}
	for _, plan := range plans {
		if plan.Resolution() != events.ConnectPlanNoRegistration || len(plan.Candidates()) != 0 {
			t.Fatalf("plan evidence = %#v, want no_registration", plan)
		}
	}
}

func TestEventBusAuthoredDeliberateEmptyUsesOutputConsumerClassification(t *testing.T) {
	entry := runtimecontracts.EventCatalogEntry{}
	entry.Swarm.Consumer = []string{"external"}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "ready", Event: "root.ready"}}}}},
		Events:     map[string]runtimecontracts.EventCatalogEntry{"root.ready": entry},
	}
	source := semanticview.Wrap(bundle)
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	evt := connectRoutePlanRootProducerEvent(uuid.NewString(), "root.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	settlement := store.settlements[evt.ID()]
	if !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryNoSubscriberByDesign {
		t.Fatalf("settlement = %#v, want authored deliberate empty", settlement)
	}

	unregistered := *bundle
	unregistered.Events = map[string]runtimecontracts.EventCatalogEntry{"root.ready": {Swarm: runtimecontracts.EventSwarmMetadata{Consumer: []string{"webhook"}}}}
	classification := runtimepinrouting.ClassifyRoutingSourceOutputConsumer(semanticview.Wrap(&unregistered), string(evt.Type()), evt.RoutingSource())
	if classification.DeliberateNoSubscriber() {
		t.Fatal("free-form webhook spelling authorized deliberate no-delivery")
	}
}

func TestEventBusConnectRecipientRegistrationExpandsWildcardOverDeclaredInputs(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id: "producer", mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{Name: "deploy_done", Event: "deploy.done"}},
		},
		{
			id: "consumer", mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID: "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						"deploy.*": existingOwnerHandlerFixture(),
					},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.deploy_done", To: "consumer.deploy_completed", Adapter: "deploy_done_to_completed",
	}}))
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID, events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	want := connectRoutePlanStaticDeliveryRoute()

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" || !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("wildcard connect plan = routes:%#v failure:%q, want %#v", plan.DeliveryRoutes, plan.TargetFailure, want)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted wildcard connect routes = %#v, want %#v", routes, want)
	}
}

func TestConnectRoutePlanDescriptorsLoadOnlyForRuntimeResolution(t *testing.T) {
	calls := 0
	resolver := connectRoutePlanResolver{
		loadDescriptors: func(context.Context) ([]runtimepinrouting.Descriptor, error) {
			calls++
			return []runtimepinrouting.Descriptor{{ID: "alpha", EntityID: "team-a", FlowInstance: "worker/alpha"}}, nil
		},
	}

	staticPlans := runtimepinrouting.CompileConnectGraph(connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done", To: "consumer.deploy_completed",
	})).Plans()
	if len(staticPlans) != 1 {
		t.Fatalf("static plans = %#v, want one", staticPlans)
	}
	if _, err := resolver.descriptorsForPlans(context.Background(), staticPlans); err != nil {
		t.Fatalf("descriptorsForPlans static: %v", err)
	}
	if calls != 0 {
		t.Fatalf("descriptor loader calls after static plan = %d, want 0", calls)
	}

	instancePlan := mustInstanceKeyConnectRoutePlan(t, connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect))
	if _, err := resolver.descriptorsForPlans(context.Background(), []runtimepinrouting.ConnectRoutePlan{instancePlan}); err != nil {
		t.Fatalf("descriptorsForPlans runtime: %v", err)
	}
	if calls != 1 {
		t.Fatalf("descriptor loader calls after runtime-resolution plan = %d, want 1", calls)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsSharedRoutePlan(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newConnectRoutePlanStaticStore()}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := connectRoutePlanStaticDeliveryRoute()
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted mutation event target = %#v, want %#v", got, want.Target)
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got != "processed" {
		t.Fatalf("pipeline receipt = %q, want processed", got)
	}
}

func TestEnginePublication_ConnectRoutePlanPersistsSharedRoutePlan(t *testing.T) {
	source := connectRoutePlanStaticSource(runtimecontracts.FlowPackageConnect{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	})
	store := &connectRoutePlanMutationStore{targetRouteMemoryStore: newConnectRoutePlanStaticStore()}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := connectRoutePlanStaticDeliveryRoute()

	plans, err := eb.PrepareEnginePublications(context.Background(), []runtimeengine.EmitIntent{{Event: evt}})
	if err != nil {
		t.Fatalf("PrepareEnginePublications: %v", err)
	}
	planned := plans[0].(EnginePublicationPlan).prepared.plan
	if planned.AuthorityState != RoutePlanAuthorityCanonicalMatched || planned.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("outbox route plan authority = %q/%q, want matched connect route plan", planned.AuthorityState, planned.AuthorityOwner)
	}
	if !deliveryRoutesContain(planned.DeliveryRoutes(), want) {
		t.Fatalf("outbox route plan delivery routes = %#v, want %#v", planned.DeliveryRoutes(), want)
	}

	if err := eb.ReleaseEnginePublications(context.Background(), plans); err != nil {
		t.Fatalf("release inspection plan: %v", err)
	}
	if err := commitSourceMutationEnginePublications(context.Background(), eb, []runtimeengine.EmitIntent{{Event: evt}}); err != nil {
		t.Fatalf("commit engine publication: %v", err)
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted outbox event target = %#v, want %#v", got, want.Target)
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventRouteSettlementDuplicateAndRecoveryPreserveOriginalFact(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      eventtest.UUID("ent-1"),
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	write := func() error {
		return commitSourceMutationEnginePublications(context.Background(), eb, []runtimeengine.EmitIntent{{Event: evt}})
	}
	if err := write(); err != nil {
		t.Fatalf("initial WriteOutbox: %v", err)
	}
	wantTarget := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: eventtest.UUID("ent-1")}
	if got := store.events[eventID].TargetRoute().Normalized(); got != wantTarget.Normalized() {
		t.Fatalf("initial persisted target = %#v, want %#v", got, wantTarget)
	}
	initialSettlement, err := json.Marshal(store.settlements[eventID])
	if err != nil {
		t.Fatalf("marshal initial route settlement: %v", err)
	}
	initialRoutes := append([]events.DeliveryRoute(nil), store.routes[eventID]...)
	if err := eb.clearPendingOutboxOperation(context.Background(), eventID); err != nil {
		t.Fatalf("clear initial pending outbox operation: %v", err)
	}

	store.flowInstances = nil
	store.flowInstanceDescriptorErr = errors.New("descriptor lookup must not run for a durable duplicate")
	store.flowInstanceDescriptorCalls = 0
	if err := write(); err != nil {
		t.Fatalf("duplicate WriteOutbox after descriptor failure: %v", err)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("duplicate descriptor calls = %d, want durable identity short-circuit", got)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != wantTarget.Normalized() {
		t.Fatalf("duplicate persisted target = %#v, want unchanged %#v", got, wantTarget)
	}
	duplicateSettlement, err := json.Marshal(store.settlements[eventID])
	if err != nil {
		t.Fatalf("marshal duplicate route settlement: %v", err)
	}
	if string(duplicateSettlement) != string(initialSettlement) || !reflect.DeepEqual(store.routes[eventID], initialRoutes) {
		t.Fatalf("exact duplicate changed durable settlement: settlement=%s routes=%#v", duplicateSettlement, store.routes[eventID])
	}
	if err := eb.clearPendingOutboxOperation(context.Background(), eventID); err != nil {
		t.Fatalf("clear duplicate pending outbox operation: %v", err)
	}
}

func TestEventBusResetInMemoryStateRefreshesConnectRoutePlanner(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
	alphaEntityID := eventtest.UUID("ent-alpha")
	betaEntityID := eventtest.UUID("ent-beta")
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID: "alpha", EntityID: alphaEntityID, FlowInstance: "consumer/alpha",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("consumer", "alpha"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(alpha): %v", err)
	}

	if err := eb.ResetInMemoryState(); err != nil {
		t.Fatalf("ResetInMemoryState: %v", err)
	}
	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID: "beta", EntityID: betaEntityID, FlowInstance: "consumer/beta",
		AddressFields: map[string]string{"entity.vertical_id": "v-1"},
	}}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity: runtimeflowidentity.DeriveRoute("consumer", "beta"),
	}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(beta): %v", err)
	}

	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	wantBeta := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-beta"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/beta", EntityID: betaEntityID})}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan after reset: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), wantBeta) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want only refreshed beta route", routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan after reset: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, wantBeta) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want only refreshed beta route", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish after reset: %v", err)
	}
	routes := store.routes[eventID]
	if !deliveryRoutesContain(routes, wantBeta) {
		t.Fatalf("persisted delivery routes = %#v, want refreshed beta route", routes)
	}
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want only refreshed beta route", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      eventtest.UUID("ent-1"),
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-one"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: eventtest.UUID("ent-1")})}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want instance-key route %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want instance-key route %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.events[eventID].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want instance-key route %#v", store.routes[eventID], want)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node-one") || !containsString(internal, "consumer-node-one") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node-one from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectOrCreateCreatesMissingTemplateInstance(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(store.activations) != 0 {
		t.Fatalf("preflight activations = %d, want 0", len(store.activations))
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	previewTarget := preflight.DeliveryRoutes[0].Target.Route()
	if previewTarget.FlowID != "consumer" || previewTarget.FlowInstance == "" || previewTarget.EntityID == "" {
		t.Fatalf("preview target = %#v, want deterministic consumer flow instance", previewTarget)
	}
	previewIdentity := runtimeflowidentity.StoredRoute("consumer", runtimeflowidentity.LogicalInstanceID(previewTarget.FlowInstance), previewTarget.FlowInstance)
	if routes := eb.RouteTable().MaterializedRoutes(previewIdentity); len(routes) != 0 {
		t.Fatalf("preview route table state leaked after preflight: %#v", routes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config["vertical_id"] != "v-1" || activation.Metadata["vertical_id"] != "v-1" {
		t.Fatalf("activation config/metadata = %#v/%#v, want vertical_id v-1", activation.Config, activation.Metadata)
	}
	if activation.Metadata["entity_type"] != "deployment" || activation.Metadata["instance_kind"] != "template" || activation.Metadata["last_source_event"] != evt.ID() {
		t.Fatalf("activation metadata = %#v, want entity_type/instance_kind/last_source_event proof", activation.Metadata)
	}
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-" + activation.Instance.InstanceID), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
	}
	if got := store.events[evt.ID()].TargetRoute().Normalized(); got != want.Target.Route().Normalized() {
		t.Fatalf("persisted event target = %#v, want %#v", got, want.Target)
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want created instance route %#v", store.routes[evt.ID()], want)
	}

	retry := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), retry); err != nil {
		t.Fatalf("Publish retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("retry activations = %d, want idempotent reuse without a second activation", len(store.activations))
	}
	reused := want
	reused.Target = events.MustExistingEntityTarget(want.Target.Route())
	if !deliveryRoutesContain(store.routes[retry.ID()], reused) || len(store.routes[retry.ID()]) != 1 {
		t.Fatalf("retry delivery routes = %#v, want existing instance route %#v", store.routes[retry.ID()], reused)
	}

	replayTarget := subscribeInternalDeliveriesForTest(t, eb, want.Recipient.ID())
	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      eventtest.UUID("ent-drift"),
		FlowInstance:  "consumer/drift",
		AddressFields: map[string]string{"entity.vertical_id": "v-1"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "drift")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	store.flowInstanceDescriptorCalls = 0
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "persisted replay after lifecycle-created descriptor drift")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("replayed delivery target = flow_instance:%q entity:%q, want persisted lifecycle-created %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because lifecycle-created persisted route/scope is authoritative", got)
	}
}

func TestCompiledConnectEvaluationStaleSnapshotReevaluatesBeforeMutation(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	store := &connectRoutePlanStaleSnapshotStore{
		connectRoutePlanLifecycleStore: &connectRoutePlanLifecycleStore{
			connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
			},
		},
		mutations: 1,
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-stale"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.flowInstanceDescriptorCalls; got < 2 {
		t.Fatalf("descriptor reads = %d, want re-evaluation after stale generation", got)
	}
	if got := len(store.activations); got != 1 {
		t.Fatalf("activations = %d, want exactly one post-fence mutation", got)
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].ConnectClaim.Empty() {
		t.Fatalf("persisted routes = %#v, want one stamped connect route", routes)
	}
}

func TestCompiledConnectEvaluationStaleSnapshotFailureLeavesLifecycleUnchanged(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	store := &connectRoutePlanStaleSnapshotStore{
		connectRoutePlanLifecycleStore: &connectRoutePlanLifecycleStore{
			connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
			},
		},
		mutations: 10,
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-stale"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	err = eb.Publish(context.Background(), evt)
	if err == nil || !strings.Contains(err.Error(), "connect route snapshot generation is stale") {
		t.Fatalf("Publish error = %v, want exhausted stale-generation failure", err)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("activations = %d, want no lifecycle mutation", got)
	}
	if _, ok := store.events[evt.ID()]; ok {
		t.Fatalf("event %s persisted despite stale-generation failure", evt.ID())
	}
	if routes := store.routes[evt.ID()]; len(routes) != 0 {
		t.Fatalf("persisted routes = %#v, want none", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanPreviewCreateFeedsLaterSelect(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	fixtureRoot := canonicalrouting.CopyTemplateCreateThenSelectSameEvent(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            semanticview.Wrap(bundle),
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/account.setup"), "", "", json.RawMessage(`{"account_id":"acct-preview"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 2 {
		t.Fatalf("preflight failure/routes = %q/%#v, want create and later-select routes", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if len(store.activations) != 0 {
		t.Fatalf("preflight activations = %#v, want request-local preview only", store.activations)
	}
	previewTarget := preflight.DeliveryRoutes[0].Target.Route().Normalized()
	for _, route := range preflight.DeliveryRoutes {
		if route.Target.Route().Normalized() != previewTarget {
			t.Fatalf("preflight route target = %#v, want shared preview-created target %#v", route.Target, previewTarget)
		}
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %#v, want one create followed by select", store.activations)
	}
	activation := store.activations[0]
	wantTarget := events.RouteIdentity{
		FlowID:       "account",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}.Normalized()
	routes := store.routes[evt.ID()]
	if len(routes) != 2 {
		t.Fatalf("persisted routes = %#v, want two distinct consumers", routes)
	}
	wantSubscribers := map[string]bool{
		"account-setup-node-" + activation.Instance.InstanceID: false,
		"account-ready-node-" + activation.Instance.InstanceID: false,
	}
	for _, route := range routes {
		if route.Target.Route().Normalized() != wantTarget {
			t.Fatalf("persisted route target = %#v, want %#v", route.Target, wantTarget)
		}
		if _, ok := wantSubscribers[route.Recipient.ID()]; !ok {
			t.Fatalf("persisted subscriber = %q, want one of %#v", route.Recipient.ID(), wantSubscribers)
		}
		wantSubscribers[route.Recipient.ID()] = true
	}
	for subscriber, found := range wantSubscribers {
		if !found {
			t.Fatalf("persisted routes = %#v, missing subscriber %s", routes, subscriber)
		}
	}
}

func TestCommittedReplayReusesPersistedSyntheticCarryWithoutReminting(t *testing.T) {
	source := connectRoutePlanCreateResolutionSource(t, runtimecontracts.FlowInputCarrySourceGeneratedUUID)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("preflight activations = %d, want 0", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	minted, _ := activation.Metadata["validation_case_id"].(string)
	if _, err := uuid.Parse(minted); err != nil {
		t.Fatalf("minted validation_case_id = %q, want uuid: %v", minted, err)
	}
	if minted == eventID {
		t.Fatalf("minted validation_case_id = source event id %q, want deterministic uuid mint distinct from event_id mint", minted)
	}
	if activation.Config["validation_case_id"] != minted || activation.Metadata["validation_case_id"] != minted {
		t.Fatalf("activation config/metadata = %#v/%#v, want carried validation_case_id %q", activation.Config, activation.Metadata, minted)
	}
	if got := activation.Metadata["last_source_event"]; got != eventID {
		t.Fatalf("last_source_event = %v, want %q", got, eventID)
	}
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator-node"), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       "validator",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": minted}),
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want create-resolution route %#v", store.routes[eventID], want)
	}

	replayTarget := subscribeInternalDeliveriesForTest(t, eb, want.Recipient.ID())
	store.flowInstanceDescriptorCalls = 0
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish same event retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("same-event retry activations = %d, want no second activation", len(store.activations))
	}
	requireNoConnectRoutePlanBusEvent(t, replayTarget, "create resolution same-event retry")

	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "create resolution explicit committed replay")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("same-event replay target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	var replayedPayload map[string]any
	if err := json.Unmarshal(replayed.Payload(), &replayedPayload); err != nil {
		t.Fatalf("decode replayed projected payload: %v", err)
	}
	if replayedPayload["candidate"] != "acct-1" || replayedPayload["validation_case_id"] != minted {
		t.Fatalf("replayed projected payload = %#v, want immutable producer candidate plus stamped validation_case_id %q", replayedPayload, minted)
	}
	var sourcePayload map[string]any
	if err := json.Unmarshal(evt.Payload(), &sourcePayload); err != nil {
		t.Fatalf("decode immutable source payload: %v", err)
	}
	if _, exists := sourcePayload["validation_case_id"]; exists {
		t.Fatalf("source payload was mutated by route projection: %#v", sourcePayload)
	}
}

func TestEventBusCheckPublishRecipientPlan_ConnectRoutePlanCreateResolutionAdmitsEmptyEventID(t *testing.T) {
	source := connectRoutePlanCreateResolutionSource(t, runtimecontracts.FlowInputCarrySourceGeneratedUUID)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent("",
		events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Time{})

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight failure/routes = %q/%#v, want one admitted preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("preflight activations = %d, want 0", got)
	}
	previewTarget := preflight.DeliveryRoutes[0].Target.Route()
	if previewTarget.FlowID != "validator" || previewTarget.FlowInstance == "" || previewTarget.EntityID == "" {
		t.Fatalf("preview target = %#v, want minted validator route from admitted event id", previewTarget)
	}
	if routes := store.routes; len(routes) != 0 {
		t.Fatalf("preflight persisted routes = %#v, want none", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreateResolutionCanMintFromEventID(t *testing.T) {
	source := connectRoutePlanCreateResolutionSource(t, runtimecontracts.FlowInputCarrySourceEventID)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Metadata["validation_case_id"] != eventID || activation.Config["validation_case_id"] != eventID {
		t.Fatalf("activation config/metadata = %#v/%#v, want event_id-minted validation_case_id %q", activation.Config, activation.Metadata, eventID)
	}
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("validator-node"), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       "validator",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
		PayloadProjection: mustDeliveryPayloadProjection(t, map[string]string{"validation_case_id": eventID}),
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want event_id create-resolution route %#v", store.routes[eventID], want)
	}
}

func TestTemplateInstanceLifecycleUsesResolutionModeWithoutContractPolicyFallback(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        runtimecontracts.FlowInputResolutionMode
		wantFailure runtimepinrouting.ConnectRoutePlanFailure
		wantAction  TemplateInstanceLifecycleAction
	}{
		{name: "create conflicts", mode: runtimecontracts.FlowInputResolutionModeCreate, wantFailure: runtimepinrouting.ConnectFailureInstanceConflict},
		{name: "select selects", mode: runtimecontracts.FlowInputResolutionModeSelect, wantAction: templateInstanceLifecycleActionSelectedExisting},
		{name: "select-or-create reuses", mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate, wantAction: templateInstanceLifecycleActionReused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
				events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			descriptors := []runtimepinrouting.Descriptor{{
				ID:            "one",
				EntityID:      eventtest.UUID("ent-1"),
				FlowInstance:  "account/one",
				AddressFields: map[string]string{"entity.account_id": "acct-1"},
			}}
			values := map[string]string{"payload.account_id": "acct-1"}
			var source semanticview.Source
			if tc.mode == runtimecontracts.FlowInputResolutionModeCreate {
				source = connectRoutePlanPayloadCreateResolutionSource(t)
				evt = connectRoutePlanStaticProducerEvent(uuid.NewString(),
					events.EventType("producer/validation.requested"), "", "", json.RawMessage(`{"candidate":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
				descriptors[0].FlowInstance = "validator/one"
				descriptors[0].AddressFields = map[string]string{"entity.validation_case_id": "acct-1"}
				values = map[string]string{"payload.candidate": "acct-1"}
			} else {
				source = connectRoutePlanCarriedKeyResolutionSource(t, tc.mode)
			}
			plan := mustInstanceKeyConnectRoutePlan(t, source)
			owner := newTemplateInstanceLifecycleOwner(source, nil, nil, nil, nil)
			materialization, decision, handled, err := owner.Materialize(context.Background(), evt, plan, values, descriptors)
			if err != nil {
				t.Fatalf("Materialize: %v", err)
			}
			if !handled {
				t.Fatal("typed instance resolution was not handled")
			}
			if materialization.Failure != tc.wantFailure {
				t.Fatalf("failure = %q, want %q", materialization.Failure, tc.wantFailure)
			}
			if decision.Action != tc.wantAction {
				t.Fatalf("action = %q, want %q", templateInstanceLifecycleActionCode(decision.Action), templateInstanceLifecycleActionCode(tc.wantAction))
			}
		})
	}
}

func TestTemplateInstanceLifecycleDecisionAndActivationConfigContainNoPolicyFacts(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	plan := mustInstanceKeyConnectRoutePlan(t, source)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("canonical resolution source does not expose a bundle")
	}
	instanceContract, err := plan.ReceiverTemplate(bundle)
	if err != nil {
		t.Fatalf("ResolveFlowTemplateInstance: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	owner := newTemplateInstanceLifecycleOwner(source, nil, nil, nil, nil)
	request, decision, failure := owner.activationRequest(evt, plan, instanceContract, []runtimecontracts.TemplateInstanceKeyValue{{
		Field: plan.InstanceKey().Field(),
		Value: "acct-1",
	}})
	if !failure.Empty() {
		t.Fatalf("activationRequest failure = %q", failure.Code())
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(TemplateInstanceLifecycleDecision{}), reflect.TypeOf(runtimepipeline.FlowInstanceActivationRequest{})} {
		for index := 0; index < typ.NumField(); index++ {
			name := strings.ToLower(typ.Field(index).Name)
			if strings.Contains(name, "onmissing") || strings.Contains(name, "onconflict") || strings.Contains(name, "policy") {
				t.Fatalf("%s retains policy field %s", typ, typ.Field(index).Name)
			}
		}
	}
	for label, facts := range map[string]map[string]any{"config": request.Config, "metadata": request.Metadata} {
		for key := range facts {
			normalized := strings.ToLower(key)
			if strings.Contains(normalized, "on_missing") || strings.Contains(normalized, "on_conflict") || strings.Contains(normalized, "policy") {
				t.Fatalf("activation %s retains policy fact %q: %#v", label, key, facts)
			}
		}
	}
	for key := range decision.ActivationVariables() {
		normalized := strings.ToLower(key)
		if strings.Contains(normalized, "on_missing") || strings.Contains(normalized, "on_conflict") || strings.Contains(normalized, "policy") {
			t.Fatalf("activation variables retain policy fact %q", key)
		}
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectResolutionUsesRenamedPayloadSourceExclusively(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{name: "renamed field only", payload: `{"external_account_id":"acct-authoritative"}`},
		{name: "conflicting same-named field", payload: `{"external_account_id":"acct-authoritative","account_id":"acct-conflicting"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanCarriedKeyResolutionSourceWithIdentitySource(t, runtimecontracts.FlowInputResolutionModeSelect, "payload.external_account_id")
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
					flowInstances: []ActiveFlowInstanceDescriptor{
						{InstanceID: "authoritative", EntityID: eventtest.UUID("ent-authoritative"), FlowInstance: "account/authoritative", AddressFields: map[string]string{"entity.account_id": "acct-authoritative"}},
						{InstanceID: "conflicting", EntityID: eventtest.UUID("ent-conflicting"), FlowInstance: "account/conflicting", AddressFields: map[string]string{"entity.account_id": "acct-conflicting"}},
					},
				},
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstanceActivator: store.Activate})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eb
			for _, instanceID := range []string{"authoritative", "conflicting"} {
				if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", instanceID)}); err != nil {
					t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
				}
			}
			eventID := uuid.NewString()
			evt := connectRoutePlanStaticProducerEvent(eventID,
				events.EventType("producer/account.ready"), "", "", json.RawMessage(tc.payload), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("account-node-authoritative"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID: "account", FlowInstance: "account/authoritative", EntityID: eventtest.UUID("ent-authoritative"),
			}),
			}
			if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
				t.Fatalf("persisted routes = %#v, want authoritative renamed-source route %#v", store.routes[eventID], want)
			}
			if len(store.activations) != 0 {
				t.Fatalf("activations = %d, want select-only reuse", len(store.activations))
			}
		})
	}
}

func TestEventBusCheckPublishRecipientPlan_RenamedInstanceSourceRemediationUsesAuthoredPath(t *testing.T) {
	for _, mode := range []runtimecontracts.FlowInputResolutionMode{
		runtimecontracts.FlowInputResolutionModeSelect,
		runtimecontracts.FlowInputResolutionModeSelectOrCreate,
	} {
		t.Run(runtimecontracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
			source := connectRoutePlanCarriedKeyResolutionSourceWithIdentitySource(t, mode, "payload.external_account_id")
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
				},
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstanceActivator: store.Activate})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eb
			evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
				events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"cannot-satisfy-authored-source"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

			preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got, want := preflight.TargetFailure, runtimepinrouting.ConnectFailureInstanceSourceValueMissing.Code(); got != want {
				t.Fatalf("preflight target failure = %q, want %q", got, want)
			}
			if len(preflight.DeliveryRoutes) != 0 || len(store.activations) != 0 {
				t.Fatalf("preflight routes/activations = %#v/%d, want unchanged fail-closed state", preflight.DeliveryRoutes, len(store.activations))
			}

			routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
			if err != nil {
				t.Fatalf("planSubscribedRoutePlan: %v", err)
			}
			want := fmt.Sprintf("Provide payload.external_account_id before publishing to account; resolution mode %s requires a carried key value.", runtimecontracts.FlowInputResolutionModeCode(mode))
			got, _ := routePlan.ExtraDetail["connect_route_plan_failure_remediation"].(string)
			if got != want {
				t.Fatalf("remediation = %q, want exact authored-source remediation %q", got, want)
			}
			if strings.Contains(got, "payload.account_id") {
				t.Fatalf("remediation = %q, must not reconstruct source from the scalar instance field", got)
			}
		})
	}
}

func mustDeliveryPayloadProjection(t *testing.T, fields map[string]string) events.DeliveryPayloadProjection {
	t.Helper()
	projection, err := events.NewDeliveryPayloadProjection(fields)
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	return projection
}

func TestEventBusPublish_ConnectRoutePlanSelectResolutionRoutesExistingInstanceAndReplaysCommittedRoute(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      eventtest.UUID("ent-1"),
				FlowInstance:  "account/one",
				FlowTemplate:  "account",
				AddressFields: map[string]string{"entity.account_id": "acct-1"},
			}},
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(one): %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("account-node-one"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "account", FlowInstance: "account/one", EntityID: eventtest.UUID("ent-1")})}

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" {
		t.Fatalf("preflight target failure = %q, want none", preflight.TargetFailure)
	}
	if !deliveryRoutesContain(preflight.DeliveryRoutes, want) || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want select existing route %#v", preflight.DeliveryRoutes, want)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("preflight activations = %d, want 0 for select", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("publish activations = %d, want 0 because select never creates", got)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want select existing route %#v", store.routes[eventID], want)
	}

	replayTarget := subscribeInternalDeliveriesForTest(t, eb, "account-node-one")
	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      eventtest.UUID("ent-drift"),
		FlowInstance:  "account/drift",
		AddressFields: map[string]string{"entity.account_id": "acct-1"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", "drift")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	store.flowInstanceDescriptorCalls = 0
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "select resolution committed replay")
	if replayed.FlowInstance() != "account/one" || replayed.EntityID() != eventtest.UUID("ent-1") {
		t.Fatalf("replayed delivery target = flow_instance:%q entity:%q, want persisted account/one ent-1", replayed.FlowInstance(), replayed.EntityID())
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted route/scope is authoritative", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectResolutionFailsClosedForTargetGaps(t *testing.T) {
	tests := []struct {
		name          string
		flowInstances []ActiveFlowInstanceDescriptor
		addRoutes     []string
		wantFailure   runtimepinrouting.TargetFailure
		wantDetail    map[string]any
	}{
		{
			name: "missing existing instance does not create",
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "other",
				EntityID:      eventtest.UUID("ent-other"),
				FlowInstance:  "account/other",
				AddressFields: map[string]string{"entity.account_id": "acct-2"},
			}},
			addRoutes:   []string{"other"},
			wantFailure: runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetUnresolved),
			wantDetail: map[string]any{
				"connect_route_plan_receiver_flow":      "account",
				"connect_route_plan_instance_key_field": "account_id",
				"connect_route_plan_instance_key_value": "acct-1",
			},
		},
		{
			name: "ambiguous existing instances fail closed",
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: eventtest.UUID("ent-1"), FlowInstance: "account/one", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
				{InstanceID: "two", EntityID: eventtest.UUID("ent-2"), FlowInstance: "account/two", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
			},
			addRoutes:   []string{"one", "two"},
			wantFailure: runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetAmbiguous),
			wantDetail: map[string]any{
				"connect_route_plan_receiver_flow":          "account",
				"connect_route_plan_instance_key_field":     "account_id",
				"connect_route_plan_instance_key_value":     "acct-1",
				"connect_route_plan_matched_instance_count": 2,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelect)
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
					flowInstances:          tc.flowInstances,
				},
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle:            source,
				TemplateInstanceActivator: store.Activate,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eb
			for _, instanceID := range tc.addRoutes {
				if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", instanceID)}); err != nil {
					t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
				}
			}
			eventID := uuid.NewString()
			evt := connectRoutePlanStaticProducerEvent(eventID,
				events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
			if err != nil {
				t.Fatalf("planSubscribedRoutePlan: %v", err)
			}
			if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
				t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
			}
			if routePlan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, tc.wantFailure)
			}
			for key, want := range tc.wantDetail {
				if got := routePlan.ExtraDetail[key]; got != want {
					t.Fatalf("route plan detail %s = %#v, want %#v; all detail %#v", key, got, want, routePlan.ExtraDetail)
				}
			}
			if remediation, _ := routePlan.ExtraDetail["connect_route_plan_failure_remediation"].(string); !strings.Contains(remediation, "select") || !strings.Contains(remediation, "account") || !strings.Contains(remediation, "account_id") {
				t.Fatalf("remediation = %q, want select/account/account_id user-facing detail", remediation)
			}

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got, want := plan.TargetFailure, tc.wantFailure.Code(); got != want {
				t.Fatalf("preflight target failure = %q, want %q", got, want)
			}
			if got := len(store.activations); got != 0 {
				t.Fatalf("preflight activations = %d, want 0 because select never creates", got)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if got := len(store.activations); got != 0 {
				t.Fatalf("publish activations = %d, want 0 because select never creates", got)
			}
			if routes := store.routes[eventID]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none for fail-closed select", routes)
			}
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectOrCreateResolutionReusesCreatesAndReplaysCommittedRoute(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      eventtest.UUID("ent-1"),
				FlowInstance:  "account/one",
				FlowTemplate:  "account",
				AddressFields: map[string]string{"entity.account_id": "acct-1"},
			}},
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(one): %v", err)
	}
	existingID := uuid.NewString()
	existing := connectRoutePlanStaticProducerEvent(existingID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	existingWant := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("account-node-one"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "account", FlowInstance: "account/one", EntityID: eventtest.UUID("ent-1")})}

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), existing)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan(existing): %v", err)
	}
	if preflight.TargetFailure != "" || !deliveryRoutesContain(preflight.DeliveryRoutes, existingWant) || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("existing preflight failure/routes = %q/%#v, want existing route %#v", preflight.TargetFailure, preflight.DeliveryRoutes, existingWant)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("existing preflight activations = %d, want 0", got)
	}
	if err := eb.Publish(context.Background(), existing); err != nil {
		t.Fatalf("Publish(existing): %v", err)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("existing publish activations = %d, want 0 because select-or-create reuses exact match", got)
	}
	if !deliveryRoutesContain(store.routes[existingID], existingWant) || len(store.routes[existingID]) != 1 {
		t.Fatalf("existing persisted routes = %#v, want %#v", store.routes[existingID], existingWant)
	}

	missingID := uuid.NewString()
	missing := connectRoutePlanStaticProducerEvent(missingID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-2"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	preflight, err = eb.CheckPublishRecipientPlan(context.Background(), missing)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan(missing): %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 {
		t.Fatalf("missing preflight failure/routes = %q/%#v, want preview route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("missing preflight activations = %d, want 0 preview-only", got)
	}
	if err := eb.Publish(context.Background(), missing); err != nil {
		t.Fatalf("Publish(missing): %v", err)
	}
	if got := len(store.activations); got != 1 {
		t.Fatalf("missing publish activations = %d, want 1 create", got)
	}
	activation := store.activations[0]
	if _, ok := activation.Config["template_instance_on_missing"]; ok {
		t.Fatalf("activation config retains on_missing policy fact: %#v", activation.Config)
	}
	if _, ok := activation.Config["template_instance_on_conflict"]; ok {
		t.Fatalf("activation config retains on_conflict policy fact: %#v", activation.Config)
	}
	if activation.Config["account_id"] != "acct-2" || activation.Metadata["account_id"] != "acct-2" {
		t.Fatalf("activation config/metadata = %#v/%#v, want carried account_id acct-2", activation.Config, activation.Metadata)
	}
	createdWant := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("account-node-" + activation.Instance.InstanceID), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       "account",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
	}
	if !deliveryRoutesContain(store.routes[missingID], createdWant) || len(store.routes[missingID]) != 1 {
		t.Fatalf("missing persisted routes = %#v, want created route %#v", store.routes[missingID], createdWant)
	}

	retryID := uuid.NewString()
	retry := connectRoutePlanStaticProducerEvent(retryID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-2"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	if err := eb.Publish(context.Background(), retry); err != nil {
		t.Fatalf("Publish(retry): %v", err)
	}
	if got := len(store.activations); got != 1 {
		t.Fatalf("retry activations = %d, want reuse without second activation", got)
	}
	reusedWant := createdWant
	reusedWant.Target = events.MustExistingEntityTarget(createdWant.Target.Route())
	if !deliveryRoutesContain(store.routes[retryID], reusedWant) || len(store.routes[retryID]) != 1 {
		t.Fatalf("retry persisted routes = %#v, want existing reused route %#v", store.routes[retryID], reusedWant)
	}

	replayTarget := subscribeInternalDeliveriesForTest(t, eb, "account-node-"+activation.Instance.InstanceID)
	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "drift",
		EntityID:      eventtest.UUID("ent-drift"),
		FlowInstance:  "account/drift",
		AddressFields: map[string]string{"entity.account_id": "acct-2"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", "drift")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(drift): %v", err)
	}
	store.flowInstanceDescriptorCalls = 0
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: missing, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "select-or-create committed replay")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("replayed target = flow_instance:%q entity:%q, want persisted %s/%s",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted route/scope is authoritative", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectOrCreateResolutionDoesNotReuseUnroutableActivationFailure(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
		failAfterDescriptorWithoutRoute: errors.New("route installation failed"),
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-partial"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	err = eb.Publish(context.Background(), evt)
	if err == nil {
		t.Fatal("Publish succeeded, want activation error preserved for unroutable descriptor")
	}
	if !strings.Contains(err.Error(), "route installation failed") {
		t.Fatalf("Publish error = %v, want original activation failure", err)
	}
	if got := len(store.activations); got != 1 {
		t.Fatalf("activations = %d, want one failed activation attempt", got)
	}
	if got := len(store.flowInstances); got != 1 {
		t.Fatalf("flow instance descriptors = %d, want descriptor visible from failed activation", got)
	}
	if routes := eb.RouteTable().MaterializedRoutes(store.activations[0].Instance.Route()); len(routes) != 0 {
		t.Fatalf("materialized routes after failed activation = %#v, want none", routes)
	}
	if routes := store.routes[eventID]; len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want the durable route to survive process-local activation failure", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectOrCreateResolutionFailsClosedForAmbiguousTarget(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: eventtest.UUID("ent-1"), FlowInstance: "account/one", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
				{InstanceID: "two", EntityID: eventtest.UUID("ent-2"), FlowInstance: "account/two", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
			},
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	for _, instanceID := range []string{"one", "two"} {
		if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("account", instanceID)}); err != nil {
			t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
		}
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.TargetFailure != runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetAmbiguous) {
		t.Fatalf("route plan authority/failure = %q/%q, want fail-closed ambiguous", routePlan.AuthorityState, routePlan.TargetFailure)
	}
	if got := routePlan.ExtraDetail["connect_route_plan_matched_instance_count"]; got != 2 {
		t.Fatalf("matched count detail = %#v, want 2; all detail %#v", got, routePlan.ExtraDetail)
	}
	if remediation, _ := routePlan.ExtraDetail["connect_route_plan_failure_remediation"].(string); !strings.Contains(remediation, "select-or-create") || !strings.Contains(remediation, "account") || !strings.Contains(remediation, "account_id") {
		t.Fatalf("remediation = %q, want select-or-create/account/account_id detail", remediation)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := len(store.activations); got != 0 {
		t.Fatalf("activations = %d, want 0 on ambiguous fail-closed", got)
	}
	if routes := store.routes[eventID]; len(routes) != 0 {
		t.Fatalf("persisted routes = %#v, want none for ambiguous fail-closed", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanSelectOrCreateResolutionConcurrentSameKeyConverges(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	base := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	store := &connectRoutePlanConcurrentLifecycleStore{connectRoutePlanLifecycleStore: base}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	eventIDs := []string{uuid.NewString(), uuid.NewString()}
	errs := make(chan error, len(eventIDs))
	var wg sync.WaitGroup
	for _, eventID := range eventIDs {
		eventID := eventID
		wg.Add(1)
		go func() {
			defer wg.Done()
			evt := connectRoutePlanStaticProducerEvent(eventID,
				events.EventType("producer/account.ready"), "", "", json.RawMessage(`{"account_id":"acct-race"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			errs <- eb.Publish(context.Background(), evt)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Publish returned error: %v", err)
		}
	}
	if got := store.ActivationCount(); got != 1 {
		t.Fatalf("activations = %d, want one same-key select-or-create activation", got)
	}
	descriptors := store.FlowInstanceDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("descriptors = %#v, want one same-key descriptor", descriptors)
	}
	wantRecipient := events.MustNodeDeliveryRecipient("account-node-" + descriptors[0].InstanceID)
	wantTarget := events.RouteIdentity{
		FlowID:       "account",
		FlowInstance: descriptors[0].FlowInstance,
		EntityID:     descriptors[0].EntityID,
	}
	materializing, existing := 0, 0
	for _, eventID := range eventIDs {
		routes, err := store.ListEventDeliveryRoutes(context.Background(), eventID)
		if err != nil {
			t.Fatalf("ListEventDeliveryRoutes(%s): %v", eventID, err)
		}
		if len(routes) != 1 || routes[0].Recipient != wantRecipient || routes[0].Target.Route() != wantTarget {
			t.Fatalf("routes[%s] = %#v, want converged same-key target %#v", eventID, routes, wantTarget)
		}
		switch {
		case routes[0].Target.MaterializingEntity():
			materializing++
		case routes[0].Target.ExistingEntity():
			existing++
		default:
			t.Fatalf("routes[%s] target ownership = %s, want materializing_entity or existing_entity", eventID, routes[0].Target.Code())
		}
	}
	if materializing != 2 || existing != 0 {
		t.Fatalf("ownership dispositions materializing/existing = %d/%d, want concurrent plans stamped with the same future identity", materializing, existing)
	}
}

func TestEventBusPublish_ConnectRoutePlanLifecycleCollisionFailsBeforeActivation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		secondPin canonicalrouting.TemplateInstanceSecondPin
		consumer  canonicalrouting.TemplateInstanceConsumer
	}{
		{name: "node/same local event", secondPin: canonicalrouting.TemplateInstanceSecondPinSameEvent, consumer: canonicalrouting.TemplateInstanceNodeConsumer},
		{name: "node/distinct local events", secondPin: canonicalrouting.TemplateInstanceSecondPinDistinctEvent, consumer: canonicalrouting.TemplateInstanceNodeConsumer},
		{name: "agent/same local event", secondPin: canonicalrouting.TemplateInstanceSecondPinSameEvent, consumer: canonicalrouting.TemplateInstanceAgentConsumer},
		{name: "agent/distinct local events", secondPin: canonicalrouting.TemplateInstanceSecondPinDistinctEvent, consumer: canonicalrouting.TemplateInstanceAgentConsumer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanTemplateInstanceMultiInputSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, tc.secondPin, tc.consumer)
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
				},
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle:            source,
				TemplateInstanceActivator: store.Activate,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eb
			if tc.consumer == canonicalrouting.TemplateInstanceAgentConsumer {
				identity, admission, entityID := connectRoutePlanLifecycleAgentRoute(t, source, tc.secondPin)
				subscribeTestAgentAdmissionWithIdentity(t, eb, admission, identity, entityID)
			}
			evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
				events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got, want := preflight.TargetFailure, runtimepinrouting.ConnectFailureDeliveryTopologyInvalid.Code(); got != want {
				t.Fatalf("preflight target failure = %q, want %q; plan=%#v", got, want, preflight)
			}
			if len(preflight.DeliveryRoutes) != 0 {
				t.Fatalf("preflight delivery routes = %#v, want none", preflight.DeliveryRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(store.activations) != 0 {
				t.Fatalf("activations = %#v, want none before rejecting receiver-pin collision", store.activations)
			}
			if len(store.flowInstances) != 0 {
				t.Fatalf("flow instance descriptors = %#v, want none", store.flowInstances)
			}
			if len(store.routes[evt.ID()]) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none", store.routes[evt.ID()])
			}
			eb.RouteTable().mu.RLock()
			materialized := len(eb.RouteTable().instanceOwners)
			eb.RouteTable().mu.RUnlock()
			if materialized != 0 {
				t.Fatalf("materialized route owners = %d, want none", materialized)
			}
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsCreatedAgentBeforeLiveCarrier(t *testing.T) {
	source := connectRoutePlanTemplateInstanceMultiInputSource(
		t,
		canonicalrouting.TemplateInstanceRouteSelectOrCreate,
		canonicalrouting.TemplateInstanceNoSecondPin,
		canonicalrouting.TemplateInstanceNodeAndAgentConsumer,
	)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	interceptor := &connectRoutePlanNodeInterceptor{}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
		Interceptors:              []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	identity, _, _ := connectRoutePlanLifecycleAgentRoute(t, source, canonicalrouting.TemplateInstanceNoSecondPin)
	evt := connectRoutePlanStaticProducerEvent(
		uuid.NewString(),
		"producer/deploy.done",
		"",
		"",
		json.RawMessage(`{"vertical_id":"v-1"}`),
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)

	preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 2 {
		t.Fatalf("preflight = failure:%q routes:%#v, want node plus pending agent route", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if slices.Contains(preflight.Recipients, identity.AgentID()) {
		t.Fatalf("live recipients = %#v, created agent must remain pending until its lifecycle route is published", preflight.Recipients)
	}
	var agentRoute events.DeliveryRoute
	for _, route := range preflight.DeliveryRoutes {
		if route.Recipient.IsAgent() {
			agentRoute = route
			break
		}
	}
	if agentRoute.AgentIdentity != identity {
		t.Fatalf("created agent delivery identity = %#v, want canonical declaration and route %#v", agentRoute.AgentIdentity, identity)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 || len(store.flowInstances) != 1 {
		t.Fatalf("activations/descriptors = %d/%d, want one admitted lifecycle instance", len(store.activations), len(store.flowInstances))
	}
	if got := interceptor.Count(); got != 1 {
		t.Fatalf("node handler interceptions = %d, want creation carrier execution", got)
	}
	if routes := store.routes[evt.ID()]; !deliveryRoutesContain(routes, agentRoute) || len(routes) != 2 {
		t.Fatalf("persisted routes = %#v, want pending agent route %#v plus node route", routes, agentRoute)
	}
}

func TestEventBusPublish_ConnectRoutePlanLifecycleAdmissionPreservesDuplicateEdgeAndDistinctSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name       string
		secondPin  canonicalrouting.TemplateInstanceSecondPin
		consumer   canonicalrouting.TemplateInstanceConsumer
		wantRoutes int
		wantNodes  int
		wantAgent  bool
	}{
		{name: "duplicate identical edge", secondPin: canonicalrouting.TemplateInstanceSecondPinDuplicateEdge, consumer: canonicalrouting.TemplateInstanceNodeConsumer, wantRoutes: 1, wantNodes: 1},
		{name: "distinct node and agent subscribers", consumer: canonicalrouting.TemplateInstanceNodeAndAgentConsumer, wantRoutes: 2, wantAgent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanTemplateInstanceMultiInputSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, tc.secondPin, tc.consumer)
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
				},
			}
			interceptor := &connectRoutePlanNodeInterceptor{}
			opts := EventBusOptions{
				ContractBundle:            source,
				TemplateInstanceActivator: store.Activate,
			}
			if !tc.wantAgent {
				opts.Interceptors = []EventInterceptor{interceptor}
			}
			eb, err := newScopedTestEventBus(store, opts)
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eb
			evt := connectRoutePlanStaticProducerEvent(uuid.NewString(), "producer/deploy.done", "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			var agentEvents <-chan *LocalDelivery
			if tc.wantAgent {
				identity, admission, entityID := connectRoutePlanLifecycleAgentRoute(t, source, tc.secondPin)
				agentEvents = subscribeTestAgentAdmissionWithIdentity(t, eb, admission, identity, entityID)
			}
			preflight, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != tc.wantRoutes {
				t.Fatalf("preflight = failure:%q routes:%#v, want %d legal routes", preflight.TargetFailure, preflight.DeliveryRoutes, tc.wantRoutes)
			}
			if tc.wantAgent {
				var agentRoute events.DeliveryRoute
				for _, route := range preflight.DeliveryRoutes {
					if route.Recipient.IsAgent() {
						agentRoute = route
						break
					}
				}
				if !agentRoute.Recipient.IsAgent() {
					t.Fatalf("preflight routes = %#v, want agent subscriber", preflight.DeliveryRoutes)
				}
				if agentRoute.AgentIdentity.FlowInstance() == "" {
					t.Fatalf("preflight agent route = %#v, want exact concrete identity", agentRoute)
				}
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(store.activations) != 1 || len(store.flowInstances) != 1 {
				t.Fatalf("activations/descriptors = %d/%d, want one admitted lifecycle instance", len(store.activations), len(store.flowInstances))
			}
			if routes := store.routes[evt.ID()]; len(routes) != tc.wantRoutes {
				t.Fatalf("persisted routes = %#v, want %d", routes, tc.wantRoutes)
			}
			if got := interceptor.Count(); got != tc.wantNodes {
				t.Fatalf("node handler interceptions = %d, want %d", got, tc.wantNodes)
			}
			if agentEvents != nil {
				select {
				case delivered := <-agentEvents:
					if delivered.ID() != evt.ID() {
						t.Fatalf("agent delivered event = %s, want %s", delivered.ID(), evt.ID())
					}
					if err := delivered.Complete(); err != nil {
						t.Fatalf("complete admitted agent delivery: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for admitted agent delivery")
				}
			}
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanCreateRejectSameEventRetryIsNoOpAndExplicitReplayUsesCommittedScope(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish initial: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("initial activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	replayTarget := subscribeInternalDeliveriesForTest(t, eb, "consumer-node-"+activation.Instance.InstanceID)
	store.flowInstanceDescriptorCalls = 0

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish retry: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("retry activations = %d, want committed replay without a second activation", len(store.activations))
	}
	requireNoConnectRoutePlanBusEvent(t, replayTarget, "same-event retry")

	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	replayed := requireBusEvent(t, replayTarget, "explicit committed replay")
	if replayed.FlowInstance() != activation.Instance.InstancePath || replayed.EntityID() != activation.Instance.EntityID {
		t.Fatalf("retry delivery target = flow_instance:%q entity:%q, want persisted %q/%q",
			replayed.FlowInstance(), replayed.EntityID(), activation.Instance.InstancePath, activation.Instance.EntityID)
	}
}

func TestEventBusPublish_ConnectRoutePlanCreatesRenamedTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, true)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"source_vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.activations) != 1 {
		t.Fatalf("activations = %d, want 1", len(store.activations))
	}
	activation := store.activations[0]
	if activation.Config["vertical_id"] != "v-1" || activation.Metadata["vertical_id"] != "v-1" {
		t.Fatalf("renamed activation config/metadata = %#v/%#v, want receiver vertical_id from adapter source_vertical_id", activation.Config, activation.Metadata)
	}
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-" + activation.Instance.InstanceID), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: activation.Instance.InstancePath,
		EntityID:     activation.Instance.EntityID,
	}),
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) || len(store.routes[evt.ID()]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want renamed-key created route %#v", store.routes[evt.ID()], want)
	}
}

func TestEventBusPublish_ConnectRoutePlanRejectsCreateConflict(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
	store := &connectRoutePlanLifecycleStore{
		connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
			targetRouteMemoryStore: newTargetRouteMemoryStore(),
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "one",
				EntityID:      eventtest.UUID("ent-1"),
				FlowInstance:  "consumer/one",
				AddressFields: map[string]string{"entity.vertical_id": "v-1"},
			}},
		},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle:            source,
		TemplateInstanceActivator: store.Activate,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store.bus = eb
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureInstanceConflict); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("delivery routes = %#v, want none on create conflict", routePlan.DeliveryRoutes())
	}
	if len(store.activations) != 0 {
		t.Fatalf("activations = %d, want 0 on conflict", len(store.activations))
	}
}

func TestEventBusPublish_ConnectRoutePlanLifecycleUnavailableBlocksLowerPrecedenceRescue(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
	}
	materializerCalled := false
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			materializerCalled = true
			return []DeliveryRouteBlueprint{{
				Recipient: events.MustNodeDeliveryRecipient("bogus-node"),
				Target:    events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus/one", EntityID: "bogus"},
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for lifecycle-unavailable canonical failure")
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureLifecycleUnavailable); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("delivery routes = %#v, want none on lifecycle-unavailable failure", routePlan.DeliveryRoutes())
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called during publish for lifecycle-unavailable canonical failure")
	}
}

func TestEventBusReplay_ConnectRoutePlanUsesPersistedInstanceKeyRouteAfterDescriptorDrift(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      eventtest.UUID("ent-1"),
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(one): %v", err)
	}
	consumerOne := subscribeInternalDeliveriesForTest(t, eb, "consumer-node-one")
	consumerTwo := subscribeInternalDeliveriesForTest(t, eb, "consumer-node-two")
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"vertical_id":"v-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())

	wantOne := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-one"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: eventtest.UUID("ent-1")})}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, consumerOne, "initial instance-key publish")
	if got.FlowInstance() != "consumer/one" || got.EntityID() != eventtest.UUID("ent-1") {
		t.Fatalf("initial delivery target = flow_instance:%q entity:%q, want consumer/one ent-1", got.FlowInstance(), got.EntityID())
	}
	if !deliveryRoutesContain(store.routes[eventID], wantOne) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want instance-key route %#v", store.routes[eventID], wantOne)
	}

	store.flowInstances = []ActiveFlowInstanceDescriptor{{
		InstanceID:    "two",
		EntityID:      eventtest.UUID("ent-2"),
		FlowInstance:  "consumer/two",
		AddressFields: map[string]string{"entity.vertical_id": "v-1"},
	}}
	store.flowInstanceDescriptorCalls = 0
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "two")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute(two): %v", err)
	}
	store.flowInstanceDescriptorCalls = 0

	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	got = requireBusEvent(t, consumerOne, "persisted replay after descriptor drift")
	if got.FlowInstance() != "consumer/one" || got.EntityID() != eventtest.UUID("ent-1") {
		t.Fatalf("replayed delivery target = flow_instance:%q entity:%q, want persisted consumer/one ent-1", got.FlowInstance(), got.EntityID())
	}
	select {
	case delivery := <-consumerTwo:
		_ = delivery.Complete()
		evt := delivery.Event()
		t.Fatalf("descriptor drift recipient received replay: flow_instance:%q entity:%q", evt.FlowInstance(), evt.EntityID())
	default:
	}
	if got := store.flowInstanceDescriptorCalls; got != 0 {
		t.Fatalf("replay descriptor calls = %d, want 0 because persisted route/scope is authoritative", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanPersistsRenamedTemplateInstanceKeyTarget(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, true)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      eventtest.UUID("ent-1"),
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"source_vertical_id":"v-1"}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node-one"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: eventtest.UUID("ent-1")})}

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !deliveryRoutesContain(routePlan.DeliveryRoutes(), want) || len(routePlan.DeliveryRoutes()) != 1 {
		t.Fatalf("route plan delivery routes = %#v, want renamed instance-key route %#v", routePlan.DeliveryRoutes(), want)
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none", plan.TargetFailure)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("preflight delivery routes = %#v, want renamed instance-key route %#v", plan.DeliveryRoutes, want)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) || len(store.routes[eventID]) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want renamed instance-key route %#v", store.routes[eventID], want)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if !containsString(live, "consumer-node-one") || !containsString(internal, "consumer-node-one") {
		t.Fatalf("replay live=%#v internal=%#v, want consumer-node-one from persisted connect route", live, internal)
	}
	if !deliveryRoutesContain(replayRoutes, want) {
		t.Fatalf("replay delivery routes = %#v, want %#v", replayRoutes, want)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForRenamedTemplateInstanceKeySourceGap(t *testing.T) {
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, true)
	store := &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
		flowInstances: []ActiveFlowInstanceDescriptor{{
			InstanceID:    "one",
			EntityID:      eventtest.UUID("ent-1"),
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", "one")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", json.RawMessage(`{"nested":{"source_vertical_id":"v-1"}}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if routePlan.TargetFailure != runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureInstanceSourceValueMissing) {
		t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, runtimepinrouting.ConnectFailureInstanceSourceValueMissing)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
		len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
		len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed renamed instance-key route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
			routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
			routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, runtimepinrouting.ConnectFailureInstanceSourceValueMissing.Code(); got != want {
		t.Fatalf("preflight target failure = %q, want %q", got, want)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
		len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
			plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 0 {
		t.Fatalf("persisted delivery routes = %#v, want none for fail-closed renamed instance-key route", routes)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForTemplateInstanceKeyGaps(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		flowInstances []ActiveFlowInstanceDescriptor
		addRoutes     []string
		wantFailure   runtimepinrouting.TargetFailure
	}{
		{
			name:        "missing source key value",
			payload:     `{}`,
			wantFailure: runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureInstanceSourceValueMissing),
		},
		{
			name:    "no receiver instance under rejecting policy",
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{{
				InstanceID:    "two",
				EntityID:      eventtest.UUID("ent-2"),
				FlowInstance:  "consumer/two",
				AddressFields: map[string]string{"entity.vertical_id": "v-2"},
			}},
			addRoutes:   []string{"two"},
			wantFailure: runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetUnresolved),
		},
		{
			name:    "ambiguous receiver instance key",
			payload: `{"vertical_id":"v-1"}`,
			flowInstances: []ActiveFlowInstanceDescriptor{
				{InstanceID: "one", EntityID: eventtest.UUID("ent-1"), FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
				{InstanceID: "two", EntityID: eventtest.UUID("ent-2"), FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			},
			addRoutes:   []string{"one", "two"},
			wantFailure: runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureTargetAmbiguous),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
			store := &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
				flowInstances:          tc.flowInstances,
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			for _, instanceID := range tc.addRoutes {
				if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("consumer", instanceID)}); err != nil {
					t.Fatalf("AddFlowInstanceRoute(%s): %v", instanceID, err)
				}
			}
			eventID := uuid.NewString()
			evt := connectRoutePlanStaticProducerEvent(eventID,
				events.EventType("producer/deploy.done"), "", "", json.RawMessage(tc.payload), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

			routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
			if err != nil {
				t.Fatalf("planSubscribedRoutePlan: %v", err)
			}
			if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
				t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
			}
			if routePlan.TargetFailure != tc.wantFailure {
				t.Fatalf("target failure = %q, want %q", routePlan.TargetFailure, tc.wantFailure)
			}
			if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
				len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
				len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
				t.Fatalf("fail-closed instance-key route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
					routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
					routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
			}

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got, want := plan.TargetFailure, tc.wantFailure.Code(); got != want {
				t.Fatalf("preflight target failure = %q, want %q", got, want)
			}
			if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
				len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
					plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
			}
			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if routes := store.routes[eventID]; len(routes) != 0 {
				t.Fatalf("persisted delivery routes = %#v, want none for fail-closed instance-key route", routes)
			}
		})
	}
}

func TestEventBusPublish_ConnectRoutePlanWithoutCanonicalSubscriberPersistsMatchedNoRecipient(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"producer-node": {
					ID:            "producer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.done": {}},
				},
			},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:    "producer.deploy_done",
		To:      "consumer.deploy_completed",
		Adapter: "deploy_done_to_completed",
	}}))
	store := newConnectRoutePlanStaticStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := connectRoutePlanStaticProducerEvent(eventID,
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalMatched || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want matched connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !routePlan.TargetFailure.Empty() {
		t.Fatalf("target failure = %q, want none for typed no-delivery settlement", routePlan.TargetFailure)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.RoutedRecipients) != 0 ||
		len(routePlan.SubscribedRecipients) != 0 || len(routePlan.RecipientIDs()) != 0 ||
		len(routePlan.PersistedRecipientIDs()) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed connect route exposed lower-precedence fallback: live=%#v intents=%#v routed=%#v subscriptions=%#v recipients=%#v persisted=%#v routes=%#v",
			routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.RoutedRecipients, routePlan.SubscribedRecipients,
			routePlan.RecipientIDs(), routePlan.PersistedRecipientIDs(), routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" {
		t.Fatalf("target failure = %q, want none for typed no-delivery settlement", plan.TargetFailure)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 || len(plan.RoutedRecipients) != 0 ||
		len(plan.SubscriptionRecipients) != 0 || len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("preflight exposed lower-precedence fallback: recipients=%#v persisted=%#v routed=%#v subscriptions=%#v routes=%#v",
			plan.Recipients, plan.PersistedRecipients, plan.RoutedRecipients, plan.SubscriptionRecipients, plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[eventID]; len(routes) != 0 {
		t.Fatalf("persisted delivery routes = %#v, want none when matched connect receiver is unsubscribed", routes)
	}
	settlement := store.settlements[eventID]
	if !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryMatchedNoRecipient || len(settlement.Ledger().Plans()) != 1 {
		t.Fatalf("route settlement = %#v, want matched_no_recipient with one plan", settlement)
	}
	if got := store.scopes[eventID]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	if got := store.receipts[eventID]; got == "dead_letter" {
		t.Fatal("matched no-recipient settlement was also committed as a target-failure dead letter")
	}
	if got := store.receiptErrs[eventID]; got != nil {
		t.Fatalf("pipeline receipt failure = %#v, want none", got)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailsClosedForInvalidLoweredPlan(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:     "consumer",
			mode:   "static",
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.deploy_done",
		To:   "consumer.missing_input",
	}}))
	eb, err := newScopedTestEventBus(newTargetRouteMemoryStore(), EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if got, want := routePlan.TargetFailure, runtimepinrouting.TargetFailureFromConnect(runtimepinrouting.ConnectFailureReceiverInputPinMissing); got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed plan has executable routes: live=%#v intents=%#v routes=%#v", routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got, want := plan.TargetFailure, "receiver_input_pin_missing"; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("delivery routes = %#v, want none when lowered plan is invalid", plan.DeliveryRoutes)
	}
}

func TestEventBusPublish_ConnectRoutePlanFailureSkipsRecipientPlanMaterializer(t *testing.T) {
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:     "consumer",
			mode:   "static",
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.deploy_done",
		To:   "consumer.missing_input",
	}}))
	materializerCalled := false
	eb, err := newScopedTestEventBus(newTargetRouteMemoryStore(), EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			materializerCalled = true
			return []DeliveryRouteBlueprint{{
				Recipient: events.MustNodeDeliveryRecipient("bogus-node"),
				Target:    events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus", EntityID: "bogus"},
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for matched lowered connect failure")
	}
	if routePlan.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if len(routePlan.LiveRecipients) != 0 || len(routePlan.DeliveryIntents) != 0 || len(routePlan.DeliveryRoutes()) != 0 {
		t.Fatalf("fail-closed plan has executable routes: live=%#v intents=%#v routes=%#v", routePlan.LiveRecipients, routePlan.DeliveryIntents, routePlan.DeliveryRoutes())
	}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if materializerCalled {
		t.Fatalf("recipient plan materializer was called for matched lowered connect failure")
	}
	if got, want := plan.TargetFailure, "receiver_input_pin_missing"; got != want {
		t.Fatalf("target failure = %q, want %q", got, want)
	}
	if len(plan.DeliveryRoutes) != 0 {
		t.Fatalf("delivery routes = %#v, want none when matched lowered plan fails", plan.DeliveryRoutes)
	}
}

func TestEventBusPlan_UnmatchedCanonicalRouteUsesLowerPrecedenceFallback(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeTestAgent(t, eb, "legacy-agent", events.EventType("legacy.event"))
	defer unsubscribeTestAgent(eb, "legacy-agent")
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("legacy.event"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan, err := eb.planSubscribedRoutePlan(context.Background(), evt, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if routePlan.AuthorityState != RoutePlanAuthorityLowerPrecedence || routePlan.AuthorityOwner != routePlanSourceAgentPolicy {
		t.Fatalf("route plan authority = %q/%q, want lower-precedence agent policy", routePlan.AuthorityState, routePlan.AuthorityOwner)
	}
	if !containsString(routePlan.RecipientIDs(), "legacy-agent") {
		t.Fatalf("route plan recipients = %#v, want legacy-agent", routePlan.RecipientIDs())
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	requireBusEvent(t, ch, "legacy fallback delivery")
}

func TestRoutePlanCanonicalFailClosedDropsExecutableRoutes(t *testing.T) {
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteFailedClosed(routeIntentProducerConnectRoutePlan, runtimepinrouting.FailureTargetNotSubscribed)
	routePlan.AddLiveRecipients(RoutePlanLiveRecipient{Recipient: events.MustNodeDeliveryRecipient("bogus-node"), PersistAsDelivery: true,
		Producer: routeIntentProducerRecipientMaterializer,
	})
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{Recipient: events.MustNodeDeliveryRecipient("bogus-node"), TargetBlueprint: events.RouteIdentity{FlowID: "bogus", FlowInstance: "bogus", EntityID: "bogus"},
		Producer: routeIntentProducerRecipientMaterializer,
		Persist:  true,
	})

	got := routePlan.Normalized()
	if got.AuthorityState != RoutePlanAuthorityCanonicalFailedClosed || got.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("route plan authority = %q/%q, want fail-closed connect route plan", got.AuthorityState, got.AuthorityOwner)
	}
	if got.TargetFailure != runtimepinrouting.FailureTargetNotSubscribed {
		t.Fatalf("target failure = %q, want %q", got.TargetFailure, runtimepinrouting.FailureTargetNotSubscribed)
	}
	if len(got.LiveRecipients) != 0 || len(got.DeliveryIntents) != 0 || len(got.DeliveryRoutes()) != 0 || len(got.PersistedRecipientIDs()) != 0 {
		t.Fatalf("fail-closed plan exposed executable routes: live=%#v intents=%#v routes=%#v persisted=%#v", got.LiveRecipients, got.DeliveryIntents, got.DeliveryRoutes(), got.PersistedRecipientIDs())
	}
}

func TestOrdinaryOperatorPublishCannotAcquireProviderTargetFreeAuthorityByEventName(t *testing.T) {
	const eventName = "inbound.telegram.text_message"
	generation := triggergeneration.FromCanonicalBytes([]byte("operator-provider-trigger-generation"))
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", eventName, "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64), generation,
	)
	source := providerOutputAuthorizedTestSource{
		Source: semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
			id: "consumer", mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name: "telegram_text", Event: eventName, Source: "external",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {ID: "consumer-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventName: {}}},
			},
		}}, nil)),
		generation: generation, authorizations: []runtimeprovideroutput.Authorization{authorization},
	}
	resolver := newConnectRoutePlanResolver(source, nil, nil, nil, nil, nil)
	externalSource, err := events.NewExternalIngressRoutingSource("consumer", eventtest.UUID("provider-ingress"), events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatalf("external routing source: %v", err)
	}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		uuid.NewString(), events.EventType(eventName), "operator-api", "", json.RawMessage(`{"chat_id":"42"}`),
		0, "run-1", "", events.EventEnvelope{}, externalSource, time.Now().UTC(),
	)
	if matched := resolver.matchedPlans(context.Background(), evt); len(matched) != 0 {
		t.Fatalf("ordinary operator event matched provider target-free plans = %#v", matched)
	}
	authorizedCtx := withProviderOutputAuthorization(context.Background(), authorization)
	if matched := resolver.matchedPlans(authorizedCtx, evt); len(matched) != 1 {
		t.Fatalf("verified provider output matched plans = %#v, want one", matched)
	}
}

func TestPublicInputAdmissionUsesCanonicalTemplateLifecycleModes(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mode           canonicalrouting.TemplateInstanceRouteMode
		seedExisting   bool
		wantActivation bool
	}{
		{name: "create", mode: canonicalrouting.TemplateInstanceRouteCreate, wantActivation: true},
		{name: "select", mode: canonicalrouting.TemplateInstanceRouteSelect, seedExisting: true},
		{name: "select-or-create", mode: canonicalrouting.TemplateInstanceRouteSelectOrCreate, wantActivation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			source := connectRoutePlanTemplateInstanceSource(t, tc.mode, false)
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("template route source has no contract bundle")
			}
			bundle.Package.Connect = nil
			bundle.Semantics.CompositionConnects = nil
			store := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
					targetRouteMemoryStore: newTargetRouteMemoryStore(),
				},
			}
			if tc.seedExisting {
				store.flowInstances = []ActiveFlowInstanceDescriptor{{
					InstanceID: "one", EntityID: eventtest.UUID("ent-public-select"),
					FlowInstance: "consumer/one", FlowTemplate: "consumer",
					AddressFields: map[string]string{"entity.vertical_id": "vertical-1"},
				}}
			}
			eventBus, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle:            source,
				TemplateInstanceActivator: store.Activate,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			store.bus = eventBus
			if tc.seedExisting {
				if err := eventBus.AddFlowInstanceRouteContext(ctx, FlowInstanceRouteMaterializationRequest{
					Identity: runtimeflowidentity.DeriveRoute("consumer", "one"),
				}); err != nil {
					t.Fatalf("seed selected flow route: %v", err)
				}
			}

			association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
			endpoint, ok := association.Endpoint()
			if !ok {
				t.Fatalf("resolve public input endpoint: %v", association.Err())
			}
			eventID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(
				eventID, events.EventType(endpoint.Event.Canonical), "operator-api", "",
				json.RawMessage(`{"vertical_id":"vertical-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if err := eventBus.PublishPublicInputAcknowledged(ctx, evt, endpoint); err != nil {
				t.Fatalf("PublishPublicInputAcknowledged: %v", err)
			}

			store.mu.Lock()
			routes := append([]events.DeliveryRoute(nil), store.routes[eventID]...)
			store.mu.Unlock()
			if len(routes) != 1 {
				t.Fatalf("persisted public-input delivery routes = %#v, want one", routes)
			}
			if target := routes[0].Target.Route().Normalized(); target.FlowID != "consumer" || !strings.HasPrefix(target.FlowInstance, "consumer/") {
				t.Fatalf("public-input target = %#v, want a concrete consumer instance", target)
			}
			if got := len(store.activations); (got == 1) != tc.wantActivation {
				t.Fatalf("committed activations = %d, want activation=%t", got, tc.wantActivation)
			}
		})
	}
}

func TestAPIEventPublicationCommittedCompletionSurvivesPostCommitLocalFailures(t *testing.T) {
	for _, test := range []struct {
		name             string
		failFinalization bool
	}{
		{name: "activation finalization", failFinalization: true},
		{name: "dispatch admission"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("template route source has no contract bundle")
			}
			bundle.Package.Connect = nil
			bundle.Semantics.CompositionConnects = nil
			lifecycleStore := &connectRoutePlanLifecycleStore{
				connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{targetRouteMemoryStore: newTargetRouteMemoryStore()},
			}
			selected := &apiEventPublicationMemoryStore{connectRoutePlanLifecycleStore: lifecycleStore}
			activationOwner := newTestFlowInstanceActivationOwner(lifecycleStore.Activate)
			var finalizer runtimepipeline.CommittedFlowInstanceActivationFinalizer = activationOwner
			if test.failFinalization {
				finalizer = runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(context.Context, runtimepipeline.CommittedFlowInstanceActivation) error {
					return errors.New("simulated local activation finalization failure")
				})
			}
			eventBus, err := newScopedTestEventBus(selected, EventBusOptions{
				ContractBundle:            source,
				TemplateInstanceActivator: lifecycleStore.Activate,
				TemplateInstancePlanner:   activationOwner,
				FlowActivationFinalizer:   finalizer,
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			lifecycleStore.bus = eventBus
			association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
			endpoint, ok := association.Endpoint()
			if !ok {
				t.Fatalf("resolve public input endpoint: %v", association.Err())
			}
			apiEndpoint, err := NewTemplateAPIEventPublicationEndpoint(source, endpoint)
			if err != nil {
				t.Fatalf("admit API event publication endpoint: %v", err)
			}
			eventID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(
				eventID, events.EventType(endpoint.Event.Canonical), "operator-api", "",
				json.RawMessage(`{"vertical_id":"vertical-1"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
			)
			completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
			committed, replay, err := eventBus.PublishAPIEventAcknowledged(ctx, evt, &apiEndpoint, apiidempotency.Request{
				Method: "event.publish", ActorTokenID: "operator", IdempotencyKey: "post-commit-" + strings.ReplaceAll(test.name, " ", "-"), RequestHash: "request-hash",
			}, completion)
			if err != nil {
				t.Fatalf("PublishAPIEventAcknowledged after committed completion: %v", err)
			}
			if replay || committed.ResourceID != eventID {
				t.Fatalf("committed API publication = %#v replay=%t, want new completion for %s", committed, replay, eventID)
			}
			if selected.completion.ResourceID != eventID {
				t.Fatalf("stored API completion = %#v, want %s", selected.completion, eventID)
			}
		})
	}
}

func TestPublicInputAdmissionFailsClosedNegativeMatrix(t *testing.T) {
	validSource := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
	validAssociation := semanticview.BuildAuthoredEventEndpointCensus(validSource).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
	validEndpoint, ok := validAssociation.Endpoint()
	if !ok {
		t.Fatalf("resolve valid public input endpoint: %v", validAssociation.Err())
	}

	tests := []struct {
		name string
		run  func(*testing.T) error
		want string
	}{
		{
			name: "missing exact endpoint",
			run: func(*testing.T) error {
				_, err := newPublicInputAdmission(validSource, semanticview.AuthoredEventEndpoint{})
				return err
			},
			want: "receiver_input_pin_missing",
		},
		{
			name: "ambiguous exact endpoint",
			run: func(t *testing.T) error {
				ambiguousSource := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteCreate, false)
				bundle, ok := semanticview.Bundle(ambiguousSource)
				if !ok {
					t.Fatal("ambiguous public input source has no bundle")
				}
				pins := bundle.Semantics.FlowInputEventPins["consumer"]
				if len(pins) != 1 {
					t.Fatalf("consumer input pins = %#v, want one", pins)
				}
				duplicate := pins[0]
				duplicate.Event = "deploy.alternate"
				bundle.Semantics.FlowInputEventPins["consumer"] = append(pins, duplicate)
				_, err := newPublicInputAdmission(ambiguousSource, validEndpoint)
				return err
			},
			want: "ambiguous",
		},
		{
			name: "unsupported non-template receiver",
			run: func(t *testing.T) error {
				staticSource := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
					id: "consumer", mode: "static",
					inputs: []runtimecontracts.FlowInputEventPin{{Name: "deploy_completed", Event: "deploy.completed"}},
				}}, nil))
				association := semanticview.BuildAuthoredEventEndpointCensus(staticSource).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
				endpoint, ok := association.Endpoint()
				if !ok {
					t.Fatalf("resolve static input endpoint: %v", association.Err())
				}
				_, err := newPublicInputAdmission(staticSource, endpoint)
				return err
			},
			want: "receiver_flow_missing",
		},
		{
			name: "runtime target failure",
			run: func(t *testing.T) error {
				source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelect, false)
				store := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{targetRouteMemoryStore: newTargetRouteMemoryStore()}}
				eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstanceActivator: store.Activate})
				if err != nil {
					t.Fatalf("NewEventBusWithOptions: %v", err)
				}
				store.bus = eventBus
				association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "deploy_completed")
				endpoint, ok := association.Endpoint()
				if !ok {
					t.Fatalf("resolve select input endpoint: %v", association.Err())
				}
				evt := eventtest.RunCreatingRootIngress(
					uuid.NewString(), events.EventType(endpoint.Event.Canonical), "operator-api", "",
					json.RawMessage(`{"vertical_id":"missing"}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
				)
				return eventBus.PublishPublicInputAcknowledged(testAuthorActivityContext(context.Background()), evt, endpoint)
			},
			want: "not routable",
		},
		{
			name: "noncanonical route owner",
			run: func(*testing.T) error {
				admission := publicInputAdmission{endpointID: validEndpoint.ID, flowID: validEndpoint.FlowID, pinName: validEndpoint.PinName, eventType: events.EventType(validEndpoint.Event.Canonical)}
				plan := newRoutePlan(connectRoutePlanStaticProducerEvent(uuid.NewString(), events.EventType(validEndpoint.Event.Canonical), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()))
				plan.MarkLowerPrecedenceRouteProduction(routeIntentProducerAgentPolicy)
				return requirePublicInputRoutePlan(withPublicInputAdmission(context.Background(), admission), plan)
			},
			want: "canonical connect route owner",
		},
		{
			name: "zero durable deliveries",
			run: func(*testing.T) error {
				admission := publicInputAdmission{endpointID: validEndpoint.ID, flowID: validEndpoint.FlowID, pinName: validEndpoint.PinName, eventType: events.EventType(validEndpoint.Event.Canonical)}
				plan := newRoutePlan(connectRoutePlanStaticProducerEvent(uuid.NewString(), events.EventType(validEndpoint.Event.Canonical), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()))
				plan.MarkCanonicalRouteMatched(routeIntentProducerConnectRoutePlan)
				return requirePublicInputRoutePlan(withPublicInputAdmission(context.Background(), admission), plan)
			},
			want: "zero durable deliveries",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("public input rejection error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestRoutePlanNormalizationPreservesAuthorityState(t *testing.T) {
	evt := connectRoutePlanStaticProducerEvent(uuid.NewString(),
		events.EventType("producer/deploy.done"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	routePlan := newRoutePlan(evt)
	routePlan.MarkCanonicalRouteMatched(routeIntentProducerConnectRoutePlan)
	staticRoute := connectRoutePlanStaticDeliveryRoute()
	routePlan.AddDeliveryIntents(RoutePlanDeliveryIntent{Recipient: staticRoute.Recipient, TargetBlueprint: staticRoute.Target.Route(), TargetOwnership: staticRoute.Target,
		Producer: routeIntentProducerConnectRoutePlan,
		Persist:  true,
	})

	got := routePlan.Normalized()
	if got.AuthorityState != RoutePlanAuthorityCanonicalMatched || got.AuthorityOwner != routePlanSourceConnectRoutePlan {
		t.Fatalf("normalized route plan authority = %q/%q, want matched connect route plan", got.AuthorityState, got.AuthorityOwner)
	}
	if !deliveryRoutesContain(got.DeliveryRoutes(), connectRoutePlanStaticDeliveryRoute()) {
		t.Fatalf("normalized route plan delivery routes = %#v, want static connect route", got.DeliveryRoutes())
	}
}

func connectRoutePlanTemplateInstanceSource(t testing.TB, mode canonicalrouting.TemplateInstanceRouteMode, renamedSource bool) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{
		Mode:          mode,
		RenamedSource: renamedSource,
	})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func connectRoutePlanTemplateInstanceMultiInputSource(
	t testing.TB,
	mode canonicalrouting.TemplateInstanceRouteMode,
	secondPin canonicalrouting.TemplateInstanceSecondPin,
	consumer canonicalrouting.TemplateInstanceConsumer,
) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{
		Mode:      mode,
		SecondPin: secondPin,
		Consumer:  consumer,
	})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func connectRoutePlanCreateResolutionSource(t testing.TB, mint string) semanticview.Source {
	t.Helper()
	mode := canonicalrouting.CreateMintUUID
	if strings.TrimSpace(mint) == runtimecontracts.FlowInputCarrySourceEventID {
		mode = canonicalrouting.CreateMintEventID
	}
	root := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: mode})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func connectRoutePlanPayloadCreateResolutionSource(t testing.TB) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func connectRoutePlanCarriedKeyResolutionSource(t testing.TB, mode runtimecontracts.FlowInputResolutionMode) semanticview.Source {
	t.Helper()
	resolutionMode := canonicalrouting.SelectResolutionSelect
	if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		resolutionMode = canonicalrouting.SelectResolutionSelectOrCreate
	} else if mode != runtimecontracts.FlowInputResolutionModeSelect {
		t.Fatalf("unsupported carried-key resolution mode %q", runtimecontracts.FlowInputResolutionModeCode(mode))
	}
	root := canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: resolutionMode})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func connectRoutePlanCarriedKeyResolutionSourceWithIdentitySource(
	t testing.TB,
	mode runtimecontracts.FlowInputResolutionMode,
	identitySource string,
) semanticview.Source {
	t.Helper()
	if strings.TrimSpace(identitySource) != "payload.external_account_id" {
		t.Fatalf("unsupported canonical carried-key identity source %q", identitySource)
	}
	resolutionMode := canonicalrouting.SelectResolutionSelect
	if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		resolutionMode = canonicalrouting.SelectResolutionSelectOrCreate
	} else if mode != runtimecontracts.FlowInputResolutionModeSelect {
		t.Fatalf("unsupported carried-key resolution mode %q", runtimecontracts.FlowInputResolutionModeCode(mode))
	}
	root := canonicalrouting.CopyTemplateSelectResolutionRenamedSource(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: resolutionMode})
	return loadConnectRoutePlanCanonicalSource(t, root)
}

func loadConnectRoutePlanCanonicalSource(t testing.TB, root string) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func mustInstanceKeyConnectRoutePlan(t testing.TB, source semanticview.Source) runtimepinrouting.ConnectRoutePlan {
	t.Helper()
	plans, issues := compiledConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v", issues)
	}
	for _, plan := range plans {
		if plan.InstanceKey() != nil {
			return plan
		}
	}
	t.Fatalf("lowered plans contain no instance-key route: %#v", plans)
	return runtimepinrouting.ConnectRoutePlan{}
}

func compiledConnectPlans(source semanticview.Source) ([]runtimepinrouting.ConnectRoutePlan, []runtimepinrouting.ConnectRoutePlanIssue) {
	graph := runtimepinrouting.CompileConnectGraph(source)
	return graph.Plans(), graph.Issues()
}

func mustBusTemplateInstanceField(t testing.TB, raw string) runtimecontracts.TemplateInstanceField {
	t.Helper()
	field, err := runtimecontracts.ParseTemplateInstanceField(raw)
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField(%q): %v", raw, err)
	}
	return field
}

func connectRoutePlanStaticSource(connect runtimecontracts.FlowPackageConnect) semanticview.Source {
	if strings.TrimSpace(connect.Adapter) == "" {
		connect.Adapter = "deploy_done_to_completed"
	}
	return semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.completed": existingOwnerHandlerFixture()},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{connect}))
}

func connectRoutePlanRootProducerStaticSource() semanticview.Source {
	bundle := connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "ready",
				Event: "root.ready",
			}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"root.ready": existingOwnerHandlerFixture()},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: ".root_ready",
		To:   "consumer.ready",
	}})
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{
			Outputs: runtimecontracts.FlowOutputPins{
				EventPins: []runtimecontracts.FlowOutputEventPin{{
					Name:  "root_ready",
					Event: "root.ready",
				}},
			},
		},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"root.ready": {},
	}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanRootProducerSingletonSource() semanticview.Source {
	bundle := connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id: "scout", mode: runtimecontracts.FlowModeSingleton,
			inputs: []runtimecontracts.FlowInputEventPin{{Name: "ready", Event: "root.ready"}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"scout-node": {
					ID: "scout-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						"root.ready": {CreateEntity: true},
					},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{From: ".root_ready", To: "scout.ready"}})
	bundle.Semantics.Name = "root"
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{
		Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "root_ready", Event: "root.ready"}}},
	}}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{"root.ready": {}}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanSingletonProducerRootReceiverSource() semanticview.Source {
	bundle := connectRoutePlanTestBundle([]connectRoutePlanTestFlow{{
		id: "scout", mode: runtimecontracts.FlowModeSingleton,
		outputs: []runtimecontracts.FlowOutputEventPin{{Name: "completed", Event: "scout.completed"}},
	}}, []runtimecontracts.FlowPackageConnect{{From: "scout.completed", To: ".scout_completed"}})
	bundle.Semantics.Name = "root"
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{
		Inputs: runtimecontracts.FlowInputPins{
			Events:    []string{"scout.completed"},
			EventPins: []runtimecontracts.FlowInputEventPin{{Name: "scout_completed", Event: "scout.completed"}},
		},
	}}
	rootHandler := existingOwnerHandlerFixture()
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"root-collector": {
			ID: "root-collector", SubscribesTo: []string{"scout.completed"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"scout.completed": rootHandler},
		},
	}
	bundle.FlowTree.Root.Schema = *bundle.RootSchema
	bundle.FlowTree.Root.Nodes = bundle.Nodes
	bundle.Semantics.NodeHandlers["root-collector"] = map[string]runtimecontracts.SystemNodeEventHandler{"scout.completed": rootHandler}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{"scout.completed": {}}
	return semanticview.Wrap(bundle)
}

func connectRoutePlanStaticDeliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("consumer-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
		EntityID:     connectRoutePlanStaticOwner().EntityID,
	}),
	}
}

func connectRoutePlanStaticOwner() ActiveTargetDescriptor {
	return testSelectedRunTargetOwner("consumer-selected-owner", "consumer", "consumer-selected-owner")
}

func newConnectRoutePlanStaticStore() *targetRouteMemoryStore {
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(connectRoutePlanStaticOwner())
	return store
}

func connectRoutePlanTestBundle(flows []connectRoutePlanTestFlow, connects []runtimecontracts.FlowPackageConnect) *runtimecontracts.WorkflowContractBundle {
	connects = append([]runtimecontracts.FlowPackageConnect(nil), connects...)
	for i := range connects {
		connects[i].SourceFile = "package.yaml"
		connects[i].SourceLine = i + 1
	}
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	flowInputs := make(map[string][]string, len(flows))
	flowOutputs := make(map[string][]string, len(flows))
	flowInputPins := make(map[string][]runtimecontracts.FlowInputEventPin, len(flows))
	flowOutputPins := make(map[string][]runtimecontracts.FlowOutputEventPin, len(flows))
	nodeHandlers := map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	agentRefs := map[string]runtimecontracts.ContractURIRef{}
	workflowName := ""
	rootEntities := runtimecontracts.EntityContractsDocument{}
	for _, flow := range flows {
		schema := runtimecontracts.FlowSchemaDocument{
			Mode: flow.mode,
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events:    connectRoutePlanInputEvents(flow.inputs),
					EventPins: flow.inputs,
				},
				Outputs: runtimecontracts.FlowOutputPins{
					Events:    connectRoutePlanOutputEvents(flow.outputs),
					EventPins: flow.outputs,
				},
			},
		}
		view := runtimecontracts.FlowContractView{
			Paths:  runtimecontracts.FlowContractPaths{ID: flow.id, Flow: flow.id},
			Schema: schema,
			Path:   flow.id,
			Agents: flow.agents,
			Nodes:  flow.nodes,
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		flowSchemas[flow.id] = schema
		flowInputs[flow.id] = append([]string(nil), schema.Pins.Inputs.Events...)
		flowOutputs[flow.id] = append([]string(nil), schema.Pins.Outputs.Events...)
		flowInputPins[flow.id] = append([]runtimecontracts.FlowInputEventPin(nil), flow.inputs...)
		flowOutputPins[flow.id] = append([]runtimecontracts.FlowOutputEventPin(nil), flow.outputs...)
		for _, node := range flow.nodes {
			if len(node.EventHandlers) > 0 {
				nodeHandlers[node.ID] = node.EventHandlers
			}
		}
		for logicalID := range flow.agents {
			agentRefs[flow.id+"/"+logicalID] = runtimecontracts.ContractURIRef{
				Kind:    "agent",
				FlowID:  flow.id,
				LocalID: logicalID,
				Path:    flow.id,
				Full:    "test://" + flow.id + "/" + logicalID,
			}
		}
		if len(flow.entityFields) > 0 && workflowName == "" {
			workflowName = flow.id
			rootEntities["test_entity"] = runtimecontracts.EntityContract{Fields: flow.entityFields}
		}
	}
	root := runtimecontracts.FlowContractView{Children: children}
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: rootEntities,
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: agentRefs,
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: byID,
		},
		FlowSchemas: flowSchemas,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version:             "1.0.0",
			Name:                workflowName,
			FlowInputs:          flowInputs,
			FlowOutputs:         flowOutputs,
			FlowInputEventPins:  flowInputPins,
			FlowOutputEventPins: flowOutputPins,
			CompositionConnects: connects,
			NodeHandlers:        nodeHandlers,
		},
	}
}

func connectRoutePlanInputEvents(pins []runtimecontracts.FlowInputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func connectRoutePlanOutputEvents(pins []runtimecontracts.FlowOutputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func requireNoConnectRoutePlanBusEvent(t testing.TB, ch <-chan *LocalDelivery, context string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s: unexpected lower-precedence bus event: %#v", context, delivery.Event())
	default:
	}
}

func subscriberListContains(in []Subscriber, id, path string) bool {
	for _, subscriber := range in {
		if subscriber.Recipient.ID() == id && subscriber.Path == path {
			return true
		}
	}
	return false
}

func subscriberListContainsRouteSource(in []Subscriber, id, path, routeSource string) bool {
	for _, subscriber := range in {
		if subscriber.Recipient.ID() == id && subscriber.Path == path && subscriber.RouteSourceCode() == routeSource {
			return true
		}
	}
	return false
}
