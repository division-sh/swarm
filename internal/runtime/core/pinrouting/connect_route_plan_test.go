package pinrouting

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestConnectSourceEndpointMatchesEventUsesImmutableSourceAcrossTargetProjection(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done")
	source := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"}
	for _, tc := range []struct {
		name   string
		target events.RouteIdentity
	}{
		{name: "root receiver", target: events.RouteIdentity{EntityID: "root-entity"}},
		{name: "different template target", target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source)
			evt := eventtest.RunCreatingRootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", envelope, time.Unix(1, 0).UTC())
			evt = eventtest.TargetRouted(evt, tc.target)
			if !connectSourceEndpointMatchesTestEvent(endpoint, evt) {
				t.Fatalf("source endpoint did not match immutable producer route; envelope = %#v", evt.NormalizedEnvelope())
			}
		})
	}
}

func TestStaticConnectReceiverAgentPreservesExactProjectScopeAndLocalCoordinate(t *testing.T) {
	omittedOwner := "test://connect/packages/first/worker"
	literalOwner := "test://connect/packages/second/worker"
	omittedEntry := runtimecontracts.AgentRegistryEntry{}
	literalEntry := runtimecontracts.AgentRegistryEntry{
		ID:             "public-worker",
		AuthoredFields: map[string]bool{"id": true},
	}
	first := runtimecontracts.FlowContractView{
		Path: "first-support",
		Paths: runtimecontracts.FlowContractPaths{
			ID: "first-support", PackageKey: "packages/first",
		},
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": omittedEntry},
		AgentURIs: map[string]string{"worker": omittedOwner},
	}
	second := runtimecontracts.FlowContractView{
		Path: "second-support",
		Paths: runtimecontracts.FlowContractPaths{
			ID: "second-support", PackageKey: "packages/second",
		},
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": literalEntry},
		AgentURIs: map[string]string{"worker": literalOwner},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{first, second}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"first-support":  &root.Children[0],
				"second-support": &root.Children[1],
			},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			omittedOwner: {Kind: "agent", FlowID: "first-support", LocalID: "worker", Full: omittedOwner},
			literalOwner: {Kind: "agent", FlowID: "second-support", LocalID: "worker", Full: literalOwner},
		}},
	})

	for _, tc := range []struct {
		name       string
		packageKey string
		flowID     string
		publicID   string
		owner      string
	}{
		{name: "omitted id", packageKey: "packages/first", flowID: "first-support", publicID: "worker", owner: omittedOwner},
		{name: "literal override", packageKey: "packages/second", flowID: "second-support", publicID: "public-worker", owner: literalOwner},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := semanticview.AuthoredEventEndpoint{
				Kind:         semanticview.EventEndpointAgent,
				PackageKey:   tc.packageKey,
				FlowID:       tc.flowID,
				AgentLocalID: "worker",
				AgentID:      tc.publicID,
			}
			route, ok := staticConnectReceiverDeliveryRoute(source, endpoint, events.RouteIdentity{})
			if !ok {
				t.Fatal("static connect receiver route was omitted")
			}
			if got := route.Recipient.ID(); got != tc.publicID {
				t.Fatalf("recipient id = %q, want %q", got, tc.publicID)
			}
			if got := route.AgentPlan.Name.Owner; got != tc.owner {
				t.Fatalf("identity owner = %q, want %q", got, tc.owner)
			}
		})
	}
}

func TestConnectReceiverPinAdmissionOwnsRuntimeCollisionIdentity(t *testing.T) {
	source := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic, "work_ready", "work.ready", "producer/work.ready")
	plan := func(pin, event string) ConnectRoutePlan {
		return ConnectRoutePlan{
			source: source,
			receiver: newConnectRoutePlanEndpoint(
				ConnectEndpointRoleConsumer,
				false,
				"consumer",
				"consumer",
				runtimecontracts.FlowModeStatic,
				pin,
				event,
				"consumer/"+event,
			),
			authoredLocation: "package.yaml:10",
		}
	}
	consumerNode := identitytest.FlowNode(t, "consumer", "consumer-node")
	route := ConnectDeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(consumerNode), Target: events.RouteIdentity{
		FlowID:       "consumer",
		FlowInstance: "consumer",
		EntityID:     flowidentity.EntityID("consumer"),
	}, Handler: MustConnectReceiverHandler(consumerNode),
	}
	var admission ConnectReceiverPinAdmission
	if err := admission.Admit(plan("accepted", "work.accepted"), []ConnectDeliveryRoute{route}); err != nil {
		t.Fatalf("admit first receiver pin: %v", err)
	}
	if err := admission.Admit(plan("audited", "work.audited"), []ConnectDeliveryRoute{route}); err != nil {
		t.Fatalf("admit second receiver pin: %v", err)
	}
	collisions := admission.Collisions()
	if len(collisions) != 1 || len(collisions[0].ReceiverPinDiagnostics()) != 2 {
		t.Fatalf("receiver-pin collisions = %#v, want one typed two-pin collision", collisions)
	}
	if !strings.Contains(collisions[0].Message(), consumerNode.Key()) || !strings.Contains(collisions[0].Message(), "multiple receiver pins") {
		t.Fatalf("collision diagnostic = %q", collisions[0].Message())
	}
}

func TestConnectSourceEndpointMatchesEventRejectsTargetIdentityAsSource(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "consumer", "consumer", runtimecontracts.FlowModeStatic, "", "deploy.done", "consumer/deploy.done")
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"})
	evt := eventtest.RunCreatingRootIngress("", "deploy.done", "", "", []byte(`{}`), 0, "", "", envelope, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, target)
	if connectSourceEndpointMatchesTestEvent(endpoint, evt) {
		t.Fatalf("consumer target matched as producer source; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestConnectSourceEndpointMatchesEventRejectsConcreteInstanceWithoutSourceRoute(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done")
	evt := eventtest.RunCreatingRootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	if connectSourceEndpointMatchesTestEvent(endpoint, evt) {
		t.Fatalf("concrete instance event matched without authoritative source route; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestConnectSourceEndpointMatchesEventEnforcesRootSourceContextMatrix(t *testing.T) {
	rootEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", "", "root.ready", "root.ready")
	staticEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done")
	templateEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done")

	tests := []struct {
		name      string
		endpoint  ConnectRoutePlanEndpoint
		eventType events.EventType
		source    events.RoutingSource
		want      bool
	}{
		{name: "typed root", endpoint: rootEndpoint, eventType: "root.ready", source: mustRootRoutingSource(t), want: true},
		{name: "absent source", endpoint: rootEndpoint, eventType: "root.ready", source: events.NoRoutingSource()},
		{name: "platform control", endpoint: rootEndpoint, eventType: "root.ready", source: events.NewPlatformControlRoutingSource()},
		{name: "child source cannot become root", endpoint: rootEndpoint, eventType: "root.ready", source: mustConcreteRoutingSource(t, "child", "child/inst-1")},
		{name: "static source control", endpoint: staticEndpoint, eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "template source control", endpoint: templateEndpoint, eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectSourceEndpointMatchesTestSource(tc.endpoint, tc.eventType, tc.source); got != tc.want {
				t.Fatalf("connectSourceEndpointMatches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConnectSourceEndpointMatchesEnforcesProducerModeMatrix(t *testing.T) {
	flowEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done")
	rootEndpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", "", "deploy.done", "deploy.done")
	tests := []struct {
		name      string
		endpoint  ConnectRoutePlanEndpoint
		eventType events.EventType
		source    events.RoutingSource
		want      bool
	}{
		{name: "root exact", endpoint: rootEndpoint, eventType: "deploy.done", source: mustRootRoutingSource(t), want: true},
		{name: "root rejects child evidence", endpoint: rootEndpoint, eventType: "deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "static requires source", endpoint: flowEndpoint, eventType: "producer/deploy.done", source: events.NoRoutingSource()},
		{name: "static exact instance", endpoint: flowEndpoint, eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "static rejects descendant instance", endpoint: flowEndpoint, eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "singleton exact instance", endpoint: withConnectSourceMode(flowEndpoint, "singleton"), eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "singleton rejects descendant instance", endpoint: withConnectSourceMode(flowEndpoint, "singleton"), eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "template concrete instance", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1"), want: true},
		{name: "template rejects base without route", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: events.NoRoutingSource()},
		{name: "template rejects static source", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer")},
		{name: "template rejects base name with concrete route", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectSourceEndpointMatchesTestSource(tc.endpoint, tc.eventType, tc.source); got != tc.want {
				t.Fatalf("connectSourceEndpointMatches(%#v, %q) = %v, want %v", tc.endpoint, tc.eventType, got, tc.want)
			}
		})
	}
}

func withConnectSourceMode(endpoint ConnectRoutePlanEndpoint, mode string) ConnectRoutePlanEndpoint {
	switch strings.TrimSpace(mode) {
	case runtimecontracts.FlowModeSingleton:
		endpoint.kind = connectEndpointSingletonFlow
	case runtimecontracts.FlowModeTemplate:
		endpoint.kind = connectEndpointTemplateFlow
	default:
		endpoint.kind = connectEndpointStaticFlow
	}
	return endpoint
}

func TestCompiledConnectEndpointPreservesReceiverModeMatrix(t *testing.T) {
	tests := []struct {
		name      string
		root      bool
		mode      string
		wantKind  connectEndpointKind
		assertion func(ConnectRoutePlanEndpoint) bool
	}{
		{name: "root", root: true, mode: "root", wantKind: connectEndpointRoot, assertion: ConnectRoutePlanEndpoint.IsRoot},
		{name: "external", mode: "external", wantKind: connectEndpointExternalIngress, assertion: ConnectRoutePlanEndpoint.IsExternalIngress},
		{name: "static", mode: runtimecontracts.FlowModeStatic, wantKind: connectEndpointStaticFlow, assertion: ConnectRoutePlanEndpoint.IsStatic},
		{name: "singleton", mode: runtimecontracts.FlowModeSingleton, wantKind: connectEndpointSingletonFlow, assertion: ConnectRoutePlanEndpoint.IsSingleton},
		{name: "template", mode: runtimecontracts.FlowModeTemplate, wantKind: connectEndpointTemplateFlow, assertion: ConnectRoutePlanEndpoint.IsTemplate},
	}
	if got, want := len(tests), int(connectEndpointKindCount-1); got != want {
		t.Fatalf("covered endpoint variants = %d, want all %d closed variants", got, want)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, test.root, "receiver", "receiver", test.mode, "input", "work.ready", "receiver/work.ready")
			if endpoint.kind != test.wantKind || !test.assertion(endpoint) {
				t.Fatalf("endpoint mode = %d, want %d", endpoint.kind, test.wantKind)
			}
		})
	}
}

func TestCompiledConnectEndpointModeCoverageIsExhaustive(t *testing.T) {
	TestCompiledConnectEndpointPreservesReceiverModeMatrix(t)
}

func TestConnectExecutionClaimIncludesReceiverMode(t *testing.T) {
	plan := ConnectRoutePlan{
		source: newConnectRoutePlanEndpoint(
			ConnectEndpointRoleProducer, true, "", "", "root", "ready", "work.ready", "work.ready",
		),
		receiver: newConnectRoutePlanEndpoint(
			ConnectEndpointRoleConsumer, false, "receiver", "receiver", runtimecontracts.FlowModeStatic,
			"ready", "work.ready", "receiver/work.ready",
		),
		targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic,
		target: events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver"},
	}
	route := ConnectDeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "receiver", "receiver-node")),
		Target: events.RouteIdentity{
			FlowID: "receiver", FlowInstance: "receiver", EntityID: eventtest.UUID("receiver-owner"),
		},
		Handler: MustConnectReceiverHandler(identitytest.FlowNode(t, "receiver", "receiver-node")),
	}
	staticClaim, err := ConnectExecutionClaim(plan, route)
	if err != nil {
		t.Fatalf("mint static receiver claim: %v", err)
	}
	plan.receiver = newConnectRoutePlanEndpoint(
		ConnectEndpointRoleConsumer, false, "receiver", "receiver", runtimecontracts.FlowModeSingleton,
		"ready", "work.ready", "receiver/work.ready",
	)
	singletonClaim, err := ConnectExecutionClaim(plan, route)
	if err != nil {
		t.Fatalf("mint singleton receiver claim: %v", err)
	}
	if staticClaim.Equal(singletonClaim) {
		t.Fatal("connect execution claim ignored the receiver ownership mode")
	}
}

func TestW2CanonicalConnectPlanIdentityIgnoresTargetSetAuthorOrderAndReadbackIsImmutable(t *testing.T) {
	source := newConnectRoutePlanEndpoint(
		ConnectEndpointRoleProducer, true, "", "", "root", "work.ready", "work.ready", "work.ready",
	).withCompiledPinDigest("source-pin-digest")
	receiver := newConnectRoutePlanEndpoint(
		ConnectEndpointRoleConsumer, false, "worker", "worker", runtimecontracts.FlowModeTemplate,
		"work.ready", "work.ready", "worker/work.ready",
	).withCompiledPinDigest("receiver-pin-digest")
	firstTargets := []events.RouteIdentity{
		{FlowID: "worker", FlowInstance: "worker/b", EntityID: "entity-b"},
		{FlowID: "worker", FlowInstance: "worker/a", EntityID: "entity-a"},
	}
	evidence := &connectProducerEventEvidence{packageKey: ".", eventName: "work.ready", acceptanceSchemaDigest: "schema-digest"}
	first, err := newConnectRoutePlan(connectRoutePlanSpec{
		source: source, receiver: receiver, targetKind: ConnectTargetKindTargetSet,
		resolutionKind: ConnectResolutionStatic, targetSet: append([]events.RouteIdentity(nil), firstTargets...),
		producerEvent: evidence, receiverEvent: evidence,
	})
	if err != nil {
		t.Fatalf("compile first connect plan: %v", err)
	}
	second, err := newConnectRoutePlan(connectRoutePlanSpec{
		source: source, receiver: receiver, targetKind: ConnectTargetKindTargetSet,
		resolutionKind: ConnectResolutionStatic, targetSet: []events.RouteIdentity{firstTargets[1], firstTargets[0]},
		producerEvent: evidence, receiverEvent: evidence,
	})
	if err != nil {
		t.Fatalf("compile second connect plan: %v", err)
	}
	firstID, err := ConnectPlanIdentity(first)
	if err != nil {
		t.Fatalf("first connect identity: %v", err)
	}
	secondID, err := ConnectPlanIdentity(second)
	if err != nil {
		t.Fatalf("second connect identity: %v", err)
	}
	if firstID.Empty() || firstID.String() != secondID.String() {
		t.Fatalf("equivalent connect identities = %q/%q, want canonical set identity", firstID.String(), secondID.String())
	}

	readback := first.Readback()
	readback.Targets[0].FlowInstance = "worker/changed"
	if got := first.Readback().Targets[0].FlowInstance; got != "worker/a" {
		t.Fatalf("route-plan readback mutation escaped into compiled owner: %q", got)
	}
}

func TestW2ConnectPlanIdentityIgnoresDiagnosticEventSchemaProvenance(t *testing.T) {
	first := testW2ConnectPlanWithEventSchemaProvenance(t, "producer", "events.yaml", "worker", "events.yaml")
	relocated := testW2ConnectPlanWithEventSchemaProvenance(t, "producer-moved", "moved/events.yaml", "worker-moved", "moved/events.yaml")

	firstID, err := ConnectPlanIdentity(first)
	if err != nil {
		t.Fatalf("first connect plan identity: %v", err)
	}
	relocatedID, err := ConnectPlanIdentity(relocated)
	if err != nil {
		t.Fatalf("relocated connect plan identity: %v", err)
	}
	if firstID.Empty() || firstID.String() != relocatedID.String() {
		t.Fatalf("connect plan identity changed with diagnostic schema provenance: %q != %q", firstID.String(), relocatedID.String())
	}

	firstReadback := first.Readback()
	relocatedReadback := relocated.Readback()
	if firstReadback.ProducerEvent.SourceFile != "events.yaml" || relocatedReadback.ProducerEvent.SourceFile != "moved/events.yaml" ||
		firstReadback.ReceiverEvent.SourceFlowID != "worker" || relocatedReadback.ReceiverEvent.SourceFlowID != "worker-moved" {
		t.Fatalf("diagnostic schema provenance was not retained in readback: first=%#v relocated=%#v", firstReadback, relocatedReadback)
	}
}

func TestW2ConnectExecutionClaimIgnoresDiagnosticEventSchemaProvenance(t *testing.T) {
	first := testW2ConnectPlanWithEventSchemaProvenance(t, "producer", "events.yaml", "worker", "events.yaml")
	relocated := testW2ConnectPlanWithEventSchemaProvenance(t, "producer-moved", "moved/events.yaml", "worker-moved", "moved/events.yaml")
	route := ConnectDeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "worker", "worker-node")),
		Target:    events.RouteIdentity{FlowID: "worker", FlowInstance: "worker", EntityID: eventtest.UUID("worker-owner")},
		Handler:   MustConnectReceiverHandler(identitytest.FlowNode(t, "worker", "worker-node")),
	}

	firstClaim, err := ConnectExecutionClaim(first, route)
	if err != nil {
		t.Fatalf("first connect execution claim: %v", err)
	}
	relocatedClaim, err := ConnectExecutionClaim(relocated, route)
	if err != nil {
		t.Fatalf("relocated connect execution claim: %v", err)
	}
	if firstClaim.Empty() || !firstClaim.Equal(relocatedClaim) {
		t.Fatal("connect execution claim changed with diagnostic schema provenance")
	}
}

func testW2ConnectPlanWithEventSchemaProvenance(t testing.TB, producerFlowID, producerFile, receiverFlowID, receiverFile string) ConnectRoutePlan {
	t.Helper()
	producerEvent := &connectProducerEventEvidence{
		packageKey: ".", eventName: "work.ready", acceptanceSchemaDigest: "producer-schema-digest",
		businessKeyField: "work_id", businessKeyType: "string", sourceFlowID: producerFlowID, sourceFile: producerFile,
	}
	receiverEvent := &connectProducerEventEvidence{
		packageKey: ".", eventName: "work.ready", acceptanceSchemaDigest: "receiver-schema-digest",
		businessKeyField: "work_id", businessKeyType: "string", sourceFlowID: receiverFlowID, sourceFile: receiverFile,
	}
	plan, err := newConnectRoutePlan(connectRoutePlanSpec{
		source: newConnectRoutePlanEndpoint(
			ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic,
			"work.ready", "work.ready", "producer/work.ready",
		).withCompiledPinDigest("source-pin-digest"),
		receiver: newConnectRoutePlanEndpoint(
			ConnectEndpointRoleConsumer, false, "worker", "worker", runtimecontracts.FlowModeStatic,
			"work.ready", "work.ready", "worker/work.ready",
		).withCompiledPinDigest("receiver-pin-digest"),
		producerEvent: producerEvent, receiverEvent: receiverEvent,
		targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic,
		target: events.RouteIdentity{FlowID: "worker", FlowInstance: "worker"},
	})
	if err != nil {
		t.Fatalf("compile connect plan with diagnostic schema provenance: %v", err)
	}
	return plan
}

func TestW2CompiledEdgeClosedOwner(t *testing.T) {
	source := newConnectRoutePlanEndpoint(
		ConnectEndpointRoleProducer, true, "", "", "root", "work.ready", "work.ready", "work.ready",
	).withCompiledPinDigest("source-pin-digest")
	receiver := newConnectRoutePlanEndpoint(
		ConnectEndpointRoleConsumer, false, "worker", "worker", runtimecontracts.FlowModeStatic,
		"work.ready", "work.ready", "worker/work.ready",
	).withCompiledPinDigest("receiver-pin-digest")
	evidence := &connectProducerEventEvidence{packageKey: ".", eventName: "work.ready", acceptanceSchemaDigest: "schema-digest"}
	targets := []events.RouteIdentity{{FlowID: "worker", FlowInstance: "worker", EntityID: "entity-a"}}
	plan, err := newConnectRoutePlan(connectRoutePlanSpec{
		source: source, receiver: receiver, producerEvent: evidence, receiverEvent: evidence,
		targetKind: ConnectTargetKindTargetSet, resolutionKind: ConnectResolutionStatic, targetSet: targets,
	})
	if err != nil {
		t.Fatalf("compile closed edge: %v", err)
	}
	targets[0].FlowInstance = "worker/changed"
	evidence.eventName = "changed"
	readback := plan.Readback()
	if len(readback.Targets) != 1 || readback.Targets[0].FlowInstance != "worker" || readback.ProducerEvent.EventName != "work.ready" {
		t.Fatalf("closed edge retained mutable constructor inputs: %#v", readback)
	}

	hostile := []connectRoutePlanSpec{
		{source: source, receiver: receiver, producerEvent: evidence, targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic},
		{source: source, receiver: receiver, producerEvent: evidence, receiverEvent: evidence, targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionInstanceKey},
		{source: source, receiver: receiver, producerEvent: evidence, receiverEvent: evidence, targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic, target: targets[0], targetSet: targets},
	}
	for index, spec := range hostile {
		if _, err := newConnectRoutePlan(spec); err == nil {
			t.Fatalf("hostile closed-edge case %d was accepted", index)
		}
	}
}

func TestConnectRoutePlanStructuralTargetOwnerProofMatrix(t *testing.T) {
	entityID := eventtest.UUID("compiled-structural-owner")
	rootOwner := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "workflow", FlowInstance: eventtest.UUID("compiled-structural-run"), EntityID: entityID,
	})
	rootRoutingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatalf("root routing source: %v", err)
	}
	rootSourceEvent, err := AdmitSourceEvent(events.EventType("validate.requested"), rootRoutingSource)
	if err != nil {
		t.Fatalf("admit root source event: %v", err)
	}
	staticTarget := events.RouteIdentity{FlowID: "child", FlowInstance: "child"}
	rootToStatic := ConnectRoutePlan{
		source:     newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", "output", "validate.requested", "validate.requested"),
		receiver:   newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "child", "child", runtimecontracts.FlowModeStatic, "validate", "validation.done", "child/validation.done"),
		targetKind: ConnectTargetKindTarget, resolutionKind: ConnectResolutionStatic, target: staticTarget,
	}
	singletonReceiver := rootToStatic
	singletonReceiver.receiver = newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "child", "child", runtimecontracts.FlowModeSingleton, "validate", "validation.done", "child/validation.done")
	templateReceiver := rootToStatic
	templateReceiver.receiver = newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "child", "child", runtimecontracts.FlowModeTemplate, "validate", "validation.done", "child/validation.done")
	nestedStatic := rootToStatic
	nestedStatic.source = newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "child", "child", runtimecontracts.FlowModeStatic, "output", "work.ready", "child/work.ready")
	nestedStatic.receiver = newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "grandchild", "child/grandchild", runtimecontracts.FlowModeStatic, "input", "work.ready", "child/grandchild/work.ready")
	nestedStatic.target = events.RouteIdentity{FlowID: "grandchild", FlowInstance: "child/grandchild"}
	nestedOwner := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: entityID})
	nestedRoutingSource, err := events.NewStaticFlowRoutingSource(nestedOwner.Route())
	if err != nil {
		t.Fatalf("nested routing source: %v", err)
	}
	nestedSourceEvent, err := AdmitSourceEvent(events.EventType("child/work.ready"), nestedRoutingSource)
	if err != nil {
		t.Fatalf("admit nested source event: %v", err)
	}
	siblingReceiver := rootToStatic
	siblingReceiver.source = newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "left", "left", runtimecontracts.FlowModeStatic, "output", "work.ready", "left/work.ready")
	siblingReceiver.receiver = newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "right", "right", runtimecontracts.FlowModeStatic, "input", "work.ready", "right/work.ready")
	siblingReceiver.target = events.RouteIdentity{FlowID: "right", FlowInstance: "right"}
	siblingOwner := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "left", FlowInstance: "left", EntityID: entityID})
	siblingRoutingSource, err := events.NewStaticFlowRoutingSource(siblingOwner.Route())
	if err != nil {
		t.Fatalf("sibling routing source: %v", err)
	}
	siblingSourceEvent, err := AdmitSourceEvent(events.EventType("left/work.ready"), siblingRoutingSource)
	if err != nil {
		t.Fatalf("admit sibling source event: %v", err)
	}
	wrongRootRoutingSource, err := events.NewRootRoutingSource(eventtest.UUID("unrelated-root-owner"))
	if err != nil {
		t.Fatalf("unrelated root routing source: %v", err)
	}
	wrongRootSourceEvent, err := AdmitSourceEvent(events.EventType("validate.requested"), wrongRootRoutingSource)
	if err != nil {
		t.Fatalf("admit unrelated root source event: %v", err)
	}
	wrongNestedRoutingSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: "child", FlowInstance: "child", EntityID: eventtest.UUID("unrelated-nested-owner"),
	})
	if err != nil {
		t.Fatalf("unrelated nested routing source: %v", err)
	}
	wrongNestedSourceEvent, err := AdmitSourceEvent(events.EventType("child/work.ready"), wrongNestedRoutingSource)
	if err != nil {
		t.Fatalf("admit unrelated nested source event: %v", err)
	}

	tests := []struct {
		name    string
		plan    ConnectRoutePlan
		target  events.RouteIdentity
		current events.DeliveryTargetOwnership
		source  SourceEvent
		want    bool
		wantErr bool
	}{
		{name: "root to static", plan: rootToStatic, target: staticTarget, current: rootOwner, source: rootSourceEvent, want: true},
		{name: "nested static descendant", plan: nestedStatic, target: nestedStatic.target, current: nestedOwner, source: nestedSourceEvent, want: true},
		{name: "singleton receiver is distinct", plan: singletonReceiver, target: staticTarget, current: rootOwner, source: rootSourceEvent},
		{name: "template receiver is distinct", plan: templateReceiver, target: staticTarget, current: rootOwner, source: rootSourceEvent},
		{name: "sibling path is not nested", plan: siblingReceiver, target: siblingReceiver.target, current: siblingOwner, source: siblingSourceEvent},
		{name: "entityless current target cannot authorize", plan: rootToStatic, target: staticTarget, current: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "workflow", FlowInstance: "root"}), source: rootSourceEvent},
		{name: "root source and current owner disagree", plan: rootToStatic, target: staticTarget, current: rootOwner, source: wrongRootSourceEvent},
		{name: "nested source and current owner disagree", plan: nestedStatic, target: nestedStatic.target, current: nestedOwner, source: wrongNestedSourceEvent},
		{name: "wrong target blueprint fails", plan: rootToStatic, target: events.RouteIdentity{FlowID: "other", FlowInstance: "other"}, current: rootOwner, source: rootSourceEvent, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proof, ok, err := test.plan.ProveStructuralTargetOwner(test.target, test.current, test.source)
			if (err != nil) != test.wantErr {
				t.Fatalf("proof error = %v, wantErr %v", err, test.wantErr)
			}
			if ok != test.want {
				t.Fatalf("proof admitted = %v, want %v", ok, test.want)
			}
			if !ok {
				if !proof.Empty() {
					t.Fatalf("rejected proof = %#v, want zero", proof)
				}
				return
			}
			if err := proof.Validate(); err != nil {
				t.Fatalf("validate proof: %v", err)
			}
			wantTarget := test.target.Normalized()
			if got := proof.TargetOwner().Route(); got.FlowID != wantTarget.FlowID || got.FlowInstance != wantTarget.FlowInstance || got.EntityID != entityID {
				t.Fatalf("proof owner = %#v, want exact static target %#v with current entity", got, wantTarget)
			}
		})
	}
}

func TestConnectRoutePlanStructuralTargetOwnerEligibilityMatrix(t *testing.T) {
	TestConnectRoutePlanStructuralTargetOwnerProofMatrix(t)
}

func TestConnectRoutePlanRootToNestedStaticUsesStructuralTargetOwner(t *testing.T) {
	TestConnectRoutePlanStructuralTargetOwnerProofMatrix(t)
}

func TestConnectSourceEndpointMatchesRejectsStaticEventWhenSourceRouteContradicts(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done")
	if connectSourceEndpointMatchesTestSource(endpoint, "producer/deploy.done", mustConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")) {
		t.Fatal("static producer event matched contradictory source route")
	}
}

func TestConnectSourceEndpointMatchesRejectsStaticEventWithoutSourceRoute(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done")
	if connectSourceEndpointMatchesTestSource(endpoint, "producer/deploy.done", events.NoRoutingSource()) {
		t.Fatal("fully scoped static producer event matched without typed source authority")
	}
}

func TestConnectRoutePlanSourceParentRouteUsesOnlyAdmittedSource(t *testing.T) {
	plan := ConnectRoutePlan{source: newConnectRoutePlanEndpoint(
		ConnectEndpointRoleProducer, false, "producer", "producer", "static", "ready", "work.ready", "producer/work.ready",
	)}
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: "producer", FlowInstance: "producer", EntityID: "producer-entity",
	})
	if err != nil {
		t.Fatalf("build static routing source: %v", err)
	}
	sourceEvent, err := AdmitSourceEvent("producer/work.ready", source)
	if err != nil {
		t.Fatalf("admit source event: %v", err)
	}
	if got := plan.SourceParentRoute(sourceEvent); got != (flowidentity.ParentRoute{
		FlowID: "producer", FlowInstance: "producer", EntityID: "producer-entity",
	}) {
		t.Fatalf("source parent route = %#v, want exact admitted source route", got)
	}

	otherSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: "other", FlowInstance: "other", EntityID: "other-entity",
	})
	if err != nil {
		t.Fatalf("build unrelated routing source: %v", err)
	}
	unrelated, err := AdmitSourceEvent("producer/work.ready", otherSource)
	if err != nil {
		t.Fatalf("admit unrelated source event: %v", err)
	}
	if got := plan.SourceParentRoute(unrelated); !got.Empty() {
		t.Fatalf("source parent route = %#v, want mismatched source rejected", got)
	}
}

func TestAdmitRuntimeControlSourceEventResolvesOnceAgainstExactSource(t *testing.T) {
	source := testRootConnectRoutePlanSource(nil, []connectRoutePlanFlow{{
		id: "producer", mode: runtimecontracts.FlowModeStatic,
		outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}},
	}}, nil)
	flowSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
		FlowID: "producer", FlowInstance: "producer", EntityID: "producer-entity",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := AdmitRuntimeControlSourceEvent(source, "producer", "work.ready", flowSource)
	if err != nil || got != "producer/work.ready" {
		t.Fatalf("admit flow event = %q, %v, want producer/work.ready", got, err)
	}
	if _, err := AdmitRuntimeControlSourceEvent(source, "consumer", "work.ready", flowSource); err == nil {
		t.Fatal("source admission accepted a foreign declared flow")
	}
	if _, err := AdmitRuntimeControlSourceEvent(source, "producer", "consumer/work.ready", flowSource); err == nil {
		t.Fatal("source admission accepted a foreign qualified event")
	}
	rootSource, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	got, err = AdmitRuntimeControlSourceEvent(source, "", "root.ready", rootSource)
	if err != nil || got != "root.ready" {
		t.Fatalf("admit root event = %q, %v, want root.ready", got, err)
	}
}

func TestConnectSourceEndpointMatchesRejectsRootEventWithChildFlowEvidence(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(ConnectEndpointRoleProducer, true, "", "", "root", "", "deploy.done", "deploy.done")
	if connectSourceEndpointMatchesTestSource(endpoint, "deploy.done", mustStaticRoutingSource(t, "child")) {
		t.Fatal("root endpoint matched child/static FlowID evidence")
	}
}

func connectSourceEndpointMatchesTestEvent(endpoint ConnectRoutePlanEndpoint, evt events.Event) bool {
	sourceEvent, err := SourceEventFromEvent(evt)
	return err == nil && connectSourceEndpointMatches(endpoint, sourceEvent)
}

func connectSourceEndpointMatchesTestSource(endpoint ConnectRoutePlanEndpoint, eventType events.EventType, source events.RoutingSource) bool {
	sourceEvent, err := AdmitSourceEvent(eventType, source)
	return err == nil && connectSourceEndpointMatches(endpoint, sourceEvent)
}

func mustRootRoutingSource(t *testing.T) events.RoutingSource {
	t.Helper()
	source, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustStaticRoutingSource(t *testing.T, flowID string) events.RoutingSource {
	t.Helper()
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: flowID, EntityID: flowID + "-entity"})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustConcreteRoutingSource(t *testing.T, flowID, flowInstance string) events.RoutingSource {
	t.Helper()
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: flowID + "-entity"})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestLowerTargetFreeInputRoutePlans_RejectsHarnessSource(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"test", "worker/work.requested", "provider.test", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("generation-test")),
	)
	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(plans) != 0 || len(issues) != 0 {
		t.Fatalf("plans = %#v issues = %#v, want harness excluded without lowering issues", plans, issues)
	}
}

func TestLowerTargetFreeInputRoutePlansUsesCanonicalRenamedIdentitySource(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyProviderRollbackRenamedSource(t, true)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load provider rollback artifact: %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("target-free-renamed-source")),
	)

	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(issues) != 0 || len(plans) != 1 || plans[0].instanceKey == nil {
		t.Fatalf("plans/issues = %#v/%#v, want one target-free instance plan", plans, issues)
	}
	plan := plans[0]
	if got, want := plan.instanceKey.Readback().SourcePath, "payload.external_chat_id"; got != want {
		t.Fatalf("typed source = %q, want %q", got, want)
	}
	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: AdmitConnectRouteMatchValues(map[string]string{
			"payload.external_chat_id": "chat-authoritative",
			"payload.chat_id":          "chat-conflicting",
		}),
		Descriptors: []Descriptor{
			{EntityID: "ent-authoritative", FlowInstance: "consumer/authoritative", AddressFields: map[string]string{"entity.chat_id": "chat-authoritative"}},
			{EntityID: "ent-conflicting", FlowInstance: "consumer/conflicting", AddressFields: map[string]string{"entity.chat_id": "chat-conflicting"}},
		},
	})
	if !materialized.Failure.Empty() || materialized.Target.FlowInstance != "consumer/authoritative" {
		t.Fatalf("materialized target/failure = %#v/%q, want renamed-source consumer/authoritative", materialized.Target, materialized.Failure)
	}
}

func TestCompileConnectPlansFromLoadedPackageFixture(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !strings.HasPrefix(plan.Readback().AuthoredLocation, filepath.Join(root, "package.yaml")+":") {
		t.Fatalf("AuthoredLocation = %q, want exact root package.yaml:line", plan.Readback().AuthoredLocation)
	}
	if got, want := plan.source.resolvedEvent.value, events.EventType("producer/work.ready"); got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.receiver.resolvedEvent.value, events.EventType("consumer/work.ready"); got != want {
		t.Fatalf("Receiver.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.resolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.target.FlowInstance != "consumer" {
		t.Fatalf("Target = %#v, want concrete static consumer route", plan.target)
	}
}

func TestCompileConnectPlansLoadedPackagePinsExactCanonicalEventIdentity(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))

	for _, tc := range []struct {
		name        string
		connectLine string
		want        string
	}{
		{name: "leading slash", connectLine: "  - event: /work.ready", want: "exact canonical event identity"},
		{name: "trailing slash", connectLine: "  - event: work.ready/", want: "exact canonical event identity"},
		{name: "normalized equal rename", connectLine: "  - event: work.ready\n    rename: /work.ready/", want: "redundant with event"},
		{name: "non-canonical rename", connectLine: "  - event: work.ready\n    rename: work.accepted/", want: "exact canonical event identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeConnectRoutePlanPackageFixture(t)
			replaceFixtureText(t, filepath.Join(root, "package.yaml"), "  - event: work.ready", tc.connectLine)
			if _, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want %q", err, tc.want)
			}
		})
	}

	root := writeConnectRoutePlanPackageFixture(t)
	replaceFixtureText(t, filepath.Join(root, "flows/consumer/schema.yaml"), "work.ready", "work/ready")
	if _, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), "exact local canonical event identity") {
		t.Fatalf("slash-qualified flow pin load error = %v, want exact local identity rejection", err)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationRejectsOtherwiseValidConnectWithoutSourceLocation(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	connects := bundle.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("connects = %#v, want one", connects)
	}
	connects[0].SourceFile = ""
	connects[0].SourceLine = 0

	plan, issue := lowerCompositionConnectRoutePlanWithLocation(source, connects[0])
	if issue.Failure != ConnectFailureSourceLocationMissing || issue.AuthoredLocation != "" || plan.Readback().AuthoredLocation != "" {
		t.Fatalf("plan = %#v issue = %#v, want source-location issue and no plan", plan, issue)
	}
}

func TestCompileConnectPlansUsesFanInStreamSingularTarget(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})

	plans, issues := compileConnectPlans(source)

	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.fanIn == nil {
		t.Fatalf("fan-in metadata = nil in %#v", plan)
	}
	if fanIn := plan.fanIn.Readback(); plan.fanIn.Aggregation() != ConnectFanInStream || fanIn.Window != "payload.period_id" || len(fanIn.DedupBy) != 1 || fanIn.DedupBy[0] != "payload.operating_id" {
		t.Fatalf("fan-in metadata = %#v, want stream/window/dedup", plan.fanIn)
	}
	if plan.targetKind != ConnectTargetKindTarget || plan.resolutionKind != ConnectResolutionStatic {
		t.Fatalf("fan-in routing shape = target_kind:%s resolution:%s, want target/static", plan.targetKind.Code(), plan.resolutionKind.Code())
	}
	if plan.target.FlowID != templatefanin.ReceiverFlowID || plan.target.FlowInstance != templatefanin.ReceiverFlowInstance || plan.target.EntityID != "" {
		t.Fatalf("fan-in target = %#v, want receiver singleton %s route blueprint without run-specific entity", plan.target, templatefanin.ReceiverFlowInstance)
	}
}

func TestCompileConnectPlansAllowsFanInStreamEventIDDedup(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{EventIDDedup: true})

	plans, issues := compileConnectPlans(source)

	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.fanIn == nil || len(plan.fanIn.Readback().DedupBy) != 1 || plan.fanIn.Readback().DedupBy[0] != "event.id" {
		t.Fatalf("fan-in metadata = %#v, want event.id dedup", plan.fanIn)
	}
}

func TestCompileConnectPlansFailsClosedForInvalidFanInStream(t *testing.T) {
	tests := []struct {
		name    string
		opts    templatefanin.Options
		failure ConnectRoutePlanFailure
		detail  string
	}{
		{name: "missing dedup", opts: templatefanin.Options{MissingDedup: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "requires dedup_by"},
		{name: "dedup tuple", opts: templatefanin.Options{DedupTuple: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "exactly one dedup_by"},
		{name: "missing window", opts: templatefanin.Options{MissingWindow: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "requires window"},
		{name: "wrong singleton", opts: templatefanin.Options{WrongSingleton: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "must be the receiver singleton route or a child"},
		{name: "non-singleton receiver", opts: templatefanin.Options{NonSingletonReceiver: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "is not mode: singleton"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templatefanin.LoadSource(t, tc.opts)

			_, issues := compileConnectPlans(source)

			if len(issues) != 1 {
				t.Fatalf("issues = %#v, want one", issues)
			}
			if issues[0].Failure != tc.failure || !strings.Contains(issues[0].Detail, tc.detail) {
				t.Fatalf("issue = %#v, want failure %s containing %q", issues[0], tc.failure.Code(), tc.detail)
			}
			if issues[0].AuthoredLocation == "" || !strings.Contains(issues[0].AuthoredLocation, "package.yaml:") {
				t.Fatalf("issue location = %q, want exact package.yaml:line", issues[0].AuthoredLocation)
			}
		})
	}
}

func TestCompileConnectPlansUsesFanInBarrierSingularTarget(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load canonical barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)
	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if fanIn := plan.fanIn.Readback(); plan.fanIn.Aggregation() != ConnectFanInBarrier || fanIn.Window != "payload.period_id" || len(fanIn.DedupBy) != 1 || fanIn.DedupBy[0] != "payload.operating_id" {
		t.Fatalf("fan-in metadata = %#v, want barrier/window/member identity", plan.fanIn)
	}
	if plan.targetKind != ConnectTargetKindTarget || plan.resolutionKind != ConnectResolutionStatic {
		t.Fatalf("barrier routing shape = %#v, want singular static target", plan)
	}
}

func requireFanInRoutePlan(t *testing.T, plans []ConnectRoutePlan) ConnectRoutePlan {
	t.Helper()
	var matches []ConnectRoutePlan
	for _, plan := range plans {
		if plan.fanIn != nil {
			matches = append(matches, plan)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("fan-in route plans = %#v in all plans %#v, want exactly one", matches, plans)
	}
	return matches[0]
}

func TestCompileConnectPlansRejectsAddresslessImplicitInstanceKey(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	schema := bundle.FlowSchemas["account"]
	for i := range schema.Pins.Inputs.EventPins {
		schema.Pins.Inputs.EventPins[i].Resolution = runtimecontracts.FlowInputPinResolution{}
	}
	bundle.FlowSchemas["account"] = schema
	bundle.FlowTree.ByID["account"].Schema = schema
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("CompileWorkflowSemantics: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none without receiver resolution", plans)
	}
	if len(issues) != 1 || issues[0].Failure != ConnectFailureReceiverResolutionMissing {
		t.Fatalf("issues = %#v, want %q", issues, ConnectFailureReceiverResolutionMissing)
	}
}

func TestCompileConnectPlansUsesCreateInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeCreateResolutionConnectRoutePlanPackageFixture(t, runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	var createPlans []ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.instanceKey != nil && candidate.instanceKey.Mode() == runtimecontracts.FlowInputResolutionModeCreate {
			createPlans = append(createPlans, candidate)
		}
	}
	if len(createPlans) != 1 {
		t.Fatalf("create plans = %#v in all plans %#v, want exactly one", createPlans, plans)
	}
	plan := createPlans[0]
	if plan.instanceKey == nil {
		t.Fatal("InstanceKey = nil, want create resolution instance-key evidence")
	}
	if got, want := plan.resolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.source.pinDigest == "" || plan.producerEvent == nil {
		t.Fatalf("source evidence = %#v/%#v, want immutable producer pin and event ownership", plan.source.Readback(), plan.producerEvent)
	}
	if got, want := plan.instanceKey.Mode(), runtimecontracts.FlowInputResolutionModeCreate; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.instanceKey.Readback().SourceKind, string(runtimecontracts.FlowInputInstanceSourceGeneratedUUID); got != want {
		t.Fatalf("InstanceKey.Source.Kind = %q, want %q", got, want)
	}
	if got, want := plan.instanceKey.Readback().SourcePath, runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath; got != want {
		t.Fatalf("InstanceKey.Source.Path = %q, want %q", got, want)
	}
	if got, want := plan.instanceKey.Field().Path(), "validation_case_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	eventID := "11111111-1111-4111-8111-111111111111"
	material, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, eventID)
	if !failure.Empty() {
		t.Fatalf("MintedInstanceKeyMaterialForConnectRoutePlan failure = %q", failure)
	}
	if len(material.Keys) != 1 || material.Keys[0].Field.Path() != "validation_case_id" || material.Keys[0].Value == "" || material.Keys[0].Value == eventID {
		t.Fatalf("minted material = %#v, want deterministic uuid material distinct from event id", material)
	}
}

func TestGeneratedUUIDMaterialIsReplayStableAndFreshForForkEventIdentity(t *testing.T) {
	plan := ConnectRoutePlan{
		receiver: newConnectRoutePlanEndpoint(ConnectEndpointRoleConsumer, false, "validator", "validator", runtimecontracts.FlowModeTemplate, "validation_requested", "", ""),
		instanceKey: &ConnectRoutePlanInstanceKey{
			mode:  runtimecontracts.FlowInputResolutionModeCreate,
			field: mustTemplateInstanceField(t, "validation_case_id"),
			source: newConnectInstanceSource(runtimecontracts.FlowInputInstanceSource{
				Kind: runtimecontracts.FlowInputInstanceSourceGeneratedUUID,
				Path: runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath,
			}),
		},
	}
	source, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "source-event-id")
	if !failure.Empty() {
		t.Fatalf("source material failure = %q", failure)
	}
	replay, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "source-event-id")
	if !failure.Empty() || !reflect.DeepEqual(replay, source) {
		t.Fatalf("replay material/failure = %#v/%q, want exact %#v", replay, failure, source)
	}
	fork, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "fork-event-id")
	if !failure.Empty() {
		t.Fatalf("fork material failure = %q", failure)
	}
	if len(source.Keys) != 1 || len(fork.Keys) != 1 || source.Keys[0].Value == fork.Keys[0].Value {
		t.Fatalf("source/fork material = %#v/%#v, want fresh deterministic fork value", source, fork)
	}
}

func TestInputPinResolutionMultiPinSatisfactionDerivesOneFlowIdentity(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 2 {
		t.Fatalf("plans/issues = %#v/%#v, want two valid input-pin plans", plans, issues)
	}
	modes := map[runtimecontracts.FlowInputResolutionMode]bool{}
	for _, plan := range plans {
		if plan.instanceKey == nil || plan.instanceKey.Field().Path() != "account_id" {
			t.Fatalf("plan instance identity = %#v, want scalar flow instance account_id", plan.instanceKey)
		}
		if readback := plan.instanceKey.Readback(); readback.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || readback.SourcePath != "payload.account_id" {
			t.Fatalf("plan source = %#v, want per-pin payload.account_id", readback)
		}
		modes[plan.instanceKey.Mode()] = true
	}
	if !modes[runtimecontracts.FlowInputResolutionModeSelect] || !modes[runtimecontracts.FlowInputResolutionModeSelectOrCreate] {
		t.Fatalf("derived modes = %#v, want independent select and select-or-create pins", modes)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationDerivesRenamedPayloadSourceFromCarry(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateSelectResolutionRenamedSource(t, canonicalrouting.TemplateSelectResolutionOptions{})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	pins := bundle.FlowInputEventPins("account")
	if len(pins) != 2 || pins[1].Resolution().From != "payload.external_account_id" {
		t.Fatalf("renamed source fixture pins = %#v, want account_ready from payload.external_account_id", pins)
	}
	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 1 || plans[0].instanceKey == nil {
		t.Fatalf("plans/issues = %#v/%#v, want one derived plan", plans, issues)
	}
	if readback := plans[0].instanceKey.Readback(); readback.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || readback.SourcePath != "payload.external_account_id" || plans[0].instanceKey.Field().Path() != "account_id" {
		t.Fatalf("derived identity/source = %#v, want account_id from renamed payload source", plans[0].instanceKey)
	}
	for _, tc := range []struct {
		name        string
		matchValues map[string]string
	}{
		{name: "renamed field only", matchValues: map[string]string{"payload.external_account_id": "acct-authoritative"}},
		{name: "conflicting same-named field", matchValues: map[string]string{
			"payload.external_account_id": "acct-authoritative",
			"payload.account_id":          "acct-conflicting",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			materialized := MaterializeConnectRoutePlan(plans[0], ConnectRoutePlanMaterializationInput{
				MatchValues: AdmitConnectRouteMatchValues(tc.matchValues),
				Descriptors: []Descriptor{
					{EntityID: "ent-authoritative", FlowInstance: "account/authoritative", AddressFields: map[string]string{"entity.account_id": "acct-authoritative"}},
					{EntityID: "ent-conflicting", FlowInstance: "account/conflicting", AddressFields: map[string]string{"entity.account_id": "acct-conflicting"}},
				},
			})
			if !materialized.Failure.Empty() || materialized.Target.FlowInstance != "account/authoritative" {
				t.Fatalf("materialized target/failure = %#v/%q, want renamed-source account/authoritative", materialized.Target, materialized.Failure)
			}
		})
	}
}

func TestCompileConnectPlansValidatesAuthoritativeInstanceSourceTypeMatrix(t *testing.T) {
	tests := []struct {
		name      string
		root      func(*testing.T) string
		wantError bool
	}{
		{
			name: "select accepts compatible aliases",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{})
			},
		},
		{
			name: "select rejects omitted annotation mismatch",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Invalidity: canonicalrouting.SelectResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
		{
			name: "select rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Invalidity: canonicalrouting.SelectResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "select-or-create rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: canonicalrouting.SelectResolutionSelectOrCreate, Invalidity: canonicalrouting.SelectResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "create accepts payload alias",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload})
			},
		},
		{
			name: "create accepts intrinsic generated uuid",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID})
			},
		},
		{
			name: "create accepts intrinsic event id",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintEventID})
			},
		},
		{
			name: "create payload rejects omitted annotation mismatch",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload, Invalidity: canonicalrouting.CreateResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
		{
			name: "create rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload, Invalidity: canonicalrouting.CreateResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "create generated uuid rejects incompatible receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID, Invalidity: canonicalrouting.CreateResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			_, issues := compileConnectPlans(semanticview.Wrap(bundle))
			gotError := false
			for _, issue := range issues {
				if strings.Contains(issue.Detail, "key_types_incompatible") {
					gotError = true
				}
			}
			if gotError != tc.wantError {
				t.Fatalf("key type issue = %v, want %v; issues = %#v", gotError, tc.wantError, issues)
			}
		})
	}
}

func TestLowerTargetFreeInputRoutePlansRejectsAuthoritativeSourceTypeMismatch(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyProviderRollbackInvalidSourceType(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load provider rollback artifact: %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("target-free-source-type")),
	)

	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(plans) != 0 || len(issues) != 1 || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("plans/issues = %#v/%#v, want target-free source type blocker", plans, issues)
	}
}

func TestLowerPublicInputRoutePlanAcceptsSyntheticProjectionWithDistinctSchemaEvidence(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, endpoint := targetFreeSyntheticProjectionFixture(t, tc.mint, false)
			plan, issue := LowerPublicInputRoutePlan(source, endpoint)
			if !issue.Failure.Empty() {
				t.Fatalf("public input issue = %#v, want accepted synthetic projection", issue)
			}
			assertTargetFreeSchemaRoles(t, plan)
		})
	}
}

func TestLowerTargetFreeInputRoutePlansAcceptsSyntheticProjectionWithDistinctSchemaEvidence(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := targetFreeSyntheticProjectionFixture(t, tc.mint, false)
			authorization := runtimeprovideroutput.MustAuthorization(
				"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
				"sha256:"+strings.Repeat("a", 64),
				triggergeneration.FromCanonicalBytes([]byte("target-free-synthetic-positive")),
			)
			plans, issues := lowerTargetFreeInputRoutePlans(source, []runtimeprovideroutput.Authorization{authorization})
			if len(issues) != 0 || len(plans) != 1 {
				t.Fatalf("provider plans/issues = %#v/%#v, want one accepted synthetic projection", plans, issues)
			}
			assertTargetFreeSchemaRoles(t, plans[0])
		})
	}
}

func TestLowerPublicInputRoutePlanRejectsSyntheticProjectionCollision(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, endpoint := targetFreeSyntheticCollisionFixture(t, tc.mint)
			_, issue := LowerPublicInputRoutePlan(source, endpoint)
			if issue.Failure != ConnectFailureResolutionProjectionCollision || !strings.Contains(issue.Detail, "field chat_id conflicts") {
				t.Fatalf("public input issue = %#v, want synthetic projection collision", issue)
			}
		})
	}
}

func TestLowerTargetFreeInputRoutePlansRejectsSyntheticProjectionCollision(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := targetFreeSyntheticCollisionFixture(t, tc.mint)
			authorization := runtimeprovideroutput.MustAuthorization(
				"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
				"sha256:"+strings.Repeat("a", 64),
				triggergeneration.FromCanonicalBytes([]byte("target-free-synthetic-collision")),
			)
			plans, issues := lowerTargetFreeInputRoutePlans(source, []runtimeprovideroutput.Authorization{authorization})
			if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != ConnectFailureResolutionProjectionCollision || !strings.Contains(issues[0].Detail, "field chat_id conflicts") {
				t.Fatalf("provider plans/issues = %#v/%#v, want synthetic projection collision", plans, issues)
			}
		})
	}
}

func targetFreeSyntheticProjectionCases() []struct {
	name string
	mint canonicalrouting.CreateMint
} {
	return []struct {
		name string
		mint canonicalrouting.CreateMint
	}{
		{name: "generated_uuid", mint: canonicalrouting.CreateMintUUID},
		{name: "event_id", mint: canonicalrouting.CreateMintEventID},
	}
}

func assertTargetFreeSchemaRoles(t testing.TB, plan ConnectRoutePlan) {
	t.Helper()
	if plan.producerEvent == nil || plan.receiverEvent == nil {
		t.Fatalf("target-free schema evidence = producer:%#v receiver:%#v", plan.producerEvent, plan.receiverEvent)
	}
	if plan.producerEvent.acceptanceSchemaDigest == "" || plan.receiverEvent.acceptanceSchemaDigest == "" || plan.producerEvent.acceptanceSchemaDigest == plan.receiverEvent.acceptanceSchemaDigest {
		t.Fatalf("target-free schema digests = producer:%q receiver:%q, want distinct immutable roles", plan.producerEvent.acceptanceSchemaDigest, plan.receiverEvent.acceptanceSchemaDigest)
	}
}

func targetFreeSyntheticProjectionFixture(t testing.TB, mint canonicalrouting.CreateMint, collision bool) (semanticview.Source, semanticview.AuthoredEventEndpoint) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyProviderRollbackSyntheticProjection(t, mint)
	if collision {
		root = canonicalrouting.CopyProviderRollbackSyntheticCollision(t, mint)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load target-free synthetic projection artifact: %v", err)
	}
	source := semanticview.Wrap(bundle)
	association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "inbound.telegram.text_message")
	endpoint, ok := association.Endpoint()
	if !ok {
		t.Fatalf("resolve target-free input endpoint: %v", association.Err())
	}
	return source, endpoint
}

func targetFreeSyntheticCollisionFixture(t testing.TB, mint canonicalrouting.CreateMint) (semanticview.Source, semanticview.AuthoredEventEndpoint) {
	return targetFreeSyntheticProjectionFixture(t, mint, true)
}

func TestConnectRoutePlanProductionAPIHasNoRetiredIdentityOrPolicyFacts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typeValue reflect.Type
		forbidden []string
	}{
		{name: "plan", typeValue: reflect.TypeOf(ConnectRoutePlan{}), forbidden: []string{"Address", "Map"}},
		{name: "instance key", typeValue: reflect.TypeOf(ConnectRoutePlanInstanceKey{}), forbidden: []string{"Fields", "Mappings", "OnMissing", "OnConflict"}},
		{name: "materialization input", typeValue: reflect.TypeOf(ConnectRoutePlanMaterializationInput{}), forbidden: []string{"SupportedAddressTargets"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, field := range tc.forbidden {
				if _, ok := tc.typeValue.FieldByName(field); ok {
					t.Fatalf("%s still exposes retired field %s", tc.typeValue, field)
				}
			}
		})
	}
}

func TestCompileConnectPlansUsesSelectInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.instanceKey == nil {
		t.Fatal("InstanceKey = nil, want select resolution instance-key evidence")
	}
	if got, want := plan.resolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if !plan.RequiresRuntimeResolution() {
		t.Fatal("select resolution should require runtime descriptor resolution")
	}
	if got, want := plan.instanceKey.Mode(), runtimecontracts.FlowInputResolutionModeSelect; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.instanceKey.Field().Path(), "account_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	if source := plan.instanceKey.Readback(); source.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || source.SourcePath != "payload.account_id" {
		t.Fatalf("InstanceKey source = %#v, want payload.account_id", source)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if !materialized.Failure.Empty() {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "account/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}

	missing := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
	})
	if missing.Failure != ConnectFailureTargetUnresolved {
		t.Fatalf("missing Failure = %q, want %q", missing.Failure, ConnectFailureTargetUnresolved)
	}

	ambiguous := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
		Descriptors: []Descriptor{
			{EntityID: "ent-1", FlowInstance: "account/one", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
			{EntityID: "ent-2", FlowInstance: "account/two", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
		},
	})
	if ambiguous.Failure != ConnectFailureTargetAmbiguous {
		t.Fatalf("ambiguous Failure = %q, want %q", ambiguous.Failure, ConnectFailureTargetAmbiguous)
	}
}

func TestCompileConnectPlansUsesSelectOrCreateInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate,
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.instanceKey == nil {
		t.Fatal("InstanceKey = nil, want select-or-create resolution instance-key evidence")
	}
	if got, want := plan.resolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if !plan.RequiresRuntimeResolution() {
		t.Fatal("select-or-create resolution should require runtime descriptor resolution")
	}
	if got, want := plan.instanceKey.Mode(), runtimecontracts.FlowInputResolutionModeSelectOrCreate; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.instanceKey.Field().Path(), "account_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	if source := plan.instanceKey.Readback(); source.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || source.SourcePath != "payload.account_id" {
		t.Fatalf("InstanceKey source = %#v, want payload.account_id", source)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: AdmitConnectRouteMatchValues(map[string]string{"payload.account_id": "acct-1"}),
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if !materialized.Failure.Empty() {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "account/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}
}

func TestCompiledPinAdmissionRejectsExtraSelectResolutionFieldsBeforeRouteLowering(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithExtraResolution(t, "          aggregation: stream\n")
	if _, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), "mode select may only declare mode and from") {
		t.Fatalf("bundle load error = %v, want canonical compiled-pin rejection before route lowering", err)
	}
}

func TestCanonicalResolutionAdmissionBlocksOutOfModeFromBeforeRouteLowering(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, tc := range []struct {
		name string
		root func(testing.TB) string
	}{
		{name: "fan-in", root: canonicalrouting.CopyFanInWithInertFrom},
		{name: "reply", root: canonicalrouting.CopyTemplateReplyWithInertFrom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), "may only declare") {
				t.Fatalf("bundle load error = %v, want canonical rejection before CompileConnectGraph", err)
			}
		})
	}
}

func TestCompileConnectPlansRejectsSelectCarryTypeMismatch(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		accountIDEntityType: "integer",
		accountIDCarryType:  "string",
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("issue = %#v, want instance resolution invalid for select carry type mismatch", issues[0])
	}
}

func TestCompileConnectPlansRejectsSelectOrCreateCarryTypeMismatch(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		mode:                runtimecontracts.FlowInputResolutionModeSelectOrCreate,
		accountIDEntityType: "integer",
		accountIDCarryType:  "string",
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select-or-create resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("issue = %#v, want instance resolution invalid for select-or-create carry type mismatch", issues[0])
	}
}

func TestCompileConnectPlansOneToOneStatic(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Event: "deploy.completed",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		Event:  "deploy.done",
		From:   "producer",
		To:     "consumer",
		Rename: "deploy.completed",
	}})

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if got, want := plan.source.flowID.value, "producer"; got != want {
		t.Fatalf("Source.FlowID = %q, want %q", got, want)
	}
	if got, want := plan.source.pin.value, "deploy.done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.source.event.value, events.EventType("deploy.done"); got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.source.resolvedEvent.value, events.EventType("producer/deploy.done"); got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.receiver.pin.value, "deploy.completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.receiver.event.value, events.EventType("deploy.completed"); got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
	if got, want := plan.targetKind, ConnectTargetKindTarget; got != want {
		t.Fatalf("TargetKind = %q, want %q", got, want)
	}
	if got, want := plan.resolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.target.FlowInstance != "consumer" {
		t.Fatalf("Target.FlowInstance = %q, want consumer", plan.target.FlowInstance)
	}
	if plan.target.EntityID != "" {
		t.Fatalf("Target.EntityID = %q, want route-only static blueprint", plan.target.EntityID)
	}
	if plan.RequiresRuntimeResolution() {
		t.Fatal("static connect blueprint does not require descriptor materialization")
	}
}

func TestCompileConnectPlansRootProducerToStaticReceiver(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyRootOutputConnect(t, canonicalrouting.RootConnectNoEmitter)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.source.IsRoot() {
		t.Fatalf("Source.Root = false, want true: %#v", plan.source)
	}
	if got, want := plan.source.flowID.value, ""; got != want {
		t.Fatalf("Source.FlowID = %q, want root empty flow id", got)
	}
	if got, want := plan.source.pin.value, "root.ready"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.source.resolvedEvent.value, events.EventType("root.ready"); got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.receiver.flowID.value, "consumer"; got != want {
		t.Fatalf("Receiver.FlowID = %q, want %q", got, want)
	}
	if plan.target.FlowInstance != "consumer" {
		t.Fatalf("Target.FlowInstance = %q, want consumer", plan.target.FlowInstance)
	}
}

func TestCompileConnectPlansSupportsRootReceiverEndpoint(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopySingletonOutputRootConnect(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.receiver.IsRoot() || plan.receiver.flowID.value != "" || plan.receiver.pin.value != "scout.completed" {
		t.Fatalf("Receiver = %#v, want root input scout.completed", plan.receiver)
	}
	if plan.target.FlowInstance != "" || plan.target.EntityID != "" || !plan.RequiresRuntimeResolution() {
		t.Fatalf("root target = %#v (runtime=%t), want publication-time root owner resolution", plan.target, plan.RequiresRuntimeResolution())
	}
}

func TestCompiledConnectProducerEvidenceOwnsConnectedRootInput(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopySingletonOutputRootConnect(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)

	resolution := ResolveFlowInputProducer(source, "", "scout.completed")
	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want compiled parent-connect evidence", resolution.Evidence)
	}
	if resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		t.Fatalf("evidence = %#v, connected root input must not retain external-ingress authority", resolution.Evidence)
	}
}

func TestCompiledConnectProducerEvidenceOwnsConnectedChildInput(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.requested"}}},
		{id: "worker", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.requested"}}},
	}, []runtimecontracts.FlowPackageConnect{{Event: "work.requested", From: "producer", To: "worker"}})

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")
	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want compiled parent-connect evidence", resolution.Evidence)
	}
	if len(resolution.ProducerPatterns()) != 0 {
		t.Fatalf("patterns = %#v, compiled connect evidence must not create auto-wire authority", resolution.ProducerPatterns())
	}
}

func TestLowerCompositionConnectRoutePlanUsesExactEventPinIdentity(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Event: "internal.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Event: "external.completed",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		Event:  "internal.done",
		From:   "producer",
		To:     "consumer",
		Rename: "external.completed",
	}})

	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || len(bundle.CompositionConnects()) != 1 {
		t.Fatal("compiled connect input is unavailable")
	}
	plan, issue := lowerCompositionConnectRoutePlanWithLocation(source, bundle.CompositionConnects()[0])
	if !issue.Failure.Empty() {
		t.Fatalf("issue = %#v, want none", issue)
	}
	if got, want := plan.source.pin.value, "internal.done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.source.event.value, events.EventType("internal.done"); got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.receiver.pin.value, "external.completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.receiver.event.value, events.EventType("external.completed"); got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationFailsClosedForInvalidInputs(t *testing.T) {
	tests := []struct {
		name    string
		connect runtimecontracts.FlowPackageConnect
		want    ConnectRoutePlanFailure
	}{
		{
			name:    "missing output pin",
			connect: runtimecontracts.FlowPackageConnect{Event: "missing.event", From: "producer", To: "consumer", Rename: "deploy.completed"},
			want:    ConnectFailureProducerOutputPinMissing,
		},
	}
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Event: "deploy.completed",
			}},
		},
	}, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, issue := lowerCompositionConnectRoutePlanWithLocation(source, tc.connect)
			if issue.Failure != tc.want {
				t.Fatalf("Failure = %q, want %q (issue %#v)", issue.Failure, tc.want, issue)
			}
		})
	}
}

func TestCompileConnectGraphFailsClosedForNonUniqueEventCandidates(t *testing.T) {
	tests := []struct {
		name       string
		outputs    []runtimecontracts.FlowOutputEventPin
		inputs     []runtimecontracts.FlowInputEventPin
		connect    runtimecontracts.FlowPackageConnect
		want       ConnectRoutePlanFailure
		wantDetail []string
	}{
		{
			name:       "zero source candidates",
			outputs:    []runtimecontracts.FlowOutputEventPin{{Event: "other.event"}},
			inputs:     []runtimecontracts.FlowInputEventPin{{Event: "work.ready"}},
			connect:    runtimecontracts.FlowPackageConnect{Event: "work.ready", From: "producer", To: "consumer"},
			want:       ConnectFailureProducerOutputPinMissing,
			wantDetail: []string{"source endpoint \"producer\"", "visible event \"work.ready\"", "matched 0 pins", "candidate pins: other.event"},
		},
		{
			name:       "zero receiver candidates",
			outputs:    []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}},
			inputs:     []runtimecontracts.FlowInputEventPin{{Event: "other.event"}},
			connect:    runtimecontracts.FlowPackageConnect{Event: "work.ready", From: "producer", To: "consumer"},
			want:       ConnectFailureReceiverInputPinMissing,
			wantDetail: []string{"receiver endpoint \"consumer\"", "visible event \"work.ready\"", "matched 0 pins", "candidate pins: other.event"},
		},
		{
			name:       "connect without rename cannot select a different receiver event",
			outputs:    []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}},
			inputs:     []runtimecontracts.FlowInputEventPin{{Event: "work.accepted"}},
			connect:    runtimecontracts.FlowPackageConnect{Event: "work.ready", From: "producer", To: "consumer"},
			want:       ConnectFailureReceiverInputPinMissing,
			wantDetail: []string{"visible event \"work.ready\"", "matched 0 pins", "candidate pins: work.accepted"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := testConnectRoutePlanSource([]connectRoutePlanFlow{
				{id: "producer", mode: "static", outputs: tc.outputs},
				{id: "consumer", mode: "static", inputs: tc.inputs},
			}, []runtimecontracts.FlowPackageConnect{tc.connect})
			plans, issues := compileConnectPlans(source)
			if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != tc.want {
				t.Fatalf("plans/issues = %#v/%#v, want no plan and %s", plans, issues, tc.want.Code())
			}
			for _, want := range tc.wantDetail {
				if !strings.Contains(issues[0].Detail, want) {
					t.Fatalf("issue detail = %q, want %q", issues[0].Detail, want)
				}
			}
		})
	}
}

func TestCompileConnectGraphRejectsFlowOutsideAuthoringPackage(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}}},
		{id: "unrelated", packageKey: "packages/other", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.ready"}}},
	}, []runtimecontracts.FlowPackageConnect{{Event: "work.ready", From: "producer", To: "unrelated"}})

	plans, issues := compileConnectPlans(source)
	if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != ConnectFailureReceiverFlowMissing {
		t.Fatalf("plans/issues = %#v/%#v, want package-visible receiver rejection", plans, issues)
	}
	for _, want := range []string{`package "."`, `endpoint "unrelated"`, "does not name a visible flow"} {
		if !strings.Contains(issues[0].Detail, want) {
			t.Fatalf("issue detail = %q, want %q", issues[0].Detail, want)
		}
	}
}

func TestCompileConnectGraphTreatsEndpointsAsExactFlowIDs(t *testing.T) {
	t.Run("retired source pin spelling is not a flow", func(t *testing.T) {
		source := testConnectRoutePlanSource([]connectRoutePlanFlow{
			{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}}},
			{id: "consumer", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.ready"}}},
		}, []runtimecontracts.FlowPackageConnect{{Event: "work.ready", From: "producer.work_output", To: "consumer"}})
		plans, issues := compileConnectPlans(source)
		if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != ConnectFailureProducerFlowMissing {
			t.Fatalf("plans/issues = %#v/%#v, want exact source-flow rejection", plans, issues)
		}
	})

	t.Run("retired receiver pin spelling is not a flow", func(t *testing.T) {
		source := testConnectRoutePlanSource([]connectRoutePlanFlow{
			{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}}},
			{id: "consumer", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.ready"}}},
		}, []runtimecontracts.FlowPackageConnect{{Event: "work.ready", From: "producer", To: "consumer.work_input"}})
		plans, issues := compileConnectPlans(source)
		if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != ConnectFailureReceiverFlowMissing {
			t.Fatalf("plans/issues = %#v/%#v, want exact receiver-flow rejection", plans, issues)
		}
	})

	t.Run("declared dotted flow ID remains exact", func(t *testing.T) {
		source := testConnectRoutePlanSource([]connectRoutePlanFlow{
			{id: "producer.v2", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Event: "work.ready"}}},
			{id: "consumer", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Event: "work.ready"}}},
		}, []runtimecontracts.FlowPackageConnect{{Event: "work.ready", From: "producer.v2", To: "consumer"}})
		plans, issues := compileConnectPlans(source)
		if len(plans) != 1 || len(issues) != 0 {
			t.Fatalf("plans/issues = %#v/%#v, want exact dotted flow ID accepted", plans, issues)
		}
	})
}

type connectRoutePlanFlow struct {
	id         string
	packageKey string
	mode       string
	inputs     []runtimecontracts.FlowInputEventPin
	outputs    []runtimecontracts.FlowOutputEventPin
}

func testConnectRoutePlanSource(flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootConnectRoutePlanSource(nil, flows, connects)
}

func testRootConnectRoutePlanSource(rootOutputs []runtimecontracts.FlowOutputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootInputOutputConnectRoutePlanSource(nil, rootOutputs, flows, connects)
}

func testRootReceiverConnectRoutePlanSource(rootInputs []runtimecontracts.FlowInputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootInputOutputConnectRoutePlanSource(rootInputs, nil, flows, connects)
}

func testRootInputOutputConnectRoutePlanSource(rootInputs []runtimecontracts.FlowInputEventPin, rootOutputs []runtimecontracts.FlowOutputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	connects = append([]runtimecontracts.FlowPackageConnect(nil), connects...)
	for i := range connects {
		connects[i].SourceFile = "package.yaml"
		connects[i].SourceLine = i + 1
	}
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	eventsCatalog := make(map[string]runtimecontracts.EventCatalogEntry)
	for _, flow := range flows {
		packageKey := strings.TrimSpace(flow.packageKey)
		if packageKey == "" {
			packageKey = "."
		}
		view := runtimecontracts.FlowContractView{
			Paths: runtimecontracts.FlowContractPaths{
				ID:         flow.id,
				Flow:       flow.id,
				PackageKey: packageKey,
				SchemaFile: filepath.Join("flows", flow.id, "schema.yaml"),
				EventsFile: filepath.Join("flows", flow.id, "events.yaml"),
			},
			Schema: runtimecontracts.FlowSchemaDocument{
				Mode: flow.mode,
				Pins: runtimecontracts.FlowPins{
					Inputs:  runtimecontracts.FlowInputPins{EventPins: flow.inputs},
					Outputs: runtimecontracts.FlowOutputPins{EventPins: flow.outputs},
				},
			},
			Path:   flow.id,
			Events: map[string]runtimecontracts.EventCatalogEntry{},
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		for _, pin := range flow.outputs {
			entry := runtimecontracts.EventCatalogEntry{}
			eventsCatalog[pin.EventType()] = entry
			viewCopy.Events[pin.EventType()] = entry
		}
		flowSchemas[flow.id] = view.Schema
	}
	for _, pin := range rootOutputs {
		eventsCatalog[pin.EventType()] = runtimecontracts.EventCatalogEntry{}
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		PackageTree: []runtimecontracts.LoadedProjectPackage{{
			Key:   ".",
			Paths: runtimecontracts.ProjectPackagePaths{PackageFile: "package.yaml"},
			Manifest: runtimecontracts.ProjectPackageDocument{
				Connect: connects,
			},
		}},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: rootInputs},
				Outputs: runtimecontracts.FlowOutputPins{EventPins: rootOutputs},
			},
		},
		Events:      eventsCatalog,
		FlowSchemas: flowSchemas,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: children,
			},
			ByID: byID,
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}

func writeConnectRoutePlanPackageFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
}

func replaceFixtureText(t testing.TB, path, old, replacement string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	if !strings.Contains(string(raw), old) {
		t.Fatalf("fixture %s does not contain %q", path, old)
	}
	raw = []byte(strings.ReplaceAll(string(raw), old, replacement))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

func writeCreateResolutionConnectRoutePlanPackageFixture(t *testing.T, source string) string {
	t.Helper()
	mode := canonicalrouting.CreateMintUUID
	if strings.TrimSpace(source) == runtimecontracts.FlowInputInstanceSourceEventIDPath {
		mode = canonicalrouting.CreateMintEventID
	}
	return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: mode})
}

func writeSelectResolutionConnectRoutePlanPackageFixture(t *testing.T) string {
	return writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{})
}

func writeSelectResolutionConnectRoutePlanPackageFixtureWithExtraResolution(t *testing.T, extraResolution string) string {
	return writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{extraResolution: extraResolution})
}

type selectResolutionConnectRoutePlanFixtureOptions struct {
	mode                runtimecontracts.FlowInputResolutionMode
	extraResolution     string
	accountIDEntityType string
	accountIDCarryType  string
}

func writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t *testing.T, options selectResolutionConnectRoutePlanFixtureOptions) string {
	t.Helper()
	mode := canonicalrouting.SelectResolutionSelect
	if options.mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		mode = canonicalrouting.SelectResolutionSelectOrCreate
	}
	invalidity := canonicalrouting.SelectResolutionValid
	switch {
	case strings.TrimSpace(options.extraResolution) != "":
		invalidity = canonicalrouting.SelectResolutionExtraAggregation
	case strings.TrimSpace(options.accountIDEntityType) == "integer":
		invalidity = canonicalrouting.SelectResolutionEntityTypeMismatch
	case strings.TrimSpace(options.accountIDCarryType) == "integer":
		invalidity = canonicalrouting.SelectResolutionSourceTypeMismatch
	}
	return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: mode, Invalidity: invalidity})
}

func mustTemplateInstanceField(t testing.TB, raw string) runtimecontracts.TemplateInstanceField {
	t.Helper()
	field, err := runtimecontracts.ParseTemplateInstanceField(raw)
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField(%q): %v", raw, err)
	}
	return field
}
