package events

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestConstructedEventClonePreservesAllOwnedFacts(t *testing.T) {
	source, err := NewConcreteTemplateInstanceRoutingSource(RouteIdentity{FlowID: "source", FlowInstance: "source/one", EntityID: "entity-source"})
	if err != nil {
		t.Fatalf("NewConcreteTemplateInstanceRoutingSource: %v", err)
	}
	envelope := EnvelopeForTargetSet(EventEnvelope{Source: source.Route()}, []RouteIdentity{
		{FlowID: "target", FlowInstance: "target/one", EntityID: "entity-one"},
		{FlowID: "target", FlowInstance: "target/two", EntityID: "entity-two"},
	})
	createdAt := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	original, err := NewChildEvent(ChildEventInput{
		Facts: EventFacts{
			ID: "event-1", Type: "phrase.completed", Producer: ProducerClaim{Type: EventProducerNode, ID: "declarative-node"},
			TaskID: "task-1", Payload: []byte(`{"text":"how are you"}`), ChainDepth: 3,
			Envelope: envelope, RoutingSource: source, CreatedAt: createdAt, ExecutionMode: executionmode.Mock,
		},
		Lineage: EventLineage{RunID: "run-1", ParentEventID: "parent-1", TaskID: "task-1", ExecutionMode: executionmode.Mock},
	})
	if err != nil {
		t.Fatalf("NewChildEvent: %v", err)
	}
	clone := original.Clone()
	if clone.AdmissionClass() != EventAdmissionChild || !clone.Producer().Equal(original.Producer()) || clone.RoutingSource().Route() != source.Route() {
		t.Fatalf("clone lost semantic ownership: %#v", clone)
	}
	if clone.RunID() != "run-1" || clone.ParentEventID() != "parent-1" || clone.ExecutionMode() != executionmode.Mock {
		t.Fatalf("clone lineage changed: run=%q parent=%q mode=%q", clone.RunID(), clone.ParentEventID(), clone.ExecutionMode())
	}
	payload := clone.Payload()
	payload[2] = 'X'
	targets := clone.TargetRoutes()
	targets[0].EntityID = "mutated"
	if string(original.Payload()) != `{"text":"how are you"}` || original.TargetRoutes()[0].EntityID != "entity-one" {
		t.Fatal("clone aliases event-owned facts")
	}
}

func TestResolvedEnvelopeCannotRewriteRoutingSource(t *testing.T) {
	source, err := NewConcreteTemplateInstanceRoutingSource(RouteIdentity{FlowID: "producer", FlowInstance: "producer/one", EntityID: "entity-one"})
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewRunCreatingRootIngressEvent(RunCreatingRootIngressEventInput{Facts: EventFacts{
		Type: "task.started", Producer: ProducerClaim{Type: EventProducerExternal, ID: "provider"}, Payload: []byte(`{}`),
		Envelope: EventEnvelope{Source: source.Route()}, RoutingSource: source, ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveEnvelope(event, EventEnvelope{Source: RouteIdentity{FlowID: "other", FlowInstance: "other/one", EntityID: "other-entity"}}); err == nil {
		t.Fatal("ResolveEnvelope rewrote routing source")
	}
	resolved, err := ResolveEnvelope(event, EnvelopeForTargetRoute(event.NormalizedEnvelope(), RouteIdentity{FlowID: "target", FlowInstance: "target/one", EntityID: "target-entity"}))
	if err != nil {
		t.Fatalf("ResolveEnvelope target: %v", err)
	}
	if resolved.TargetRoute().FlowInstance != "target/one" || resolved.RoutingSource().Route() != source.Route() {
		t.Fatalf("resolved event = %#v", resolved.NormalizedEnvelope())
	}
}

func TestDeclaredIngressRoutingSourceRemainsOpaqueToEnvelopeRouting(t *testing.T) {
	source, err := NewExternalIngressRoutingSource("telegram-ingress", "entity-one", RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewRunCreatingRootIngressEvent(RunCreatingRootIngressEventInput{Facts: EventFacts{
		Type: "inbound.telegram", Producer: ProducerClaim{Type: EventProducerExternal, ID: "inbound-gateway"},
		Payload: []byte(`{}`), RoutingSource: source, ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceRoute() != (RouteIdentity{}) {
		t.Fatalf("declared ingress envelope source = %#v, want absent until the canonical routing evaluator interprets it", event.SourceRoute())
	}
	if got := event.RoutingSource().Route(); got != source.Route() {
		t.Fatalf("typed routing source = %#v, want %#v", got, source.Route())
	}
	resolved, err := ResolveEnvelope(event, EnvelopeForTargetRoute(event.NormalizedEnvelope(), RouteIdentity{FlowID: "telegram-chat", FlowInstance: "telegram-chat/one", EntityID: "entity-one"}))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.SourceRoute() != (RouteIdentity{}) || resolved.RoutingSource().Route() != source.Route() {
		t.Fatalf("resolved ingress source facts = envelope:%#v typed:%#v", resolved.SourceRoute(), resolved.RoutingSource().Route())
	}
}

func TestRoutingSourceVariantsRequireExactClaims(t *testing.T) {
	for _, tc := range []struct {
		name      string
		route     RouteIdentity
		wantError bool
	}{
		{name: "absent", wantError: true},
		{name: "flow context only", route: RouteIdentity{FlowID: "root"}, wantError: true},
		{name: "entity fact only", route: RouteIdentity{EntityID: "entity-1"}, wantError: true},
		{name: "flow and entity without instance", route: RouteIdentity{FlowID: "root", EntityID: "entity-1"}, wantError: true},
		{name: "entityless exact instance", route: RouteIdentity{FlowID: "root", FlowInstance: "root/one"}},
		{name: "exact instance", route: RouteIdentity{FlowID: "root", FlowInstance: "root/one", EntityID: "entity-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := NewConcreteTemplateInstanceRoutingSource(tc.route)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %t", err, tc.wantError)
			}
			if err == nil && source.Empty() {
				t.Fatal("complete concrete-template source is empty")
			}
		})
	}
}

func TestRoutingSourceKindCoverageIsExhaustive(t *testing.T) {
	kinds := []RoutingSourceKind{
		RoutingSourceAbsent,
		RoutingSourceExternalIngress,
		RoutingSourceRoot,
		RoutingSourceStaticFlow,
		RoutingSourceConcreteTemplateInstance,
		RoutingSourceFlowOwnedControl,
		RoutingSourcePlatformControl,
	}
	if len(kinds) != int(routingSourceKindCount) {
		t.Fatalf("routing-source coverage has %d variants, want %d", len(kinds), routingSourceKindCount)
	}
	seen := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		code := kind.StorageCode()
		if code == "" {
			t.Fatalf("routing-source kind %d has no storage code", kind)
		}
		if _, ok := seen[code]; ok {
			t.Fatalf("routing-source storage code %q is duplicated", code)
		}
		seen[code] = struct{}{}
	}
}

func TestRestoreRoutingSourceRejectsCrossVariantClaims(t *testing.T) {
	exactFlowRoute := RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/one", EntityID: "entity-1"}
	for _, tc := range []struct {
		name      string
		kind      RoutingSourceKind
		route     RouteIdentity
		authority string
	}{
		{name: "root with flow id", kind: RoutingSourceRoot, route: RouteIdentity{FlowID: "flow-a", EntityID: "entity-1"}},
		{name: "root with flow instance", kind: RoutingSourceRoot, route: RouteIdentity{FlowInstance: "flow-a/one", EntityID: "entity-1"}},
		{name: "root with authority", kind: RoutingSourceRoot, route: RouteIdentity{EntityID: "entity-1"}, authority: RoutingSourceAuthorityProviderAdmissionPlan.StorageCode()},
		{name: "external ingress with flow instance", kind: RoutingSourceExternalIngress, route: exactFlowRoute, authority: RoutingSourceAuthorityProviderAdmissionPlan.StorageCode()},
		{name: "static flow with authority", kind: RoutingSourceStaticFlow, route: exactFlowRoute, authority: RoutingSourceAuthorityProviderAdmissionPlan.StorageCode()},
		{name: "concrete template with authority", kind: RoutingSourceConcreteTemplateInstance, route: exactFlowRoute, authority: RoutingSourceAuthorityProviderAdmissionPlan.StorageCode()},
		{name: "flow owned control with authority", kind: RoutingSourceFlowOwnedControl, route: exactFlowRoute, authority: RoutingSourceAuthorityProviderAdmissionPlan.StorageCode()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := RestoreRoutingSource(tc.kind.StorageCode(), tc.route, tc.authority); err == nil {
				t.Fatal("RestoreRoutingSource accepted cross-variant routing facts")
			}
		})
	}
}

func TestEventDeliveryProjectionCannotPersist(t *testing.T) {
	event, err := NewRunCreatingRootIngressEvent(RunCreatingRootIngressEventInput{Facts: EventFacts{
		Type: "message.received", Producer: ProducerClaim{Type: EventProducerExternal, ID: "telegram"},
		Payload: []byte(`{"text":"hello"}`), ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := NewDeliveryPayloadProjection(map[string]string{"chat_id": "123"})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := NewDeliveryEvent(event, DeliveryRoute{PayloadProjection: projection, Context: DeliveryContext{Reply: &ReplyContextRef{ID: "reply-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(delivery.JournalEvent().Payload()) != `{"text":"hello"}` {
		t.Fatalf("journal payload changed: %s", delivery.JournalEvent().Payload())
	}
	if !strings.Contains(string(delivery.Event().Payload()), `"chat_id":"123"`) || delivery.Event().DeliveryContext().ReplyContextID() != "reply-1" {
		t.Fatalf("delivery view = %s / %#v", delivery.Event().Payload(), delivery.Event().DeliveryContext())
	}
}

func TestDeliveryEventTargetlessRouteDoesNotConsumeJournalTargetSetProjection(t *testing.T) {
	source := RouteIdentity{FlowID: "producer", FlowInstance: "producer/one", EntityID: "source-entity"}
	target := RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: "target-entity"}
	routingSource, err := NewStaticFlowRoutingSource(source)
	if err != nil {
		t.Fatalf("NewStaticFlowRoutingSource: %v", err)
	}
	event, err := NewChildEvent(ChildEventInput{
		Facts: EventFacts{
			ID: "11111111-2222-3333-4444-555555555555", Type: "work.ready",
			Producer:      ProducerClaim{Type: EventProducerNode, ID: "producer-node"},
			Envelope:      EnvelopeForTargetSet(EnvelopeForSourceRoute(EventEnvelope{}, source), []RouteIdentity{target}),
			RoutingSource: routingSource, ExecutionMode: executionmode.Live,
		},
		Lineage: EventLineage{RunID: "run-one", ParentEventID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatalf("NewChildEvent: %v", err)
	}
	name, err := agentidentity.DeclaredName("observer", "fixture")
	if err != nil {
		t.Fatalf("DeclaredName: %v", err)
	}
	identity, err := agentidentity.New(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("agentidentity.New: %v", err)
	}
	delivery, err := NewDeliveryEvent(event, DeliveryRoute{
		Recipient:     MustAgentDeliveryRecipient("observer"),
		AgentIdentity: identity,
	})
	if err != nil {
		t.Fatalf("NewDeliveryEvent: %v", err)
	}
	if got := delivery.Event().TargetRoute(); !got.Empty() {
		t.Fatalf("targetless delivery target = %#v, want empty", got)
	}
	if got := delivery.Event().EntityID(); got != source.EntityID {
		t.Fatalf("targetless delivery entity = %q, want source %q", got, source.EntityID)
	}
	if got := delivery.JournalEvent().TargetRoutes(); len(got) != 1 || got[0] != target {
		t.Fatalf("journal target_set = %#v, want immutable projection %#v", got, target)
	}
}

func TestValidateEventContextReferenceRejectsLegacyReceiverProjections(t *testing.T) {
	for _, ref := range []string{"entity_id", "flow_instance"} {
		if err := ValidateEventContextReference(ref); err == nil || !strings.Contains(err.Error(), "_entity.") {
			t.Fatalf("ValidateEventContextReference(%q) = %v", ref, err)
		}
	}
}

func TestDeliveryPayloadProjectionIsCanonicalAndIsolated(t *testing.T) {
	input := map[string]string{" validation_case_id ": " case-1 "}
	projection, err := NewDeliveryPayloadProjection(input)
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	input[" validation_case_id "] = "mutated"
	fields := projection.Fields()
	fields["validation_case_id"] = "mutated-again"
	if got := projection.Fields()["validation_case_id"]; got != "case-1" {
		t.Fatalf("projection owner mutated = %q", got)
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip DeliveryPayloadProjection
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip != projection {
		t.Fatalf("round trip = %#v, want %#v", roundTrip, projection)
	}
}

func TestValidateDeliveryRouteProjectionsRejectsConflictingFacts(t *testing.T) {
	first, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	second, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	route := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(identitytest.FlowNode(t, "validation", "validator")), Target: MustEntitylessReceiverTarget(RouteIdentity{FlowID: "validation", FlowInstance: "validation/one"})}
	left, right := route, route
	left.PayloadProjection, right.PayloadProjection = first, second
	if err := ValidateDeliveryRouteProjections([]DeliveryRoute{left, right}); err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
		t.Fatalf("ValidateDeliveryRouteProjections error = %v", err)
	}
}

func TestValidateDeliveryRoutesRejectsContradictoryTargetOwnershipForExactSlot(t *testing.T) {
	route := RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: "entity-one"}
	left := DeliveryRoute{
		Recipient: MustNodeDeliveryRecipient(identitytest.FlowNode(t, "validation", "validator")),
		Target:    MustExistingEntityTarget(route),
	}
	right := left
	right.Target = MustMaterializingEntityTarget(route)

	err := ValidateDeliveryRoutes([]DeliveryRoute{left, right})
	if err == nil || !strings.Contains(err.Error(), "conflicting target ownership kinds") {
		t.Fatalf("ValidateDeliveryRoutes error = %v, want exact ownership contradiction", err)
	}
}

func TestValidateDeliveryRouteProjectionsPreservesDistinctExactAgentIdentities(t *testing.T) {
	firstProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	secondProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	firstIdentity := testDeliveryAgentIdentity(t, "worker", "review", "one", "review/one")
	secondIdentity := testDeliveryAgentIdentity(t, "worker", "review", "two", "review/two")

	routes := []DeliveryRoute{
		{Recipient: MustAgentDeliveryRecipient("worker"), AgentIdentity: firstIdentity, PayloadProjection: firstProjection},
		{Recipient: MustAgentDeliveryRecipient("worker"), AgentIdentity: secondIdentity, PayloadProjection: secondProjection},
	}
	if err := ValidateDeliveryRoutes(routes); err != nil {
		t.Fatalf("ValidateDeliveryRoutes distinct exact agents: %v", err)
	}
}

func TestValidateDeliveryRouteProjectionsRejectsSameExactAgentIdentityConflict(t *testing.T) {
	firstProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	secondProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	identity := testDeliveryAgentIdentity(t, "worker", "review", "one", "review/one")
	left := DeliveryRoute{Recipient: MustAgentDeliveryRecipient("worker"), AgentIdentity: identity, PayloadProjection: firstProjection}
	right := DeliveryRoute{Recipient: MustAgentDeliveryRecipient("worker"), AgentIdentity: identity, PayloadProjection: secondProjection}

	err := ValidateDeliveryRoutes([]DeliveryRoute{left, right})
	if err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
		t.Fatalf("ValidateDeliveryRoutes error = %v, want exact agent projection conflict", err)
	}
}

func TestValidateDeliveryRoutesRejectsSameAgentSlotOwnershipDisagreementAcrossTargets(t *testing.T) {
	identity := testDeliveryAgentIdentity(t, "worker", "review", "one", "review/one")
	base := DeliveryRoute{
		Recipient:     MustAgentDeliveryRecipient("worker"),
		AgentIdentity: identity,
		Target: MustExistingEntityTarget(RouteIdentity{
			FlowID: "review", FlowInstance: "review/one", EntityID: "entity-one",
		}),
	}

	tests := []struct {
		name  string
		other DeliveryTargetOwnership
	}{
		{
			name: "different target route",
			other: MustExistingEntityTarget(RouteIdentity{
				FlowID: "review", FlowInstance: "review/two", EntityID: "entity-two",
			}),
		},
		{name: "targetless versus targeted", other: DeliveryTargetOwnership{}},
		{
			name: "different ownership kind",
			other: MustMaterializingEntityTarget(RouteIdentity{
				FlowID: "review", FlowInstance: "review/one", EntityID: "entity-one",
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			other := base
			other.Target = test.other
			if err := ValidateDeliveryRoutes([]DeliveryRoute{base, other}); err == nil || !strings.Contains(err.Error(), "conflicting target ownership") {
				t.Fatalf("ValidateDeliveryRoutes error = %v, want exact agent-slot ownership contradiction", err)
			}
		})
	}
}

func TestValidateDeliveryRouteProjectionsRejectsSameAgentSlotConflictAcrossTargets(t *testing.T) {
	firstProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	secondProjection, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	identity := testDeliveryAgentIdentity(t, "worker", "review", "one", "review/one")
	left := DeliveryRoute{
		Recipient: MustAgentDeliveryRecipient("worker"), AgentIdentity: identity,
		Target:            MustExistingEntityTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: "entity-one"}),
		PayloadProjection: firstProjection,
	}
	right := left
	right.Target = MustExistingEntityTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/two", EntityID: "entity-two"})
	right.PayloadProjection = secondProjection

	if err := ValidateDeliveryRoutes([]DeliveryRoute{left, right}); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("ValidateDeliveryRoutes error = %v, want same-agent-slot contradiction", err)
	}
}

func testDeliveryAgentIdentity(t testing.TB, agentID, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, "test://delivery-route")
	if err != nil {
		t.Fatalf("agentidentity.DeclaredName: %v", err)
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		t.Fatalf("agentidentity.PresentRoute: %v", err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("agentidentity.New: %v", err)
	}
	return identity
}

func TestDeliveryRouteRejectsConnectClaimForAnotherRecipient(t *testing.T) {
	digest := sha256.Sum256([]byte("connect-edge"))
	receiverPinDigest := sha256.Sum256([]byte("receiver-pin"))
	handlerNode := identitytest.FlowNode(t, "review", "node-b")
	claim := ConnectExecutionClaim{
		digest: digest, receiverPinDigest: receiverPinDigest,
		recipientKind: deliveryRecipientNode, recipientID: handlerNode.Key(),
		handlerNode: handlerNode, handlerEvent: "flow.completed", present: true,
	}
	recipientNode := identitytest.FlowNode(t, "review", "node-a")
	route := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(recipientNode), ConnectClaim: claim}
	if _, err := route.Identity(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("route identity error = %v, want recipient mismatch", err)
	}
	if _, err := json.Marshal(route); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("route marshal error = %v, want recipient mismatch", err)
	}

	raw, err := json.Marshal(deliveryRouteWire{
		SubscriberType: "node", SubscriberID: recipientNode.Key(), ConnectClaim: claim,
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded DeliveryRoute
	if err := json.Unmarshal(raw, &decoded); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("route decode error = %v, want recipient mismatch", err)
	}
}

func TestDeliveryRoutesAndConnectClaimsPreserveSameFlowPackageOwners(t *testing.T) {
	first := identitytest.ExecutableNode(t, "a/review", "shared")
	second := identitytest.ExecutableNode(t, "b/review", "shared")
	digest := sha256.Sum256([]byte("connect-edge"))
	pinDigest := sha256.Sum256([]byte("receiver-pin"))
	routes := make([]DeliveryRoute, 0, 2)
	target := MustEntitylessReceiverTarget(RouteIdentity{FlowID: "review", FlowInstance: "review/one"})
	for _, node := range []runtimeidentity.ExecutableNode{first, second} {
		recipient := MustNodeDeliveryRecipient(node)
		claim, err := AdmitConnectExecutionClaim(digest, pinDigest, recipient, node, EventType("review.received"))
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, DeliveryRoute{Recipient: recipient, Target: target, ConnectClaim: claim})
	}
	if routes[0].Recipient.ID() == routes[1].Recipient.ID() || routes[0].ConnectClaim.Equal(routes[1].ConnectClaim) {
		t.Fatalf("same-flow package routes collapsed: %#v", routes)
	}
	if err := ValidateDeliveryRoutes(routes); err != nil {
		t.Fatalf("validate distinct package routes: %v", err)
	}
	for index, route := range routes {
		raw, err := json.Marshal(route)
		if err != nil {
			t.Fatal(err)
		}
		var decoded DeliveryRoute
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if !SameDeliveryRouteIdentity(decoded, route) {
			t.Fatalf("route %d round trip = %#v, want %#v", index, decoded, route)
		}
	}
	hostile := routes[0]
	hostile.Recipient = routes[1].Recipient
	if _, err := hostile.Identity(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("cross-package recipient/claim mismatch = %v", err)
	}
}

func TestConnectExecutionClaimRejectsEveryPartialEmptyEncoding(t *testing.T) {
	var empty ConnectExecutionClaim
	if err := json.Unmarshal([]byte(`{}`), &empty); err != nil || !empty.Empty() {
		t.Fatalf("empty claim = %#v, %v", empty, err)
	}

	for _, raw := range []string{
		`null`,
		`{"sha256":""}`,
		`{"receiver_pin_sha256":""}`,
		`{"recipient_kind":"node"}`,
		`{"recipient_id":"node-a"}`,
		`{"handler_event":"flow.completed"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var claim ConnectExecutionClaim
			if err := json.Unmarshal([]byte(raw), &claim); err == nil {
				t.Fatalf("partial empty claim %s decoded as %#v", raw, claim)
			}
		})
	}
}
