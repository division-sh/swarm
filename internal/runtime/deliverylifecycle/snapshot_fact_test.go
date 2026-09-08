package deliverylifecycle_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestDecodeHistoricalSnapshotCompleteRouteRoundTrip(t *testing.T) {
	for _, fixture := range historicalRouteFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			for _, status := range []deliverylifecycle.Status{
				deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress,
				deliverylifecycle.StatusFailed, deliverylifecycle.StatusDelivered, deliverylifecycle.StatusDeadLetter,
			} {
				t.Run(string(status), func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, status)
					decoded, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
					if err != nil {
						t.Fatalf("decode complete historical route: %v", err)
					}
					assertHistoricalRoute(t, decoded, fixture.route)
					if decoded.DeliveryID != fact["delivery_id"] || decoded.EventID != fact["event_id"] || decoded.RunID != fact["run_id"] || decoded.Status != status {
						t.Fatalf("historical delivery coordinates changed: %#v", decoded)
					}
					if decoded.Authority.Validate() == nil {
						t.Fatal("historical evidence unexpectedly grants live execution authority")
					}
					if status == deliverylifecycle.StatusInProgress && (decoded.ClaimVersion != 2 || decoded.ClaimExpiresAt.IsZero()) {
						t.Fatal("historical lease evidence was lost while excluding its token")
					}
				})
			}
		})
	}
}

func TestDecodeHistoricalSnapshotNonConnectPreservesEmptyClaim(t *testing.T) {
	for _, fixture := range historicalRouteFixtures(t) {
		if !fixture.route.ConnectClaim.Empty() {
			continue
		}
		t.Run(fixture.name, func(t *testing.T) {
			fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
			if got := string(historicalJSON(t, fact["connect_execution_claim"])); got != "{}" {
				t.Fatalf("canonical non-connect evidence = %s, want empty claim object", got)
			}
			decoded, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
			if err != nil {
				t.Fatal(err)
			}
			assertHistoricalRoute(t, decoded, fixture.route)
		})
	}
}

func TestDecodeHistoricalSnapshotRouteIdentitySensitivity(t *testing.T) {
	node := historicalRouteFixtures(t)[0].route
	agent := historicalRouteFixtures(t)[3].route
	tests := []struct {
		name   string
		base   events.DeliveryRoute
		change func(*events.DeliveryRoute)
	}{
		{"connect_absent", node, func(r *events.DeliveryRoute) { r.ConnectClaim = events.ConnectExecutionClaim{} }},
		{"edge_digest", node, func(r *events.DeliveryRoute) {
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "other-edge", "pin", "review.received")
		}},
		{"receiver_pin_digest", node, func(r *events.DeliveryRoute) {
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "edge", "other-pin", "review.received")
		}},
		{"handler_event", node, func(r *events.DeliveryRoute) {
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "edge", "pin", "review.other")
		}},
		{"node_recipient_and_bound_handler", node, func(r *events.DeliveryRoute) {
			r.Recipient = events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "other"))
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "edge", "pin", "review.received")
		}},
		{"node_package_owner", node, func(r *events.DeliveryRoute) {
			r.Recipient = events.MustNodeDeliveryRecipient(identitytest.ExecutableNode(t, "other/review", "receive"))
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "edge", "pin", "review.received")
		}},
		{"target_flow", node, func(r *events.DeliveryRoute) {
			target := r.Target.Route()
			target.FlowID = "other"
			r.Target = events.MustExistingEntityTarget(target)
		}},
		{"target_instance", node, func(r *events.DeliveryRoute) {
			target := r.Target.Route()
			target.FlowInstance = "review/two"
			r.Target = events.MustExistingEntityTarget(target)
		}},
		{"target_entity", node, func(r *events.DeliveryRoute) {
			target := r.Target.Route()
			target.EntityID = "c2353fa7-e6af-4c55-8ce3-1785994ce718"
			r.Target = events.MustExistingEntityTarget(target)
		}},
		{"target_ownership_kind", node, func(r *events.DeliveryRoute) { r.Target = events.MustMaterializingEntityTarget(r.Target.Route()) }},
		{"reply_context", node, func(r *events.DeliveryRoute) {
			r.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-two"}}
		}},
		{"payload_projection", node, func(r *events.DeliveryRoute) { r.PayloadProjection = historicalProjection(t, "two") }},
		{"agent_name_owner", agent, func(r *events.DeliveryRoute) {
			name, err := agentidentity.DeclaredName(r.Recipient.ID(), "test://other-owner")
			if err != nil {
				t.Fatal(err)
			}
			r.AgentIdentity = historicalAgentIdentity(t, name, r.AgentIdentity.Route)
		}},
		{"agent_recipient_and_identity", agent, func(r *events.DeliveryRoute) {
			name, err := agentidentity.DeclaredName("other-worker", r.AgentIdentity.Name.Owner)
			if err != nil {
				t.Fatal(err)
			}
			r.Recipient = events.MustAgentDeliveryRecipient("other-worker")
			r.AgentIdentity = historicalAgentIdentity(t, name, r.AgentIdentity.Route)
			r.ConnectClaim = historicalConnectClaim(t, r.Recipient, "edge", "pin", "review.received")
		}},
		{"recipient_class_and_bound_evidence", node, func(r *events.DeliveryRoute) {
			r.Recipient, r.AgentIdentity, r.ConnectClaim = agent.Recipient, agent.AgentIdentity, agent.ConnectClaim
		}},
		{"agent_name_source", agent, func(r *events.DeliveryRoute) {
			name, err := agentidentity.RuntimeName(r.Recipient.ID(), r.AgentIdentity.Name.Owner)
			if err != nil {
				t.Fatal(err)
			}
			r.AgentIdentity = historicalAgentIdentity(t, name, r.AgentIdentity.Route)
		}},
		{"agent_scope", agent, func(r *events.DeliveryRoute) {
			route, err := agentidentity.PresentRoute("other", "one", "review/one")
			if err != nil {
				t.Fatal(err)
			}
			r.AgentIdentity = historicalAgentIdentity(t, r.AgentIdentity.Name, route)
		}},
		{"agent_instance_id", agent, func(r *events.DeliveryRoute) {
			route, err := agentidentity.PresentRoute("review", "two", "review/one")
			if err != nil {
				t.Fatal(err)
			}
			r.AgentIdentity = historicalAgentIdentity(t, r.AgentIdentity.Name, route)
		}},
		{"agent_instance_path", agent, func(r *events.DeliveryRoute) {
			route, err := agentidentity.PresentRoute("review", "one", "review/two")
			if err != nil {
				t.Fatal(err)
			}
			r.AgentIdentity = historicalAgentIdentity(t, r.AgentIdentity.Name, route)
		}},
		{"agent_root_presence", agent, func(r *events.DeliveryRoute) {
			r.AgentIdentity = historicalAgentIdentity(t, r.AgentIdentity.Name, agentidentity.RootRoute())
		}},
		{"agent_targetless", agent, func(r *events.DeliveryRoute) { r.Target = events.DeliveryTargetOwnership{} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := tc.base
			tc.change(&changed)
			baseID, err := tc.base.Identity()
			if err != nil {
				t.Fatal(err)
			}
			changedID, err := changed.Identity()
			if err != nil {
				t.Fatalf("changed route is not a valid independent control: %v", err)
			}
			if baseID == changedID || events.SameDeliveryRouteIdentity(tc.base, changed) {
				t.Fatal("changed route evidence did not change complete identity")
			}
			fact := historicalRouteFact(t, changed, deliverylifecycle.StatusPending)
			decoded, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
			if err != nil {
				t.Fatalf("decode positive changed-route control: %v", err)
			}
			assertHistoricalRoute(t, decoded, changed)
			fact["route_identity"] = events.EncodeDeliveryRouteIdentity(baseID)
			if _, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact)); !errors.Is(err, deliverylifecycle.ErrConflict) {
				t.Fatalf("changed evidence with original hash: %v, want conflict", err)
			}
		})
	}
}

func TestDecodeHistoricalSnapshotRejectsCorruptRouteEvidence(t *testing.T) {
	for _, fixture := range historicalRouteFixtures(t) {
		if fixture.route.ConnectClaim.Empty() {
			continue
		}
		t.Run(fixture.name, func(t *testing.T) {
			for _, field := range []string{"route_identity", "subscriber_type", "subscriber_id", "delivery_context", "delivery_payload_projection", "connect_execution_claim"} {
				t.Run("missing_"+field, func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					delete(fact, field)
					assertHistoricalRejects(t, fact)
				})
			}
			for _, field := range []string{"sha256", "receiver_pin_sha256", "recipient_kind", "recipient_id", "handler_event"} {
				t.Run("claim_missing_"+field, func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					claim := historicalClaimFields(t, fixture.route.ConnectClaim)
					delete(claim, field)
					fact["connect_execution_claim"] = claim
					assertHistoricalRejects(t, fact)
				})
			}
			for _, tc := range []struct {
				name  string
				field string
				value any
			}{
				{"unknown", "unexpected", true},
				{"short_digest", "sha256", "ab"},
				{"nonhex_digest", "sha256", strings.Repeat("g", 64)},
				{"uppercase_digest", "sha256", strings.Repeat("A", 64)},
				{"wrong_digest_type", "sha256", 17},
				{"different_digest", "sha256", strings.Repeat("a", 64)},
				{"nonhex_pin", "receiver_pin_sha256", strings.Repeat("z", 64)},
				{"uppercase_pin", "receiver_pin_sha256", strings.Repeat("A", 64)},
				{"different_pin", "receiver_pin_sha256", strings.Repeat("b", 64)},
				{"unknown_recipient_kind", "recipient_kind", "system"},
				{"foreign_recipient", "recipient_id", "foreign"},
				{"empty_handler", "handler_event", ""},
				{"noncanonical_handler", "handler_event", " review.received "},
				{"different_handler", "handler_event", "review.other"},
			} {
				t.Run("claim_"+tc.name, func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					claim := historicalClaimFields(t, fixture.route.ConnectClaim)
					claim[tc.field] = tc.value
					fact["connect_execution_claim"] = claim
					assertHistoricalRejects(t, fact)
				})
			}
			for _, value := range []any{nil, "invalid", []any{}, map[string]any{}} {
				t.Run("claim_shape_"+string(historicalJSON(t, value)), func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					fact["connect_execution_claim"] = value
					assertHistoricalRejects(t, fact)
				})
			}
			if !fixture.route.Target.Empty() {
				t.Run("missing_target", func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					delete(fact, "delivery_target_ownership")
					assertHistoricalRejects(t, fact)
				})
			}
			if fixture.route.Recipient.IsAgent() {
				t.Run("missing_agent_identity", func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusPending)
					delete(fact, "agent_identity")
					assertHistoricalRejects(t, fact)
				})
			}
		})
	}
}

func TestDecodeHistoricalSnapshotRejectsMisboundClaim(t *testing.T) {
	node := historicalRouteFixtures(t)[0].route
	agent := historicalRouteFixtures(t)[3].route
	foreign := identitytest.FlowNode(t, "review", "foreign")
	for _, tc := range []struct {
		name   string
		route  events.DeliveryRoute
		mutate func(map[string]any)
	}{
		{"node_handler_missing", node, func(c map[string]any) { delete(c, "handler_node") }},
		{"node_handler_foreign", node, func(c map[string]any) { c["handler_node"] = foreign }},
		{"agent_has_node_handler", agent, func(c map[string]any) { c["handler_node"] = foreign }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := historicalClaimFields(t, tc.route.ConnectClaim)
			tc.mutate(claim)
			var canonical events.ConnectExecutionClaim
			if err := json.Unmarshal(historicalJSON(t, claim), &canonical); err == nil {
				t.Fatal("canonical claim codec accepted misbound handler")
			}
			fact := historicalRouteFact(t, tc.route, deliverylifecycle.StatusPending)
			fact["connect_execution_claim"] = claim
			assertHistoricalRejects(t, fact)
		})
	}
	for _, tc := range []struct {
		name      string
		route     events.DeliveryRoute
		recipient events.DeliveryRecipient
	}{
		{"node", node, events.MustNodeDeliveryRecipient(foreign)},
		{"agent", agent, events.MustAgentDeliveryRecipient("other-worker")},
	} {
		t.Run("valid_claim_for_wrong_route_recipient_"+tc.name, func(t *testing.T) {
			claim := historicalConnectClaim(t, tc.recipient, "edge", "pin", "review.received")
			var control events.ConnectExecutionClaim
			if err := json.Unmarshal(historicalJSON(t, claim), &control); err != nil || !control.Equal(claim) {
				t.Fatalf("foreign claim must be valid independently: %v", err)
			}
			fact := historicalRouteFact(t, tc.route, deliverylifecycle.StatusPending)
			fact["connect_execution_claim"] = claim
			assertHistoricalRejects(t, fact)
		})
	}
}

func TestDecodeHistoricalSnapshotRejectsUnknownFieldsAndLiveTokens(t *testing.T) {
	for _, fixture := range historicalRouteFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusInProgress)
			decoded, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(historicalJSON(t, decoded), []byte("claim_token")) || decoded.Authority.Validate() == nil {
				t.Fatal("historical snapshot exposed a live capability")
			}
			for _, field := range []string{"unexpected", "claim_token", "lease_token", "execution_authority"} {
				t.Run(field, func(t *testing.T) {
					fact := historicalRouteFact(t, fixture.route, deliverylifecycle.StatusInProgress)
					fact[field] = "f03e9ff9-7a7f-45dc-89d3-a15ed401388a"
					assertHistoricalRejects(t, fact)
				})
			}
		})
	}
}

type historicalRouteFixture struct {
	name  string
	route events.DeliveryRoute
}

func historicalRouteFixtures(t testing.TB) []historicalRouteFixture {
	t.Helper()
	node := identitytest.FlowNode(t, "review", "receive")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: "4e1e8a5a-b6b2-44cb-a54f-7d31cfcbfe82"}
	base := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target),
		Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-one"}}, PayloadProjection: historicalProjection(t, "one"),
	}
	base.ConnectClaim = historicalConnectClaim(t, base.Recipient, "edge", "pin", "review.received")
	materializing := base
	materializing.Target = events.MustMaterializingEntityTarget(target)
	entityless := base
	target.EntityID = ""
	entityless.Target = events.MustEntitylessReceiverTarget(target)
	name, err := agentidentity.DeclaredName("worker", "test://historical-delivery")
	if err != nil {
		t.Fatal(err)
	}
	agentRoute, err := agentidentity.PresentRoute("review", "one", "review/one")
	if err != nil {
		t.Fatal(err)
	}
	agent := base
	agent.Recipient = events.MustAgentDeliveryRecipient("worker")
	agent.AgentIdentity = historicalAgentIdentity(t, name, agentRoute)
	agent.ConnectClaim = historicalConnectClaim(t, agent.Recipient, "edge", "pin", "review.received")
	rootAgent := agent
	rootAgent.AgentIdentity = historicalAgentIdentity(t, name, agentidentity.RootRoute())
	rootAgent.Target = events.DeliveryTargetOwnership{}
	directNode, directAgent, directRootAgent := base, agent, rootAgent
	directNode.ConnectClaim, directAgent.ConnectClaim, directRootAgent.ConnectClaim = events.ConnectExecutionClaim{}, events.ConnectExecutionClaim{}, events.ConnectExecutionClaim{}
	return []historicalRouteFixture{
		{"node_connected_existing", base}, {"node_connected_materializing", materializing}, {"node_connected_entityless", entityless},
		{"agent_connected", agent}, {"agent_connected_root_targetless", rootAgent},
		{"node_direct", directNode}, {"agent_direct", directAgent}, {"agent_direct_root_targetless", directRootAgent},
	}
}

func historicalConnectClaim(t testing.TB, recipient events.DeliveryRecipient, edge, pin, event string) events.ConnectExecutionClaim {
	t.Helper()
	node, _ := recipient.Node()
	if recipient.IsAgent() {
		node = runtimeidentity.ExecutableNode{}
	}
	// Codec fixtures exercise the admitted digest boundary, not compiled-edge execution.
	claim, err := events.AdmitConnectExecutionClaim(sha256.Sum256([]byte(edge)), sha256.Sum256([]byte(pin)), recipient, node, events.EventType(event))
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func historicalAgentIdentity(t testing.TB, name agentidentity.Name, route agentidentity.Route) agentidentity.Identity {
	t.Helper()
	value, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func historicalProjection(t testing.TB, value string) events.DeliveryPayloadProjection {
	t.Helper()
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"validation_case_id": value, "chat_id": "conversation-one"})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func historicalRouteFact(t testing.TB, route events.DeliveryRoute, status deliverylifecycle.Status) map[string]any {
	t.Helper()
	var fact map[string]any
	if err := json.Unmarshal(historicalJSON(t, route), &fact); err != nil {
		t.Fatal(err)
	}
	identity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	const eventID = "56295375-36b6-4b8f-85b7-33a45c938204"
	id, err := deliverylifecycle.DeliveryID(eventID, route)
	if err != nil {
		t.Fatal(err)
	}
	class, err := deliverylifecycle.ParseSubscriberClass(route.Recipient.Code())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 1, 0, 0, 0, time.UTC)
	fact["delivery_id"], fact["event_id"], fact["run_id"] = id, eventID, "d34a7a1b-f738-42b2-8758-9e4ad5417ad0"
	fact["route_identity"], fact["status"], fact["max_retries"] = events.EncodeDeliveryRouteIdentity(identity), status, class.MaxRetries()
	fact["created_at"], fact["updated_at"] = now, now
	switch status {
	case deliverylifecycle.StatusPending:
		fact["next_eligible_at"] = now
	case deliverylifecycle.StatusInProgress:
		fact["claim_version"], fact["started_at"], fact["claim_expires_at"] = 2, now, now.Add(time.Minute)
	case deliverylifecycle.StatusFailed:
		fact["retry_count"], fact["next_eligible_at"] = 1, now.Add(time.Second)
		fallthrough
	case deliverylifecycle.StatusDeadLetter:
		failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassDependencyUnavailable, "historical_fixture_failure", "test", "snapshot", nil))
		if !ok {
			t.Fatal("missing typed failure fixture")
		}
		fact["failure"] = failure
		if status == deliverylifecycle.StatusDeadLetter {
			fact["reason_code"], fact["settled_at"] = "terminal_failure", now
		}
	case deliverylifecycle.StatusDelivered:
		fact["settled_at"] = now
	default:
		t.Fatalf("unsupported test status %q", status)
	}
	return fact
}

func historicalClaimFields(t testing.TB, claim events.ConnectExecutionClaim) map[string]any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(historicalJSON(t, claim), &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func historicalJSON(t testing.TB, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertHistoricalRejects(t testing.TB, fact map[string]any) {
	t.Helper()
	if _, err := deliverylifecycle.DecodeHistoricalSnapshot(historicalJSON(t, fact)); err == nil {
		t.Fatal("historical codec accepted corrupt or foreign evidence")
	}
}

func assertHistoricalRoute(t testing.TB, decoded deliverylifecycle.Snapshot, want events.DeliveryRoute) {
	t.Helper()
	if !reflect.DeepEqual(decoded.Route, want.Normalized()) || !events.SameDeliveryRouteIdentity(decoded.Route, want) {
		t.Fatalf("historical route lost semantic evidence:\ngot  %s\nwant %s", historicalJSON(t, decoded.Route), historicalJSON(t, want))
	}
	identity, err := want.Identity()
	if err != nil || decoded.RouteIdentity != identity || decoded.SubscriberID != want.Recipient.ID() || string(decoded.SubscriberClass) != want.Recipient.Code() {
		t.Fatalf("historical identity mismatch: %#v, %v", decoded, err)
	}
	if !decoded.Route.ConnectClaim.Equal(want.ConnectClaim) {
		t.Fatal("historical route lost connect proof")
	}
	if node, ok := want.Recipient.Node(); ok && !want.ConnectClaim.Empty() {
		handler, found := decoded.Route.ConnectClaim.NodeHandlerEvent(node)
		wantHandler, wantFound := want.ConnectClaim.NodeHandlerEvent(node)
		if !found || !wantFound || handler != wantHandler {
			t.Fatalf("exact historical node handler binding lost: %q, %t", handler, found)
		}
		foreign := identitytest.FlowNode(t, "foreign", "receive")
		if _, found := decoded.Route.ConnectClaim.NodeHandlerEvent(foreign); found {
			t.Fatal("historical claim authorized a foreign node handler")
		}
	}
}
