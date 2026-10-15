package deliverylifecycle_test

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestAgentClaimProfilePreservesOriginalAdmissionRatherThanCurrentGrant(t *testing.T) {
	identity := agentidentitytest.RootRuntime(t, "worker", "claim-profile")
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "source", "", nil, 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, uuid.NewString(), 3)
	if err != nil {
		t.Fatal(err)
	}
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	profile := deliverylifecycle.AgentClaimProfile{
		DeliveryID: id, EventID: event.ID(), Identity: identity, ProfileID: "anthropic",
		ConfigurationRevision: strings.Repeat("b", 64), BundleHash: source.BundleHash(),
		ExecutionID: authority.ExecutionID(), ExecutionGeneration: authority.Generation(), AgentRuntimeEpoch: 2, AgentGeneration: 4,
	}
	if !profile.MatchesFirstAdmission(event, route, authority) {
		t.Fatal("exact first admission refused")
	}
	next, err := deliverylifecycle.NewNormalExecutionAuthority(source, uuid.NewString(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if profile.MatchesFirstAdmission(event, route, next) {
		t.Fatal("a different first-admission generation was accepted")
	}
	snapshot := deliverylifecycle.Snapshot{DeliveryID: id, EventID: event.ID(), RunID: identity.RunID, Route: route, Authority: next, SubscriberClass: deliverylifecycle.SubscriberAgent, SubscriberID: identity.AgentID()}
	if !profile.MatchesDelivery(snapshot) {
		t.Fatal("current grant replaced the immutable profile witness")
	}
	for _, name := range []string{"missing", "profile", "revision", "source", "generation", "epoch", "agent", "delivery", "event"} {
		t.Run(name, func(t *testing.T) {
			bad := profile
			switch name {
			case "missing":
				bad = deliverylifecycle.AgentClaimProfile{}
			case "profile":
				bad.ProfileID = "Anthropic"
			case "revision":
				bad.ConfigurationRevision = "foreign"
			case "source":
				bad.BundleHash = "foreign"
			case "generation":
				bad.AgentGeneration = 0
			case "epoch":
				bad.AgentRuntimeEpoch = 0
			case "agent":
				bad.Identity.Name.AgentID = "sibling"
			case "delivery":
				bad.DeliveryID = uuid.NewString()
			case "event":
				bad.EventID = uuid.NewString()
			}
			if bad.MatchesFirstAdmission(event, route, authority) || bad.MatchesDelivery(snapshot) {
				t.Fatal("missing/corrupt/foreign capture accepted")
			}
		})
	}
}
