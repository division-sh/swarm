package bus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type bindingCommitProbeStore struct {
	runtimedelivery.Store
	commit runtimedelivery.ClaimCommit
	err    error
	calls  int
}

func (s *bindingCommitProbeStore) BindAgentSession(context.Context, runtimedelivery.Claim, string) (runtimedelivery.ClaimCommit, error) {
	s.calls++
	return s.commit, s.err
}

func TestEventBusBindingConsumesTypedClaimCommit(t *testing.T) {
	routeIdentity, err := events.ParseDeliveryRouteIdentity("delivery-route-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := runtimedelivery.AdmitPersistedClaim(uuid.NewString(), uuid.NewString(), events.EncodeDeliveryRouteIdentity(routeIdentity), uuid.NewString(), 1, runtimedelivery.SubscriberAgent, "agent-a")
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.NewString()
	exact := runtimedelivery.ClaimCommit{Acknowledged: true, Snapshot: runtimedelivery.Snapshot{
		DeliveryID: claim.DeliveryID(), RunID: claim.RunID(), RouteIdentity: routeIdentity,
		ClaimVersion: claim.Version(), SubscriberClass: claim.SubscriberClass(), SubscriberID: claim.SubscriberID(),
		Status: runtimedelivery.StatusInProgress, ActiveSessionID: sessionID,
	}}
	wrong := exact
	wrong.Snapshot.ActiveSessionID = uuid.NewString()
	cleanup := errors.New("binding cleanup failed after commit")
	stale := errors.New("stale binding claim")
	for _, tc := range []struct {
		name      string
		commit    runtimedelivery.ClaimCommit
		err       error
		wantBound bool
		wantErr   error
	}{
		{"acknowledged_cleanup", exact, cleanup, true, cleanup},
		{"acknowledged_wrong_session", wrong, cleanup, false, runtimedelivery.ErrConflict},
		{"unacknowledged_stale", runtimedelivery.ClaimCommit{}, stale, false, stale},
		{"unacknowledged_without_error", runtimedelivery.ClaimCommit{}, nil, false, runtimedelivery.ErrConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &bindingCommitProbeStore{commit: tc.commit, err: tc.err}
			bus := &EventBus{
				store:              InMemoryEventStore{},
				sourceArtifactFact: sourceartifactfixture.Fact(),
				durable:            DurableDependencies{DeliveryLifecycle: owner},
			}
			bound, err := bus.MarkDeliveryInProgress(runtimedelivery.WithClaim(context.Background(), claim), "agent-a", sessionID)
			if bound != tc.wantBound || !errors.Is(err, tc.wantErr) || owner.calls != 1 {
				t.Fatalf("binding outcome = bound:%v err:%v calls:%d, want bound:%v err:%v calls:1", bound, err, owner.calls, tc.wantBound, tc.wantErr)
			}
		})
	}
}
