package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ManagerDeliveryNativeFixture is the test-only import-cycle bridge. Its Store
// is the original selected owner; setup and fixed fault cuts are explicit.
type ManagerDeliveryNativeFixture struct {
	runtimedelivery.Store
	Authority         runtimedelivery.ExecutionAuthority
	Context           context.Context
	SelectedEvent     events.Event
	SelectedAdmission managedexecution.Admission
	SelectedRoute     events.DeliveryRoute
	SemanticSource    semanticview.Source
	RequireRun        func(*testing.T, string)
	NormalAuthority   runtimedelivery.ExecutionAuthority
	Publish           func(*testing.T, context.Context, events.Event, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority)
	ClaimHeartbeat    func(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error)
	RenewHeartbeat    func(context.Context, runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error)
	RetryReady        func(*testing.T, events.Event, events.DeliveryRoute)
	AgeClaim          func(*testing.T, runtimedelivery.Claim, time.Time)
	Transitions       func(*testing.T) []string
}

type managerDeliveryNativeFactory func(*testing.T) *ManagerDeliveryNativeFixture

func (s *ManagerDeliveryNativeFixture) managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority {
	return s.Authority
}

func (s *ManagerDeliveryNativeFixture) seedAgentDeliveries(t *testing.T, agentID string, pending []events.Event) {
	t.Helper()
	for _, event := range pending {
		s.Publish(t, s.Context, event, []events.DeliveryRoute{managerAgentDeliveryRouteForRun(event.RunID(), agentID)}, s.Authority)
	}
}

func (s *ManagerDeliveryNativeFixture) seedClaim(t *testing.T, ctx context.Context, event events.Event, agentID string) {
	t.Helper()
	authority := s.Authority
	if admission, present := managedexecution.FromContext(ctx); present {
		var err error
		authority, err = runtimedelivery.NewExecutionAuthority(authority.SourceArtifact(), admission)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Publish(t, ctx, event, []events.DeliveryRoute{managerAgentDeliveryRouteForRun(event.RunID(), agentID)}, authority)
}

func (s *ManagerDeliveryNativeFixture) claimExact(ctx context.Context, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	result, err := s.Store.ClaimDelivery(ctx, s.Authority, event, route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("manager native delivery disposition = %s", result.Disposition)
	}
	return claimed, nil
}

func (s *ManagerDeliveryNativeFixture) makeDeliveryDueNow(t *testing.T, event events.Event, agentID string) {
	t.Helper()
	s.RetryReady(t, event, managerAgentDeliveryRouteForRun(event.RunID(), agentID))
}

func (s *ManagerDeliveryNativeFixture) activityTransitions(t *testing.T) []string {
	t.Helper()
	return s.Transitions(t)
}
