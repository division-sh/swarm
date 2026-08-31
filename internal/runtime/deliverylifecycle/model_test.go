package deliverylifecycle

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func TestNewObligationRejectsAgentIdentityFromAnotherRun(t *testing.T) {
	runID := uuid.NewString()
	otherRunID := uuid.NewString()
	identity := agentidentitytest.RootRuntimeForRun(t, otherRunID, "worker", "delivery-test")
	source, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("construct bundle source fact: %v", err)
	}
	authority, err := NewNormalExecutionAuthority(source, "delivery-test", 1)
	if err != nil {
		t.Fatalf("construct execution authority: %v", err)
	}

	_, err = NewObligation(uuid.NewString(), runID, events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(identity.AgentID()),
		AgentIdentity: identity,
	}, authority)
	if err == nil || !strings.Contains(err.Error(), "agent run does not match obligation run") {
		t.Fatalf("NewObligation error = %v, want exact run mismatch", err)
	}
}
