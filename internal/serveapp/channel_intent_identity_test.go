package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// Capture once before the verified ingress occurrence or temporal mutation.
// This readback is not an assertion of current binding or execution authority.
func captureChannelIntentIdentity(t *testing.T, h *channelOnboardingE2EHarness) channeldelivery.IntentObservationQuery {
	t.Helper()
	var channels struct {
		Channels []channelonboarding.ConnectedChannelReadback `json:"channels"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.list", map[string]any{}, &channels)
	if len(channels.Channels) != 1 {
		t.Fatalf("intent proof needs the exact ingress identity, not guessed binding: %+v", channels)
	}
	row := channels.Channels[0]
	if row.Identity.Interface.Validate() != nil {
		t.Fatalf("intent ingress readback has incomplete interface: %+v", row)
	}
	provider := ""
	if row.Activation != nil {
		if row.Identity.Interface != row.Activation.Interface {
			t.Fatalf("intent interface contradicts its retained activation: %+v", row)
		}
		provider = row.Activation.Provider
	} else if row.Operation == nil && row.Recovery != nil {
		// activationRecovery resolves this exact interface against the CURRENT
		// compiled candidate catalog. A live activation is not identity evidence.
		provider = row.Recovery.Provider
	}
	if provider == "" || row.Recovery != nil && row.Recovery.Provider != provider {
		t.Fatalf("intent ingress has missing or contradictory provider evidence: %+v", row)
	}
	return channeldelivery.IntentObservationQuery{Provider: provider, InterfaceKey: row.Identity.Interface.Key()}
}

func TestChannelNativeDispositionDecisionPreservesAbsencePendingAndFailClosedSettlement(t *testing.T) {
	for _, probe := range []struct {
		name        string
		result      channeldelivery.IntentObservation
		found, want bool
		error       bool
	}{
		{"absent", channeldelivery.IntentObservation{}, false, false, false},
		{"absent_foreign_settled_is_not_evidence", channeldelivery.IntentObservation{State: "settled", Disposition: "rejected"}, false, false, false},
		{"pending", channeldelivery.IntentObservation{State: "pending"}, true, false, false},
		{"stale", channeldelivery.IntentObservation{State: "settled", Disposition: "stale"}, true, true, false},
		{"rejected", channeldelivery.IntentObservation{State: "settled", Disposition: "rejected"}, true, true, false},
		{"wrong_settlement", channeldelivery.IntentObservation{State: "settled", Disposition: "applied"}, true, false, true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			got, err := channelDispositionAccepted(probe.result, probe.found, []string{"stale", "rejected"})
			if got != probe.want || (err != nil) != probe.error {
				t.Fatalf("settlement decision changed: got=%t err=%v", got, err)
			}
		})
	}
	if accepted, err := channelDispositionAccepted(channeldelivery.IntentObservation{State: "settled", Disposition: "rejected"}, true, nil); err == nil || accepted {
		t.Fatal("empty allowed set admitted a settlement")
	}
}
