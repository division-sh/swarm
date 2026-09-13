package decisioncard

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

func TestMutationRefusalRetainsCardStateBeforeFormerAnchor(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   error
	}{
		{StatusPending, nil},
		{StatusDecided, ErrAlreadyTerminal},
		{StatusSuperseded, ErrSuperseded},
		{StatusExpired, ErrAlreadyTerminal},
	} {
		t.Run(tc.status, func(t *testing.T) {
			card := Card{Status: tc.status}
			if got := card.RequirePendingMutation(); !errors.Is(got, tc.want) {
				t.Fatalf("status refusal = %v, want %v", got, tc.want)
			}
			if tc.want != nil {
				// A previous stage or continuation may have advanced, disappeared or
				// been superseded. It cannot overwrite the card's terminal reason.
				for _, got := range []error{
					card.RequireGateMutation(gateruntime.Activation{}, false, "later_stage"),
					(HumanTaskContinuation{}).RequirePendingMutation(card),
					(ProposedEffectContinuation{}).RequirePendingMutation(card),
				} {
					if !errors.Is(got, tc.want) {
						t.Fatalf("former anchor overwrote terminal reason: %v, want %v", got, tc.want)
					}
				}
			}
		})
	}
	for _, status := range []string{"", "Pending", " superseded", "unknown"} {
		err := (Card{Status: status}).RequirePendingMutation()
		if err == nil || errors.Is(err, ErrSuperseded) || errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("invalid status acquired a domain meaning: %q: %v", status, err)
		}
	}
}
