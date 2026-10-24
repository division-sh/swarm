package contracts

import (
	"strings"
	"testing"
)

func TestGuardTerminationJoinCombinationsRemainForbidden(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		handler := SystemNodeEventHandler{
			Guard: &GuardSpec{Check: "false"},
			Join:  &JoinSpec{Stage: "ready", Members: JoinMembersSpec{FromFanOut: delivery}},
		}
		if delivery {
			handler.FanOut = &FanOutSpec{}
		}
		if err := ValidateJoinHandlerIsolation(handler); err == nil || !strings.Contains(err.Error(), "guard") {
			t.Fatalf("guard/join source widened: delivery=%v err=%v", delivery, err)
		}
	}
}
