package pipeline

import (
	"strings"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestHandlerSettlementEvidenceRetainsAcknowledgedCommitOnContradiction(t *testing.T) {
	claim, err := runtimedelivery.AdmitPersistedClaim("delivery", "run", "route", "token", 1, runtimedelivery.SubscriberNode, "node")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := runtimedelivery.AdmitPersistedClaim("delivery", "run", "route", "foreign", 1, runtimedelivery.SubscriberNode, "node")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		result    contractHandlerExecutionResult
		wantError string
	}{
		{"exact commit", contractHandlerExecutionResult{Committed: true, SettledDeliveryClaim: &claim}, ""},
		{"committed missing claim", contractHandlerExecutionResult{Committed: true}, "without exact delivery settlement"},
		{"committed foreign claim", contractHandlerExecutionResult{Committed: true, SettledDeliveryClaim: &foreign}, "different delivery claim"},
		{"uncommitted settlement", contractHandlerExecutionResult{SettledDeliveryClaim: &claim}, "without an acknowledged commit"},
		{"uncommitted attempt", contractHandlerExecutionResult{}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := consumeHandlerSettlementEvidence(test.result, claim)
			if (err == nil) != (test.wantError == "") ||
				(err != nil && !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("error=%v, want error containing %q", err, test.wantError)
			}
		})
	}
}
