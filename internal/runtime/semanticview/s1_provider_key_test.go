package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestS1ProviderKeySpellingCannotMintInputOrSubscription(t *testing.T) {
	source := Wrap(&contracts.WorkflowContractBundle{})
	for _, name := range []string{"inbound.telegram-sales.message", "inbound.telegram_sales.message"} {
		if pin, ok := SelectedRootInputPin(source, name); ok || !pin.Empty() {
			t.Fatalf("spelling minted a public input: %q", name)
		}
		admission := ClassifyAuthoredSubscription(source, AuthoredSubscriptionRequest{
			ConsumerKind: AuthoredSubscriptionConsumerNode, ConsumerID: "observer", FlowID: ".", FlowPath: ".",
			Authored: name, LocalEvents: map[string]struct{}{name: {}}, InputEvents: []string{name},
		})
		if admission.Admitted() || admission.Failure() != AuthoredSubscriptionFailureReceiverEventMissing || admission.Matches(name) {
			t.Fatalf("spelling or caller maps minted receiver evidence: %+v", admission)
		}
	}
}
