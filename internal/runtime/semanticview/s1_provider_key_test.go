package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1ProviderKeySpellingCannotMintInputOrSubscription(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "selected", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource("", rooted, "", contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)
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
