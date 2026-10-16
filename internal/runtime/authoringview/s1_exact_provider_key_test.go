package authoringview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestS1StandingProviderReadbackDoesNotRepairIdentity(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", " telegram_sales", "telegram_sales "} {
		view := standingIngressProviderView(runtimecontracts.ProjectFlowIngressProvider{Provider: key})
		if view.Provider != key {
			t.Errorf("readback changed raw provider %q to %q", key, view.Provider)
		}
	}
}
