package sessionprovider

import (
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

// RuntimeIncomingOptions supplies existing runtime owners, not raw capture,
// a connected flag or a caller-defined authority issuer.
type RuntimeIncomingOptions struct {
	Alias   string
	Trigger providertriggers.InboundAdmissionPlan
	Bus     *bus.EventBus
	Posture executionposture.Posture
}
