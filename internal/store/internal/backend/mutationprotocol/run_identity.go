package mutationprotocol

import (
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

// physicalRunKey changes only bookkeeping identity, never payload or SQL spelling.
// An unrecognized key remains exact but cannot mint an active-run admission.
func physicalRunKey(dialect privateactivity.Dialect, runID string) (string, bool) {
	return counterprojection.PhysicalRunKey(dialect, runID)
}
