package runfork

import "github.com/division-sh/swarm/internal/runtime/runlifecycle"

// MaterializationRunOrigin projects a validated fork point into persisted run origin.
func (p RunForkPoint) MaterializationRunOrigin(sourceRunID string) (runlifecycle.RunOrigin, error) {
	if err := p.Validate(); err != nil {
		return runlifecycle.RunOrigin{}, err
	}
	return runlifecycle.ForkMaterializationRunOrigin(sourceRunID, p.Kind, p.Revision, p.EventID)
}
