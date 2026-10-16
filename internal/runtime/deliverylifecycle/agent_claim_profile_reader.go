package deliverylifecycle

import "context"

// FirstAgentClaimProfileReader observes immutable history, not live claim or
// provider admission authority.
type FirstAgentClaimProfileReader interface {
	ReadFirstAgentClaimProfile(context.Context, Snapshot) (AgentClaimProfile, error)
}
