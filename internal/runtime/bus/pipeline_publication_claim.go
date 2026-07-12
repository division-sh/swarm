package bus

import (
	"context"
	"fmt"
	"strings"
	"sync"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type pipelinePublicationClaim struct {
	bus     *EventBus
	eventID string
	lease   runtimeownership.Lease
	once    sync.Once
}

func (eb *EventBus) claimPipelinePublication(ctx context.Context, eventID string) (*pipelinePublicationClaim, error) {
	if eb == nil || !runtimereplayclaim.SupportsPersistedReplay(eb.store) {
		return nil, nil
	}
	if _, ok := eb.store.(runtimereplayclaim.Owner); !ok {
		return nil, nil
	}
	owner, ok := eb.store.(runtimereplayclaim.PublicationOwner)
	if !ok || owner == nil {
		return nil, runtimereplayclaim.ErrMissingPublicationClaimOwner
	}
	eventID = strings.TrimSpace(eventID)
	lease, claimed, err := owner.ClaimPipelinePublication(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("claim foreground pipeline publication %s: %w", eventID, err)
	}
	if !claimed || lease == nil {
		return nil, fmt.Errorf("%w: %s", runtimereplayclaim.ErrPublicationClaimBusy, eventID)
	}
	return &pipelinePublicationClaim{bus: eb, eventID: eventID, lease: lease}, nil
}

func (c *pipelinePublicationClaim) Release(ctx context.Context) {
	if c == nil || c.lease == nil {
		return
	}
	c.once.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		if err := c.lease.Release(context.WithoutCancel(ctx)); err != nil && c.bus != nil {
			c.bus.logRuntime(context.WithoutCancel(ctx), "error", "Releasing foreground pipeline publication claim failed", "eventbus", "pipeline_publication_claim_release_failed", c.eventID, "", "", "", "", nil, nil, eventBusDependencyFailure(err, "pipeline_publication_claim_release_failed", "release_pipeline_publication_claim"), 0)
		}
	})
}
