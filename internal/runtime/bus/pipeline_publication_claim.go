package bus

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type pipelinePublicationClaim struct {
	bus      *EventBus
	eventID  string
	claim    runtimepipelineobligation.Claim
	released atomic.Bool
}

func (c *pipelinePublicationClaim) durable() bool {
	return c != nil && c.bus != nil && c.bus.pipelineObligations != nil
}

func (eb *EventBus) claimPipelinePublication(ctx context.Context, eventID string) (*pipelinePublicationClaim, error) {
	if eb == nil || eb.pipelineObligations == nil {
		if eb != nil && eb.ephemeral {
			return &pipelinePublicationClaim{bus: eb, eventID: strings.TrimSpace(eventID)}, nil
		}
		return nil, fmt.Errorf("pipeline obligation owner is required")
	}
	eventID = strings.TrimSpace(eventID)
	claim, err := eb.pipelineObligations.ClaimPublication(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("claim foreground pipeline publication %s: %w", eventID, err)
	}
	return &pipelinePublicationClaim{bus: eb, eventID: eventID, claim: claim}, nil
}

func (c *pipelinePublicationClaim) Release(ctx context.Context) {
	if c == nil || c.bus == nil || c.bus.pipelineObligations == nil {
		if c != nil && c.bus != nil && c.bus.ephemeral {
			c.released.CompareAndSwap(false, true)
			return
		}
		panic("pipeline publication claim owner is required")
	}
	if !c.released.CompareAndSwap(false, true) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.bus.pipelineObligations.Release(context.WithoutCancel(ctx), c.claim); err != nil {
		c.bus.logRuntime(context.WithoutCancel(ctx), "error", "Releasing foreground pipeline publication claim failed", "eventbus", "pipeline_publication_claim_release_failed", c.eventID, "", "", "", "", nil, nil, eventBusDependencyFailure(err, "pipeline_publication_claim_release_failed", "release_pipeline_publication_claim"), 0)
	}
}

func (c *pipelinePublicationClaim) Settle(ctx context.Context, disposition runtimepipelineobligation.Disposition) error {
	if c == nil || c.bus == nil || c.bus.pipelineObligations == nil {
		if c != nil && c.bus != nil && c.bus.ephemeral {
			if !c.released.CompareAndSwap(false, true) {
				return runtimepipelineobligation.ErrStaleClaim
			}
			return nil
		}
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	if !c.released.CompareAndSwap(false, true) {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.bus.pipelineObligations.Settle(ctx, c.claim, disposition); err != nil {
		c.released.Store(false)
		return err
	}
	return nil
}

func (c *pipelinePublicationClaim) MarkDecisionProcessed(ctx context.Context) error {
	if c == nil || c.bus == nil || c.bus.pipelineObligations == nil {
		if c != nil && c.bus != nil && c.bus.ephemeral {
			if c.released.Load() {
				return runtimepipelineobligation.ErrStaleClaim
			}
			return nil
		}
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	if c.released.Load() {
		return runtimepipelineobligation.ErrStaleClaim
	}
	return c.bus.pipelineObligations.MarkDecisionProcessed(ctx, c.claim)
}

func (c *pipelinePublicationClaim) Claim() runtimepipelineobligation.Claim {
	if c == nil {
		return runtimepipelineobligation.Claim{}
	}
	return c.claim
}
