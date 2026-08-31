package bus

import (
	"context"
	"errors"
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

func (eb *EventBus) claimPipelinePublication(ctx context.Context, eventID string) (*pipelinePublicationClaim, error) {
	var err error
	ctx, err = eb.admitSourceArtifactFact(ctx)
	if err != nil {
		return nil, err
	}
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

func (c *pipelinePublicationClaim) Release(ctx context.Context) error {
	if c == nil || c.bus == nil {
		panic("pipeline publication claim owner is required")
	}
	var err error
	ctx, err = c.bus.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	if !c.released.CompareAndSwap(false, true) {
		return nil
	}
	if c.bus.pipelineObligations == nil {
		if c.bus.ephemeral {
			return nil
		}
		panic("pipeline publication claim owner is required")
	}
	return c.bus.pipelineObligations.Release(context.WithoutCancel(ctx), c.claim)
}

func (c *pipelinePublicationClaim) releaseAndLog(ctx context.Context) {
	if err := c.Release(ctx); err != nil && c != nil && c.bus != nil {
		c.bus.logRuntime(context.WithoutCancel(ctx), "error", "Releasing foreground pipeline publication claim failed", "eventbus", "pipeline_publication_claim_release_failed", c.eventID, "", "", "", "", nil, nil, eventBusDependencyFailure(err, "pipeline_publication_claim_release_failed", "release_pipeline_publication_claim"), 0)
	}
}

func (c *pipelinePublicationClaim) Settle(ctx context.Context, disposition runtimepipelineobligation.Disposition) error {
	if c == nil || c.bus == nil {
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	var err error
	ctx, err = c.bus.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	if !c.released.CompareAndSwap(false, true) {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if c.bus.pipelineObligations == nil {
		if c.bus.ephemeral {
			return nil
		}
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	outcome, err := c.bus.settlePipelineObligationOutcome(ctx, c.claim, disposition)
	if err != nil {
		if outcome.Committed() {
			return fmt.Errorf("settle pipeline publication %s: %w", c.eventID, err)
		}
		releaseErr := c.bus.pipelineObligations.Release(context.WithoutCancel(ctx), c.claim)
		if errors.Is(releaseErr, runtimepipelineobligation.ErrStaleClaim) {
			releaseErr = nil
		}
		return errors.Join(fmt.Errorf("settle pipeline publication %s: %w", c.eventID, err), releaseErr)
	}
	return nil
}

func (eb *EventBus) settlePipelineObligation(
	ctx context.Context,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) error {
	_, err := eb.settlePipelineObligationOutcome(ctx, claim, disposition)
	return err
}

func (eb *EventBus) settlePipelineObligationOutcome(
	ctx context.Context,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) (runtimepipelineobligation.SettlementOutcome, error) {
	if eb == nil || eb.pipelineObligations == nil {
		return runtimepipelineobligation.SettlementOutcome{}, errors.New("pipeline obligation owner is required")
	}
	outcome, err := eb.pipelineObligations.Settle(ctx, claim, disposition)
	if outcome.DeliveryHandoffCommitted() {
		eb.SignalDeliveryContinuations()
	}
	return outcome, err
}

func (c *pipelinePublicationClaim) MarkDecisionProcessed(ctx context.Context) error {
	if c == nil || c.bus == nil {
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	var err error
	ctx, err = c.bus.admitSourceArtifactFact(ctx)
	if err != nil {
		return err
	}
	if c.released.Load() {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if c.bus.pipelineObligations == nil {
		if c.bus.ephemeral {
			return nil
		}
		return fmt.Errorf("pipeline publication claim owner is required")
	}
	if err := c.bus.pipelineObligations.MarkDecisionProcessed(ctx, c.claim); err != nil {
		return fmt.Errorf("mark decision publication %s processed: %w", c.eventID, err)
	}
	return nil
}

func (c *pipelinePublicationClaim) Claim() runtimepipelineobligation.Claim {
	if c == nil {
		return runtimepipelineobligation.Claim{}
	}
	return c.claim
}
