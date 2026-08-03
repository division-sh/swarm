package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type ProposedEffectRouteCommand struct {
	CardID       string
	RouteEventID string
	OccurredAt   time.Time
	Publication  runtimeengine.DurablePublicationPlan
}

func (c ProposedEffectRouteCommand) Validate() error {
	if strings.TrimSpace(c.CardID) == "" || strings.TrimSpace(c.RouteEventID) == "" || c.OccurredAt.IsZero() {
		return fmt.Errorf("proposed-effect route requires exact card, event, and occurrence identity")
	}
	if c.Publication == nil {
		return fmt.Errorf("proposed-effect route requires one durable publication")
	}
	return c.Publication.ValidateDurablePublicationPlan()
}

type CommittedProposedEffectRoute struct {
	Publication runtimeengine.CommittedDurablePublication
}

func (r CommittedProposedEffectRoute) Validate() error {
	if r.Publication == nil {
		return fmt.Errorf("committed proposed-effect route requires publication evidence")
	}
	return r.Publication.ValidateCommittedDurablePublication()
}

type HumanTaskDeferredRouteCommand struct {
	CardID       string
	RouteEventID string
	OccurredAt   time.Time
	Publication  runtimeengine.DurablePublicationPlan
}

func (c HumanTaskDeferredRouteCommand) Validate() error {
	if strings.TrimSpace(c.CardID) == "" || strings.TrimSpace(c.RouteEventID) == "" || c.OccurredAt.IsZero() {
		return fmt.Errorf("human-task deferred route requires exact card, event, and occurrence identity")
	}
	if c.Publication == nil {
		return fmt.Errorf("human-task deferred route requires one durable publication")
	}
	return c.Publication.ValidateDurablePublicationPlan()
}

type HumanTaskOutcomeRouteCommand struct {
	CardID       string
	RouteEventID string
	OccurredAt   time.Time
	Publication  runtimeengine.DurablePublicationPlan
}

func (c HumanTaskOutcomeRouteCommand) Validate() error {
	if strings.TrimSpace(c.CardID) == "" || strings.TrimSpace(c.RouteEventID) == "" || c.OccurredAt.IsZero() {
		return fmt.Errorf("human-task outcome route requires exact card, event, and occurrence identity")
	}
	if c.Publication == nil {
		return fmt.Errorf("human-task outcome route requires one durable publication")
	}
	return c.Publication.ValidateDurablePublicationPlan()
}

type CommittedHumanTaskRoute struct {
	Publication runtimeengine.CommittedDurablePublication
}

func (r CommittedHumanTaskRoute) Validate() error {
	if r.Publication == nil {
		return fmt.Errorf("committed human-task route requires publication evidence")
	}
	return r.Publication.ValidateCommittedDurablePublication()
}

type WorkflowDecisionRouteOwner interface {
	CommitProposedEffectRoute(context.Context, ProposedEffectRouteCommand) (CommittedProposedEffectRoute, error)
	CommitHumanTaskDeferredRoute(context.Context, HumanTaskDeferredRouteCommand) (CommittedHumanTaskRoute, error)
	CommitHumanTaskOutcomeRoute(context.Context, HumanTaskOutcomeRouteCommand) (CommittedHumanTaskRoute, error)
}
