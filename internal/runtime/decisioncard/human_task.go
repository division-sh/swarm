package decisioncard

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

const (
	HumanTaskContinuationPending           = "pending"
	HumanTaskContinuationDecisionCommitted = "decision_committed"
	HumanTaskContinuationOutcomeDispatched = "outcome_dispatched"
	HumanTaskContinuationExpired           = "expired"
	HumanTaskContinuationSuperseded        = "superseded"
)

type HumanTaskContinuation struct {
	CardID            string
	RunID             string
	RequesterRoute    events.RouteIdentity
	ReplyContextID    string
	SourceEventID     string
	DeadlineAt        time.Time
	BudgetBundleHash  string
	BudgetLimit       int
	BudgetWindowStart time.Time
	BudgetWindowEnd   time.Time
	RequeueCount      int
	DeferCause        string
	DeferredUntil     time.Time
	State             string
	OutcomeEventID    string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (c HumanTaskContinuation) Canonical() HumanTaskContinuation {
	c.RequesterRoute = c.RequesterRoute.Normalized()
	c.ReplyContextID = strings.TrimSpace(c.ReplyContextID)
	c.SourceEventID = strings.TrimSpace(c.SourceEventID)
	c.BudgetBundleHash = strings.TrimSpace(c.BudgetBundleHash)
	c.DeferCause = strings.TrimSpace(c.DeferCause)
	c.State = strings.TrimSpace(c.State)
	c.OutcomeEventID = strings.TrimSpace(c.OutcomeEventID)
	c.DeadlineAt = CanonicalTimestamp(c.DeadlineAt)
	c.BudgetWindowStart = CanonicalTimestamp(c.BudgetWindowStart)
	c.BudgetWindowEnd = CanonicalTimestamp(c.BudgetWindowEnd)
	c.DeferredUntil = CanonicalTimestamp(c.DeferredUntil)
	c.CreatedAt = CanonicalTimestamp(c.CreatedAt)
	c.UpdatedAt = CanonicalTimestamp(c.UpdatedAt)
	return c
}

func (c HumanTaskContinuation) Validate(card Card) error {
	if card.Anchor.Kind() != AnchorKindHumanTask {
		return fmt.Errorf("human-task continuation requires a human_task card")
	}
	if strings.TrimSpace(c.CardID) == "" || c.CardID != card.CardID {
		return fmt.Errorf("human-task continuation card_id does not match its card")
	}
	if strings.TrimSpace(c.RunID) == "" || c.RunID != card.RunID {
		return fmt.Errorf("human-task continuation run_id does not match its card")
	}
	if strings.TrimSpace(c.SourceEventID) == "" {
		return fmt.Errorf("human-task continuation source_event_id is required")
	}
	if c.DeadlineAt.IsZero() {
		return fmt.Errorf("human-task continuation deadline_at is required")
	}
	if c.CreatedAt.IsZero() || c.UpdatedAt.IsZero() || c.DeadlineAt.Before(c.CreatedAt) {
		return fmt.Errorf("human-task continuation timestamps are invalid")
	}
	if strings.TrimSpace(c.BudgetBundleHash) == "" {
		return fmt.Errorf("human-task continuation budget_bundle_hash is required")
	}
	if c.BudgetLimit < 0 || c.RequeueCount < 0 {
		return fmt.Errorf("human-task continuation policy counts cannot be negative")
	}
	if c.BudgetWindowStart.IsZero() || !c.BudgetWindowEnd.After(c.BudgetWindowStart) {
		return fmt.Errorf("human-task continuation budget window is invalid")
	}
	switch c.State {
	case HumanTaskContinuationPending, HumanTaskContinuationDecisionCommitted,
		HumanTaskContinuationOutcomeDispatched, HumanTaskContinuationExpired,
		HumanTaskContinuationSuperseded:
	default:
		return fmt.Errorf("human-task continuation state %q is invalid", c.State)
	}
	return nil
}

type HumanTaskCreationStore interface {
	CreateHumanTaskCard(context.Context, Card, HumanTaskContinuation) error
}

type HumanTaskStore interface {
	HumanTaskCreationStore
	LoadHumanTaskContinuation(context.Context, string) (HumanTaskContinuation, error)
	CompleteHumanTaskOutcome(context.Context, string, string, time.Time) (HumanTaskContinuation, error)
}

type HumanTaskExpiryStore interface {
	ExpireHumanTaskCards(context.Context, time.Time, int) (int, error)
}
