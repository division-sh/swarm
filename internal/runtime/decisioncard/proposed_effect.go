package decisioncard

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

const (
	ProposedEffectPending           = "pending"
	ProposedEffectDecisionCommitted = "decision_committed"
	ProposedEffectRequestReleased   = "request_released"
	ProposedEffectOutcomeDispatched = "outcome_dispatched"
	ProposedEffectSuperseded        = "superseded"
)

type ProposedEffectContinuation struct {
	CardID            string
	RunID             string
	RequestEventID    string
	ActivityID        string
	Tool              string
	BundleHash        string
	WorkflowVersion   string
	Input             semanticvalue.Value
	EffectContentHash string
	EffectClass       runtimecontracts.ActivityEffectClass
	SuccessEvent      string
	FailureEvent      string
	RevisionEvent     string
	RejectedEvent     string
	RetryMaxAttempts  int
	RetryBackoff      string
	ForkPolicy        runtimecontracts.ActivityForkPolicy
	EntityID          string
	NodeID            string
	FlowID            string
	FlowInstance      string
	HandlerEventKey   string
	SourceEventID     string
	SourceRunID       string
	SourceTaskID      string
	ParentEventID     string
	ChainDepth        int
	Attempt           int
	Generation        attemptgeneration.Generation
	LoopStage         string
	ReplyContextID    string
	State             string
	Verdict           string
	DecisionEventID   string
	RouteEventID      string
	SupersededReason  string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func ProposedEffectCardID(requestEventID, decision string) string {
	identity := strings.TrimSpace(requestEventID) + "\x00" + strings.TrimSpace(decision)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.proposed-effect.card.v1\x00"+identity)).String()
}

func (c ProposedEffectContinuation) Canonical() ProposedEffectContinuation {
	c.CardID = strings.TrimSpace(c.CardID)
	c.RunID = strings.TrimSpace(c.RunID)
	c.RequestEventID = strings.TrimSpace(c.RequestEventID)
	c.ActivityID = strings.TrimSpace(c.ActivityID)
	c.Tool = strings.TrimSpace(c.Tool)
	c.BundleHash = strings.TrimSpace(c.BundleHash)
	c.WorkflowVersion = strings.TrimSpace(c.WorkflowVersion)
	c.EffectContentHash = strings.TrimSpace(c.EffectContentHash)
	c.SuccessEvent = strings.TrimSpace(c.SuccessEvent)
	c.FailureEvent = strings.TrimSpace(c.FailureEvent)
	c.RevisionEvent = strings.TrimSpace(c.RevisionEvent)
	c.RejectedEvent = strings.TrimSpace(c.RejectedEvent)
	c.RetryBackoff = strings.TrimSpace(c.RetryBackoff)
	c.EntityID = strings.TrimSpace(c.EntityID)
	c.NodeID = strings.TrimSpace(c.NodeID)
	c.FlowID = strings.TrimSpace(c.FlowID)
	c.FlowInstance = strings.Trim(strings.TrimSpace(c.FlowInstance), "/")
	c.HandlerEventKey = strings.TrimSpace(c.HandlerEventKey)
	c.SourceEventID = strings.TrimSpace(c.SourceEventID)
	c.SourceRunID = strings.TrimSpace(c.SourceRunID)
	c.SourceTaskID = strings.TrimSpace(c.SourceTaskID)
	c.ParentEventID = strings.TrimSpace(c.ParentEventID)
	c.Generation = c.Generation.Normalize()
	c.LoopStage = strings.TrimSpace(c.LoopStage)
	c.ReplyContextID = strings.TrimSpace(c.ReplyContextID)
	c.State = strings.TrimSpace(c.State)
	c.Verdict = strings.TrimSpace(c.Verdict)
	c.DecisionEventID = strings.TrimSpace(c.DecisionEventID)
	c.RouteEventID = strings.TrimSpace(c.RouteEventID)
	c.SupersededReason = strings.TrimSpace(c.SupersededReason)
	c.CreatedAt = CanonicalTimestamp(c.CreatedAt)
	c.UpdatedAt = CanonicalTimestamp(c.UpdatedAt)
	if c.Attempt <= 0 {
		c.Attempt = 1
	}
	if c.Input.Kind() == semanticvalue.KindNull {
		c.Input = semanticvalue.EmptyObject()
	}
	return c
}

func (c ProposedEffectContinuation) EffectValue() (semanticvalue.Value, error) {
	c = c.Canonical()
	value, err := canonicaljson.FromGo(map[string]any{
		"request_event_id":   c.RequestEventID,
		"activity_id":        c.ActivityID,
		"tool":               c.Tool,
		"bundle_hash":        c.BundleHash,
		"workflow_version":   c.WorkflowVersion,
		"effect_class":       string(c.EffectClass),
		"success_event":      c.SuccessEvent,
		"failure_event":      c.FailureEvent,
		"revision_event":     c.RevisionEvent,
		"rejected_event":     c.RejectedEvent,
		"retry_max_attempts": c.RetryMaxAttempts,
		"retry_backoff":      c.RetryBackoff,
		"fork_policy":        string(c.ForkPolicy),
		"entity_id":          c.EntityID,
		"node_id":            c.NodeID,
		"flow_id":            c.FlowID,
		"flow_instance":      c.FlowInstance,
		"handler_event_key":  c.HandlerEventKey,
		"source_event_id":    c.SourceEventID,
		"source_run_id":      c.SourceRunID,
		"source_task_id":     c.SourceTaskID,
		"parent_event_id":    c.ParentEventID,
		"chain_depth":        c.ChainDepth,
		"attempt":            c.Attempt,
		"loop_generation":    c.Generation,
		"loop_stage":         c.LoopStage,
		"reply_context_id":   c.ReplyContextID,
	})
	if err != nil {
		return semanticvalue.Value{}, err
	}
	return value.With("input", c.Input)
}

func (c ProposedEffectContinuation) Validate(card Card) error {
	c = c.Canonical()
	if card.Anchor.Kind() != AnchorKindProposedEffect {
		return fmt.Errorf("proposed-effect continuation requires a proposed_effect card")
	}
	anchor, err := card.Anchor.ProposedEffect()
	if err != nil {
		return err
	}
	if c.CardID == "" || c.CardID != card.CardID || c.RunID == "" || c.RunID != card.RunID {
		return fmt.Errorf("proposed-effect continuation card/run identity does not match its card")
	}
	if c.RequestEventID != anchor.RequestEventID || c.ActivityID != anchor.ActivityID {
		return fmt.Errorf("proposed-effect continuation identity does not match its anchor")
	}
	if c.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return fmt.Errorf("proposed-effect continuation requires effect_class non_idempotent_write")
	}
	if c.Input.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("proposed-effect input must be a semantic object")
	}
	for name, value := range map[string]string{
		"request_event_id": c.RequestEventID, "activity_id": c.ActivityID, "tool": c.Tool,
		"bundle_hash": c.BundleHash, "workflow_version": c.WorkflowVersion,
		"success_event": c.SuccessEvent, "failure_event": c.FailureEvent, "revision_event": c.RevisionEvent,
		"rejected_event": c.RejectedEvent, "source_event_id": c.SourceEventID, "source_run_id": c.SourceRunID,
	} {
		if value == "" {
			return fmt.Errorf("proposed-effect continuation %s is required", name)
		}
	}
	if c.BundleHash != card.BundleHash || c.WorkflowVersion != card.WorkflowVersion {
		return fmt.Errorf("proposed-effect continuation contract identity does not match its card")
	}
	effectValue, err := c.EffectValue()
	if err != nil {
		return fmt.Errorf("encode proposed effect: %w", err)
	}
	effectHash, err := canonicaljson.HashValue(effectValue)
	if err != nil {
		return fmt.Errorf("hash proposed effect: %w", err)
	}
	if c.EffectContentHash != effectHash || card.EffectContentHash != effectHash {
		return fmt.Errorf("proposed-effect content hash does not match its immutable effect")
	}
	switch c.State {
	case ProposedEffectPending:
		if c.Verdict != "" || c.DecisionEventID != "" || c.RouteEventID != "" || c.SupersededReason != "" {
			return fmt.Errorf("pending proposed effect carries terminal evidence")
		}
	case ProposedEffectDecisionCommitted:
		if c.Verdict == "" || c.DecisionEventID == "" || c.RouteEventID != "" {
			return fmt.Errorf("decision-committed proposed effect has invalid evidence")
		}
	case ProposedEffectRequestReleased, ProposedEffectOutcomeDispatched:
		if c.Verdict == "" || c.DecisionEventID == "" || c.RouteEventID == "" {
			return fmt.Errorf("routed proposed effect has invalid evidence")
		}
	case ProposedEffectSuperseded:
		if c.SupersededReason == "" {
			return fmt.Errorf("superseded proposed effect requires a reason")
		}
	default:
		return fmt.Errorf("proposed-effect continuation state %q is invalid", c.State)
	}
	if c.CreatedAt.IsZero() || c.UpdatedAt.IsZero() {
		return fmt.Errorf("proposed-effect continuation timestamps are required")
	}
	return nil
}

type ProposedEffectStore interface {
	CreateProposedEffectCard(context.Context, Card, ProposedEffectContinuation) error
	LoadProposedEffectContinuation(context.Context, string) (ProposedEffectContinuation, error)
	CompleteProposedEffectRoute(context.Context, string, string, time.Time) (ProposedEffectContinuation, error)
	SupersedeProposedEffectsForLoopGenerations(context.Context, string, string, []attemptgeneration.Generation, string, time.Time) error
	ProposedEffectReadback(context.Context, string) (ProposedEffectReadback, error)
}

type ProposedEffectReadback struct {
	ContinuationState string `json:"continuation_state"`
	DispatchState     string `json:"dispatch_state"`
	RequestEventID    string `json:"request_event_id"`
	ActivityID        string `json:"activity_id"`
}
