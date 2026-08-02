package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

type DecisionCardMutationIdempotencyRequest struct {
	Method         string
	ActorTokenID   string
	IdempotencyKey string
	RequestHash    string
	ResourceID     string
	TTL            time.Duration
	Now            time.Time
}

type DecisionCardMutationIdempotencyCompletion struct {
	ResourceID string
	Response   json.RawMessage
}

type DecisionCardMutationIdempotency interface {
	WithDecisionCardMutationIdempotency(
		context.Context,
		DecisionCardMutationIdempotencyRequest,
		func(context.Context) (DecisionCardMutationIdempotencyCompletion, error),
	) (DecisionCardMutationIdempotencyCompletion, bool, error)
}

type decisionCardMutationKind uint8

const (
	decisionCardMutationDecide decisionCardMutationKind = iota + 1
	decisionCardMutationDefer
	decisionCardMutationBeginInput
	decisionCardMutationCancelInput
)

type DecisionCardMutation struct {
	kind                decisionCardMutationKind
	decide              decisioncard.DecideRequest
	deferral            decisioncard.DeferRequest
	beginInput          decisioncard.BeginInputRequest
	cancelInput         decisioncard.CancelInputRequest
	observedContentHash string
}

func NewDecisionCardDecision(req decisioncard.DecideRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: decisionCardMutationDecide, decide: req}
}

func NewDecisionCardDeferral(req decisioncard.DeferRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: decisionCardMutationDefer, deferral: req}
}

func NewDecisionCardInputBegin(req decisioncard.BeginInputRequest, observedContentHash string) DecisionCardMutation {
	return DecisionCardMutation{kind: decisionCardMutationBeginInput, beginInput: req, observedContentHash: strings.TrimSpace(observedContentHash)}
}

func NewDecisionCardInputCancellation(req decisioncard.CancelInputRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: decisionCardMutationCancelInput, cancelInput: req}
}

func (m DecisionCardMutation) Decision() (decisioncard.DecideRequest, bool) {
	return m.decide, m.kind == decisionCardMutationDecide
}

func (m DecisionCardMutation) Deferral() (decisioncard.DeferRequest, bool) {
	return m.deferral, m.kind == decisionCardMutationDefer
}

func (m DecisionCardMutation) InputBegin() (decisioncard.BeginInputRequest, string, bool) {
	return m.beginInput, m.observedContentHash, m.kind == decisionCardMutationBeginInput
}

func (m DecisionCardMutation) InputCancellation() (decisioncard.CancelInputRequest, bool) {
	return m.cancelInput, m.kind == decisionCardMutationCancelInput
}

type decisionCardMutationResult struct {
	OK              bool   `json:"ok"`
	CardID          string `json:"card_id"`
	Status          string `json:"status"`
	Verdict         string `json:"verdict,omitempty"`
	DecisionEventID string `json:"decision_event_id,omitempty"`
	ChangeID        int64  `json:"change_id"`
}

type decisionCardInputMutationResult struct {
	OK           bool   `json:"ok"`
	CardID       string `json:"card_id"`
	InputDraftID string `json:"input_draft_id"`
	Verdict      string `json:"verdict"`
	Status       string `json:"status"`
	ExpiresAt    string `json:"expires_at"`
}

func (pc *PipelineCoordinator) CommitDecisionCardMutation(
	ctx context.Context,
	idempotency DecisionCardMutationIdempotency,
	idempotencyRequest DecisionCardMutationIdempotencyRequest,
	mutation DecisionCardMutation,
) (json.RawMessage, bool, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return nil, false, fmt.Errorf("decision-card mutation requires workflow persistence")
	}
	if pc.decisionCards == nil {
		return nil, false, fmt.Errorf("decision-card mutation requires the decision-card owner")
	}
	if idempotency == nil {
		return nil, false, fmt.Errorf("decision-card mutation requires the idempotency owner")
	}
	var completion DecisionCardMutationIdempotencyCompletion
	var replayed bool
	err := pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
		var err error
		completion, replayed, err = idempotency.WithDecisionCardMutationIdempotency(txctx, idempotencyRequest, func(callbackCtx context.Context) (DecisionCardMutationIdempotencyCompletion, error) {
			result, err := pc.applyDecisionCardMutation(callbackCtx, mutation)
			if err != nil {
				return DecisionCardMutationIdempotencyCompletion{}, err
			}
			raw, err := canonicaljson.Bytes(result)
			if err != nil {
				return DecisionCardMutationIdempotencyCompletion{}, err
			}
			return DecisionCardMutationIdempotencyCompletion{ResourceID: idempotencyRequest.ResourceID, Response: raw}, nil
		})
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return append(json.RawMessage(nil), completion.Response...), replayed, nil
}

func (pc *PipelineCoordinator) applyDecisionCardMutation(ctx context.Context, mutation DecisionCardMutation) (any, error) {
	switch mutation.kind {
	case decisionCardMutationDecide:
		return pc.applyDecisionCardDecision(ctx, mutation.decide)
	case decisionCardMutationDefer:
		return pc.applyDecisionCardDeferral(ctx, mutation.deferral)
	case decisionCardMutationBeginInput:
		return pc.applyDecisionCardInputBegin(ctx, mutation.beginInput, mutation.observedContentHash)
	case decisionCardMutationCancelInput:
		return pc.applyDecisionCardInputCancellation(ctx, mutation.cancelInput)
	default:
		return nil, fmt.Errorf("decision-card mutation kind is required")
	}
}

func (pc *PipelineCoordinator) applyDecisionCardDecision(ctx context.Context, req decisioncard.DecideRequest) (any, error) {
	card, err := pc.decisionCards.GetDecisionCard(ctx, req.CardID)
	if err != nil {
		return nil, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, card.RunID)
	if err := pc.workflowStore.CommitDecision(ctx, card, req.DecisionEventID, req.Now.UTC()); err != nil {
		return nil, err
	}
	outcome, err := pc.decisionCards.DecideDecisionCard(ctx, req)
	if err != nil {
		return nil, err
	}
	if pc.gatePublisher == nil {
		return nil, fmt.Errorf("decision-card mutation requires transactional event publication")
	}
	if outcome.ForcedDeferred {
		payload, err := canonicaljson.Bytes(map[string]any{
			"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(),
			"until": outcome.Card.DeferredUntil.UTC().Format(time.RFC3339Nano),
			"cause": "weekly_budget_exhausted",
		})
		if err != nil {
			return nil, err
		}
		evt, err := decisionCardRuntimeControlEvent(uuid.NewString(), string(decisionCardDeferredEventType), card, payload, req.Now)
		if err != nil {
			return nil, err
		}
		if err := pc.gatePublisher.PublishInMutation(ctx, evt); err != nil {
			return nil, fmt.Errorf("publish budget-deferred decision card event: %w", err)
		}
		return decisionCardMutationResult{OK: true, CardID: card.CardID, Status: outcome.Card.Status, ChangeID: outcome.ChangeID}, nil
	}
	payloadFields := map[string]semanticvalue.Value{"fields": req.Fields, "anchor": card.Anchor.SemanticValue()}
	for name, text := range map[string]string{
		"card_id": card.CardID, "anchor_kind": string(card.Anchor.Kind()), "decision_id": card.Snapshot.Decision,
		"verdict": req.Verdict, "card_content_hash": card.CardContentHash,
		"decision_schema_hash": card.DecisionSchemaHash, "bundle_hash": card.BundleHash,
	} {
		payloadFields[name], err = semanticvalue.String(text)
		if err != nil {
			return nil, fmt.Errorf("admit decision lifecycle %s: %w", name, err)
		}
	}
	payloadValue, err := semanticvalue.ObjectFromMap(payloadFields)
	if err != nil {
		return nil, err
	}
	payload, err := canonicaljson.Encode(payloadValue)
	if err != nil {
		return nil, err
	}
	evt, err := decisionCardRuntimeControlEvent(req.DecisionEventID, string(workflowGateDecisionEventType), card, payload, req.Now)
	if err != nil {
		return nil, err
	}
	if err := pc.gatePublisher.PublishInMutation(ctx, evt); err != nil {
		return nil, fmt.Errorf("publish decision card event: %w", err)
	}
	return decisionCardMutationResult{
		OK: true, CardID: card.CardID, Status: outcome.Card.Status, Verdict: req.Verdict,
		DecisionEventID: req.DecisionEventID, ChangeID: outcome.ChangeID,
	}, nil
}

func (pc *PipelineCoordinator) applyDecisionCardDeferral(ctx context.Context, req decisioncard.DeferRequest) (any, error) {
	outcome, err := pc.decisionCards.DeferDecisionCard(ctx, req)
	if err != nil {
		return nil, err
	}
	if pc.gatePublisher == nil {
		return nil, fmt.Errorf("decision-card mutation requires transactional event publication")
	}
	payload, err := canonicaljson.Bytes(map[string]any{"card_id": req.CardID, "until": req.Until.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return nil, err
	}
	evt, err := decisionCardRuntimeControlEvent(uuid.NewString(), string(decisionCardDeferredEventType), outcome.Card, payload, req.Now)
	if err != nil {
		return nil, err
	}
	if err := pc.gatePublisher.PublishInMutation(ctx, evt); err != nil {
		return nil, fmt.Errorf("publish decision card deferred event: %w", err)
	}
	return decisionCardMutationResult{OK: true, CardID: req.CardID, Status: outcome.Card.Status, ChangeID: outcome.ChangeID}, nil
}

func (pc *PipelineCoordinator) applyDecisionCardInputBegin(ctx context.Context, req decisioncard.BeginInputRequest, observedContentHash string) (any, error) {
	card, err := pc.decisionCards.GetDecisionCard(ctx, req.CardID)
	if err != nil {
		return nil, err
	}
	if observedContentHash == "" || observedContentHash != card.CardContentHash {
		return nil, decisioncard.ErrStaleContent
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(card.EffectiveCadence.InputDraftTTL))
	if err != nil || ttl <= 0 {
		return nil, fmt.Errorf("decision card input draft TTL is invalid")
	}
	req.TTL = ttl
	draft, err := pc.decisionCards.BeginDecisionCardInput(ctx, req)
	if err != nil {
		return nil, err
	}
	return decisionCardInputMutationResult{
		OK: true, CardID: req.CardID, InputDraftID: draft.InputDraftID, Verdict: draft.Verdict,
		Status: draft.Status, ExpiresAt: draft.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func (pc *PipelineCoordinator) applyDecisionCardInputCancellation(ctx context.Context, req decisioncard.CancelInputRequest) (any, error) {
	draft, err := pc.decisionCards.CancelDecisionCardInput(ctx, req)
	if err != nil {
		return nil, err
	}
	return decisionCardInputMutationResult{
		OK: true, CardID: req.CardID, InputDraftID: draft.InputDraftID, Verdict: draft.Verdict,
		Status: draft.Status, ExpiresAt: draft.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func decisionCardRuntimeControlEvent(eventID, eventName string, card decisioncard.Card, payload []byte, createdAt time.Time) (events.Event, error) {
	var noEvent events.Event
	scope, err := card.Anchor.Scope()
	if err != nil {
		return noEvent, err
	}
	routingSource, err := card.Anchor.ControlRoutingSource()
	if err != nil {
		return noEvent, err
	}
	return events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		Facts: events.EventFacts{
			ID: eventID, Type: events.EventType(eventName),
			Producer:      events.ProducerClaim{Type: events.EventProducerPlatform, ID: "platform"},
			Payload:       payload,
			Envelope:      events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, scope.EntityID), scope.FlowInstance),
			RoutingSource: routingSource, CreatedAt: createdAt.UTC(), ExecutionMode: card.ExecutionMode,
		},
		RunID: card.RunID,
	})
}
