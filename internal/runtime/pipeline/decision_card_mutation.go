package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
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

type DecisionCardMutationKind uint8

const (
	DecisionCardMutationDecide DecisionCardMutationKind = iota + 1
	DecisionCardMutationDefer
	DecisionCardMutationBeginInput
	DecisionCardMutationCancelInput
)

type DecisionCardMutation struct {
	kind                DecisionCardMutationKind
	decide              decisioncard.DecideRequest
	deferral            decisioncard.DeferRequest
	beginInput          decisioncard.BeginInputRequest
	cancelInput         decisioncard.CancelInputRequest
	observedContentHash string
}

func (m DecisionCardMutation) Kind() DecisionCardMutationKind { return m.kind }

func NewDecisionCardDecision(req decisioncard.DecideRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: DecisionCardMutationDecide, decide: req}
}

func NewDecisionCardDeferral(req decisioncard.DeferRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: DecisionCardMutationDefer, deferral: req}
}

func NewDecisionCardInputBegin(req decisioncard.BeginInputRequest, observedContentHash string) DecisionCardMutation {
	return DecisionCardMutation{kind: DecisionCardMutationBeginInput, beginInput: req, observedContentHash: strings.TrimSpace(observedContentHash)}
}

func NewDecisionCardInputCancellation(req decisioncard.CancelInputRequest) DecisionCardMutation {
	return DecisionCardMutation{kind: DecisionCardMutationCancelInput, cancelInput: req}
}

func (m DecisionCardMutation) Decision() (decisioncard.DecideRequest, bool) {
	return m.decide, m.kind == DecisionCardMutationDecide
}

func (m DecisionCardMutation) Deferral() (decisioncard.DeferRequest, bool) {
	return m.deferral, m.kind == DecisionCardMutationDefer
}

func (m DecisionCardMutation) InputBegin() (decisioncard.BeginInputRequest, string, bool) {
	return m.beginInput, m.observedContentHash, m.kind == DecisionCardMutationBeginInput
}

func (m DecisionCardMutation) InputCancellation() (decisioncard.CancelInputRequest, bool) {
	return m.cancelInput, m.kind == DecisionCardMutationCancelInput
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

// DecisionCardMutationCommand is the complete selected-store mutation. The
// runtime prepares semantic state and publication alternatives; the store
// chooses and commits exactly one alternative under the same private
// transaction as the authoritative card facts.
type DecisionCardMutationCommand struct {
	Mutation                  DecisionCardMutation
	GateState                 *WorkflowEngineStateRecord
	Publication               runtimeengine.DurablePublicationPlan
	ForcedDeferralPublication runtimeengine.DurablePublicationPlan
}

func (c DecisionCardMutationCommand) Validate() error {
	switch c.Mutation.kind {
	case DecisionCardMutationDecide:
		if c.Publication == nil {
			return fmt.Errorf("decision-card decision requires its publication plan")
		}
	case DecisionCardMutationDefer:
		if c.Publication == nil || c.ForcedDeferralPublication != nil || c.GateState != nil {
			return fmt.Errorf("decision-card deferral requires exactly one publication plan")
		}
	case DecisionCardMutationBeginInput, DecisionCardMutationCancelInput:
		if c.Publication != nil || c.ForcedDeferralPublication != nil || c.GateState != nil {
			return fmt.Errorf("decision-card input mutation cannot carry workflow or publication facts")
		}
	default:
		return fmt.Errorf("decision-card mutation kind is required")
	}
	if c.GateState != nil {
		if err := c.GateState.Validate(); err != nil {
			return fmt.Errorf("decision-card gate state: %w", err)
		}
	}
	for name, publication := range map[string]runtimeengine.DurablePublicationPlan{
		"publication":                 c.Publication,
		"forced deferral publication": c.ForcedDeferralPublication,
	} {
		if publication != nil {
			if err := publication.ValidateDurablePublicationPlan(); err != nil {
				return fmt.Errorf("decision-card %s: %w", name, err)
			}
		}
	}
	return nil
}

type CommittedDecisionCardMutation struct {
	Kind           DecisionCardMutationKind
	Outcome        decisioncard.DecisionOutcome
	Draft          decisioncard.InputDraft
	Publication    runtimeengine.CommittedDurablePublication
	HasPublication bool
}

func (r CommittedDecisionCardMutation) Validate() error {
	switch r.Kind {
	case DecisionCardMutationDecide, DecisionCardMutationDefer:
		if err := r.Outcome.Card.Validate(); err != nil {
			return err
		}
		if !r.HasPublication || r.Publication == nil {
			return fmt.Errorf("committed decision-card mutation requires publication evidence")
		}
	case DecisionCardMutationBeginInput, DecisionCardMutationCancelInput:
		if r.HasPublication || r.Publication != nil {
			return fmt.Errorf("committed decision-card input mutation cannot carry publication evidence")
		}
		if strings.TrimSpace(r.Draft.InputDraftID) == "" {
			return fmt.Errorf("committed decision-card input mutation requires draft evidence")
		}
	default:
		return fmt.Errorf("committed decision-card mutation kind is required")
	}
	if r.HasPublication {
		return r.Publication.ValidateCommittedDurablePublication()
	}
	return nil
}

type DecisionCardMutationOwner interface {
	CommitDecisionCardOperation(context.Context, DecisionCardMutationCommand) (CommittedDecisionCardMutation, error)
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
	completion, replayed, err := idempotency.WithDecisionCardMutationIdempotency(ctx, idempotencyRequest, func(callbackCtx context.Context) (DecisionCardMutationIdempotencyCompletion, error) {
		result, err := pc.commitDecisionCardMutation(callbackCtx, mutation)
		if err != nil {
			return DecisionCardMutationIdempotencyCompletion{}, err
		}
		raw, err := canonicaljson.Bytes(result)
		if err != nil {
			return DecisionCardMutationIdempotencyCompletion{}, err
		}
		return DecisionCardMutationIdempotencyCompletion{ResourceID: idempotencyRequest.ResourceID, Response: raw}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return append(json.RawMessage(nil), completion.Response...), replayed, nil
}

func (pc *PipelineCoordinator) commitDecisionCardMutation(ctx context.Context, mutation DecisionCardMutation) (any, error) {
	if pc.workflowStore.cardMutations == nil {
		return nil, fmt.Errorf("decision-card mutation requires the selected-store operation owner")
	}
	command, plans, err := pc.prepareDecisionCardMutation(ctx, mutation)
	if err != nil {
		return nil, err
	}
	planner, _ := pc.bus.(EnginePublicationPlanner)
	release := func(values []runtimeengine.DurablePublicationPlan) error {
		if planner == nil || len(values) == 0 {
			return nil
		}
		return planner.ReleaseEnginePublications(context.WithoutCancel(ctx), values)
	}
	committed, err := pc.workflowStore.cardMutations.CommitDecisionCardOperation(ctx, command)
	if err != nil {
		return nil, errors.Join(err, release(plans))
	}
	if err := committed.Validate(); err != nil {
		return nil, errors.Join(err, release(plans))
	}
	if committed.HasPublication {
		chosenID := committed.Publication.CommittedDurablePublicationEventID()
		unused := make([]runtimeengine.DurablePublicationPlan, 0, len(plans)-1)
		for _, plan := range plans {
			if plan.DurablePublicationEventID() != chosenID {
				unused = append(unused, plan)
			}
		}
		if err := release(unused); err != nil {
			return nil, err
		}
		if planner == nil {
			return nil, fmt.Errorf("decision-card mutation requires the publication planner")
		}
		if err := planner.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{committed.Publication}); err != nil {
			return nil, err
		}
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return nil, fmt.Errorf("decision-card mutation requires the post-commit dispatcher")
		}
		if err := dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), []runtimeengine.EmitIntent{committed.Publication.CommittedDurablePublicationIntent()}); err != nil {
			return nil, err
		}
	}
	switch committed.Kind {
	case DecisionCardMutationDecide:
		outcome := committed.Outcome
		result := decisionCardMutationResult{OK: true, CardID: outcome.Card.CardID, Status: outcome.Card.Status, ChangeID: outcome.ChangeID}
		if !outcome.ForcedDeferred {
			result.Verdict = outcome.Card.Verdict
			result.DecisionEventID = outcome.Card.DecisionEventID
		}
		return result, nil
	case DecisionCardMutationDefer:
		return decisionCardMutationResult{OK: true, CardID: committed.Outcome.Card.CardID, Status: committed.Outcome.Card.Status, ChangeID: committed.Outcome.ChangeID}, nil
	case DecisionCardMutationBeginInput, DecisionCardMutationCancelInput:
		draft := committed.Draft
		return decisionCardInputMutationResult{OK: true, CardID: draft.CardID, InputDraftID: draft.InputDraftID, Verdict: draft.Verdict, Status: draft.Status, ExpiresAt: draft.ExpiresAt.UTC().Format(time.RFC3339Nano)}, nil
	default:
		return nil, fmt.Errorf("committed decision-card mutation kind is required")
	}
}

func (pc *PipelineCoordinator) prepareDecisionCardMutation(
	ctx context.Context,
	mutation DecisionCardMutation,
) (DecisionCardMutationCommand, []runtimeengine.DurablePublicationPlan, error) {
	cardID := ""
	switch mutation.kind {
	case DecisionCardMutationDecide:
		cardID = mutation.decide.CardID
	case DecisionCardMutationDefer:
		cardID = mutation.deferral.CardID
	case DecisionCardMutationBeginInput:
		cardID = mutation.beginInput.CardID
	case DecisionCardMutationCancelInput:
		cardID = mutation.cancelInput.CardID
	default:
		return DecisionCardMutationCommand{}, nil, fmt.Errorf("decision-card mutation kind is required")
	}
	card, err := pc.decisionCards.GetDecisionCard(ctx, cardID)
	if err != nil {
		return DecisionCardMutationCommand{}, nil, err
	}
	if mutation.kind == DecisionCardMutationDecide || mutation.kind == DecisionCardMutationDefer {
		if err := pc.executionPosture.Admit(card.ExecutionMode, "decision-card decision or deferral mutation"); err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
	}
	ctx = runtimecorrelation.WithRunID(ctx, card.RunID)
	command := DecisionCardMutationCommand{Mutation: mutation}
	intents := make([]runtimeengine.EmitIntent, 0, 2)
	switch mutation.kind {
	case DecisionCardMutationDecide:
		req := mutation.decide
		state, err := pc.prepareDecisionCardGateCommit(ctx, card, req.DecisionEventID, req.Now)
		if err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
		command.GateState = state
		event, err := decisionCardDecidedEvent(card, req)
		if err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
		intents = append(intents, runtimeengine.EmitIntent{Event: event})
		if card.Anchor.Kind() == decisioncard.AnchorKindHumanTask && strings.TrimSpace(req.Verdict) == "approve" {
			if pc.humanTasks == nil {
				return DecisionCardMutationCommand{}, nil, fmt.Errorf("human-task decision requires its continuation owner")
			}
			continuation, err := pc.humanTasks.LoadHumanTaskContinuation(ctx, card.CardID)
			if err != nil {
				return DecisionCardMutationCommand{}, nil, err
			}
			forced, err := decisionCardForcedDeferralEvent(card, continuation.BudgetWindowEnd, req.Now)
			if err != nil {
				return DecisionCardMutationCommand{}, nil, err
			}
			intents = append(intents, runtimeengine.EmitIntent{Event: forced})
		}
	case DecisionCardMutationDefer:
		req := mutation.deferral
		payload, err := canonicaljson.Bytes(map[string]any{"card_id": req.CardID, "until": req.Until.UTC().Format(time.RFC3339Nano)})
		if err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
		event, err := decisionCardRuntimeControlEvent(uuid.NewString(), string(decisionCardDeferredEventType), card, payload, req.Now)
		if err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
		intents = append(intents, runtimeengine.EmitIntent{Event: event})
	case DecisionCardMutationBeginInput:
		if mutation.observedContentHash == "" || mutation.observedContentHash != card.CardContentHash {
			return DecisionCardMutationCommand{}, nil, decisioncard.ErrStaleContent
		}
		ttl, err := time.ParseDuration(strings.TrimSpace(card.EffectiveCadence.InputDraftTTL))
		if err != nil || ttl <= 0 {
			return DecisionCardMutationCommand{}, nil, fmt.Errorf("decision card input draft TTL is invalid")
		}
		req := mutation.beginInput
		req.TTL = ttl
		command.Mutation = NewDecisionCardInputBegin(req, mutation.observedContentHash)
	}
	if len(intents) == 0 {
		if err := command.Validate(); err != nil {
			return DecisionCardMutationCommand{}, nil, err
		}
		return command, nil, nil
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return DecisionCardMutationCommand{}, nil, fmt.Errorf("decision-card mutation requires the publication planner")
	}
	plans, err := planner.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return DecisionCardMutationCommand{}, nil, err
	}
	if len(plans) != len(intents) {
		releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return DecisionCardMutationCommand{}, nil, errors.Join(fmt.Errorf("decision-card publication planner returned %d plans for %d events", len(plans), len(intents)), releaseErr)
	}
	command.Publication = plans[0]
	if len(plans) == 2 {
		command.ForcedDeferralPublication = plans[1]
	}
	if err := command.Validate(); err != nil {
		return DecisionCardMutationCommand{}, nil, errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	return command, plans, nil
}

func (pc *PipelineCoordinator) prepareDecisionCardGateCommit(
	ctx context.Context,
	card decisioncard.Card,
	eventID string,
	now time.Time,
) (*WorkflowEngineStateRecord, error) {
	if card.Anchor.Kind() != decisioncard.AnchorKindStageGate {
		return nil, nil
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return nil, err
	}
	instance, found, err := pc.workflowStore.Load(ctx, anchor.Route)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("decision card workflow instance is missing")
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return nil, err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return nil, err
	}
	if !found || activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID || activation.Stage != anchor.Stage || instance.CurrentState != anchor.Stage {
		return nil, fmt.Errorf("decision card is superseded by the current stage activation")
	}
	if err := activation.CommitDecision(eventID, now.UTC()); err != nil {
		return nil, err
	}
	if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
		return nil, err
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	record, err := workflowEngineStateRecord(card.RunID, anchor.Route, instance, instance.CurrentState, instance.Revision, false, now.UTC())
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func decisionCardDecidedEvent(card decisioncard.Card, req decisioncard.DecideRequest) (events.Event, error) {
	var noEvent events.Event
	payloadFields := map[string]semanticvalue.Value{"fields": req.Fields, "anchor": card.Anchor.SemanticValue()}
	for name, text := range map[string]string{
		"card_id": card.CardID, "anchor_kind": string(card.Anchor.Kind()), "decision_id": card.Snapshot.Decision,
		"verdict": req.Verdict, "card_content_hash": card.CardContentHash,
		"decision_schema_hash": card.DecisionSchemaHash, "bundle_hash": card.BundleHash,
	} {
		value, err := semanticvalue.String(text)
		if err != nil {
			return noEvent, fmt.Errorf("admit decision lifecycle %s: %w", name, err)
		}
		payloadFields[name] = value
	}
	payloadValue, err := semanticvalue.ObjectFromMap(payloadFields)
	if err != nil {
		return noEvent, err
	}
	payload, err := canonicaljson.Encode(payloadValue)
	if err != nil {
		return noEvent, err
	}
	return decisionCardRuntimeControlEvent(req.DecisionEventID, string(workflowGateDecisionEventType), card, payload, req.Now)
}

func decisionCardForcedDeferralEvent(card decisioncard.Card, until, now time.Time) (events.Event, error) {
	var noEvent events.Event
	if until.IsZero() {
		return noEvent, fmt.Errorf("forced decision-card deferral requires its exact budget window end")
	}
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(),
		"until": until.UTC().Format(time.RFC3339Nano), "cause": "weekly_budget_exhausted",
	})
	if err != nil {
		return noEvent, err
	}
	return decisionCardRuntimeControlEvent(uuid.NewString(), string(decisionCardDeferredEventType), card, payload, now)
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
