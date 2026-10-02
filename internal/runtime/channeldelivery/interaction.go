package channeldelivery

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

type PreparedCardAction struct {
	Request  apiidempotency.Request
	Mutation pipeline.DecisionCardMutation
}

// PrepareCardAction consumes a verified, exact-current callback and the
// canonical card. The card's outcome schema, not provider data, chooses the
// mutation kind and field requirement.
func PrepareCardAction(pending PendingAction, resolved ResolvedAction, card decisioncard.Card) (PreparedCardAction, error) {
	if err := pending.Fact.Validate(); err != nil {
		return PreparedCardAction{}, err
	}
	if pending.PublicationID != pending.Fact.PublicationID || pending.ReceivedAt.IsZero() ||
		!resolved.CurrentRender || resolved.SourceKind != "card" || resolved.SourceID != card.CardID ||
		resolved.PrincipalID == "" || resolved.Action.Token != pending.Fact.Token ||
		(resolved.Action.Kind != "verdict" && resolved.Action.Kind != "cancel_input") ||
		resolved.ReceiptOperationID == "" || resolved.RenderHash == "" {
		return PreparedCardAction{}, fmt.Errorf("channel callback is not exact current card authority")
	}
	if err := card.Validate(); err != nil {
		return PreparedCardAction{}, err
	}
	if card.Status != decisioncard.StatusPending {
		return PreparedCardAction{}, fmt.Errorf("channel card is no longer pending")
	}
	now := pending.ReceivedAt.UTC()
	method := "mailbox.decide"
	var mutation pipeline.DecisionCardMutation
	if resolved.Action.Kind == "cancel_input" {
		if uuid.Validate(resolved.Action.DraftID) != nil {
			return PreparedCardAction{}, fmt.Errorf("channel cancel action has no exact draft")
		}
		method = "mailbox.cancel_input"
		mutation = pipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{
			CardID: card.CardID, InputDraftID: resolved.Action.DraftID,
			PrincipalID: resolved.PrincipalID, Now: now,
		})
	} else {
		outcome, exists := card.Snapshot.Outcomes[resolved.Action.Verdict]
		if !exists {
			return PreparedCardAction{}, fmt.Errorf("channel verdict is absent from canonical card")
		}
		if len(outcome.Input) > 0 {
			method = "mailbox.begin_input"
			mutation = pipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{
				CardID: card.CardID, Verdict: resolved.Action.Verdict, PrincipalID: resolved.PrincipalID,
				DeliveryReceiptID: resolved.ReceiptOperationID, Now: now,
			}, card.CardContentHash)
		} else {
			publicationID, _ := uuid.Parse(pending.PublicationID)
			mutation = pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: resolved.Action.Verdict, Fields: semanticvalue.EmptyObject(),
				PrincipalID: resolved.PrincipalID, ObservedContentHash: card.CardContentHash,
				DeliveryReceiptID: resolved.ReceiptOperationID, DeliveryRenderHash: resolved.RenderHash,
				DecisionEventID: uuid.NewSHA1(publicationID, []byte("card-decision")).String(), Now: now,
			})
		}
	}
	var err error
	mutation, err = mutation.WithChannelAction(pending.Fact)
	if err != nil {
		return PreparedCardAction{}, err
	}
	request := apiidempotency.Request{
		Method: method, Actor: apiidempotency.PrincipalActor(resolved.PrincipalID),
		IdempotencyKey: pending.PublicationID, ResourceID: card.CardID,
		RequestHash: operatorchannel.Hash("channel-card-action-v1", method, pending.PublicationID,
			pending.Fact.ProviderAuthorization, pending.Fact.Interface.Key(), pending.Fact.Token,
			resolved.RenderHash, card.CardContentHash, resolved.Action.Verdict, resolved.Action.DraftID),
		TTL: 24 * time.Hour, Now: now,
	}
	if err := mutation.ValidateRequest(request); err != nil {
		return PreparedCardAction{}, err
	}
	return PreparedCardAction{Request: request, Mutation: mutation}, nil
}
