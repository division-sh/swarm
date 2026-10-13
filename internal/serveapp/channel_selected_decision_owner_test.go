package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

type selectedChannelDecisionFixture struct {
	decisioncard.Store
	channeldelivery.Store
	card      decisioncard.Card
	refusal   error
	acquired  int
	committed int
	request   apiidempotency.Request
}

func (f *selectedChannelDecisionFixture) GetDecisionCard(context.Context, string) (decisioncard.Card, error) {
	return f.card, nil
}

func (f *selectedChannelDecisionFixture) ListCurrentChannelInputDrafts(context.Context, operatorchannel.InboundText, time.Time, string, int) ([]channeldelivery.InputDraftCandidate, string, error) {
	return []channeldelivery.InputDraftCandidate{{CardID: f.card.CardID, DraftID: uuid.NewString()}}, "", nil
}

func (f *selectedChannelDecisionFixture) AcquireSelectedDecisionCardControl(_ context.Context, runID string, operation runfork.SelectedControl) (*runforkexecution.SelectedDecisionCardControl, bool, error) {
	if runID != f.card.RunID || operation != runfork.ControlMailboxDecide {
		return nil, true, errors.New("input changed its owning run or operation")
	}
	f.acquired++
	return nil, true, f.refusal
}

func (f *selectedChannelDecisionFixture) CommitSelectedDecisionCardMutation(_ context.Context, runID string, request apiidempotency.Request, mutation pipeline.DecisionCardMutation) (json.RawMessage, bool, bool, error) {
	if runID != f.card.RunID {
		return nil, false, true, errors.New("channel changed its card's run")
	}
	if err := mutation.ValidateRequest(request); err != nil {
		return nil, false, true, err
	}
	f.committed++
	f.request = request
	if f.refusal != nil {
		return nil, false, true, f.refusal
	}
	return json.RawMessage(`{"ok":true}`), true, true, nil
}

func TestChannelSelectedForkDecisionUsesOwnOwnerWithoutOrdinaryRuntime(t *testing.T) {
	principal, at := uuid.NewString(), time.Now().UTC()
	f := &selectedChannelDecisionFixture{card: decisioncard.Card{CardID: uuid.NewString(), RunID: uuid.NewString()}}
	d := &serveChannelDeliveryDispatcher{selectedDecisions: f}
	for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"} {
		t.Run(method, func(t *testing.T) {
			request := apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal),
				ResourceID: f.card.CardID, RequestHash: "channel-exact", IdempotencyKey: uuid.NewString(), Now: at, TTL: time.Hour}
			var mutation pipeline.DecisionCardMutation
			switch method {
			case "mailbox.decide":
				mutation = pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: f.card.CardID, PrincipalID: principal, Verdict: "reject", Fields: semanticvalue.EmptyObject(), Now: at})
			case "mailbox.defer":
				mutation = pipeline.NewDecisionCardDeferral(decisioncard.DeferRequest{CardID: f.card.CardID, PrincipalID: principal, Until: at.Add(time.Hour), Now: at})
			case "mailbox.begin_input":
				mutation = pipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{CardID: f.card.CardID, PrincipalID: principal, Verdict: "reject", Now: at}, "sha256:observed")
			case "mailbox.cancel_input":
				mutation = pipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{CardID: f.card.CardID, PrincipalID: principal, InputDraftID: uuid.NewString(), Now: at})
			}
			f.refusal = nil
			before := f.committed
			response, replayed, err := d.commitChannelCardMutation(context.Background(), f.card, request, mutation)
			if err != nil || !replayed || string(response) != `{"ok":true}` || f.committed != before+1 || f.request != request {
				t.Fatalf("channel lost the child's owner/operator/acknowledgment: response=%s replay=%t request=%+v error=%v", response, replayed, f.request, err)
			}
			f.refusal = errors.New("current selected generation retired")
			if _, _, err := d.commitChannelCardMutation(context.Background(), f.card, request, mutation); !errors.Is(err, f.refusal) {
				t.Fatalf("selected refusal fell through to ordinary runtime lookup: %v", err)
			}
		})
	}
}

func TestChannelSelectedForkInputAcquiresOwnTurnBeforePreviewOrWrite(t *testing.T) {
	for _, input := range []string{"skip", "chosen", "plain_or_quoted"} {
		t.Run(input, func(t *testing.T) {
			f := &selectedChannelDecisionFixture{card: decisioncard.Card{CardID: uuid.NewString(), RunID: uuid.NewString()}, refusal: errors.New("selected turn is not current")}
			d := &serveChannelDeliveryDispatcher{store: f, cards: f, selectedDecisions: f}
			resolved := channeldelivery.ResolvedAction{Action: channeldelivery.Action{CardID: f.card.CardID}}
			var err error
			switch input {
			case "skip":
				err = d.processInputSkip(context.Background(), channeldelivery.PendingAction{}, resolved)
			case "chosen":
				err = d.processDraftChoice(context.Background(), channeldelivery.PendingAction{}, resolved)
			default:
				err = d.processPendingInputText(context.Background(), channeldelivery.PendingText{})
			}
			if !errors.Is(err, f.refusal) || f.acquired != 1 || f.committed != 0 {
				t.Fatalf("input proceeded without its exact current turn: acquired=%d committed=%d error=%v", f.acquired, f.committed, err)
			}
		})
	}
}
