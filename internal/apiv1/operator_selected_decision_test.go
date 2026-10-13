package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

type selectedDecisionAPIFixture struct {
	decisioncard.Store
	card      decisioncard.Card
	request   apiidempotency.Request
	calls     int
	refusal   error
	wrongPath int
}

func (f *selectedDecisionAPIFixture) GetDecisionCard(context.Context, string) (decisioncard.Card, error) {
	return f.card, nil
}

func (f *selectedDecisionAPIFixture) RequireNormalControl(context.Context, string, runfork.SelectedControl) error {
	f.wrongPath++
	return errors.New("selected card must not use ordinary control admission")
}

func (f *selectedDecisionAPIFixture) AdmitSourceArtifactFact(context.Context) (context.Context, error) {
	f.wrongPath++
	return nil, errors.New("selected card must not select an ordinary source runtime")
}

func (f *selectedDecisionAPIFixture) CommitDecisionCardMutation(context.Context, apiidempotency.Request, pipeline.DecisionCardMutation) (json.RawMessage, bool, error) {
	f.wrongPath++
	return nil, false, errors.New("selected card must not use the ordinary coordinator")
}

func (f *selectedDecisionAPIFixture) CommitSelectedDecisionCardMutation(_ context.Context, runID string, request apiidempotency.Request, mutation pipeline.DecisionCardMutation) (json.RawMessage, bool, bool, error) {
	f.calls++
	f.request = request
	if runID != f.card.RunID {
		return nil, false, true, errors.New("wrong child identity")
	}
	if err := mutation.ValidateRequest(request); err != nil {
		return nil, false, true, err
	}
	if f.refusal != nil {
		return nil, false, true, f.refusal
	}
	response, err := json.Marshal(map[string]any{"card_id": f.card.CardID})
	return response, true, true, err
}

func TestSelectedForkDecisionAPIRoutesEveryMutationToOwnExecution(t *testing.T) {
	at := time.Now().UTC()
	principal := uuid.NewString()
	for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"} {
		t.Run(method, func(t *testing.T) {
			fixture := &selectedDecisionAPIFixture{card: decisioncard.Card{CardID: uuid.NewString(), RunID: uuid.NewString()}}
			handlers := OperatorDecisionCardHandlers(DecisionCardHandlerOptions{Cards: fixture, Authority: fixture,
				SourceArtifact: fixture, SelectedForkControls: fixture, SelectedForkDecisions: fixture, Now: func() time.Time { return at }})
			req := Request{Method: method, OperatorPrincipalID: principal, RequestHash: "selected-card-input",
				SemanticParams: semanticvalue.EmptyObject(),
				Params: map[string]any{"card_id": fixture.card.CardID, "verdict": "reject", "observed_content_hash": "sha256:observed",
					"input_draft_id": uuid.NewString(), "until": at.Add(time.Hour).Format(time.RFC3339Nano), "idempotency_key": "own-child"}}
			result, err := handlers[method](context.Background(), req)
			if err != nil || fixture.calls != 1 || fixture.wrongPath != 0 {
				t.Fatalf("selected decision did not use its exact owner: result=%+v error=%v calls=%d ordinary=%d", result, err, fixture.calls, fixture.wrongPath)
			}
			if fixture.request.Actor != apiidempotency.PrincipalActor(principal) || fixture.request.ResourceID != fixture.card.CardID ||
				fixture.request.Method != method || fixture.request.IdempotencyKey != "own-child" || fixture.request.RequestHash != req.RequestHash {
				t.Fatalf("operator or request identity changed: %+v", fixture.request)
			}
			if projected := asMap(t, result.(semanticvalue.Value).Interface()); projected["card_id"] != fixture.card.CardID || projected["idempotency_replayed"] != true {
				t.Fatalf("acknowledged selected response changed: %+v", result)
			}
			fixture.refusal = errors.New("selected generation is no longer current")
			if _, err := handlers[method](context.Background(), req); !errors.Is(err, fixture.refusal) || fixture.wrongPath != 0 {
				t.Fatalf("selected refusal fell through to ordinary execution: %v", err)
			}
		})
	}
}
