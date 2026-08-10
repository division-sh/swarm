package pipeline

import (
	"context"
	"testing"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type postureDecisionCardStore struct {
	unavailablePipelineTestDecisionCards
	card  decisioncard.Card
	reads int
}

func (s *postureDecisionCardStore) GetDecisionCard(context.Context, string) (decisioncard.Card, error) {
	s.reads++
	return s.card, nil
}

func TestMockOnlyPostureRejectsLiveDecisionAndDeferralBeforeMutationPreparation(t *testing.T) {
	for _, mutation := range []DecisionCardMutation{
		NewDecisionCardDecision(decisioncard.DecideRequest{CardID: "card-live"}),
		NewDecisionCardDeferral(decisioncard.DeferRequest{CardID: "card-live"}),
	} {
		store := &postureDecisionCardStore{card: decisioncard.Card{
			CardID: "card-live", ExecutionMode: executionmode.Live,
		}}
		coordinator := &PipelineCoordinator{
			decisionCards:    store,
			executionPosture: executionposture.MockOnly,
		}
		if _, _, err := coordinator.prepareDecisionCardMutation(context.Background(), mutation); err == nil {
			t.Fatalf("mock_only posture admitted live decision-card mutation kind %d", mutation.Kind())
		}
		if store.reads != 1 {
			t.Fatalf("decision-card reads = %d, want one authority read before rejection", store.reads)
		}
	}
}
