package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/decisioncardtest"
	"github.com/google/uuid"
)

type acknowledgementProbeBus struct {
	*recordingPipelineBus
	released  []string
	finalized []string
}

func (b *acknowledgementProbeBus) ReleaseEnginePublications(ctx context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	for _, plan := range plans {
		b.released = append(b.released, plan.DurablePublicationEventID())
	}
	return b.recordingPipelineBus.ReleaseEnginePublications(ctx, plans)
}

func (b *acknowledgementProbeBus) FinalizeEnginePublications(ctx context.Context, publications []runtimeengine.CommittedDurablePublication) error {
	for _, publication := range publications {
		b.finalized = append(b.finalized, publication.CommittedDurablePublicationEventID())
	}
	return b.recordingPipelineBus.FinalizeEnginePublications(ctx, publications)
}

func acknowledgementTestEvent(runID string) events.Event {
	return eventtest.RuntimeControl(uuid.NewString(), "mailbox.card_expired", "platform", "", []byte(`{"card_id":"card-a"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
}

func TestDecisionCardAcknowledgedErrorStillDispatchesOnce(t *testing.T) {
	ctx := context.Background()
	runID := uuid.NewString()
	source := eventtest.ConcreteTemplateRoutingSource("provider", "provider/instance-a", "11111111-1111-1111-1111-111111111111")
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "requester-agent", OperationID: decisioncardtest.HumanOperation(t, runID, "provider-turn/tool-call-1"),
		Category: "review", Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("human_task", "Review provider result", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve"},
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Snapshot: snapshot,
		ExecutionMode: "live", BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chosen := acknowledgementTestEvent(runID)
	unused := acknowledgementTestEvent(runID)
	chosenIntent := runtimeengine.EmitIntent{Event: chosen}
	plans := []runtimeengine.DurablePublicationPlan{
		pipelineTestPublicationPlan{intent: chosenIntent},
		pipelineTestPublicationPlan{intent: runtimeengine.EmitIntent{Event: unused}},
	}
	completion := apiidempotency.Completion{Response: json.RawMessage(`{"ok":true}`)}
	committed := CommittedDecisionCardMutation{
		Acknowledged: true, Completion: completion, Kind: DecisionCardMutationDefer,
		Outcome:     decisioncard.DecisionOutcome{Card: card},
		Publication: pipelineTestCommittedPublication{eventID: chosen.ID(), intent: chosenIntent}, HasPublication: true,
	}
	commitErr := errors.New("post-commit confirmation failed")
	for _, tc := range []struct {
		name         string
		acknowledged bool
		invalidCard  bool
		wantRelease  []string
		wantEffects  int
	}{
		{name: "acknowledged", acknowledged: true, wantRelease: []string{unused.ID()}, wantEffects: 1},
		{name: "acknowledged invalid card", acknowledged: true, invalidCard: true, wantRelease: []string{unused.ID()}, wantEffects: 1},
		{name: "unacknowledged", wantRelease: []string{chosen.ID(), unused.ID()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := &acknowledgementProbeBus{recordingPipelineBus: &recordingPipelineBus{}}
			pc := &PipelineCoordinator{bus: bus}
			result := committed
			result.Acknowledged = tc.acknowledged
			if tc.invalidCard {
				result.Outcome.Card = decisioncard.Card{}
			}
			response, err := pc.finishDecisionCardMutation(ctx, result, commitErr, plans)
			if !errors.Is(err, commitErr) {
				t.Fatalf("error = %v, want original commit error", err)
			}
			if tc.invalidCard && err == commitErr {
				t.Fatal("post-commit validation error was lost")
			}
			if tc.acknowledged && string(response) != string(completion.Response) {
				t.Fatalf("completion = %s, want %s", response, completion.Response)
			}
			if !tc.acknowledged && len(response) != 0 {
				t.Fatalf("unacknowledged completion escaped: %s", response)
			}
			if len(bus.released) != len(tc.wantRelease) {
				t.Fatalf("released = %v, want %v", bus.released, tc.wantRelease)
			}
			for index, want := range tc.wantRelease {
				if bus.released[index] != want {
					t.Fatalf("released = %v, want %v", bus.released, tc.wantRelease)
				}
			}
			if len(bus.finalized) != tc.wantEffects || len(bus.publishes) != tc.wantEffects || len(bus.outboxIntents) != tc.wantEffects {
				t.Fatalf("effects: finalized=%v published=%d outbox=%d, want %d each", bus.finalized, len(bus.publishes), len(bus.outboxIntents), tc.wantEffects)
			}
		})
	}
}

func TestHumanTaskExpiryAcknowledgementControlsPublicationEffects(t *testing.T) {
	for _, tc := range []struct {
		name         string
		acknowledged bool
		wantRelease  int
		wantEffects  int
	}{
		{name: "acknowledged error", acknowledged: true, wantEffects: 1},
		{name: "unacknowledged error", wantRelease: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newSQLiteWorkflowInstanceStoreTestDB(t)
			workflowStore := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, &recordingRuntimeMutationRunner{db: db})
			bus := &acknowledgementProbeBus{recordingPipelineBus: &recordingPipelineBus{}}
			commitErr := errors.New("commit result reported an error")
			expiry := &transactionProbeHumanTaskExpiry{
				event: acknowledgementTestEvent(uuid.NewString()), acknowledged: tc.acknowledged, commitErr: commitErr,
			}
			pc := &PipelineCoordinator{bus: bus, workflowStore: workflowStore}
			err := pc.expireHumanTaskCards(context.Background(), expiry, time.Now().UTC(), 10)
			if !errors.Is(err, commitErr) {
				t.Fatalf("error = %v, want original commit error", err)
			}
			if expiry.commitCalls != 1 || len(bus.released) != tc.wantRelease || len(bus.finalized) != tc.wantEffects || len(bus.publishes) != tc.wantEffects || len(bus.outboxIntents) != tc.wantEffects {
				t.Fatalf("commit=%d released=%v finalized=%v published=%d outbox=%d", expiry.commitCalls, bus.released, bus.finalized, len(bus.publishes), len(bus.outboxIntents))
			}
		})
	}
}
