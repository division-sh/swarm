package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

type transactionProbeHumanTaskExpiry struct {
	event       events.Event
	commitCalls int
}

func (e *transactionProbeHumanTaskExpiry) ListDueHumanTaskExpiryEvents(context.Context, time.Time, int) ([]events.Event, error) {
	return []events.Event{e.event}, nil
}

func (e *transactionProbeHumanTaskExpiry) CommitHumanTaskExpirations(ctx context.Context, command HumanTaskExpiryCommand) (CommittedHumanTaskExpiry, error) {
	if _, ok := PipelineSQLTxFromContext(ctx); ok {
		return CommittedHumanTaskExpiry{}, errors.New("runtime received selected-store transaction authority")
	}
	if err := command.Validate(); err != nil {
		return CommittedHumanTaskExpiry{}, err
	}
	e.commitCalls++
	committed := make([]runtimeengine.CommittedDurablePublication, 0, len(command.Publications))
	for index, publication := range command.Publications {
		plan, ok := publication.(pipelineTestPublicationPlan)
		if !ok {
			return CommittedHumanTaskExpiry{}, errors.New("unexpected publication plan")
		}
		if plan.DurablePublicationEventID() != e.event.ID() || index != 0 {
			return CommittedHumanTaskExpiry{}, errors.New("selected-store operation received the wrong expiry plan")
		}
		committed = append(committed, pipelineTestCommittedPublication{eventID: plan.DurablePublicationEventID(), intent: plan.intent})
	}
	return CommittedHumanTaskExpiry{Publications: committed}, nil
}

func TestHumanTaskExpiryUsesClosedSelectedStoreCommitEvidence(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	workflowStore := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	expiry := &transactionProbeHumanTaskExpiry{
		event: eventtest.RuntimeControl(
			uuid.NewString(), events.EventType("mailbox.card_expired"), "platform", "", []byte(`{"card_id":"card-a"}`),
			0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
		),
	}
	bus := &recordingPipelineBus{publishErr: errors.New("injected event persistence failure")}
	coordinator := &PipelineCoordinator{bus: bus, workflowStore: workflowStore, gatePublisher: bus}

	if err := coordinator.expireHumanTaskCards(context.Background(), expiry, time.Now().UTC(), 10); err == nil {
		t.Fatal("expiry succeeded when publication planning failed")
	}
	if expiry.commitCalls != 0 {
		t.Fatalf("selected-store commits after planning failure = %d, want 0", expiry.commitCalls)
	}

	bus.publishErr = nil
	if err := coordinator.expireHumanTaskCards(context.Background(), expiry, time.Now().UTC(), 10); err != nil {
		t.Fatalf("expiry with selected-store commit evidence: %v", err)
	}
	if expiry.commitCalls != 1 {
		t.Fatalf("selected-store commits = %d, want 1", expiry.commitCalls)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].ID() != expiry.event.ID() {
		t.Fatalf("post-commit expiry publishes = %#v", bus.publishes)
	}
}
