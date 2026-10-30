package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type transactionProbeHumanTaskExpiry struct {
	event        events.Event
	commitCalls  int
	acknowledged bool
	commitErr    error
}

type expiryDiagnosticSQLCause string

func (expiryDiagnosticSQLCause) Error() string          { return "secret-bearing database prose" }
func (cause expiryDiagnosticSQLCause) SQLState() string { return string(cause) }

func (e *transactionProbeHumanTaskExpiry) ListDueHumanTaskExpiryEvents(context.Context, time.Time, int) ([]events.Event, error) {
	return []events.Event{e.event}, nil
}

func (e *transactionProbeHumanTaskExpiry) CommitHumanTaskExpirations(ctx context.Context, command HumanTaskExpiryCommand) (CommittedHumanTaskExpiry, error) {
	if err := command.Validate(); err != nil {
		return CommittedHumanTaskExpiry{}, err
	}
	e.commitCalls++
	if !e.acknowledged && e.commitErr != nil {
		return CommittedHumanTaskExpiry{}, e.commitErr
	}
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
	return CommittedHumanTaskExpiry{Acknowledged: true, Publications: committed}, e.commitErr
}

func VerifyHumanTaskExpiryUsesClosedSelectedStoreCommitEvidenceForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	fixture := open(t)
	runID := uuid.NewString()
	expiry := &transactionProbeHumanTaskExpiry{
		acknowledged: true,
		event: eventtest.RuntimeControl(
			uuid.NewString(), events.EventType("mailbox.card_expired"), "platform", "", []byte(`{"card_id":"card-a"}`),
			0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
		),
	}
	bus := &recordingPipelineBus{publishErr: errors.New("injected event persistence failure")}
	coordinator := &PipelineCoordinator{bus: bus, workflowStore: fixture.Persistence.store}

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
	t.Run("sanitized_original_cause", func(t *testing.T) {
		if humanTaskExpiryFailure("list", nil) != nil {
			t.Fatal("successful expiry became a failure")
		}
		original := fmt.Errorf("private account: %w", errors.Join(expiryDiagnosticSQLCause("40P01"), errors.New("credential=secret")))
		failure := runtimefailures.FromError(humanTaskExpiryFailure("commit", original), "", "")
		if failure.Failure.Detail.Code != "human_task_expiry_failed" || failure.Failure.Detail.Attributes["stage"] != "commit" ||
			failure.Failure.Retryable || failure.Failure.Deterministic || !errors.Is(failure, original) {
			t.Fatalf("expiry cause classification or original chain changed: %#v", failure.Failure)
		}
		body, err := json.Marshal(failure.Failure)
		if err != nil || !strings.Contains(string(body), "40P01") || strings.Contains(string(body), "private account") || strings.Contains(string(body), "secret") {
			t.Fatalf("expiry diagnostic omitted SQLSTATE or exposed prose: %s (%v)", body, err)
		}
		for _, original := range []error{
			runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "test_dependency_unavailable", "test", "expiry", nil),
			errors.Join(runtimefailures.New(runtimefailures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "test", "expiry", nil), errors.New("cleanup failed")),
		} {
			if observed := humanTaskExpiryFailure("prepare", original); observed != original {
				t.Fatal("existing typed classification or joined error chain was replaced")
			}
		}
		deep := error(expiryDiagnosticSQLCause("secret"))
		for index := 0; index < 40; index++ {
			deep = fmt.Errorf("secret wrapper: %w", deep)
		}
		branches := make([]error, 40)
		for index := range branches {
			branches[index] = errors.New("secret branch")
		}
		wide := errors.Join(branches...)
		for _, original := range []error{deep, wide, expiryDiagnosticSQLCause("secret")} {
			failure := runtimefailures.FromError(humanTaskExpiryFailure("list", original), "", "")
			body, err := json.Marshal(failure.Failure)
			if err != nil || strings.Contains(string(body), "secret") {
				t.Fatalf("bounded capture exposed invalid SQLSTATE or prose: %s (%v)", body, err)
			}
			if (original == deep || original == wide) && failure.Failure.Detail.Attributes["truncated"] != true {
				t.Fatal("capture truncation was not disclosed")
			}
		}
	})
}
