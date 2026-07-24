package ingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type transitionFailurePublisher struct {
	publishErr error
	releaseN   int
	releaseErr error
	events     []events.Event
}

func (p *transitionFailurePublisher) Publish(_ context.Context, event events.Event) error {
	p.events = append(p.events, event)
	return p.publishErr
}

func (p *transitionFailurePublisher) ReleaseRuntimeIngressQueue(context.Context, int) (int, error) {
	return p.releaseN, p.releaseErr
}

func TestSafetyPauseAndResumePreserveInboundLineage(t *testing.T) {
	runID, parentID := uuid.NewString(), uuid.NewString()
	inbound := eventtest.RunCreatingRootIngressWithMode(
		parentID, "work.received", "gateway", "task-1", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
		executionmode.Mock,
	)
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), inbound)
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	publisher := &transitionFailurePublisher{}
	controller := NewController(nil, publisher, Options{Now: func() time.Time { return now }})

	if _, err := controller.SafetyPause(ctx, TransitionRequest{Reason: "active work failed", Now: now}); err != nil {
		t.Fatalf("SafetyPause: %v", err)
	}
	if _, err := controller.Resume(ctx, TransitionRequest{Reason: "operator resumed", Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(publisher.events) != 2 {
		t.Fatalf("published transitions = %d, want 2", len(publisher.events))
	}
	for _, event := range publisher.events {
		if event.RunID() != runID || event.ParentEventID() != parentID || event.TaskID() != "task-1" || event.ExecutionMode() != executionmode.Mock {
			t.Fatalf("%s lineage = run:%q parent:%q task:%q mode:%q", event.Type(), event.RunID(), event.ParentEventID(), event.TaskID(), event.ExecutionMode())
		}
	}
}

func TestTransitionPostCommitFailuresRemainCallerSuccess(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)
	publisher := &transitionFailurePublisher{publishErr: errors.New("publish unavailable")}
	controller := NewController(nil, publisher, Options{Now: func() time.Time { return now }})

	if _, err := controller.Pause(ctx, TransitionRequest{Now: now}); err != nil {
		t.Fatalf("Pause post-commit publish failure error = %v, want nil", err)
	}
	if paused, err := controller.QueueableIngressPaused(ctx); err != nil {
		t.Fatalf("QueueableIngressPaused: %v", err)
	} else if !paused {
		t.Fatal("runtime ingress paused = false, want true after committed pause")
	}

	publisher.publishErr = nil
	publisher.releaseN = 1
	publisher.releaseErr = errors.New("release unavailable")
	resumed, err := controller.Resume(ctx, TransitionRequest{Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("Resume post-commit release failure error = %v, want nil", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want partial count 1", resumed.ReleasedCount)
	}
	if paused, err := controller.QueueableIngressPaused(ctx); err != nil {
		t.Fatalf("QueueableIngressPaused after resume: %v", err)
	} else if paused {
		t.Fatal("runtime ingress paused = true, want false after committed resume")
	}
}
