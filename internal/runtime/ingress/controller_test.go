package ingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"swarm/internal/events"
)

type transitionFailurePublisher struct {
	publishErr error
	releaseN   int
	releaseErr error
}

func (p *transitionFailurePublisher) Publish(context.Context, events.Event) error {
	return p.publishErr
}

func (p *transitionFailurePublisher) ReleaseRuntimeIngressQueue(context.Context, time.Duration, int) (int, error) {
	return p.releaseN, p.releaseErr
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
