package pipelinepersistence

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPipelineParentTransitionOwnsExactRecoveryAcquisition(t *testing.T) {
	var registry pipelineRecoveryTransitions
	// Reserve before a claim exists: this is the original acquisition/scan
	// association gap that terminalization must not race through.
	finishClaim, ok := registry.reserve("target")
	if !ok {
		t.Fatal("initial scan acquisition refused")
	}
	defer finishClaim()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		transition, err := registry.begin(ctx, "target")
		if transition != nil {
			transition.Done()
		}
		result <- err
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		registry.mu.Lock()
		pending := registry.runs["target"].transitions != 0
		registry.mu.Unlock()
		if pending {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("parent transition never entered recovery admission")
		}
	}
	if done, admitted := registry.reserve("target"); admitted {
		done()
		t.Fatal("new scan acquisition bypassed pending parent transition")
	}
	unrelated, err := registry.begin(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	unrelated.Done()
	select {
	case err := <-result:
		t.Fatalf("parent transition passed a pending acquisition: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled parent admission=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not release parent admission")
	}
	finishClaim()
	transition, err := registry.begin(context.Background(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if done, admitted := registry.reserve("target"); admitted {
		done()
		t.Fatal("scan entered while parent transaction owns exclusion")
	}
	transition.Done()
	transition.Done()
	done, admitted := registry.reserve("target")
	if !admitted {
		t.Fatal("parent settlement did not release scan admission")
	}
	done()
	done()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.runs) != 0 {
		t.Fatalf("settled recovery admission leaked run entries: %d", len(registry.runs))
	}
}
