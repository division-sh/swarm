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

func TestPipelineForkTransitionJoinsPublicationWithoutChangingStopOrOtherRun(t *testing.T) {
	var registry pipelineRecoveryTransitions
	finishPublication, admitted := registry.reservePublication("source", false)
	if !admitted {
		t.Fatal("source publication refused before fork")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		transition, err := registry.beginFork(ctx, "source")
		if transition != nil {
			transition.Done()
		}
		finished <- err
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		registry.mu.Lock()
		state := registry.runs["source"]
		fenced := state != nil && state.forks != 0
		registry.mu.Unlock()
		if fenced {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("fork transition did not fence publication admission")
		}
	}
	if done, ok := registry.reservePublication("source", false); ok {
		done()
		t.Fatal("fresh publication crossed source fork fence")
	}
	accepted, ok := registry.reservePublication("source", true)
	if !ok {
		t.Fatal("accepted source publication could not finish after fork fence")
	}
	if done, ok := registry.reserve("source"); ok {
		done()
		t.Fatal("fresh recovery crossed source fork fence")
	}
	other, ok := registry.reservePublication("other", false)
	if !ok {
		t.Fatal("unrelated publication was fenced")
	}
	other()
	stop, err := registry.begin(ctx, "source")
	if err != nil {
		t.Fatalf("stop waited for foreground publication: %v", err)
	}
	stop.Done()
	select {
	case err := <-finished:
		t.Fatalf("fork did not join held publication: %v", err)
	default:
	}
	finishPublication()
	accepted()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("fork did not join settled publication")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.runs) != 0 {
		t.Fatalf("settled publication retained run membership: %d", len(registry.runs))
	}
}
