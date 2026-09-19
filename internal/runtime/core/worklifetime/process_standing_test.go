package worklifetime

import (
	"context"
	"testing"
	"time"
)

func TestProcessStandingWorkIsCanceledAndJoinedBeforeRelease(t *testing.T) {
	process := NewProcess()
	standing, err := process.BeginStanding(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if owner, ok := ProcessFromContext(standing.Context()); !ok || owner != process {
		t.Fatal("standing observer lost its exact process owner")
	}
	process.Retire()
	select {
	case <-standing.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("process retirement did not cancel its observer")
	}
	if _, err := process.BeginStanding(context.Background()); err == nil {
		t.Fatal("retired process admitted another observer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := process.Join(ctx); err == nil {
		t.Fatal("process join passed before its observer settled")
	}
	standing.Done()
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if process.ActiveCount() != 0 {
		t.Fatal("joined process retained observer work")
	}
}
