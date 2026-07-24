package pipeline_test

import (
	"context"
	"errors"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type recoveryOwnerProbe struct {
	results []int
	err     error
	calls   int
}

func (p *recoveryOwnerProbe) SweepUndispatched(context.Context, int) (int, error) {
	p.calls++
	if p.err != nil {
		return 0, p.err
	}
	if len(p.results) == 0 {
		return 0, nil
	}
	result := p.results[0]
	p.results = p.results[1:]
	return result, nil
}

func TestRecoveryManagerDrainsCanonicalOwnerUntilPartialPage(t *testing.T) {
	owner := &recoveryOwnerProbe{results: []int{5000, 5000, 7}}
	if err := runtimepipeline.NewRecoveryManagerWith(owner).Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if owner.calls != 3 {
		t.Fatalf("SweepUndispatched calls = %d, want 3", owner.calls)
	}
}

func TestRecoveryManagerPropagatesCanonicalOwnerFailure(t *testing.T) {
	want := errors.New("recovery failed")
	owner := &recoveryOwnerProbe{err: want}
	err := runtimepipeline.NewRecoveryManagerWith(owner).Recover(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Recover error = %v, want %v", err, want)
	}
	if owner.calls != 1 {
		t.Fatalf("SweepUndispatched calls = %d, want 1", owner.calls)
	}
}

func TestRecoveryManagerWithoutOwnerIsNoop(t *testing.T) {
	if err := runtimepipeline.NewRecoveryManager().Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}
