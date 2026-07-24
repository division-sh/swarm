package pipeline_test

import (
	"context"
	"errors"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type recoveryOwnerProbe struct {
	results []runtimepipelineobligation.SweepResult
	err     error
	calls   int
}

func (p *recoveryOwnerProbe) SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	p.calls++
	if p.err != nil {
		return runtimepipelineobligation.SweepResult{}, p.err
	}
	if len(p.results) == 0 {
		return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
	}
	result := p.results[0]
	p.results = p.results[1:]
	return result, nil
}

func TestRecoveryManagerDrainsCanonicalOwnerUntilExplicitExhaustion(t *testing.T) {
	owner := &recoveryOwnerProbe{results: []runtimepipelineobligation.SweepResult{
		{Settled: 0, Examined: 5000},
		{Settled: 5000, Examined: 5000},
		{Settled: 7, Examined: 7, Exhausted: true},
	}}
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

func TestRecoveryManagerRequiresCanonicalOwner(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewRecoveryManagerWith(nil) did not fail closed")
		}
	}()
	_ = runtimepipeline.NewRecoveryManagerWith(nil)
}
