package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type shutdownRetryGrant struct {
	startupownership.LiveGenerationGrant
	failure error
	calls   int
}

func (g *shutdownRetryGrant) Retire(ctx context.Context) error {
	g.calls++
	if g.calls == 1 {
		return g.failure
	}
	return g.LiveGenerationGrant.Retire(ctx)
}

func TestRuntimeShutdownRetriesFailedGenerationRetirement(t *testing.T) {
	rt := &Runtime{workOccurrence: runtimeTestOccurrence(t, runtimeTestBundleHash)}
	newShutdownFanOutRegistration(t, rt)
	underlying := rt.startupGrant
	failure := errors.New("one-shot generation retirement refusal")
	grant := &shutdownRetryGrant{LiveGenerationGrant: underlying, failure: failure}
	rt.startupGrant = grant
	if err := rt.Shutdown(); !errors.Is(err, failure) {
		t.Fatalf("first shutdown must preserve the exact retirement failure: %v", err)
	}
	if evidence, err := underlying.Evidence(); err != nil || evidence.State != startupownership.GrantAdmitted {
		t.Fatalf("refused retirement unexpectedly retired the underlying generation: %+v err=%v", evidence, err)
	}
	if rt.startupGrant != grant {
		t.Error("shutdown discarded the still-admitted generation handle after failed Retire")
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("second shutdown should retry and settle retirement: %v", err)
	}
	if grant.calls != 2 {
		t.Errorf("Retire calls=%d, want one refusal followed by one successful retry", grant.calls)
	}
	select {
	case <-underlying.Done():
	default:
		t.Error("second shutdown returned success with the generation still admitted")
	}
	if rt.startupGrant != nil {
		t.Error("successful retirement retained the completed generation handle")
	}
}
