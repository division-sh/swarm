package activityjournal

import (
	"context"
	"errors"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

// Contract-only staging for E's resume delta, not an installed recovery writer.
// In particular this stub cannot issue a cut or return publication candidates.
type displacedHTTPActivityRecoveryStub struct {
	seen *mutationprotocol.Attempt
}

type displacedHTTPActivityRecoveryPort interface {
	ReconcileDisplacedHTTPActivitiesTx(context.Context, *mutationprotocol.Attempt, storestartup.DisplacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error)
}

var _ displacedHTTPActivityRecoveryPort = (*displacedHTTPActivityRecoveryStub)(nil)

var errActivityRecoveryStubNotInstalled = errors.New("activity recovery adapter is not installed")

func (s *displacedHTTPActivityRecoveryStub) ReconcileDisplacedHTTPActivitiesTx(ctx context.Context, attempt *mutationprotocol.Attempt, _ storestartup.DisplacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error) {
	if ctx == nil {
		return runtimepipeline.ActivityRecoverySummary{}, errors.New("activity recovery requires its original context")
	}
	if err := ctx.Err(); err != nil {
		return runtimepipeline.ActivityRecoverySummary{}, err
	}
	s.seen = attempt
	return runtimepipeline.ActivityRecoverySummary{}, errActivityRecoveryStubNotInstalled
}

func TestDisplacedHTTPActivityRecoveryStubCannotEnableRecovery(t *testing.T) {
	for _, cell := range []string{"missing_attempt", "inactive_attempt", "cancelled", "missing_context"} {
		t.Run(cell, func(t *testing.T) {
			ctx := context.Background()
			attempt := &mutationprotocol.Attempt{}
			want := errActivityRecoveryStubNotInstalled
			switch cell {
			case "missing_attempt":
				attempt = nil
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
				want = context.Canceled
			case "missing_context":
				ctx = nil
			}
			stub := &displacedHTTPActivityRecoveryStub{}
			result, err := stub.ReconcileDisplacedHTTPActivitiesTx(ctx, attempt, storestartup.DisplacedHTTPActivityCut{})
			if err == nil || len(result.Results) != 0 {
				t.Fatalf("non-enabling stub produced recovery candidates: result=%+v err=%v", result, err)
			}
			if ctx != nil && !errors.Is(err, want) {
				t.Fatalf("recovery refusal lost its cause: %v", err)
			}
			if ctx != nil && ctx.Err() == nil && stub.seen != attempt {
				t.Fatal("adapter substituted the supplied mutation attempt")
			}
			if (ctx == nil || ctx.Err() != nil) && stub.seen != nil {
				t.Fatal("missing or canceled context reached recovery work")
			}
		})
	}
}
