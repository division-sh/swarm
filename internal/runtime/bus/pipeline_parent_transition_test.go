package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type blockingPipelineSweepStore struct {
	runtimepipelineobligation.Store

	issuer  *runtimepipelineobligation.ScanIssuer
	entered chan struct{}
	release chan struct{}
}

func (s *blockingPipelineSweepStore) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	return s.issuer.Issue()
}

func (s *blockingPipelineSweepStore) ClaimBatch(context.Context, runtimepipelineobligation.Scan, int) (runtimepipelineobligation.ScanBatch, error) {
	close(s.entered)
	<-s.release
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (s *blockingPipelineSweepStore) CloseScan(context.Context, runtimepipelineobligation.Scan) error {
	return nil
}

func TestPipelineParentTransitionAdmissionHonorsContextWhileSweepIsActive(t *testing.T) {
	owner := &blockingPipelineSweepStore{
		issuer:  runtimepipelineobligation.NewScanIssuer(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	eb := &EventBus{
		pipelineObligations: owner,
		sourceArtifactFact:  sourceMutationFact(t, "e"),
	}
	sweepResult := make(chan error, 1)
	go func() {
		_, err := eb.sweepPipelineObligations(context.Background(), runtimepipelineobligation.GlobalScanRequest(), 1)
		sweepResult <- err
	}()
	<-owner.entered

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, acquireErr := eb.BeginPipelineParentTransition(ctx)
		result <- acquireErr
	}()
	<-started
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked transition error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked transition ignored context cancellation")
	}

	close(owner.release)
	if err := <-sweepResult; err != nil {
		t.Fatalf("active pipeline sweep: %v", err)
	}
	next, err := eb.BeginPipelineParentTransition(context.Background())
	if err != nil {
		t.Fatalf("begin transition after release: %v", err)
	}
	next.Done()
}
