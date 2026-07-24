package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type periodicContinuationStore struct {
	runtimepipelineobligation.Store

	mu        sync.Mutex
	issuer    *runtimepipelineobligation.ScanIssuer
	scan      runtimepipelineobligation.Scan
	openCount int
	claimScan []runtimepipelineobligation.Scan
	exhausted chan struct{}
}

func newPeriodicContinuationStore() *periodicContinuationStore {
	return &periodicContinuationStore{
		issuer:    runtimepipelineobligation.NewScanIssuer(),
		exhausted: make(chan struct{}),
	}
}

func (s *periodicContinuationStore) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openCount++
	scan, err := s.issuer.Issue()
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	s.scan = scan
	return scan, nil
}

func (s *periodicContinuationStore) ClaimBatch(_ context.Context, scan runtimepipelineobligation.Scan, limit int) (runtimepipelineobligation.ScanBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.issuer.Token(scan); err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	s.claimScan = append(s.claimScan, scan)
	if len(s.claimScan) == 1 {
		return runtimepipelineobligation.ScanBatch{Examined: limit}, nil
	}
	if len(s.claimScan) == 2 {
		close(s.exhausted)
	}
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (s *periodicContinuationStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	_, err := s.issuer.Token(scan)
	return err
}

func TestPeriodicSweeperRetainsCursorAcrossTicksUntilExplicitExhaustion(t *testing.T) {
	owner := newPeriodicContinuationStore()
	bus, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PipelineObligations: owner,
	})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := bus.StartOutboxSweeper(ctx, runtimebus.OutboxSweeperConfig{
		Interval: time.Millisecond,
		Limit:    2,
	}); err != nil {
		cancel()
		t.Fatalf("StartOutboxSweeper: %v", err)
	}
	select {
	case <-owner.exhausted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("periodic sweeper did not continue to explicit exhaustion")
	}
	cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := bus.WaitForOutboxSweeper(waitCtx); err != nil {
		t.Fatalf("WaitForOutboxSweeper: %v", err)
	}

	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.openCount != 1 {
		t.Fatalf("periodic cursor opens = %d, want one cursor retained across ticks", owner.openCount)
	}
	if len(owner.claimScan) != 2 || owner.claimScan[0] != owner.claimScan[1] {
		t.Fatalf("periodic claim scans = %#v, want the same cursor through explicit exhaustion", owner.claimScan)
	}
}
