package bus_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type periodicContinuationStore struct {
	runtimepipelineobligation.Store

	mu         sync.Mutex
	issuer     *runtimepipelineobligation.ScanIssuer
	scan       runtimepipelineobligation.Scan
	openCount  int
	claimScan  []runtimepipelineobligation.Scan
	exhausted  chan struct{}
	allowClose chan struct{}
	closeOnce  sync.Once
}

func newPeriodicContinuationStore() *periodicContinuationStore {
	return &periodicContinuationStore{
		issuer:     runtimepipelineobligation.NewScanIssuer(),
		exhausted:  make(chan struct{}),
		allowClose: make(chan struct{}),
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
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (s *periodicContinuationStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	if _, err := s.issuer.Token(scan); err != nil {
		return err
	}
	s.closeOnce.Do(func() { close(s.exhausted) })
	<-s.allowClose
	return nil
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
		close(owner.allowClose)
		t.Fatal("periodic sweeper did not continue to explicit exhaustion")
	}
	cancel()
	close(owner.allowClose)
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

type terminalScanCloseStore struct {
	runtimepipelineobligation.Store

	issuer                  *runtimepipelineobligation.ScanIssuer
	firstScan               runtimepipelineobligation.Scan
	openCount               int
	closeAttempts           map[string]int
	claimScans              []runtimepipelineobligation.Scan
	firstCloseFailed        bool
	openedAfterCloseFailure bool
	persistentCloseFailure  bool
}

func newTerminalScanCloseStore(persistent bool) *terminalScanCloseStore {
	return &terminalScanCloseStore{
		issuer:                 runtimepipelineobligation.NewScanIssuer(),
		closeAttempts:          map[string]int{},
		persistentCloseFailure: persistent,
	}
}

func (s *terminalScanCloseStore) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if s.firstCloseFailed {
		s.openedAfterCloseFailure = true
	}
	scan, err := s.issuer.Issue()
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	s.openCount++
	if s.openCount == 1 {
		s.firstScan = scan
	}
	return scan, nil
}

func (s *terminalScanCloseStore) ClaimBatch(_ context.Context, scan runtimepipelineobligation.Scan, _ int) (runtimepipelineobligation.ScanBatch, error) {
	if _, err := s.issuer.Token(scan); err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	s.claimScans = append(s.claimScans, scan)
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (s *terminalScanCloseStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	token, err := s.issuer.Token(scan)
	if err != nil {
		return err
	}
	s.closeAttempts[token]++
	if scan == s.firstScan {
		s.firstCloseFailed = true
		return errTerminalPipelineScanClose
	}
	if s.persistentCloseFailure {
		return errTerminalPipelineScanClose
	}
	return nil
}

var errTerminalPipelineScanClose = errors.New("terminal pipeline scan close failure")

func TestSweepTreatsFailedScanCloseAsTerminal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		persistent bool
	}{
		{name: "fail once"},
		{name: "persistent failure", persistent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newTerminalScanCloseStore(tc.persistent)
			bus, err := newScopedTestEventBus(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
				PipelineObligations: owner,
			})
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}

			first, err := bus.SweepPipelineObligations(context.Background(), 1)
			if !errors.Is(err, errTerminalPipelineScanClose) {
				t.Fatalf("first sweep error = %v, want terminal close failure", err)
			}
			if !first.Exhausted {
				t.Fatalf("first sweep result = %#v, want exhausted result before close failure", first)
			}

			second, err := bus.SweepPipelineObligations(context.Background(), 1)
			if tc.persistent {
				if !errors.Is(err, errTerminalPipelineScanClose) {
					t.Fatalf("second sweep error = %v, want independent terminal close failure", err)
				}
			} else if err != nil {
				t.Fatalf("second sweep: %v", err)
			}
			if !second.Exhausted {
				t.Fatalf("second sweep result = %#v, want replacement scan exhaustion", second)
			}
			firstToken, _ := owner.issuer.Token(owner.firstScan)
			if owner.closeAttempts[firstToken] != 1 {
				t.Fatalf("first cursor close attempts = %d, want one terminal attempt", owner.closeAttempts[firstToken])
			}
			if !owner.openedAfterCloseFailure {
				t.Fatal("replacement cursor was not opened after terminal close failure")
			}
			if owner.openCount != 2 {
				t.Fatalf("scan opens = %d, want fresh cursor after terminal failure", owner.openCount)
			}
			if len(owner.claimScans) != 2 || owner.claimScans[0] == owner.claimScans[1] {
				t.Fatalf("claimed scans = %#v, want distinct fresh cursor per sweep", owner.claimScans)
			}
		})
	}
}
