package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestDeliveryContinuationSignalRegistrationRejectsDuplicateAuthority(t *testing.T) {
	store := &workflowInstanceStore{}
	authority := deliveryContinuationSignalTestAuthority(t, 1)
	registration, err := store.RegisterDeliveryContinuationSignal(authority, func() {})
	if err != nil {
		t.Fatalf("register first signal owner: %v", err)
	}
	defer registration.Release()

	if _, err := store.RegisterDeliveryContinuationSignal(authority, func() {}); err == nil {
		t.Fatal("duplicate signal authority registration succeeded")
	}
}

func TestDeliveryContinuationSignalRegistrationRoutesCommitToCurrentAuthorities(t *testing.T) {
	store, begin, acknowledge := deliveryContinuationReceiptUnit(t, StandingServiceReconciliation{DeliveryContinuationRequired: true}, nil)
	predecessorAuthority := deliveryContinuationSignalTestAuthority(t, 1)
	var predecessorSignals atomic.Int32
	predecessor, err := store.RegisterDeliveryContinuationSignal(predecessorAuthority, func() { predecessorSignals.Add(1) })
	if err != nil {
		t.Fatalf("register predecessor signal owner: %v", err)
	}
	defer predecessor.Release()
	begin()

	successorAuthority := deliveryContinuationSignalTestAuthority(t, 2)
	var successorSignals atomic.Int32
	successor, err := store.RegisterDeliveryContinuationSignal(successorAuthority, func() { successorSignals.Add(1) })
	if err != nil {
		t.Fatalf("register successor signal owner: %v", err)
	}
	defer successor.Release()
	predecessor.Release()
	predecessor.Release()

	if predecessorSignals.Load() != 0 || successorSignals.Load() != 0 {
		t.Fatal("continuation escaped before the persistence receipt returned")
	}
	if err := acknowledge(); err != nil {
		t.Fatalf("acknowledge predecessor operation: %v", err)
	}
	if got := predecessorSignals.Load(); got != 0 {
		t.Fatalf("retired predecessor signals = %d, want 0", got)
	}
	if got := successorSignals.Load(); got != 1 {
		t.Fatalf("current successor signals = %d, want 1", got)
	}

	if _, err := store.SuspendStandingService(context.Background(), StandingServiceOperation{}); err != nil {
		t.Fatalf("acknowledge successor operation: %v", err)
	}
	if got := successorSignals.Load(); got != 2 {
		t.Fatalf("successor signals = %d, want 2", got)
	}
}

func TestDeliveryContinuationSignalQueuedBeforeRegistrationSignalsCurrentAuthority(t *testing.T) {
	store, begin, acknowledge := deliveryContinuationReceiptUnit(t, StandingServiceReconciliation{DeliveryContinuationRequired: true}, nil)
	begin()

	var signals atomic.Int32
	registration, err := store.RegisterDeliveryContinuationSignal(deliveryContinuationSignalTestAuthority(t, 1), func() { signals.Add(1) })
	if err != nil {
		t.Fatalf("register authority before callback: %v", err)
	}
	defer registration.Release()
	if signals.Load() != 0 {
		t.Fatal("registration signaled before the persistence receipt returned")
	}
	if err := acknowledge(); err != nil {
		t.Fatalf("acknowledge operation: %v", err)
	}
	if got := signals.Load(); got != 1 {
		t.Fatalf("post-commit signals = %d, want 1 for authority registered before callback", got)
	}
}

func TestDeliveryContinuationSignalCallbackBeforeGenerationAdmissionDoesNotReplay(t *testing.T) {
	store, begin, acknowledge := deliveryContinuationReceiptUnit(t, StandingServiceReconciliation{DeliveryContinuationRequired: true}, nil)
	begin()
	if err := acknowledge(); err != nil {
		t.Fatalf("acknowledge pre-admission operation: %v", err)
	}

	var signals atomic.Int32
	registration, err := store.RegisterDeliveryContinuationSignal(deliveryContinuationSignalTestAuthority(t, 1), func() { signals.Add(1) })
	if err != nil {
		t.Fatalf("register authority after callback: %v", err)
	}
	defer registration.Release()
	if got := signals.Load(); got != 0 {
		t.Fatalf("completed pre-admission callback replayed %d signals, want 0", got)
	}
}

func TestDeliveryContinuationSignalWithoutPersistenceOwnerFailsClosed(t *testing.T) {
	store := &workflowInstanceStore{}
	var signals atomic.Int32
	registration, err := store.RegisterDeliveryContinuationSignal(deliveryContinuationSignalTestAuthority(t, 1), func() { signals.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	if result, err := store.SuspendStandingService(context.Background(), StandingServiceOperation{}); err == nil || result.DeliveryContinuationRequired || signals.Load() != 0 {
		t.Fatalf("missing persistence owner: result=%+v, error=%v, signals=%d", result, err, signals.Load())
	}
}

func TestDeliveryContinuationSignalConsumesOnlyAcknowledgedReceipt(t *testing.T) {
	cleanupFault := errors.New("acknowledged cleanup failed")
	for _, test := range []struct {
		name   string
		result StandingServiceReconciliation
		err    error
		want   int32
	}{
		{name: "no_committed_continuation"},
		{name: "unacknowledged_failure", err: errors.New("mutation refused")},
		{name: "acknowledged_cleanup_failure", result: StandingServiceReconciliation{DeliveryContinuationRequired: true}, err: cleanupFault, want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, begin, acknowledge := deliveryContinuationReceiptUnit(t, test.result, test.err)
			var signals atomic.Int32
			registration, err := store.RegisterDeliveryContinuationSignal(deliveryContinuationSignalTestAuthority(t, 1), func() { signals.Add(1) })
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			begin()
			if signals.Load() != 0 {
				t.Fatal("continuation escaped before receipt consumption")
			}
			if err := acknowledge(); !errors.Is(err, test.err) {
				t.Fatalf("owner error=%v,want%v", err, test.err)
			}
			if got := signals.Load(); got != test.want {
				t.Fatalf("receipt continuation signals=%d,want%d", got, test.want)
			}
		})
	}
}

// This unit gates a typed receipt; real commit/cleanup behavior is proved by
// TestStandingServiceAcknowledgedCleanupErrorStillSignalsContinuationBothStores.
type deliveryContinuationReceiptUnitOwner struct {
	StandingServicePersistence
	entered chan struct{}
	ready   chan struct{}
	result  StandingServiceReconciliation
	err     error
}

func (o *deliveryContinuationReceiptUnitOwner) SuspendStandingService(ctx context.Context, _ StandingServiceOperation) (StandingServiceReconciliation, error) {
	select {
	case o.entered <- struct{}{}:
	default:
	}
	select {
	case <-o.ready:
		return o.result, o.err
	case <-ctx.Done():
		return StandingServiceReconciliation{}, ctx.Err()
	}
}

func deliveryContinuationReceiptUnit(t *testing.T, receipt StandingServiceReconciliation, ownerErr error) (*workflowInstanceStore, func(), func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	owner := &deliveryContinuationReceiptUnitOwner{entered: make(chan struct{}, 1), ready: make(chan struct{}), result: receipt, err: ownerErr}
	store := &workflowInstanceStore{standingServices: owner}
	done := make(chan error, 1)
	started := false
	t.Cleanup(func() {
		cancel()
		if started {
			<-done
		}
	})
	begin := func() {
		started = true
		go func() {
			result, err := store.SuspendStandingService(ctx, StandingServiceOperation{})
			if !reflect.DeepEqual(result, receipt) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("standing receipt changed: %+v", result)
			}
			done <- err
			close(done)
		}()
		select {
		case <-owner.entered:
		case <-ctx.Done():
			t.Fatal("standing persistence owner was not entered")
		}
	}
	return store, begin, func() error {
		close(owner.ready)
		return <-done
	}
}

func deliveryContinuationSignalTestAuthority(t *testing.T, generation uint64) runtimedelivery.ExecutionAuthority {
	t.Helper()
	source, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("build signal test source: %v", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "signal-test-authority", generation)
	if err != nil {
		t.Fatalf("build signal test authority: %v", err)
	}
	return authority
}
