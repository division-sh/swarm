package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

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
	store := &workflowInstanceStore{}
	predecessorAuthority := deliveryContinuationSignalTestAuthority(t, 1)
	var predecessorSignals atomic.Int32
	predecessor, err := store.RegisterDeliveryContinuationSignal(predecessorAuthority, func() { predecessorSignals.Add(1) })
	if err != nil {
		t.Fatalf("register predecessor signal owner: %v", err)
	}

	postCommit := make([]OwnerAction, 0, 2)
	rollback := make([]OwnerAction, 0, 2)
	ctx := withPipelinePostCommitActions(context.Background(), &postCommit)
	ctx = withPipelineRollbackActions(ctx, &rollback)
	if err := store.queueDeliveryContinuationSignal(ctx); err != nil {
		t.Fatalf("queue predecessor signal: %v", err)
	}

	successorAuthority := deliveryContinuationSignalTestAuthority(t, 2)
	var successorSignals atomic.Int32
	successor, err := store.RegisterDeliveryContinuationSignal(successorAuthority, func() { successorSignals.Add(1) })
	if err != nil {
		t.Fatalf("register successor signal owner: %v", err)
	}
	defer successor.Release()
	predecessor.Release()
	predecessor.Release()

	flushPipelinePostCommitActions(postCommit)
	if got := predecessorSignals.Load(); got != 0 {
		t.Fatalf("retired predecessor signals = %d, want 0", got)
	}
	if got := successorSignals.Load(); got != 1 {
		t.Fatalf("current successor signals = %d, want 1", got)
	}

	postCommit = postCommit[:0]
	rollback = rollback[:0]
	if err := store.queueDeliveryContinuationSignal(ctx); err != nil {
		t.Fatalf("queue successor signal: %v", err)
	}
	flushPipelinePostCommitActions(postCommit)
	if got := successorSignals.Load(); got != 2 {
		t.Fatalf("successor signals = %d, want 2", got)
	}
}

func TestDeliveryContinuationSignalQueuedBeforeRegistrationSignalsCurrentAuthority(t *testing.T) {
	store := &workflowInstanceStore{}
	postCommit := make([]OwnerAction, 0, 1)
	rollback := make([]OwnerAction, 0, 1)
	ctx := withPipelinePostCommitActions(context.Background(), &postCommit)
	ctx = withPipelineRollbackActions(ctx, &rollback)
	if err := store.queueDeliveryContinuationSignal(ctx); err != nil {
		t.Fatalf("queue signal before registration: %v", err)
	}

	var signals atomic.Int32
	registration, err := store.RegisterDeliveryContinuationSignal(deliveryContinuationSignalTestAuthority(t, 1), func() { signals.Add(1) })
	if err != nil {
		t.Fatalf("register authority before callback: %v", err)
	}
	defer registration.Release()
	flushPipelinePostCommitActions(postCommit)
	if got := signals.Load(); got != 1 {
		t.Fatalf("post-commit signals = %d, want 1 for authority registered before callback", got)
	}
}

func TestDeliveryContinuationSignalCallbackBeforeGenerationAdmissionDoesNotReplay(t *testing.T) {
	store := &workflowInstanceStore{}
	postCommit := make([]OwnerAction, 0, 1)
	rollback := make([]OwnerAction, 0, 1)
	ctx := withPipelinePostCommitActions(context.Background(), &postCommit)
	ctx = withPipelineRollbackActions(ctx, &rollback)
	if err := store.queueDeliveryContinuationSignal(ctx); err != nil {
		t.Fatalf("queue pre-admission post-commit signal: %v", err)
	}
	flushPipelinePostCommitActions(postCommit)

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

func TestDeliveryContinuationSignalWithoutTransactionAuthorityFailsClosed(t *testing.T) {
	store := &workflowInstanceStore{}
	if err := store.queueDeliveryContinuationSignal(context.Background()); err == nil {
		t.Fatal("standing signal without transaction-owned callback succeeded")
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
