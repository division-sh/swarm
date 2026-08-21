package effects

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type ProviderDrainTarget string

const (
	ProviderDrainTargetRunning    ProviderDrainTarget = "running"
	ProviderDrainTargetTerminated ProviderDrainTarget = "terminated"
	ProviderDrainTargetFailed     ProviderDrainTarget = "failed"
)

func (t ProviderDrainTarget) Valid() bool {
	switch t {
	case ProviderDrainTargetRunning, ProviderDrainTargetTerminated, ProviderDrainTargetFailed:
		return true
	default:
		return false
	}
}

type ProviderAttemptDrainCapture struct {
	Predecessor           LifecycleToken
	SuccessorRuntimeEpoch int64
	SuccessorGeneration   uint64
	Target                ProviderDrainTarget
	LifecycleOperationID  string
	LifecycleTransitionID string
	CapturedAt            time.Time
	ExpiresAt             time.Time
}

func (c ProviderAttemptDrainCapture) Validate() error {
	if !c.Predecessor.Valid() || c.SuccessorRuntimeEpoch <= 0 || c.SuccessorGeneration == 0 ||
		!c.Target.Valid() || strings.TrimSpace(c.LifecycleOperationID) == "" ||
		strings.TrimSpace(c.LifecycleTransitionID) == "" || c.CapturedAt.IsZero() ||
		!c.ExpiresAt.After(c.CapturedAt) {
		return fmt.Errorf("provider attempt drain capture is incomplete")
	}
	return nil
}

type ProviderAttemptDrainCaptureResult struct {
	Captured int
}

type ProviderDrainFinalization struct {
	Token  LifecycleToken
	Target ProviderDrainTarget
}

type CompletionSettlementObservation struct {
	AttemptID             string
	Disposition           CompletionSettlementDisposition
	OriginDelivery        runtimedelivery.Claim
	OriginDeliverySettled bool
	Finalization          *ProviderDrainFinalization
}

type completionSettlementObserver struct {
	mu          sync.Mutex
	observation CompletionSettlementObservation
}

type completionSettlementObserverKey struct{}

func WithCompletionSettlementObserver(ctx context.Context) (context.Context, func() CompletionSettlementObservation) {
	if ctx == nil {
		ctx = context.Background()
	}
	observer := &completionSettlementObserver{}
	return context.WithValue(ctx, completionSettlementObserverKey{}, observer), func() CompletionSettlementObservation {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		return observer.observation
	}
}

func recordCompletionSettlementObservation(ctx context.Context, observation CompletionSettlementObservation) {
	if ctx == nil {
		return
	}
	observer, ok := ctx.Value(completionSettlementObserverKey{}).(*completionSettlementObserver)
	if !ok || observer == nil {
		return
	}
	observer.mu.Lock()
	observer.observation = observation
	observer.mu.Unlock()
}
