package runtime

import (
	"context"
	"sync/atomic"
)

type runtimeEpochContextKey struct{}

var globalRuntimeEpoch atomic.Int64
var globalRuntimeIngressPaused atomic.Bool

func init() {
	globalRuntimeEpoch.Store(1)
}

func WithRuntimeEpoch(ctx context.Context, epoch int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if epoch <= 0 {
		epoch = CurrentRuntimeEpoch()
	}
	return context.WithValue(ctx, runtimeEpochContextKey{}, epoch)
}

func RuntimeEpochFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	v := ctx.Value(runtimeEpochContextKey{})
	epoch, ok := v.(int64)
	if !ok || epoch <= 0 {
		return 0, false
	}
	return epoch, true
}

func WithCurrentRuntimeEpoch(ctx context.Context) context.Context {
	if epoch, ok := RuntimeEpochFromContext(ctx); ok && epoch > 0 {
		return ctx
	}
	return WithRuntimeEpoch(ctx, CurrentRuntimeEpoch())
}

func CurrentRuntimeEpoch() int64 {
	epoch := globalRuntimeEpoch.Load()
	if epoch <= 0 {
		globalRuntimeEpoch.CompareAndSwap(0, 1)
		epoch = globalRuntimeEpoch.Load()
	}
	if epoch <= 0 {
		return 1
	}
	return epoch
}

func BumpRuntimeEpoch() int64 {
	next := globalRuntimeEpoch.Add(1)
	if next <= 0 {
		globalRuntimeEpoch.Store(1)
		return 1
	}
	return next
}

func IsCurrentRuntimeEpoch(epoch int64) bool {
	if epoch <= 0 {
		return false
	}
	return epoch == CurrentRuntimeEpoch()
}

func PauseRuntimeIngress() {
	globalRuntimeIngressPaused.Store(true)
}

func ResumeRuntimeIngress() {
	globalRuntimeIngressPaused.Store(false)
}

func RuntimeIngressPaused() bool {
	return globalRuntimeIngressPaused.Load()
}

func EnterRuntimeResetMode() int64 {
	PauseRuntimeIngress()
	resetMCPTurnContexts()
	return BumpRuntimeEpoch()
}

func ExitRuntimeResetMode() {
	ResumeRuntimeIngress()
}
