package runtime

import (
	"context"

	runtimebus "empireai/internal/runtime/bus"
)

func WithRuntimeEpoch(ctx context.Context, epoch int64) context.Context {
	return runtimebus.WithRuntimeEpoch(ctx, epoch)
}

func RuntimeEpochFromContext(ctx context.Context) (int64, bool) {
	return runtimebus.RuntimeEpochFromContext(ctx)
}

func WithCurrentRuntimeEpoch(ctx context.Context) context.Context {
	return runtimebus.WithCurrentRuntimeEpoch(ctx)
}

func CurrentRuntimeEpoch() int64 {
	return runtimebus.CurrentRuntimeEpoch()
}

func BumpRuntimeEpoch() int64 {
	return runtimebus.BumpRuntimeEpoch()
}

func IsCurrentRuntimeEpoch(epoch int64) bool {
	return runtimebus.IsCurrentRuntimeEpoch(epoch)
}

func PauseRuntimeIngress() {
	runtimebus.PauseRuntimeIngress()
}

func ResumeRuntimeIngress() {
	runtimebus.ResumeRuntimeIngress()
}

func RuntimeIngressPaused() bool {
	return runtimebus.RuntimeIngressPaused()
}

func EnterRuntimeResetMode() int64 {
	return runtimebus.EnterRuntimeResetMode()
}

func ExitRuntimeResetMode() {
	runtimebus.ExitRuntimeResetMode()
}
