package runtime

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

type genericScheduleRuntimeLogger struct {
	logger *RuntimeLogger
}

func NewGenericScheduleRuntimeLogger(logger *RuntimeLogger) genericschedule.Logger {
	return genericScheduleRuntimeLogger{logger: logger}
}

func (l genericScheduleRuntimeLogger) GenericScheduleFailure(ctx context.Context, action, activationID string, err error) {
	if l.logger == nil || err == nil {
		return
	}
	handleRuntimeLogPersistenceError("generic_schedule", action, l.logger.Error(ctx, "generic_schedule", action, map[string]any{
		"activation_id": activationID,
	}, err))
}

func (l genericScheduleRuntimeLogger) GenericScheduleCatchupWarning(ctx context.Context, activationID string, depth int) {
	if l.logger == nil {
		return
	}
	handleRuntimeLogPersistenceError("generic_schedule", "catchup_depth", l.logger.Warn(ctx, "generic_schedule", "catchup_depth", map[string]any{
		"activation_id": activationID,
		"depth":         depth,
	}, fmt.Errorf("generic schedule catch-up depth reached %d", depth)))
}
