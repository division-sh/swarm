package runtimepersistence

import (
	"context"

	"testing"
)

type forkCompletionScheduleLogger struct{ t *testing.T }

func (l forkCompletionScheduleLogger) GenericScheduleFailure(_ context.Context, operation, activation string, err error) {
	l.t.Logf("completion schedule %s activation=%s: %v", operation, activation, err)
}

func (l forkCompletionScheduleLogger) GenericScheduleCatchupWarning(_ context.Context, activation string, count int) {
	l.t.Logf("completion schedule catchup activation=%s count=%d", activation, count)
}
