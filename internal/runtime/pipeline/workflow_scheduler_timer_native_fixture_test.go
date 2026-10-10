package pipeline

import (
	"context"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

type WorkflowSchedulerTimerNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	AdmitSchedule        func(context.Context, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error)
	CountSchedulerTimers func(context.Context, string, string) (int64, error)
}
