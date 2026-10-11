package pipeline

import "context"

type WorkflowBookkeepingNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	SetPlatformBookkeeping func(context.Context, string, string) (int64, error)
}
