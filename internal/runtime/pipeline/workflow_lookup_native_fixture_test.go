package pipeline

import "context"

type WorkflowLookupNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	CountHeaders func(context.Context) (int64, error)
}
