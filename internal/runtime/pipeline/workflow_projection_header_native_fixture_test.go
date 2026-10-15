package pipeline

import "context"

type WorkflowProjectionHeaderNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	List func(context.Context, string) ([]WorkflowInstance, error)
}
