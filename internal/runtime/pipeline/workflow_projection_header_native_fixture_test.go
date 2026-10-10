package pipeline

import "context"

type WorkflowProjectionHeaderNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	ObsoleteFieldRows func(context.Context, string) (int64, error)
	List              func(context.Context, string) ([]WorkflowInstance, error)
}
