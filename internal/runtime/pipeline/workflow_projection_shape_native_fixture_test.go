package pipeline

import "context"

type WorkflowProjectionShapeNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	FieldsArray                func(context.Context, string, string) (int64, error)
	NumericGate                func(context.Context, string, string) (int64, error)
	AccumulatorArray           func(context.Context, string, string) (int64, error)
	MalformedTransitionHistory func(context.Context, string, string) (int64, error)
	ConflictingInstanceID      func(context.Context, string, string) (int64, error)
	SlashOnlyFlowPath          func(context.Context, string, string) (int64, error)
}
