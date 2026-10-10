package pipeline

import "context"

// Projection component inputs consume the original native construction owner;
// this carrier cannot supply a raw pool or a reconstructed transaction runner.
type WorkflowProjectionNativeFixtureForTest struct {
	Persistence WorkflowPersistence
	Context     context.Context
	Construct   func(context.Context, WorkflowInstance) error
}
