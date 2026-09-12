package pipeline

import "github.com/division-sh/swarm/internal/runtime/core/identity"

// External integration tests supply the real source, persistence and captured
// selected-fork context. This exposes the adapter, not an alternate executor.
func NewCoordinatorHandlerExecutionEngineForTest(pc *PipelineCoordinator, node identity.ExecutableNode) HandlerExecutionEngine {
	return newCoordinatorHandlerExecutionEngine(pc, node)
}
