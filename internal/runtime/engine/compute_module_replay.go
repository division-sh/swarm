package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

type ComputeModuleReplayEvidenceLoader interface {
	LoadComputeModuleReplayEvidenceForExecution(ctx context.Context, runID, eventID, nodeID string) ([]computemodule.ReplayEnvelope, error)
}

func (e *Executor) ExecuteWithPersistedComputeModuleReplayEvidence(ctx context.Context, loader ComputeModuleReplayEvidenceLoader, runID string, req ExecutionRequest) (ExecutionResult, error) {
	if e == nil {
		return ExecutionResult{}, fmt.Errorf("compute_module persisted replay requires executor")
	}
	if loader == nil {
		return ExecutionResult{}, fmt.Errorf("compute_module persisted replay requires evidence loader")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ExecutionResult{}, fmt.Errorf("compute_module persisted replay requires run id")
	}
	eventID := strings.TrimSpace(req.Event.ID())
	if eventID == "" {
		return ExecutionResult{}, fmt.Errorf("compute_module persisted replay requires request event id")
	}
	nodeID := strings.TrimSpace(string(req.NodeID))
	if nodeID == "" {
		return ExecutionResult{}, fmt.Errorf("compute_module persisted replay requires request node id")
	}
	if req.ExpectedComputeModuleTraces != nil {
		return ExecutionResult{}, &computemodule.Error{
			Code: computemodule.CodeReplay,
			Err:  fmt.Errorf("compute_module persisted replay cannot combine loaded evidence with explicit expected traces"),
		}
	}
	evidence, err := loader.LoadComputeModuleReplayEvidenceForExecution(ctx, runID, eventID, nodeID)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("load compute_module replay evidence for run %s event %s node %s: %w", runID, eventID, nodeID, err)
	}
	req.ExpectedComputeModuleTraces = make([]ComputeModuleTrace, 0, len(evidence))
	for _, trace := range evidence {
		req.ExpectedComputeModuleTraces = append(req.ExpectedComputeModuleTraces, trace.Normalized())
	}
	return e.Execute(ctx, req)
}
