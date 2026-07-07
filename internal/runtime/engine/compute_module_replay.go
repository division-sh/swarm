package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

type ComputeModuleReplayEvidenceLoader interface {
	LoadComputeModuleReplayEvidence(ctx context.Context, runID string) ([]computemodule.ReplayEnvelope, error)
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
	if req.ExpectedComputeModuleTraces != nil {
		return ExecutionResult{}, &computemodule.Error{
			Code: computemodule.CodeReplay,
			Err:  fmt.Errorf("compute_module persisted replay cannot combine loaded evidence with explicit expected traces"),
		}
	}
	evidence, err := loader.LoadComputeModuleReplayEvidence(ctx, runID)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("load compute_module replay evidence for run %s: %w", runID, err)
	}
	req.ExpectedComputeModuleTraces = make([]ComputeModuleTrace, 0, len(evidence))
	for _, trace := range evidence {
		req.ExpectedComputeModuleTraces = append(req.ExpectedComputeModuleTraces, trace.Normalized())
	}
	return e.Execute(ctx, req)
}
