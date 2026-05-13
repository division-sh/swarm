package tools

import (
	"context"
	"fmt"
	"os"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/flowdata"
)

type flowDataReadInput struct {
	Filename string `json:"filename"`
}

func (e *Executor) execReadFlowData(_ context.Context, actor models.AgentConfig, input any) (any, error) {
	var in flowDataReadInput
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	resolved, err := flowdata.Resolve(source, actor, in.Filename)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(resolved.Path)
	if err != nil {
		return nil, fmt.Errorf("read_flow_data failed for %s: %w", resolved.Filename, err)
	}
	return map[string]any{
		"content":      string(content),
		"content_type": resolved.ContentType,
		"size_bytes":   len(content),
	}, nil
}
