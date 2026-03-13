package pipeline

import (
	"context"
	"strings"
)

func (pc *FactoryPipelineCoordinator) countVerticalsInStage(ctx context.Context, stage string) int {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || strings.TrimSpace(stage) == "" {
		return 0
	}
	items, err := pc.workflowStore.List(ctx)
	if err != nil {
		return 0
	}
	count := 0
	target := strings.TrimSpace(stage)
	for _, item := range items {
		metadata := workflowMetadataSnapshot(item)
		if strings.TrimSpace(asString(metadata["instance_kind"])) != "vertical" {
			continue
		}
		if strings.TrimSpace(item.CurrentState) == target {
			count++
		}
	}
	return count
}
