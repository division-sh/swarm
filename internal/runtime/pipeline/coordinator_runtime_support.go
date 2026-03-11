package pipeline

import (
	"context"
	"strings"
)

func (pc *FactoryPipelineCoordinator) countVerticalsInStage(ctx context.Context, stage string) int {
	if pc == nil || pc.db == nil || strings.TrimSpace(stage) == "" {
		return 0
	}
	var count int
	_ = dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM verticals
		WHERE stage = $1
	`, strings.TrimSpace(stage)).Scan(&count)
	return count
}
