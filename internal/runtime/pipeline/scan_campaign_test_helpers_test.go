package pipeline_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtime "empireai/internal/runtime"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

func newScanCampaignHooksForPipelineTest() runtimepipeline.ScanCampaignHooks {
	return runtimepipeline.ScanCampaignHooks{
		Warnf: runtime.RuntimeWarnForTest,
		RecordTransition: func(ctx context.Context, db *sql.DB, in runtimepipeline.ScanCampaignTransitionInput) error {
			return runtime.RecordPipelineTransition(ctx, db, runtime.PipelineTransitionInput{
				EventID:       in.EventID,
				EventType:     in.EventType,
				Handler:       in.Handler,
				PipelineType:  in.PipelineType,
				PipelineID:    in.PipelineID,
				Action:        in.Action,
				StateBefore:   in.StateBefore,
				StateAfter:    in.StateAfter,
				EventsEmitted: in.EventsEmitted,
				DropReason:    in.DropReason,
				Error:         in.Error,
				Duration:      in.Duration,
			})
		},
		EnsureDirectiveGeography: runtimepipeline.EnsureDirectiveGeography,
	}
}

func asIntForPipelineTest(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(t), "%d", &n); err == nil {
			return n
		}
	}
	return 0
}
