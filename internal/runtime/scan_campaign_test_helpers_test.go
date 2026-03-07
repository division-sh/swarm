package runtime

import (
	"context"
	"database/sql"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func newScanCampaignHooksForTest() runtimepipeline.ScanCampaignHooks {
	return runtimepipeline.ScanCampaignHooks{
		Warnf: runtimeWarn,
		RecordTransition: func(ctx context.Context, db *sql.DB, in runtimepipeline.ScanCampaignTransitionInput) error {
			return RecordPipelineTransition(ctx, db, PipelineTransitionInput{
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
