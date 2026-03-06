package runtime

import (
	"context"
	"database/sql"

	"empireai/internal/events"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

type ScanCampaignManager struct {
	inner *runtimepipeline.ScanCampaignManager
}

const defaultCampaignTimeCap = runtimepipeline.DefaultCampaignTimeCap

func NewScanCampaignManager(bus *EventBus, store ScanCampaignPersistence, db ...*sql.DB) *ScanCampaignManager {
	hooks := runtimepipeline.ScanCampaignHooks{
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
	return &ScanCampaignManager{inner: runtimepipeline.NewScanCampaignManager(bus, store, hooks, db...)}
}

func (m *ScanCampaignManager) Run(ctx context.Context) {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.Run(ctx)
}

func (m *ScanCampaignManager) onEvent(ctx context.Context, evt events.Event) {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.OnEventForTest(ctx, evt)
}

func (m *ScanCampaignManager) onDirective(ctx context.Context, evt events.Event) {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.OnDirectiveForTest(ctx, evt)
}

func (m *ScanCampaignManager) tick(ctx context.Context) {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.TickForTest(ctx)
}

func (m *ScanCampaignManager) emitCampaignCompletedIfDone(ctx context.Context, campaignID string, discoveries int, sourceEventID string) bool {
	if m == nil || m.inner == nil {
		return false
	}
	return m.inner.EmitCampaignCompletedIfDoneForTest(ctx, campaignID, discoveries, sourceEventID)
}

func (m *ScanCampaignManager) shouldPauseForBackpressure(ctx context.Context) bool {
	if m == nil || m.inner == nil {
		return true
	}
	return m.inner.ShouldPauseForBackpressureForTest(ctx)
}

func (m *ScanCampaignManager) pendingMailboxCount(ctx context.Context) (int, error) {
	if m == nil || m.inner == nil {
		return 0, nil
	}
	return m.inner.PendingMailboxCountForTest(ctx)
}

func (m *ScanCampaignManager) resetFlags() {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.ResetFlagsForTest()
}

func (m *ScanCampaignManager) setBudgetPausedForTest(v bool) {
	if m == nil || m.inner == nil {
		return
	}
	m.inner.SetBudgetPausedForTest(v)
}

func (m *ScanCampaignManager) budgetPausedForTest() bool {
	if m == nil || m.inner == nil {
		return false
	}
	return m.inner.BudgetPausedForTest()
}

func (m *ScanCampaignManager) backpressurePausedForTest() bool {
	if m == nil || m.inner == nil {
		return false
	}
	return m.inner.BackpressurePausedForTest()
}

func ensureDirectiveGeography(ctx context.Context, db *sql.DB, name, country, region string) (string, error) {
	return runtimepipeline.EnsureDirectiveGeography(ctx, db, name, country, region)
}
