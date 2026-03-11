package pipeline

import (
	"context"
	"encoding/json"
	"time"
)

type SchedulePersistence interface {
	UpsertSchedule(ctx context.Context, sc Schedule) error
	CancelSchedule(ctx context.Context, agentID, eventType string) error
	LoadActiveSchedules(ctx context.Context) ([]Schedule, error)
	MarkScheduleFired(ctx context.Context, sc Schedule) error
}

type ExactSchedulePersistence interface {
	CancelScheduleExact(ctx context.Context, sc Schedule) error
	MarkScheduleFiredExact(ctx context.Context, sc Schedule) error
}

type ScanCampaign struct {
	ID               string
	GeographyID      string
	DirectiveID      string
	Mode             string
	Categories       []string
	Priority         string
	Status           string
	Discoveries      int
	RescanInterval   string
	StrategicContext json.RawMessage
	CreatedAt        time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	DeadlineAt       *time.Time
	NextRescanAt     *time.Time
}

type CreateScanCampaignInput struct {
	GeographyID      string
	DirectiveID      string
	Mode             string
	Categories       []string
	Priority         string
	Status           string
	RescanInterval   string
	StrategicContext json.RawMessage
	DeadlineAt       *time.Time
	NextRescanAt     *time.Time
}

type ScanCampaignFilter struct {
	Status string
	Limit  int
}

type ScanCampaignPersistence interface {
	CreateScanCampaign(ctx context.Context, in CreateScanCampaignInput) (ScanCampaign, error)
	ListScanCampaigns(ctx context.Context, filter ScanCampaignFilter) ([]ScanCampaign, error)

	ClaimNextDueScanCampaign(ctx context.Context) (ScanCampaign, bool, error)
	LookupGeographyLabel(ctx context.Context, geographyID string) (string, error)
	MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error
	RequeueDueRescans(ctx context.Context, now time.Time) (int, error)
	PauseQueuedScanCampaigns(ctx context.Context) (int, error)
	ResumePausedScanCampaigns(ctx context.Context) (int, error)
}
