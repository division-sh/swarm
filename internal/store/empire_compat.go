package store

import (
	"context"
	"strconv"
	"strings"
	"time"

	empirestore "empireai/internal/empire/store"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
)

func (s *PostgresStore) empireCompat() *empirestore.PostgresStore {
	if s == nil {
		return nil
	}
	return empirestore.NewPostgresStore(s.DB)
}

func (s *PostgresStore) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	return s.empireCompat().InsertMailboxItem(ctx, item)
}

func (s *PostgresStore) ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	return s.empireCompat().ListMailboxItems(ctx, status, limit)
}

func (s *PostgresStore) CountMailboxItems(ctx context.Context, status string) (int, error) {
	return s.empireCompat().CountMailboxItems(ctx, status)
}

func (s *PostgresStore) GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	return s.empireCompat().GetMailboxItem(ctx, id)
}

func (s *PostgresStore) DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error {
	return s.empireCompat().DecideMailboxItem(ctx, id, status, decision, notes)
}

func (s *PostgresStore) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	return s.empireCompat().ExpireMailboxItems(ctx, limit)
}

func (s *PostgresStore) ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	return s.empireCompat().ListUnnotifiedCriticalMailboxItems(ctx, limit)
}

func (s *PostgresStore) MarkMailboxItemNotified(ctx context.Context, id string) error {
	return s.empireCompat().MarkMailboxItemNotified(ctx, id)
}

func (s *PostgresStore) CreateScanCampaign(ctx context.Context, in runtimepipeline.CreateScanCampaignInput) (runtimepipeline.ScanCampaign, error) {
	return s.empireCompat().CreateScanCampaign(ctx, in)
}

func (s *PostgresStore) ListScanCampaigns(ctx context.Context, filter runtimepipeline.ScanCampaignFilter) ([]runtimepipeline.ScanCampaign, error) {
	return s.empireCompat().ListScanCampaigns(ctx, filter)
}

func (s *PostgresStore) ClaimNextDueScanCampaign(ctx context.Context) (runtimepipeline.ScanCampaign, bool, error) {
	return s.empireCompat().ClaimNextDueScanCampaign(ctx)
}

func (s *PostgresStore) LookupGeographyLabel(ctx context.Context, geographyID string) (string, error) {
	return s.empireCompat().LookupGeographyLabel(ctx, geographyID)
}

func (s *PostgresStore) MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error {
	return s.empireCompat().MarkScanCampaignCompleted(ctx, campaignID, discoveries)
}

func (s *PostgresStore) RequeueDueRescans(ctx context.Context, now time.Time) (int, error) {
	return s.empireCompat().RequeueDueRescans(ctx, now)
}

func (s *PostgresStore) PauseQueuedScanCampaigns(ctx context.Context) (int, error) {
	return s.empireCompat().PauseQueuedScanCampaigns(ctx)
}

func (s *PostgresStore) ResumePausedScanCampaigns(ctx context.Context) (int, error) {
	return s.empireCompat().ResumePausedScanCampaigns(ctx)
}

func parseRescanInterval(raw string) time.Duration {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 0
	}
	if strings.HasSuffix(raw, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(raw, "d"))
		if err != nil || n <= 0 {
			return 0
		}
		return time.Duration(n) * 24 * time.Hour
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

func joinGeographyLabel(name, region, country string) string {
	parts := make([]string, 0, 3)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		for _, existing := range parts {
			if strings.EqualFold(existing, v) {
				return
			}
		}
		parts = append(parts, v)
	}
	add(name)
	add(region)
	add(country)
	return strings.Join(parts, ", ")
}
