package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"empireai/internal/runtime"
	"github.com/lib/pq"
)

func (s *PostgresStore) CreateScanCampaign(ctx context.Context, in runtime.CreateScanCampaignInput) (runtime.ScanCampaign, error) {
	if s == nil || s.DB == nil {
		return runtime.ScanCampaign{}, fmt.Errorf("postgres store is required")
	}
	in.GeographyID = strings.TrimSpace(in.GeographyID)
	in.Mode = strings.TrimSpace(in.Mode)
	in.Priority = strings.TrimSpace(in.Priority)
	in.Status = strings.TrimSpace(in.Status)
	in.RescanInterval = strings.TrimSpace(in.RescanInterval)
	if in.GeographyID == "" || in.Mode == "" {
		return runtime.ScanCampaign{}, fmt.Errorf("geography_id and mode are required")
	}
	if in.Priority == "" {
		in.Priority = "normal"
	}
	if in.Status == "" {
		in.Status = "queued"
	}
	strategicRaw := strings.TrimSpace(string(in.StrategicContext))
	if strategicRaw == "" {
		strategicRaw = "{}"
	}

	var cats any
	if len(in.Categories) > 0 {
		cats = pq.Array(in.Categories)
	}

	var out runtime.ScanCampaign
	var catsOut pq.StringArray
	var started, completed, deadline, next sql.NullTime
	err := s.DB.QueryRowContext(ctx, `
		INSERT INTO scan_campaigns (
			geography_id, directive_id, mode, categories, priority, status, rescan_interval, strategic_context, deadline_at, next_rescan_at
		) VALUES (
			$1::uuid, NULLIF($2,'')::uuid, $3, $4::text[], $5, $6, NULLIF($7,''), $8::jsonb, $9, $10
		)
		RETURNING
			id,
			geography_id::text,
			COALESCE(directive_id::text, ''),
			mode,
			categories,
			priority,
			status,
			COALESCE(discoveries, 0),
			COALESCE(rescan_interval, ''),
			COALESCE(strategic_context, '{}'::jsonb),
			created_at,
			started_at,
			completed_at,
			deadline_at,
			next_rescan_at
	`, in.GeographyID, strings.TrimSpace(in.DirectiveID), in.Mode, cats, in.Priority, in.Status, in.RescanInterval, strategicRaw, in.DeadlineAt, in.NextRescanAt).Scan(
		&out.ID,
		&out.GeographyID,
		&out.DirectiveID,
		&out.Mode,
		&catsOut,
		&out.Priority,
		&out.Status,
		&out.Discoveries,
		&out.RescanInterval,
		&out.StrategicContext,
		&out.CreatedAt,
		&started,
		&completed,
		&deadline,
		&next,
	)
	if err != nil {
		return runtime.ScanCampaign{}, fmt.Errorf("create scan campaign: %w", err)
	}
	out.Categories = []string(catsOut)
	if started.Valid {
		t := started.Time
		out.StartedAt = &t
	}
	if completed.Valid {
		t := completed.Time
		out.CompletedAt = &t
	}
	if deadline.Valid {
		t := deadline.Time
		out.DeadlineAt = &t
	}
	if next.Valid {
		t := next.Time
		out.NextRescanAt = &t
	}
	return out, nil
}

func (s *PostgresStore) ListScanCampaigns(ctx context.Context, filter runtime.ScanCampaignFilter) ([]runtime.ScanCampaign, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	status := strings.TrimSpace(filter.Status)
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	args := []any{limit}
	where := ""
	if status != "" {
		where = "WHERE status = $2"
		args = append(args, status)
	}

	q := fmt.Sprintf(`
		SELECT
			id::text,
			geography_id::text,
			COALESCE(directive_id::text, ''),
			mode,
			categories,
			priority,
			status,
			COALESCE(discoveries, 0),
			COALESCE(rescan_interval, ''),
			COALESCE(strategic_context, '{}'::jsonb),
			created_at,
			started_at,
			completed_at,
			deadline_at,
			next_rescan_at
		FROM scan_campaigns
		%s
		ORDER BY created_at DESC
		LIMIT $1
	`, where)

	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list scan campaigns: %w", err)
	}
	defer rows.Close()

	out := make([]runtime.ScanCampaign, 0, 16)
	for rows.Next() {
		var c runtime.ScanCampaign
		var cats pq.StringArray
		var started, completed, deadline, next sql.NullTime
		if err := rows.Scan(
			&c.ID,
			&c.GeographyID,
			&c.DirectiveID,
			&c.Mode,
			&cats,
			&c.Priority,
			&c.Status,
			&c.Discoveries,
			&c.RescanInterval,
			&c.StrategicContext,
			&c.CreatedAt,
			&started,
			&completed,
			&deadline,
			&next,
		); err != nil {
			return nil, fmt.Errorf("scan scan_campaigns row: %w", err)
		}
		c.Categories = []string(cats)
		if started.Valid {
			t := started.Time
			c.StartedAt = &t
		}
		if completed.Valid {
			t := completed.Time
			c.CompletedAt = &t
		}
		if deadline.Valid {
			t := deadline.Time
			c.DeadlineAt = &t
		}
		if next.Valid {
			t := next.Time
			c.NextRescanAt = &t
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scan campaigns: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ClaimNextDueScanCampaign(ctx context.Context) (runtime.ScanCampaign, bool, error) {
	if s == nil || s.DB == nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("postgres store is required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM scan_campaigns WHERE status = 'active' LIMIT 1)
	`).Scan(&active); err != nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("check active scan campaigns: %w", err)
	}
	if active {
		return runtime.ScanCampaign{}, false, nil
	}

	var id string
	if err := tx.QueryRowContext(ctx, `
		SELECT id::text
		FROM scan_campaigns
		WHERE status = 'queued'
		ORDER BY
			CASE priority
				WHEN 'high' THEN 0
				WHEN 'normal' THEN 1
				WHEN 'low' THEN 2
				ELSE 3
			END,
			created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return runtime.ScanCampaign{}, false, nil
		}
		return runtime.ScanCampaign{}, false, fmt.Errorf("select next scan campaign: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'active',
		    started_at = COALESCE(started_at, now()),
		    next_rescan_at = NULL
		WHERE id = $1::uuid
	`, id); err != nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("mark scan campaign active: %w", err)
	}

	var out runtime.ScanCampaign
	var cats pq.StringArray
	var started, completed, deadline, next sql.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT
			id::text,
			geography_id::text,
			COALESCE(directive_id::text, ''),
			mode,
			categories,
			priority,
			status,
			COALESCE(discoveries, 0),
			COALESCE(rescan_interval, ''),
			COALESCE(strategic_context, '{}'::jsonb),
			created_at,
			started_at,
			completed_at,
			deadline_at,
			next_rescan_at
		FROM scan_campaigns
		WHERE id = $1::uuid
	`, id).Scan(
		&out.ID,
		&out.GeographyID,
		&out.DirectiveID,
		&out.Mode,
		&cats,
		&out.Priority,
		&out.Status,
		&out.Discoveries,
		&out.RescanInterval,
		&out.StrategicContext,
		&out.CreatedAt,
		&started,
		&completed,
		&deadline,
		&next,
	); err != nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("load claimed scan campaign: %w", err)
	}
	out.Categories = []string(cats)
	if started.Valid {
		t := started.Time
		out.StartedAt = &t
	}
	if completed.Valid {
		t := completed.Time
		out.CompletedAt = &t
	}
	if deadline.Valid {
		t := deadline.Time
		out.DeadlineAt = &t
	}
	if next.Valid {
		t := next.Time
		out.NextRescanAt = &t
	}

	if err := tx.Commit(); err != nil {
		return runtime.ScanCampaign{}, false, fmt.Errorf("commit claim tx: %w", err)
	}
	return out, true, nil
}

func (s *PostgresStore) LookupGeographyLabel(ctx context.Context, geographyID string) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres store is required")
	}
	geographyID = strings.TrimSpace(geographyID)
	if geographyID == "" {
		return "", fmt.Errorf("geography_id is required")
	}
	var name, country, region string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(name, ''), COALESCE(country, ''), COALESCE(region, '')
		FROM geographies
		WHERE id = $1::uuid
	`, geographyID).Scan(&name, &country, &region); err != nil {
		return "", err
	}
	parts := make([]string, 0, 3)
	if strings.TrimSpace(name) != "" {
		parts = append(parts, strings.TrimSpace(name))
	}
	if strings.TrimSpace(region) != "" {
		parts = append(parts, strings.TrimSpace(region))
	}
	if strings.TrimSpace(country) != "" {
		parts = append(parts, strings.TrimSpace(country))
	}
	return strings.Join(parts, ", "), nil
}

func (s *PostgresStore) MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	campaignID = strings.TrimSpace(campaignID)
	if campaignID == "" {
		return fmt.Errorf("campaign_id is required")
	}
	if discoveries < 0 {
		discoveries = 0
	}

	var intervalRaw string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(rescan_interval, '')
		FROM scan_campaigns
		WHERE id = $1::uuid
	`, campaignID).Scan(&intervalRaw); err != nil {
		return fmt.Errorf("load rescan_interval: %w", err)
	}

	var next any
	if d := parseRescanInterval(intervalRaw); d > 0 {
		next = time.Now().UTC().Add(d)
	}

	if _, err := s.DB.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'completed',
		    discoveries = $2,
		    completed_at = now(),
		    next_rescan_at = $3,
		    started_at = COALESCE(started_at, now())
		WHERE id = $1::uuid
	`, campaignID, discoveries, next); err != nil {
		return fmt.Errorf("mark scan campaign completed: %w", err)
	}
	return nil
}

func (s *PostgresStore) RequeueDueRescans(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'queued',
		    started_at = NULL,
		    completed_at = NULL,
		    discoveries = 0,
		    next_rescan_at = NULL
		WHERE status = 'completed'
		  AND next_rescan_at IS NOT NULL
		  AND next_rescan_at <= $1
	`, now)
	if err != nil {
		return 0, fmt.Errorf("requeue due rescans: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *PostgresStore) PauseQueuedScanCampaigns(ctx context.Context) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'paused'
		WHERE status = 'queued'
	`)
	if err != nil {
		return 0, fmt.Errorf("pause queued scan campaigns: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *PostgresStore) ResumePausedScanCampaigns(ctx context.Context) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'queued'
		WHERE status = 'paused'
	`)
	if err != nil {
		return 0, fmt.Errorf("resume paused scan campaigns: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func parseRescanInterval(raw string) time.Duration {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 0
	}
	// Support "30d" / "90d" from spec as a minimal format.
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
