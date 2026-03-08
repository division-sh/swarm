package dashboard

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func workflowAuditSummary(ctx context.Context, db *sql.DB, contractVersion string, now time.Time) map[string]any {
	out := map[string]any{
		"instances_total":                0,
		"active_verticals_without_state": 0,
		"stage_drift_count":              0,
		"version_mismatch_count":         0,
		"stale_stage_count_24h":          0,
		"active_timer_instances":         0,
		"revisioned_instances":           0,
		"warnings":                       []string{},
	}
	if db == nil {
		out["error"] = "database unavailable"
		return out
	}

	var instancesTotal, missingState, stageDrift, versionMismatch, staleStage24h, activeTimerInstances, revisionedInstances int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_instances`).Scan(&instancesTotal)
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM verticals v
		LEFT JOIN workflow_instances wi ON wi.instance_id = v.id
		WHERE v.stage NOT IN ('killed', 'winding_down')
		  AND wi.instance_id IS NULL
	`).Scan(&missingState)
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM verticals v
		JOIN workflow_instances wi ON wi.instance_id = v.id
		WHERE COALESCE(v.stage, '') <> COALESCE(wi.current_stage, '')
	`).Scan(&stageDrift)
	if strings.TrimSpace(contractVersion) != "" {
		_ = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM workflow_instances wi
			WHERE COALESCE(wi.workflow_version, '') <> $1
		`, strings.TrimSpace(contractVersion)).Scan(&versionMismatch)
	}
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM workflow_instances wi
		WHERE wi.entered_stage_at <= now() - interval '24 hours'
		  AND wi.current_stage NOT IN ('killed', 'winding_down', 'operating', 'expanding')
	`).Scan(&staleStage24h)
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM workflow_instances wi
		WHERE EXISTS (
			SELECT 1
			FROM jsonb_array_elements(COALESCE(wi.timer_state, '[]'::jsonb)) AS timer
			WHERE COALESCE((timer->>'cancelled')::boolean, false) = false
		)
	`).Scan(&activeTimerInstances)
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM workflow_instances wi
		WHERE COALESCE((wi.metadata->>'revision_count')::int, 0) > 0
	`).Scan(&revisionedInstances)

	out["instances_total"] = instancesTotal
	out["active_verticals_without_state"] = missingState
	out["stage_drift_count"] = stageDrift
	out["version_mismatch_count"] = versionMismatch
	out["stale_stage_count_24h"] = staleStage24h
	out["active_timer_instances"] = activeTimerInstances
	out["revisioned_instances"] = revisionedInstances

	warnings := make([]string, 0, 8)
	appendWarning := func(msg string) {
		msg = strings.TrimSpace(msg)
		if msg != "" {
			warnings = append(warnings, msg)
		}
	}
	if missingState > 0 {
		appendWarning(fmt.Sprintf("%d active verticals are missing workflow_instances state", missingState))
	}
	if stageDrift > 0 {
		appendWarning(fmt.Sprintf("%d verticals have stage drift between verticals.stage and workflow_instances.current_stage", stageDrift))
	}
	if versionMismatch > 0 && strings.TrimSpace(contractVersion) != "" {
		appendWarning(fmt.Sprintf("%d workflow instances are not on contract version %s", versionMismatch, strings.TrimSpace(contractVersion)))
	}
	if staleStage24h > 0 {
		appendWarning(fmt.Sprintf("%d workflow instances have been in the same non-terminal stage for more than 24h", staleStage24h))
	}
	if activeTimerInstances > 0 {
		appendWarning(fmt.Sprintf("%d workflow instances currently have active timers", activeTimerInstances))
	}
	if revisionedInstances > 0 {
		appendWarning(fmt.Sprintf("%d workflow instances have revision_count > 0", revisionedInstances))
	}
	out["warnings"] = warnings

	driftRows, err := db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(v.slug, ''), v.id::text), COALESCE(v.stage, ''), COALESCE(wi.current_stage, '')
		FROM verticals v
		JOIN workflow_instances wi ON wi.instance_id = v.id
		WHERE COALESCE(v.stage, '') <> COALESCE(wi.current_stage, '')
		ORDER BY wi.updated_at DESC
		LIMIT 10
	`)
	if err == nil {
		defer driftRows.Close()
		items := make([]map[string]any, 0, 10)
		for driftRows.Next() {
			var label, dbStage, workflowStage string
			if err := driftRows.Scan(&label, &dbStage, &workflowStage); err != nil {
				continue
			}
			items = append(items, map[string]any{
				"vertical":       strings.TrimSpace(label),
				"db_stage":       strings.TrimSpace(dbStage),
				"workflow_stage": strings.TrimSpace(workflowStage),
			})
		}
		out["drift_preview"] = items
	}

	staleRows, err := db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(v.slug, ''), v.id::text), wi.current_stage, wi.entered_stage_at
		FROM workflow_instances wi
		LEFT JOIN verticals v ON v.id = wi.instance_id
		WHERE wi.entered_stage_at <= now() - interval '24 hours'
		  AND wi.current_stage NOT IN ('killed', 'winding_down', 'operating', 'expanding')
		ORDER BY wi.entered_stage_at ASC
		LIMIT 10
	`)
	if err == nil {
		defer staleRows.Close()
		items := make([]map[string]any, 0, 10)
		for staleRows.Next() {
			var label, stage string
			var entered time.Time
			if err := staleRows.Scan(&label, &stage, &entered); err != nil {
				continue
			}
			items = append(items, map[string]any{
				"vertical":         strings.TrimSpace(label),
				"stage":            strings.TrimSpace(stage),
				"entered_stage_at": entered.UTC(),
				"age":              compactAge(now.UTC().Sub(entered.UTC())),
			})
		}
		out["stale_preview"] = items
	}

	return out
}
