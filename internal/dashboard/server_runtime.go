package dashboard

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	postgres := map[string]any{}
	var activeConnections, maxConnections int
	var commits int64
	var blksHit, blksRead int64
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()`).Scan(&activeConnections)
	_ = s.db.QueryRowContext(ctx, `SELECT setting::int FROM pg_settings WHERE name = 'max_connections'`).Scan(&maxConnections)
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(xact_commit,0), COALESCE(blks_hit,0), COALESCE(blks_read,0)
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(&commits, &blksHit, &blksRead)
	postgres["active_connections"] = activeConnections
	postgres["max_connections"] = maxConnections
	postgres["xact_commit"] = commits
	postgres["blks_hit"] = blksHit
	postgres["blks_read"] = blksRead

	spend := map[string]any{}
	var api24h, api7d, infra24h, spend24h int64
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(api_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '1 day'`).Scan(&api24h)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(api_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '7 days'`).Scan(&api7d)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(infra_cost_cents),0) FROM vertical_metrics WHERE period_end >= current_date - interval '1 day'`).Scan(&infra24h)
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(amount_cents),0) FROM spend_ledger WHERE created_at >= now() - interval '24 hours'`).Scan(&spend24h)
	spend["api_cost_24h_cents"] = api24h
	spend["api_cost_daily_avg_7d_cents"] = api7d / 7
	spend["infra_cost_24h_cents"] = infra24h
	spend["spend_ledger_24h_cents"] = spend24h

	auth := map[string]any{}
	auth["oauth_token_configured"] = strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")) != ""
	var authErr1h, authErr24h int
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM agent_turns
		WHERE created_at >= now() - interval '1 hour'
		  AND lower(COALESCE(error, '')) LIKE '%not logged in%'
	`).Scan(&authErr1h)
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM agent_turns
		WHERE created_at >= now() - interval '24 hours'
		  AND lower(COALESCE(error, '')) LIKE '%not logged in%'
	`).Scan(&authErr24h)
	auth["auth_errors_1h"] = authErr1h
	auth["auth_errors_24h"] = authErr24h

	runtimeHealth := map[string]any{}
	if s.manager != nil {
		runtimeHealth["running"] = s.manager.IsRunning()
		runtimeHealth["loaded_agents"] = s.manager.Count()
	} else {
		runtimeHealth["running"] = false
		runtimeHealth["loaded_agents"] = 0
	}

	verticalHealth := make([]map[string]any, 0, 100)
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			v.id::text,
			COALESCE(v.slug, ''),
			v.stage,
			COALESCE(d.environment, ''),
			COALESCE(d.status, ''),
			COALESCE(d.health_status, 'unknown'),
			d.last_health_at,
			COALESCE(d.url, '')
		FROM verticals v
		LEFT JOIN LATERAL (
			SELECT environment, status, health_status, last_health_at, url
			FROM deployments d
			WHERE d.vertical_id = v.id
			ORDER BY d.created_at DESC
			LIMIT 1
		) d ON true
		WHERE v.stage IN ('building', 'pre_launch', 'launched', 'operating', 'expanding')
		ORDER BY v.updated_at DESC
		LIMIT 100
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, slug, stage, env, depStatus, healthStatus, url string
			var lastHealth sql.NullTime
			if err := rows.Scan(&id, &slug, &stage, &env, &depStatus, &healthStatus, &lastHealth, &url); err != nil {
				continue
			}
			row := map[string]any{
				"vertical_id":   id,
				"slug":          slug,
				"stage":         stage,
				"environment":   env,
				"deploy_status": depStatus,
				"health_status": healthStatus,
				"url":           url,
			}
			if lastHealth.Valid {
				row["last_health_at"] = lastHealth.Time
			}
			verticalHealth = append(verticalHealth, row)
		}
	}

	containers, dockerErr := dockerContainers(ctx)
	contractSummary := dashboardContractSummary()
	workflowVersion := ""
	if workflow, ok := contractSummary["workflow"].(map[string]any); ok {
		workflowVersion = strings.TrimSpace(asString(workflow["version"]))
	}
	out := map[string]any{
		"generated_at":    s.now().UTC(),
		"postgres":        postgres,
		"spend":           spend,
		"auth":            auth,
		"runtime":         runtimeHealth,
		"containers":      containers,
		"contracts":       contractSummary,
		"workflow_audit":  workflowAuditSummary(ctx, s.db, workflowVersion, s.now()),
		"vertical_health": verticalHealth,
	}
	if dockerErr != nil {
		out["container_error"] = dockerErr.Error()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePipelineHealth(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	ctx := r.Context()

	campaigns := map[string]any{
		"active":    0,
		"paused":    0,
		"completed": 0,
	}
	campaignRows, err := s.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM scan_campaigns
		GROUP BY status
	`)
	if err == nil {
		defer campaignRows.Close()
		for campaignRows.Next() {
			var status string
			var count int
			if err := campaignRows.Scan(&status, &count); err != nil {
				continue
			}
			switch strings.TrimSpace(status) {
			case "active":
				campaigns["active"] = count
			case "paused":
				campaigns["paused"] = count
			case "completed":
				campaigns["completed"] = count
			}
		}
	}

	validations := map[string]any{
		"active":   0,
		"packaged": 0,
		"parked":   0,
		"rejected": 0,
		"approved": 0,
	}
	var validationActive, validationPackaged, validationParked, validationRejected, validationApproved int
	_ = s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE stage IN ('researching','mvp_speccing','cto_spec_review','branding')) AS active,
			COUNT(*) FILTER (WHERE stage = 'ready_for_review') AS packaged,
			COUNT(*) FILTER (WHERE stage = 'marginal_review') AS parked,
			COUNT(*) FILTER (WHERE stage = 'killed') AS rejected,
			COUNT(*) FILTER (WHERE stage IN ('approved','full_speccing','building','pre_launch','launched','operating','expanding')) AS approved
		FROM verticals
	`).Scan(&validationActive, &validationPackaged, &validationParked, &validationRejected, &validationApproved)
	validations["active"] = validationActive
	validations["packaged"] = validationPackaged
	validations["parked"] = validationParked
	validations["rejected"] = validationRejected
	validations["approved"] = validationApproved

	scans := map[string]any{
		"active":             campaigns["active"],
		"timed_out_last_24h": 0,
	}
	var scanTimeouts int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM pipeline_transitions
		WHERE pipeline_type = 'scan'
		  AND created_at >= now() - interval '24 hours'
		  AND (
			(action = 'error' AND lower(COALESCE(error, '')) LIKE '%timeout%')
			OR
			(action = 'dropped' AND lower(COALESCE(drop_reason, '')) LIKE '%timeout%')
		  )
	`).Scan(&scanTimeouts)
	scans["timed_out_last_24h"] = scanTimeouts

	marginals := map[string]any{
		"parked":      validationParked,
		"oldest_days": 0,
	}
	var marginalOldestDays int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - COALESCE(parked_at, updated_at))) / 86400), 0)::int
		FROM verticals
		WHERE stage = 'marginal_review'
	`).Scan(&marginalOldestDays)
	marginals["oldest_days"] = marginalOldestDays

	alerts := make([]string, 0, 32)
	validationAlerts, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), id::text), stage, updated_at
		FROM verticals
		WHERE stage IN ('researching','mvp_speccing','cto_spec_review','branding','ready_for_review')
		  AND updated_at <= now() - interval '2 hours'
		ORDER BY updated_at ASC
		LIMIT 25
	`)
	if err == nil {
		defer validationAlerts.Close()
		for validationAlerts.Next() {
			var verticalLabel, stage string
			var updatedAt time.Time
			if err := validationAlerts.Scan(&verticalLabel, &stage, &updatedAt); err != nil {
				continue
			}
			alerts = append(alerts, fmt.Sprintf(
				"validation %s: no transition in %s (stage=%s)",
				strings.TrimSpace(verticalLabel),
				compactAge(s.now().UTC().Sub(updatedAt.UTC())),
				strings.TrimSpace(stage),
			))
		}
	}
	campaignAlerts, err := s.db.QueryContext(ctx, `
		SELECT id::text, status, COALESCE(completed_at, started_at, created_at) AS activity_at
		FROM scan_campaigns
		WHERE (status = 'paused' AND COALESCE(started_at, created_at) <= now() - interval '6 hours')
		   OR (status = 'active' AND COALESCE(started_at, created_at) <= now() - interval '90 minutes')
		ORDER BY COALESCE(started_at, created_at) ASC
		LIMIT 25
	`)
	if err == nil {
		defer campaignAlerts.Close()
		for campaignAlerts.Next() {
			var id, status string
			var updatedAt time.Time
			if err := campaignAlerts.Scan(&id, &status, &updatedAt); err != nil {
				continue
			}
			reason := "running too long"
			if status == "paused" {
				reason = "paused"
			}
			alerts = append(alerts, fmt.Sprintf(
				"campaign %s: %s for %s",
				strings.TrimSpace(id),
				reason,
				compactAge(s.now().UTC().Sub(updatedAt.UTC())),
			))
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"campaigns":    campaigns,
		"validations":  validations,
		"scans":        scans,
		"marginals":    marginals,
		"alerts":       alerts,
		"generated_at": s.now().UTC(),
	})
}
