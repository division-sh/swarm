package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	runtimepipeline "empireai/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func (s *Server) handlePipelineShards(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	ctx := r.Context()

	var shardsTable bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&shardsTable); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !shardsTable {
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": s.now().UTC(),
			"scans":        []map[string]any{},
		})
		return
	}

	limit := clamp(parseInt(r.URL.Query().Get("limit"), 25), 1, 200)
	rows, err := s.db.QueryContext(ctx, `
		WITH scan_rollup AS (
			SELECT
				scan_id::text AS scan_id,
				COALESCE(MAX(scope->>'mode'), '') AS mode,
				COALESCE(MAX(scope->>'geography'), '') AS geography,
				COUNT(*) AS shards_total,
				COUNT(*) FILTER (WHERE status = 'pending') AS shards_pending,
				COUNT(*) FILTER (WHERE status = 'assigned') AS shards_assigned,
				COUNT(*) FILTER (WHERE status = 'completed') AS shards_completed,
				COUNT(*) FILTER (WHERE status IN ('failed', 'timed_out')) AS shards_failed,
				COUNT(*) FILTER (
					WHERE status = 'assigned'
					  AND (
						deadline_at <= now()
						OR COALESCE(assigned_at, created_at) <= now() - interval '10 minutes'
					  )
				) AS shards_stuck,
				COALESCE(SUM(spend_cents), 0) AS spend_cents,
				MAX(COALESCE(completed_at, assigned_at, created_at)) AS updated_at
			FROM shards
			GROUP BY scan_id
		)
		SELECT
			scan_id,
			mode,
			geography,
			shards_total,
			shards_pending,
			shards_assigned,
			shards_completed,
			shards_failed,
			shards_stuck,
			spend_cents,
			updated_at
		FROM scan_rollup
		ORDER BY updated_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	scans := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			scanID, mode, geography                            string
			total, pending, assigned, completed, failed, stuck int
			spendCents                                         int
			updatedAt                                          time.Time
		)
		if err := rows.Scan(
			&scanID,
			&mode,
			&geography,
			&total,
			&pending,
			&assigned,
			&completed,
			&failed,
			&stuck,
			&spendCents,
			&updatedAt,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		progress := 0.0
		if total > 0 {
			progress = float64(completed+failed) / float64(total)
		}
		scans = append(scans, map[string]any{
			"scan_id":          scanID,
			"mode":             mode,
			"geography":        geography,
			"shards_total":     total,
			"shards_pending":   pending,
			"shards_assigned":  assigned,
			"shards_completed": completed,
			"shards_failed":    failed,
			"shards_stuck":     stuck,
			"spend_cents":      spendCents,
			"progress":         progress,
			"updated_at":       updatedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"scans":        scans,
	})
}

func (s *Server) handlePipelineShardDetail(w http.ResponseWriter, r *http.Request) {
	prefix := "/dashboard/api/pipeline/shards/"
	if strings.HasPrefix(r.URL.Path, "/api/pipeline/shards/") {
		prefix = "/api/pipeline/shards/"
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handlePipelineShardScanDetail(w, r, strings.TrimSpace(parts[0]))
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		shardID := strings.TrimSpace(parts[0])
		switch strings.TrimSpace(parts[1]) {
		case "retry":
			s.handlePipelineShardAction(w, r, shardID, "retry")
			return
		case "cancel":
			s.handlePipelineShardAction(w, r, shardID, "cancel")
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) handlePipelineShardScanDetail(w http.ResponseWriter, r *http.Request, scanID string) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	if _, err := uuid.Parse(scanID); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid scan id: %s", scanID))
		return
	}
	ctx := r.Context()
	ok, err := s.shardsTableAvailable(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": s.now().UTC(),
			"scan_id":      scanID,
			"shards":       []map[string]any{},
		})
		return
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			id::text,
			stage,
			shard_index,
			shard_count,
			shard_key,
			scope,
			status,
			COALESCE(agent_id, ''),
			retry_count,
			budget_cents,
			spend_cents,
			deadline_at,
			assigned_at,
			completed_at,
			COALESCE(error, ''),
			created_at
		FROM shards
		WHERE scan_id = $1::uuid
		ORDER BY shard_index ASC, created_at ASC
	`, scanID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	now := s.now().UTC()
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		var (
			id, stage, key, status, agentID, errTx string
			index, count, retries                  int
			budgetCents, spendCents                int
			scopeRaw                               []byte
			deadlineAt, createdAt                  time.Time
			assignedAt, completedAt                *time.Time
		)
		if err := rows.Scan(
			&id,
			&stage,
			&index,
			&count,
			&key,
			&scopeRaw,
			&status,
			&agentID,
			&retries,
			&budgetCents,
			&spendCents,
			&deadlineAt,
			&assignedAt,
			&completedAt,
			&errTx,
			&createdAt,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		var scope map[string]any
		_ = json.Unmarshal(scopeRaw, &scope)

		stuckState := ""
		if strings.EqualFold(strings.TrimSpace(status), "assigned") {
			if deadlineAt.Before(now) || deadlineAt.Equal(now) {
				stuckState = "critical"
			} else {
				startAt := createdAt
				if assignedAt != nil && !assignedAt.IsZero() {
					startAt = assignedAt.UTC()
				}
				if startAt.Before(now.Add(-10 * time.Minute)) {
					stuckState = "warning"
				}
			}
		}

		updatedAt := createdAt.UTC()
		if assignedAt != nil && !assignedAt.IsZero() && assignedAt.UTC().After(updatedAt) {
			updatedAt = assignedAt.UTC()
		}
		if completedAt != nil && !completedAt.IsZero() && completedAt.UTC().After(updatedAt) {
			updatedAt = completedAt.UTC()
		}

		durationMS := int64(0)
		if assignedAt != nil && !assignedAt.IsZero() {
			end := now
			if completedAt != nil && !completedAt.IsZero() {
				end = completedAt.UTC()
			}
			if end.After(assignedAt.UTC()) {
				durationMS = end.Sub(assignedAt.UTC()).Milliseconds()
			}
		}
		reportsCount, highSignalCount, statsErr := s.shardEventStats(ctx, stage, scanID, agentID)
		if statsErr != nil {
			writeErr(w, http.StatusInternalServerError, statsErr)
			return
		}

		out = append(out, map[string]any{
			"id":                id,
			"stage":             stage,
			"shard_index":       index,
			"shard_count":       count,
			"shard_key":         key,
			"scope":             scope,
			"status":            status,
			"stuck_state":       stuckState,
			"agent_id":          agentID,
			"retry_count":       retries,
			"budget_cents":      budgetCents,
			"spend_cents":       spendCents,
			"deadline_at":       deadlineAt.UTC(),
			"assigned_at":       assignedAt,
			"completed_at":      completedAt,
			"created_at":        createdAt.UTC(),
			"updated_at":        updatedAt,
			"duration_ms":       durationMS,
			"reports_count":     reportsCount,
			"high_signal_count": highSignalCount,
			"error":             errTx,
		})
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"scan_id":      scanID,
		"shards":       out,
	})
}

func (s *Server) handlePipelineShardAction(w http.ResponseWriter, r *http.Request, shardID, action string) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("database unavailable"))
		return
	}
	if _, err := uuid.Parse(shardID); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid shard id: %s", shardID))
		return
	}
	ctx := r.Context()
	ok, err := s.shardsTableAvailable(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("shards table unavailable"))
		return
	}

	switch action {
	case "retry":
		res, err := s.db.ExecContext(ctx, `
			UPDATE shards
			SET status = 'pending',
			    agent_id = NULL,
			    assigned_at = NULL,
			    completed_at = NULL,
			    error = NULL
			WHERE id = $1::uuid
			  AND status IN ('failed', 'timed_out')
		`, shardID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("shard %s is not retryable (expected status failed or timed_out)", shardID))
			return
		}
	case "cancel":
		res, err := s.db.ExecContext(ctx, `
			UPDATE shards
			SET status = 'failed',
			    completed_at = now(),
			    error = COALESCE(NULLIF(error, ''), 'manual cancel via dashboard')
			WHERE id = $1::uuid
			  AND status IN ('pending', 'assigned')
		`, shardID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("shard %s is not cancelable (expected status pending or assigned)", shardID))
			return
		}
	default:
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unsupported shard action: %s", action))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"shard_id": shardID,
		"action":   action,
	})
}

func (s *Server) shardsTableAvailable(ctx context.Context) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database unavailable")
	}
	var ok bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

func (s *Server) shardEventStats(ctx context.Context, stage, scanID, agentID string) (reportsCount, highSignalCount int, err error) {
	if s == nil || s.db == nil {
		return 0, 0, fmt.Errorf("database unavailable")
	}
	stage = strings.TrimSpace(stage)
	scanID = strings.TrimSpace(scanID)
	agentID = strings.TrimSpace(agentID)
	if stage == "" || scanID == "" || agentID == "" {
		return 0, 0, nil
	}
	eventType := shardCompletionEventType(stage)
	if eventType == "" {
		return 0, 0, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE source_agent = $1
		  AND type = $2
		  AND COALESCE(payload->>'scan_id', '') = $3
		  AND COALESCE(payload->'shard'->>'terminal', 'false') <> 'true'
	`, agentID, eventType, scanID)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var payloadRaw []byte
		if err := rows.Scan(&payloadRaw); err != nil {
			return 0, 0, err
		}
		payload := map[string]any{}
		if len(payloadRaw) > 0 {
			_ = json.Unmarshal(payloadRaw, &payload)
		}
		if n := int(math.Round(asFloatAny(payload["reports_count"]))); n > 0 {
			reportsCount += n
		} else {
			reportsCount++
		}
		if n := int(math.Round(asFloatAny(payload["high_signal_count"]))); n > 0 {
			highSignalCount += n
			continue
		}
		if asFloatAny(payload["signal_strength"]) >= 70 {
			highSignalCount++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	return reportsCount, highSignalCount, nil
}

func shardCompletionEventType(stage string) string {
	switch strings.TrimSpace(stage) {
	case runtimepipeline.ShardStageMarketResearch:
		return "market_research.scan_complete"
	case runtimepipeline.ShardStageTrendResearch:
		return "trend_research.scan_complete"
	default:
		return ""
	}
}
