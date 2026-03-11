package dashboard

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"github.com/google/uuid"
)

func (s *Server) handleMailbox(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 100), 1, 500)

	var pending, critical, decided int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM mailbox WHERE status = 'pending'`).Scan(&pending)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM mailbox WHERE status = 'pending' AND priority = 'critical'`).Scan(&critical)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM mailbox WHERE status <> 'pending'`).Scan(&decided)

	where := "1=1"
	args := []any{}
	if status != "" && status != "all" {
		where = "m.status = $1"
		args = append(args, status)
	}
	args = append(args, limit)
	q := fmt.Sprintf(`
		SELECT
			m.id::text,
			COALESCE(m.event_id::text, ''),
			COALESCE(m.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(m.from_agent, ''),
			m.type,
			COALESCE(m.priority, 'normal'),
			COALESCE(m.status, ''),
			COALESCE(m.summary, ''),
			COALESCE(m.decision, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.created_at, now()),
			m.decided_at,
			COALESCE((extract(epoch from (COALESCE(m.decided_at, now()) - COALESCE(m.created_at, now())) ) / 60)::bigint, 0) AS response_minutes
		FROM mailbox m
		LEFT JOIN verticals v ON v.id = m.vertical_id
		WHERE %s
		ORDER BY m.created_at DESC
		LIMIT $%d
	`, where, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var id, eventID, verticalID, verticalSlug, fromAgent, typ, priority, stat, summary, decision, notes string
		var created time.Time
		var decided sql.NullTime
		var responseMin int64
		if err := rows.Scan(&id, &eventID, &verticalID, &verticalSlug, &fromAgent, &typ, &priority, &stat, &summary, &decision, &notes, &created, &decided, &responseMin); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		item := map[string]any{
			"id":               id,
			"event_id":         eventID,
			"vertical_id":      verticalID,
			"vertical_slug":    verticalSlug,
			"from_agent":       fromAgent,
			"type":             typ,
			"priority":         priority,
			"status":           stat,
			"summary":          summary,
			"decision":         decision,
			"decision_notes":   notes,
			"created_at":       created,
			"response_minutes": responseMin,
		}
		if decided.Valid {
			item["decided_at"] = decided.Time
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"summary": map[string]any{
			"pending":  pending,
			"critical": critical,
			"decided":  decided,
		},
		"items": items,
	})
}

type taskView struct {
	ID              string     `json:"id"`
	RequestingAgent string     `json:"requesting_agent"`
	VerticalID      string     `json:"vertical_id"`
	VerticalSlug    string     `json:"vertical_slug"`
	Category        string     `json:"category"`
	Description     string     `json:"description"`
	Priority        string     `json:"priority"`
	Status          string     `json:"status"`
	AssignedTo      string     `json:"assigned_to"`
	Deadline        *time.Time `json:"deadline,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	ReviewedAt      *time.Time `json:"reviewed_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	Result          string     `json:"result,omitempty"`
	Outcome         string     `json:"outcome,omitempty"`
	FollowUpNeeded  bool       `json:"follow_up_needed"`
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 200), 1, 1000)
	if status == "" {
		status = "open"
	}

	where := "1=1"
	args := []any{}
	if status != "all" && status != "open" {
		where = "t.status = $1"
		args = append(args, status)
	} else if status == "open" {
		where = "t.status IN ('pending_review', 'approved', 'assigned')"
	}
	args = append(args, limit)

	q := fmt.Sprintf(`
		SELECT
			t.id::text,
			t.requesting_agent,
			COALESCE(t.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			t.category,
			t.description,
			COALESCE(t.priority, 'medium'),
			t.status,
			COALESCE(t.assigned_to, ''),
			t.deadline,
			COALESCE(t.created_at, now()),
			t.reviewed_at,
			t.completed_at,
			COALESCE(t.result, ''),
			COALESCE(t.outcome, ''),
			COALESCE(t.follow_up_needed, false)
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE %s
		ORDER BY t.created_at DESC
		LIMIT $%d
	`, where, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	out := make([]taskView, 0, limit)
	for rows.Next() {
		var tv taskView
		var deadline sql.NullTime
		var reviewed sql.NullTime
		var completed sql.NullTime
		if err := rows.Scan(
			&tv.ID,
			&tv.RequestingAgent,
			&tv.VerticalID,
			&tv.VerticalSlug,
			&tv.Category,
			&tv.Description,
			&tv.Priority,
			&tv.Status,
			&tv.AssignedTo,
			&deadline,
			&tv.CreatedAt,
			&reviewed,
			&completed,
			&tv.Result,
			&tv.Outcome,
			&tv.FollowUpNeeded,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if deadline.Valid {
			t := deadline.Time
			tv.Deadline = &t
		}
		if reviewed.Valid {
			t := reviewed.Time
			tv.ReviewedAt = &t
		}
		if completed.Valid {
			t := completed.Time
			tv.CompletedAt = &t
		}
		out = append(out, tv)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	weekStart := runtime.WeekStartUTC(s.now(), "")
	maxPerWeek := 0
	resetDay := "monday"
	if s.cfg != nil {
		if strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset) != "" {
			resetDay = strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset)
		}
		weekStart = runtime.WeekStartUTC(s.now(), resetDay)
		maxPerWeek = s.cfg.Budget().HumanTasks.MaxTasksPerWeek
	}
	var approvedThisWeek int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(count(*), 0)
		FROM human_tasks
		WHERE reviewed_at >= $1
		  AND status IN ('approved', 'assigned', 'completed')
	`, weekStart).Scan(&approvedThisWeek)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"status":       status,
		"tasks":        out,
		"weekly_budget": map[string]any{
			"reset_day":          resetDay,
			"week_start_utc":     weekStart.UTC().Format(time.RFC3339),
			"max_tasks_per_week": maxPerWeek,
			"approved_this_week": approvedThisWeek,
		},
	})
}

func (s *Server) handleTaskDetail(w http.ResponseWriter, r *http.Request) {
	prefix := "/dashboard/api/tasks/"
	if strings.HasPrefix(r.URL.Path, "/api/tasks/") {
		prefix = "/api/tasks/"
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(path, "/")
	taskID := strings.TrimSpace(parts[0])
	if taskID == "" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handleTaskView(w, r, taskID)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch strings.TrimSpace(parts[1]) {
		case "claim":
			s.handleTaskClaim(w, r, taskID)
			return
		case "complete":
			s.handleTaskComplete(w, r, taskID)
			return
		case "reject":
			s.handleTaskReject(w, r, taskID)
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) handleTaskReject(w http.ResponseWriter, r *http.Request, taskID string) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	ctx := r.Context()
	var req struct {
		Reason string `json:"reason"`
	}
	_ = decodeJSONBody(r, &req)
	req.Reason = strings.TrimSpace(req.Reason)

	requeueAt := runtime.NextWeekResetUTC(s.now(), func() string {
		if s.cfg != nil && strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset) != "" {
			return strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset)
		}
		return "monday"
	}()).UTC().Format(time.RFC3339)

	decisionObj := map[string]any{
		"decision":      "deferred",
		"defer_reason":  "human_pushback",
		"human_reason":  req.Reason,
		"requeue_date":  requeueAt,
		"decided_by":    "human",
		"decided_at":    s.now().UTC().Format(time.RFC3339),
		"priority_rank": 0,
	}
	decisionJSON, _ := json.Marshal(decisionObj)

	var requestingAgent string
	var verticalID string
	err := s.db.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'deferred',
		    reviewed_at = now(),
		    review_decision = $2::jsonb,
		    requeue_count = COALESCE(requeue_count, 0) + 1,
		    assigned_to = NULL
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, string(decisionJSON)).Scan(&requestingAgent, &verticalID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("reject task: %w", err))
		return
	}

	if s.eventStore != nil {
		_ = s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("human_task.deferred"),
			SourceAgent: "dashboard",
			VerticalID:  verticalID,
			Payload: mustJSON(map[string]any{
				"task_id":          taskID,
				"requesting_agent": strings.TrimSpace(requestingAgent),
				"vertical_id":      strings.TrimSpace(verticalID),
				"defer_reason":     "human_pushback",
				"human_reason":     req.Reason,
				"requeue_date":     requeueAt,
			}),
			CreatedAt: s.now(),
		}, []string{strings.TrimSpace(requestingAgent), "empire-coordinator"})
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "task_id": taskID, "status": "deferred", "requeue_date": requeueAt})
}

func (s *Server) handleTaskStats(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	// Weekly budget summary.
	resetDay := "monday"
	maxPerWeek := 0
	if s.cfg != nil {
		if strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset) != "" {
			resetDay = strings.TrimSpace(s.cfg.Budget().HumanTasks.BudgetReset)
		}
		maxPerWeek = s.cfg.Budget().HumanTasks.MaxTasksPerWeek
	}
	weekStart := runtime.WeekStartUTC(s.now(), resetDay)
	var approvedThisWeek int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(count(*), 0)
		FROM human_tasks
		WHERE reviewed_at >= $1
		  AND status IN ('approved', 'assigned', 'completed')
	`, weekStart).Scan(&approvedThisWeek)

	// 30d stats by category/outcome.
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			category,
			COALESCE(outcome, ''),
			count(*)
		FROM human_tasks
		WHERE created_at >= now() - interval '30 days'
		GROUP BY category, COALESCE(outcome, '')
		ORDER BY category ASC, COALESCE(outcome,'') ASC
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	byCategory := map[string]map[string]int{}
	for rows.Next() {
		var category, outcome string
		var n int
		if err := rows.Scan(&category, &outcome, &n); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		category = strings.TrimSpace(category)
		outcome = strings.TrimSpace(outcome)
		if category == "" {
			category = "unknown"
		}
		if outcome == "" {
			outcome = "none"
		}
		if byCategory[category] == nil {
			byCategory[category] = map[string]int{}
		}
		byCategory[category][outcome] = n
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"window":       "30d",
		"by_category":  byCategory,
		"weekly_budget": map[string]any{
			"reset_day":          resetDay,
			"week_start_utc":     weekStart.UTC().Format(time.RFC3339),
			"max_tasks_per_week": maxPerWeek,
			"approved_this_week": approvedThisWeek,
		},
	})
}

func (s *Server) handleTaskView(w http.ResponseWriter, r *http.Request, taskID string) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	var tv taskView
	var deadline sql.NullTime
	var reviewed sql.NullTime
	var completed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT
			t.id::text,
			t.requesting_agent,
			COALESCE(t.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			t.category,
			t.description,
			COALESCE(t.priority, 'medium'),
			t.status,
			COALESCE(t.assigned_to, ''),
			t.deadline,
			COALESCE(t.created_at, now()),
			t.reviewed_at,
			t.completed_at,
			COALESCE(t.result, ''),
			COALESCE(t.outcome, ''),
			COALESCE(t.follow_up_needed, false)
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE t.id = $1::uuid
	`, taskID).Scan(
		&tv.ID,
		&tv.RequestingAgent,
		&tv.VerticalID,
		&tv.VerticalSlug,
		&tv.Category,
		&tv.Description,
		&tv.Priority,
		&tv.Status,
		&tv.AssignedTo,
		&deadline,
		&tv.CreatedAt,
		&reviewed,
		&completed,
		&tv.Result,
		&tv.Outcome,
		&tv.FollowUpNeeded,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, fmt.Errorf("task not found: %s", taskID))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if deadline.Valid {
		t := deadline.Time
		tv.Deadline = &t
	}
	if reviewed.Valid {
		t := reviewed.Time
		tv.ReviewedAt = &t
	}
	if completed.Valid {
		t := completed.Time
		tv.CompletedAt = &t
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": tv, "generated_at": s.now().UTC()})
}

func (s *Server) handleTaskClaim(w http.ResponseWriter, r *http.Request, taskID string) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	ctx := r.Context()
	var req struct {
		AssignedTo string `json:"assigned_to"`
	}
	_ = decodeJSONBody(r, &req)
	req.AssignedTo = strings.TrimSpace(req.AssignedTo)
	if req.AssignedTo == "" {
		req.AssignedTo = strings.TrimSpace(os.Getenv("EMPIREAI_HUMAN_ID"))
	}
	if req.AssignedTo == "" {
		req.AssignedTo = "founder"
	}

	var requestingAgent string
	var verticalID string
	err := s.db.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'assigned',
		    assigned_to = $2
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, req.AssignedTo).Scan(&requestingAgent, &verticalID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("claim task: %w", err))
		return
	}

	if s.eventStore != nil {
		_ = s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("human_task.assigned"),
			SourceAgent: "dashboard",
			VerticalID:  verticalID,
			Payload: mustJSON(map[string]any{
				"task_id":          taskID,
				"requesting_agent": strings.TrimSpace(requestingAgent),
				"vertical_id":      strings.TrimSpace(verticalID),
				"assigned_to":      req.AssignedTo,
			}),
			CreatedAt: s.now(),
		}, []string{strings.TrimSpace(requestingAgent)})
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "task_id": taskID, "status": "assigned", "assigned_to": req.AssignedTo})
}

func (s *Server) handleTaskComplete(w http.ResponseWriter, r *http.Request, taskID string) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	ctx := r.Context()
	var req struct {
		ResultText     string `json:"result_text"`
		Result         string `json:"result"`
		Outcome        string `json:"outcome"`
		FollowUpNeeded *bool  `json:"follow_up_needed"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	result := strings.TrimSpace(coalesce(req.ResultText, req.Result))
	outcome := strings.TrimSpace(req.Outcome)
	follow := false
	if req.FollowUpNeeded != nil {
		follow = *req.FollowUpNeeded
	}
	if result == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("result_text is required"))
		return
	}
	if outcome == "" {
		outcome = "success"
	}

	var requestingAgent string
	var verticalID string
	err := s.db.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'completed',
		    result = $2,
		    outcome = $3,
		    follow_up_needed = $4,
		    completed_at = now()
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, result, outcome, follow).Scan(&requestingAgent, &verticalID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("complete task: %w", err))
		return
	}

	if s.eventStore != nil {
		_ = s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("human_task.completed"),
			SourceAgent: "dashboard",
			VerticalID:  verticalID,
			Payload: mustJSON(map[string]any{
				"task_id":          taskID,
				"requesting_agent": strings.TrimSpace(requestingAgent),
				"vertical_id":      strings.TrimSpace(verticalID),
				"result_text":      result,
				"outcome":          outcome,
				"follow_up_needed": follow,
			}),
			CreatedAt: s.now(),
		}, []string{strings.TrimSpace(requestingAgent)})
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "task_id": taskID, "status": "completed"})
}
