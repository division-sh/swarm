package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

func (e *Executor) execHumanTaskRequest(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	cfg := e.cfg
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
	}

	var in struct {
		VerticalID      string `json:"vertical_id"`
		Category        string `json:"category"`
		Description     string `json:"description"`
		TalkingPoints   any    `json:"talking_points"`
		ExpectedValue   string `json:"expected_value"`
		Priority        string `json:"priority"`
		Deadline        string `json:"deadline"`
		DeadlineAt      string `json:"deadline_at"`
		DeadlineRFC3339 string `json:"deadline_rfc3339"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}

	in.VerticalID = strings.TrimSpace(coalesce(in.VerticalID, actor.VerticalID))
	in.Category = strings.TrimSpace(in.Category)
	in.Description = strings.TrimSpace(in.Description)
	in.ExpectedValue = strings.TrimSpace(in.ExpectedValue)
	in.Priority = strings.TrimSpace(in.Priority)
	deadlineStr := strings.TrimSpace(coalesce(in.Deadline, in.DeadlineAt, in.DeadlineRFC3339))

	if strings.TrimSpace(actor.ID) == "" {
		return nil, errors.New("actor id is required")
	}
	if in.Category == "" {
		return nil, errors.New("category is required")
	}
	if cfg != nil && len(cfg.Budget.HumanTasks.CategoriesEnabled) > 0 {
		enabled := false
		for _, c := range cfg.Budget.HumanTasks.CategoriesEnabled {
			if strings.EqualFold(strings.TrimSpace(c), in.Category) {
				enabled = true
				break
			}
		}
		if !enabled {
			return nil, fmt.Errorf("category %q is not enabled for human tasks", in.Category)
		}
	}
	if in.Description == "" {
		return nil, errors.New("description is required")
	}
	if in.Priority == "" {
		in.Priority = "medium"
	}

	var deadline sql.NullTime
	if deadlineStr != "" {
		t, err := time.Parse(time.RFC3339, deadlineStr)
		if err != nil {
			return nil, fmt.Errorf("invalid deadline (expected RFC3339): %w", err)
		}
		deadline = sql.NullTime{Time: t, Valid: true}
	}

	talkingJSON := []byte("null")
	if in.TalkingPoints != nil {
		if b, err := json.Marshal(in.TalkingPoints); err == nil && len(b) > 0 {
			talkingJSON = b
		}
	}

	var taskID string
	const q = `
		INSERT INTO human_tasks (
			requesting_agent, vertical_id, category, description,
			talking_points, expected_value, priority, deadline, status
		) VALUES (
			$1, NULLIF($2,'')::uuid, $3, $4,
			$5::jsonb, NULLIF($6,''), $7, $8, 'pending_review'
		)
		RETURNING id::text
	`
	if err := db.QueryRowContext(ctx, q,
		actor.ID,
		in.VerticalID,
		in.Category,
		in.Description,
		talkingJSON,
		in.ExpectedValue,
		in.Priority,
		deadline,
	).Scan(&taskID); err != nil {
		return nil, fmt.Errorf("insert human task: %w", err)
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": actor.ID,
		"task_type":        in.Category,
		"description":      in.Description,
	}

	if err := e.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.requested"),
		SourceAgent: actor.ID,
		VerticalID:  in.VerticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"tool-executor",
			"failed to publish human_task.requested task_id=%s actor=%s: %v",
			strings.TrimSpace(taskID),
			strings.TrimSpace(actor.ID),
			err,
		)
	}

	return map[string]any{
		"task_id": taskID,
		"status":  "pending_review",
	}, nil
}

func (e *Executor) execHumanTaskDecide(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	cfg := e.cfg
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
	}
	if actor.Role != "empire-coordinator" {
		return nil, fmt.Errorf("role %s is not authorized to decide human tasks", actor.Role)
	}

	var in struct {
		TaskID       string `json:"task_id"`
		Decision     string `json:"decision"`
		Reason       string `json:"reason"`
		PriorityRank int    `json:"priority_rank"`
		RequeueDate  string `json:"requeue_date"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	in.TaskID = strings.TrimSpace(in.TaskID)
	in.Decision = strings.ToLower(strings.TrimSpace(in.Decision))
	in.Reason = strings.TrimSpace(in.Reason)
	in.RequeueDate = strings.TrimSpace(in.RequeueDate)
	if in.TaskID == "" {
		return nil, errors.New("task_id is required")
	}
	if in.Decision == "" {
		return nil, errors.New("decision is required (approve|reject|defer)")
	}
	var newStatus string
	var evtType events.EventType
	switch in.Decision {
	case "approve", "approved":
		newStatus = "approved"
		evtType = events.EventType("human_task.approved")
	case "reject", "rejected":
		newStatus = "rejected"
		evtType = events.EventType("human_task.rejected")
	case "defer", "deferred":
		newStatus = "deferred"
		evtType = events.EventType("human_task.deferred")
	default:
		return nil, fmt.Errorf("unknown decision: %s", in.Decision)
	}

	if newStatus == "approved" && cfg != nil && cfg.Budget.HumanTasks.MaxTasksPerWeek > 0 {
		var requeueCount int
		_ = db.QueryRowContext(ctx, `SELECT COALESCE(requeue_count, 0) FROM human_tasks WHERE id = $1::uuid`, in.TaskID).Scan(&requeueCount)
		if requeueCount > 0 {
			goto skipBudget
		}
		weekStart := WeekStartUTC(time.Now(), cfg.Budget.HumanTasks.BudgetReset)
		var approvedThisWeek int
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(count(*), 0)
			FROM human_tasks
			WHERE reviewed_at >= $1
			  AND status IN ('approved', 'assigned', 'completed')
		`, weekStart).Scan(&approvedThisWeek); err == nil {
			if approvedThisWeek >= cfg.Budget.HumanTasks.MaxTasksPerWeek {
				newStatus = "deferred"
				evtType = events.EventType("human_task.deferred")
				if in.Reason == "" {
					in.Reason = "weekly human task budget exhausted"
				} else {
					in.Reason = "weekly human task budget exhausted: " + in.Reason
				}
				if in.RequeueDate == "" {
					in.RequeueDate = NextWeekResetUTC(time.Now(), cfg.Budget.HumanTasks.BudgetReset).Format(time.RFC3339)
				}
			}
		}
	}
skipBudget:

	decisionObj := map[string]any{
		"decision":      newStatus,
		"reason":        in.Reason,
		"decided_by":    actor.ID,
		"decided_at":    time.Now().UTC().Format(time.RFC3339),
		"priority_rank": in.PriorityRank,
	}
	if in.RequeueDate != "" {
		decisionObj["requeue_date"] = in.RequeueDate
	}
	decisionJSON, _ := json.Marshal(decisionObj)

	var requestingAgent string
	var verticalID string
	const q = `
		UPDATE human_tasks
		SET status = $2,
		    reviewed_at = now(),
		    review_decision = $3::jsonb
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`
	if err := db.QueryRowContext(ctx, q, in.TaskID, newStatus, decisionJSON).Scan(&requestingAgent, &verticalID); err != nil {
		return nil, fmt.Errorf("update human task decision: %w", err)
	}

	outPayload := map[string]any{
		"task_id":          in.TaskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
	}
	switch string(evtType) {
	case "human_task.approved":
		outPayload["approved_reason"] = in.Reason
		outPayload["priority_rank"] = in.PriorityRank
	case "human_task.rejected":
		outPayload["rejection_reason"] = in.Reason
	case "human_task.deferred":
		outPayload["defer_reason"] = in.Reason
		if in.RequeueDate != "" {
			outPayload["requeue_date"] = in.RequeueDate
		}
	}

	if err := e.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        evtType,
		SourceAgent: actor.ID,
		VerticalID:  verticalID,
		Payload:     mustJSON(outPayload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"tool-executor",
			"failed to publish human task decision event=%s task_id=%s actor=%s: %v",
			strings.TrimSpace(string(evtType)),
			strings.TrimSpace(in.TaskID),
			strings.TrimSpace(actor.ID),
			err,
		)
	}

	return map[string]any{
		"task_id": in.TaskID,
		"status":  newStatus,
	}, nil
}
