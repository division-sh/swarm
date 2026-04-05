package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
)

func (e *Executor) execHumanTaskRequest(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	cfg := e.cfg
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	var in struct {
		EntityID        string `json:"entity_id"`
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

	entityID := strings.TrimSpace(coalesce(in.EntityID, actor.EffectiveEntityID()))
	in.EntityID = entityID
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
	if cfg != nil && len(cfg.Budget().HumanTasks.CategoriesEnabled) > 0 {
		enabled := false
		for _, c := range cfg.Budget().HumanTasks.CategoriesEnabled {
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
	if err := insertHumanTaskMailboxSpec(ctx, db, actor.ID, in.EntityID, in.Category, in.Description, talkingJSON, in.ExpectedValue, in.Priority, deadline, &taskID); err != nil {
		return nil, err
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
	if !runtimeauthority.ProviderOrNoop(e.authority).CanDecideHumanTasks(actor.Role) {
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
	switch in.Decision {
	case "approve", "approved":
		newStatus = "approved"
	case "reject", "rejected":
		newStatus = "rejected"
	case "defer", "deferred":
		newStatus = "deferred"
	default:
		return nil, fmt.Errorf("unknown decision: %s", in.Decision)
	}

	if newStatus == "approved" && cfg != nil && cfg.Budget().HumanTasks.MaxTasksPerWeek > 0 {
		var requeueCount int
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE((payload->>'requeue_count')::int, 0)
			FROM mailbox
			WHERE item_id = $1::uuid
			  AND item_type = 'human_task'
		`, in.TaskID).Scan(&requeueCount); err != nil {
			return nil, fmt.Errorf("load human task requeue count: %w", err)
		}
		if requeueCount > 0 {
			goto skipBudget
		}
		weekStart := WeekStartUTC(time.Now(), cfg.Budget().HumanTasks.BudgetReset)
		var approvedThisWeek int
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(count(*), 0)
			FROM mailbox
			WHERE item_type = 'human_task'
			  AND decided_at >= $1
			  AND decision IN ('approved', 'assigned', 'completed')
		`, weekStart).Scan(&approvedThisWeek); err != nil {
			return nil, fmt.Errorf("count approved human tasks this week: %w", err)
		}
		if approvedThisWeek >= cfg.Budget().HumanTasks.MaxTasksPerWeek {
			newStatus = "deferred"
			if in.Reason == "" {
				in.Reason = "weekly human task budget exhausted"
			} else {
				in.Reason = "weekly human task budget exhausted: " + in.Reason
			}
			if in.RequeueDate == "" {
				in.RequeueDate = NextWeekResetUTC(time.Now(), cfg.Budget().HumanTasks.BudgetReset).Format(time.RFC3339)
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

	if err := updateHumanTaskMailboxSpec(ctx, db, in.TaskID, newStatus, actor.ID, decisionJSON, nil, nil); err != nil {
		return nil, err
	}

	return map[string]any{
		"task_id": in.TaskID,
		"status":  newStatus,
	}, nil
}

func insertHumanTaskMailboxSpec(ctx context.Context, db *sql.DB, actorID, entityID, category, description string, talkingJSON []byte, expectedValue, priority string, deadline sql.NullTime, taskID *string) error {
	payload := map[string]any{
		"category":       strings.TrimSpace(category),
		"description":    strings.TrimSpace(description),
		"expected_value": strings.TrimSpace(expectedValue),
		"priority":       strings.TrimSpace(priority),
		"requeue_count":  0,
	}
	if len(talkingJSON) > 0 && string(talkingJSON) != "null" {
		var talking any
		if json.Unmarshal(talkingJSON, &talking) == nil {
			payload["talking_points"] = talking
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("insert human task: marshal payload: %w", err)
	}
	var expiresAt any
	if deadline.Valid {
		expiresAt = deadline.Time
	}
	return db.QueryRowContext(ctx, `
		INSERT INTO mailbox (
			item_id, entity_id, flow_instance, scope, item_type, from_agent,
			severity, summary, payload, status, notified, expires_at, created_at
		)
		VALUES (
			gen_random_uuid(), NULLIF($1,'')::uuid, NULL,
			CASE WHEN NULLIF($1,'') IS NULL THEN 'global' ELSE 'entity' END,
			'human_task', $2, $3, $4, $5::jsonb, 'pending', false, $6, now()
		)
		RETURNING item_id::text
	`, entityID, actorID, humanTaskSeverity(priority), description, string(payloadJSON), expiresAt).Scan(taskID)
}

func updateHumanTaskMailboxSpec(ctx context.Context, db *sql.DB, taskID, newStatus, actorID string, decisionJSON []byte, requestingAgent, entityID *string) error {
	return db.QueryRowContext(ctx, `
		UPDATE mailbox
		SET status = CASE WHEN $2 = 'timed_out' THEN 'expired' ELSE 'decided' END,
		    decision = $2,
		    decision_notes = COALESCE(NULLIF(($3::jsonb)->>'reason', ''), decision_notes),
		    decided_by = NULLIF($4,''),
		    decided_at = now(),
		    payload = CASE
				WHEN $2 = 'deferred' THEN
					jsonb_set(
						COALESCE(payload, '{}'::jsonb),
						'{requeue_count}',
						to_jsonb(COALESCE((payload->>'requeue_count')::int, 0) + 1),
						true
					)
				ELSE payload
			END
		WHERE item_id = $1::uuid
		  AND item_type = 'human_task'
		RETURNING COALESCE(from_agent, ''), COALESCE(entity_id::text, '')
	`, taskID, newStatus, string(decisionJSON), actorID).Scan(requestingAgent, entityID)
}

func humanTaskSeverity(priority string) string {
	switch strings.TrimSpace(strings.ToLower(priority)) {
	case "critical":
		return "critical"
	case "high", "urgent":
		return "urgent"
	default:
		return "normal"
	}
}
