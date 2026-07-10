package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func (e *Executor) execHumanTaskRequest(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, err := e.humanTaskStoreDependency()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	cfg := e.cfg
	e.mu.RUnlock()
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

	var deadline time.Time
	if deadlineStr != "" {
		t, err := time.Parse(time.RFC3339, deadlineStr)
		if err != nil {
			return nil, fmt.Errorf("invalid deadline (expected RFC3339): %w", err)
		}
		deadline = t.UTC()
	}

	talkingJSON := json.RawMessage("null")
	if in.TalkingPoints != nil {
		if b, err := json.Marshal(in.TalkingPoints); err == nil && len(b) > 0 {
			talkingJSON = b
		}
	}

	flowInstance := actor.CanonicalFlowPath()
	var sourceEventID string
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		sourceEventID = inbound.ID()
		if target := inbound.TargetRoute().Normalized(); target.FlowInstance != "" {
			flowInstance = target.FlowInstance
		} else if inbound.FlowInstance() != "" {
			flowInstance = inbound.FlowInstance()
		}
	}
	taskID, err := store.CreateHumanTask(ctx, HumanTaskCreateRecord{
		ActorID:       actor.ID,
		EntityID:      in.EntityID,
		FlowInstance:  flowInstance,
		Category:      in.Category,
		Description:   in.Description,
		TalkingPoints: talkingJSON,
		ExpectedValue: in.ExpectedValue,
		Priority:      in.Priority,
		Deadline:      deadline,
		SourceEventID: sourceEventID,
		Context:       events.DeliveryContextFromContext(ctx),
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"task_id": taskID,
		"status":  "pending_review",
	}, nil
}

func (e *Executor) execHumanTaskDecide(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, err := e.humanTaskStoreDependency()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	cfg := e.cfg
	e.mu.RUnlock()
	if !runtimeauthority.ProviderOrNoop(e.authority).CanDecideHumanTasks(actor.Role) {
		return nil, failures.New(failures.ClassAuthorizationDenied, "human_task_decision_forbidden", "tool-executor", "human_task_decide.authorize", map[string]any{"action": "human_task_decide", "actor_id": actor.ID, "role": actor.Role})
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
		requeueCount, err := store.HumanTaskRequeueCount(ctx, in.TaskID)
		if err != nil {
			return nil, fmt.Errorf("load human task requeue count: %w", err)
		}
		if requeueCount > 0 {
			goto skipBudget
		}
		weekStart := WeekStartUTC(time.Now(), cfg.Budget().HumanTasks.BudgetReset)
		approvedThisWeek, err := store.CountApprovedHumanTasksSince(ctx, weekStart)
		if err != nil {
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
	var decisionEventPublish func(context.Context, events.Event) error
	if publisher, ok := e.bus.(MutationEventPublisher); ok && publisher != nil {
		decisionEventPublish = publisher.PublishInMutation
	}

	if err := store.DecideHumanTask(ctx, HumanTaskDecisionRecord{
		TaskID:               in.TaskID,
		Status:               newStatus,
		ActorID:              actor.ID,
		Reason:               in.Reason,
		PriorityRank:         in.PriorityRank,
		RequeueDate:          in.RequeueDate,
		DecidedAt:            time.Now().UTC(),
		DecisionEventPublish: decisionEventPublish,
	}); err != nil {
		return nil, err
	}

	return map[string]any{
		"task_id": in.TaskID,
		"status":  newStatus,
	}, nil
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
