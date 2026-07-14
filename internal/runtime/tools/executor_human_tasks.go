package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
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
		Scope         string   `json:"scope"`
		EntityID      string   `json:"entity_id"`
		Category      string   `json:"category"`
		Description   string   `json:"description"`
		TalkingPoints []string `json:"talking_points"`
		ExpectedValue string   `json:"expected_value"`
		Priority      string   `json:"priority"`
		DeadlineAt    string   `json:"deadline_at"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	in.Scope = strings.TrimSpace(in.Scope)
	in.EntityID = strings.TrimSpace(in.EntityID)
	in.Category = strings.TrimSpace(in.Category)
	in.Description = strings.TrimSpace(in.Description)
	in.ExpectedValue = strings.TrimSpace(in.ExpectedValue)
	in.Priority = strings.TrimSpace(in.Priority)
	in.DeadlineAt = strings.TrimSpace(in.DeadlineAt)
	if strings.TrimSpace(actor.ID) == "" {
		return nil, errors.New("actor id is required")
	}
	if in.Scope == "" {
		return nil, errors.New("scope is required (entity|flow|global)")
	}
	if in.Category == "" {
		return nil, errors.New("category is required")
	}
	if cfg != nil && len(cfg.Budget().HumanTasks.CategoriesEnabled) > 0 {
		enabled := false
		for _, category := range cfg.Budget().HumanTasks.CategoriesEnabled {
			if strings.TrimSpace(category) == in.Category {
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
	switch in.Priority {
	case "low", "medium", "high", "critical":
	default:
		return nil, fmt.Errorf("priority %q is invalid; use low, medium, high, or critical", in.Priority)
	}
	for index, point := range in.TalkingPoints {
		in.TalkingPoints[index] = strings.TrimSpace(point)
		if in.TalkingPoints[index] == "" {
			return nil, fmt.Errorf("talking_points[%d] must not be empty", index)
		}
	}

	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return nil, errors.New("human_task_request requires an admitted run")
	}
	operationID, ok := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	if !ok {
		return nil, errors.New("human_task_request requires canonical logical tool-operation identity")
	}
	bundleFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return nil, errors.New("human_task_request requires pinned bundle identity")
	}
	bundleHash := strings.TrimSpace(bundleFact.BundleHash)
	if bundleHash == "" {
		bundleHash = strings.TrimSpace(bundleFact.BundleFingerprint)
	}
	if bundleHash == "" {
		return nil, errors.New("human_task_request requires pinned bundle hash")
	}

	flowInstance := strings.Trim(actor.CanonicalFlowPath(), "/")
	requesterEntityID := actor.EffectiveEntityID()
	var sourceEventID string
	var createdAt time.Time
	if inbound, found := runtimebus.InboundEventFromContext(ctx); found {
		sourceEventID = strings.TrimSpace(inbound.ID())
		createdAt = inbound.CreatedAt().UTC()
		target := inbound.TargetRoute().Normalized()
		if target.FlowInstance != "" {
			flowInstance = target.FlowInstance
		} else if inbound.FlowInstance() != "" {
			flowInstance = inbound.FlowInstance()
		}
		if target.EntityID != "" {
			requesterEntityID = target.EntityID
		}
	}
	if sourceEventID == "" || createdAt.IsZero() {
		return nil, errors.New("human_task_request requires an admitted source event with a durable timestamp")
	}
	if flowInstance == "" && in.Scope != string(decisioncard.ScopeGlobal) {
		return nil, errors.New("human_task_request flow scope requires an admitted flow instance")
	}
	scope := decisioncard.Scope{Kind: decisioncard.ScopeKind(in.Scope)}
	switch scope.Kind {
	case decisioncard.ScopeEntity:
		scope.FlowInstance = flowInstance
		scope.EntityID = strings.TrimSpace(firstNonEmptyHumanTask(in.EntityID, actor.EffectiveEntityID()))
	case decisioncard.ScopeFlow:
		scope.FlowInstance = flowInstance
	case decisioncard.ScopeGlobal:
	default:
		return nil, fmt.Errorf("scope %q is invalid; use entity, flow, or global", in.Scope)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}

	now := decisioncard.CanonicalTimestamp(createdAt)
	deadline := time.Time{}
	if in.DeadlineAt != "" {
		deadline, err = time.Parse(time.RFC3339Nano, in.DeadlineAt)
		if err != nil {
			return nil, fmt.Errorf("deadline_at must be RFC3339: %w", err)
		}
		deadline = decisioncard.CanonicalTimestamp(deadline)
		if !deadline.After(now) {
			return nil, errors.New("deadline_at must be in the future")
		}
	} else {
		hours := 168
		if cfg != nil && cfg.Budget().HumanTasks.AutoExpireHours > 0 {
			hours = cfg.Budget().HumanTasks.AutoExpireHours
		}
		deadline = decisioncard.CanonicalTimestamp(now.Add(time.Duration(hours) * time.Hour))
	}

	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: actor.ID, OperationID: operationID, Category: in.Category, Scope: scope,
	})
	if err != nil {
		return nil, err
	}
	contextSnapshot := map[string]any{
		"category": in.Category, "description": in.Description, "talking_points": in.TalkingPoints,
		"expected_value": in.ExpectedValue, "priority": in.Priority,
		"deadline_at": deadline.Format(time.RFC3339Nano),
	}
	outcomes := map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", Label: "Approve", Input: map[string]runtimecontracts.WorkflowGateInputField{}},
		"reject": {Verdict: "reject", Label: "Reject", Input: map[string]runtimecontracts.WorkflowGateInputField{
			"reason": {Type: "text", Required: true, Label: "Reason"},
		}},
	}
	snapshot, err := decisioncard.FreezeSnapshot("human_task", in.Description, contextSnapshot, outcomes)
	if err != nil {
		return nil, err
	}
	provenanceMap := map[string]any{"requester_agent_id": actor.ID, "logical_operation_id": operationID}
	if sourceEventID != "" {
		provenanceMap["source_event_id"] = sourceEventID
	}
	provenance, err := canonicaljson.FromGo(provenanceMap)
	if err != nil {
		return nil, err
	}
	cardID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.card.v1\x00"+runID+"\x00"+operationID)).String()
	cadence := decisioncard.CadencePolicy{}
	if cfg != nil {
		cadence = decisioncard.CadencePolicy{
			FirstReminderDelay: cfg.Runtime.DecisionCardFirstReminder,
			UrgencyDelay:       cfg.Runtime.DecisionCardUrgency,
			InputDraftTTL:      cfg.Runtime.DecisionCardInputDraftTTL,
			ReminderInterval:   cfg.Runtime.DecisionCardReminderInterval,
		}
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: cardID, RunID: runID, Anchor: anchor, Snapshot: snapshot,
		BundleHash: bundleHash, EffectiveCadence: cadence.Stamp(now), Provenance: provenance, CreatedAt: now,
	})
	if err != nil {
		return nil, err
	}
	reset := "monday"
	limit := 0
	if cfg != nil {
		reset = cfg.Budget().HumanTasks.BudgetReset
		limit = cfg.Budget().HumanTasks.MaxTasksPerWeek
	}
	windowStart := WeekStartUTC(now, reset)
	windowEnd := NextWeekResetUTC(now, reset)
	continuation := decisioncard.HumanTaskContinuation{
		CardID: card.CardID, RunID: runID,
		RequesterRoute: events.RouteIdentity{FlowInstance: flowInstance, EntityID: requesterEntityID},
		ReplyContextID: events.DeliveryContextFromContext(ctx).ReplyContextID(),
		SourceEventID:  sourceEventID, DeadlineAt: deadline, BudgetBundleHash: bundleHash,
		BudgetLimit: limit, BudgetWindowStart: windowStart, BudgetWindowEnd: windowEnd,
		State: decisioncard.HumanTaskContinuationPending, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateHumanTaskCard(ctx, card, continuation); err != nil {
		return nil, err
	}
	return map[string]any{"card_id": card.CardID, "status": decisioncard.StatusPending}, nil
}

func firstNonEmptyHumanTask(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
