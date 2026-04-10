package pipeline

import (
	"context"
	"strings"

	"swarm/internal/events"
	"swarm/internal/runtime/diaglog"
	runtimeengine "swarm/internal/runtime/engine"
)

const accumulatorCompletionOutcomeAction = "accumulator_completion_outcome"

type accumulatorCompletionDecisionOutcome string

const (
	accumulatorCompletionDecisionCompleted  accumulatorCompletionDecisionOutcome = "completed"
	accumulatorCompletionDecisionRolledBack accumulatorCompletionDecisionOutcome = "rolled_back"
)

type accumulatorCompletionDecisionReason string

const (
	accumulatorCompletionReasonCommitted        accumulatorCompletionDecisionReason = "completion_committed"
	accumulatorCompletionReasonEvaluationFailed accumulatorCompletionDecisionReason = "on_complete_evaluation_failed"
	accumulatorCompletionReasonCommitFailed     accumulatorCompletionDecisionReason = "transaction_commit_failed"
	accumulatorCompletionReasonPreCommitFailed  accumulatorCompletionDecisionReason = "pre_commit_failure"
)

type accumulatorCompletionRuntimeRecord struct {
	NodeID             string
	Event              events.Event
	Diagnostics        runtimeengine.AccumulatorCompletionDiagnostics
	DecisionOutcome    accumulatorCompletionDecisionOutcome
	DecisionReasonCode accumulatorCompletionDecisionReason
	ErrorText          string
}

func newAccumulatorCompletionRuntimeRecord(nodeID string, evt events.Event, diagnostics runtimeengine.AccumulatorCompletionDiagnostics, err error) (accumulatorCompletionRuntimeRecord, bool) {
	if !diagnostics.Relevant || !diagnostics.CompletionReached || !diagnostics.OnCompleteDeclared {
		return accumulatorCompletionRuntimeRecord{}, false
	}
	record := accumulatorCompletionRuntimeRecord{
		NodeID:      strings.TrimSpace(nodeID),
		Event:       evt,
		Diagnostics: diagnostics,
		ErrorText:   strings.TrimSpace(errorText(err)),
	}
	switch diagnostics.CommitOutcome {
	case runtimeengine.AccumulatorCompletionCommitCommitted:
		record.DecisionOutcome = accumulatorCompletionDecisionCompleted
		record.DecisionReasonCode = accumulatorCompletionReasonCommitted
	default:
		record.DecisionOutcome = accumulatorCompletionDecisionRolledBack
		switch diagnostics.EvaluationOutcome {
		case runtimeengine.AccumulatorCompletionEvaluationFailed:
			record.DecisionReasonCode = accumulatorCompletionReasonEvaluationFailed
		case runtimeengine.AccumulatorCompletionEvaluationSucceeded:
			record.DecisionReasonCode = accumulatorCompletionReasonCommitFailed
		default:
			record.DecisionReasonCode = accumulatorCompletionReasonPreCommitFailed
		}
	}
	return record, true
}

func (r accumulatorCompletionRuntimeRecord) level() diaglog.Level {
	if r.DecisionOutcome == accumulatorCompletionDecisionCompleted {
		return diaglog.LevelInfo
	}
	return diaglog.LevelWarn
}

func (r accumulatorCompletionRuntimeRecord) message() string {
	switch r.DecisionReasonCode {
	case accumulatorCompletionReasonEvaluationFailed:
		return "Accumulator completion rolled back after on_complete evaluation failed"
	case accumulatorCompletionReasonCommitFailed:
		return "Accumulator completion rolled back after commit-phase failure"
	case accumulatorCompletionReasonPreCommitFailed:
		return "Accumulator completion rolled back before on_complete evaluation completed"
	default:
		return "Accumulator completion committed successfully"
	}
}

func (r accumulatorCompletionRuntimeRecord) detail() map[string]any {
	detail := map[string]any{
		"decision_family":      "accumulator_completion",
		"decision_outcome":     string(r.DecisionOutcome),
		"decision_reason_code": string(r.DecisionReasonCode),
		"completion_reached":   r.Diagnostics.CompletionReached,
		"completion_mode":      strings.TrimSpace(r.Diagnostics.CompletionMode),
		"received_count":       r.Diagnostics.ReceivedCount,
		"expected_count":       r.Diagnostics.ExpectedCount,
		"on_complete_declared": r.Diagnostics.OnCompleteDeclared,
		"evaluation_outcome":   string(r.Diagnostics.EvaluationOutcome),
		"commit_outcome":       string(r.Diagnostics.CommitOutcome),
		"handler_node":         strings.TrimSpace(r.NodeID),
		"event_id":             strings.TrimSpace(r.Event.ID),
		"event_type":           strings.TrimSpace(string(r.Event.Type)),
		"entity_id":            strings.TrimSpace(r.Event.EntityID()),
		"flow_instance":        strings.TrimSpace(r.Event.FlowInstance()),
	}
	if ruleID := strings.TrimSpace(r.Diagnostics.SelectedRuleID); ruleID != "" {
		detail["selected_rule_id"] = ruleID
	}
	if errText := strings.TrimSpace(r.ErrorText); errText != "" {
		detail["error"] = errText
		detail["error_code"] = string(r.DecisionReasonCode)
	}
	return detail
}

func logAccumulatorCompletionOutcome(ctx context.Context, bus Bus, nodeID string, evt events.Event, diagnostics runtimeengine.AccumulatorCompletionDiagnostics, err error) {
	if bus == nil {
		return
	}
	record, ok := newAccumulatorCompletionRuntimeRecord(nodeID, evt, diagnostics, err)
	if !ok {
		return
	}
	entry := RuntimeLogEntry{
		Level:     record.level(),
		Component: "workflow-runtime",
		Action:    accumulatorCompletionOutcomeAction,
		Message:   record.message(),
		EventID:   strings.TrimSpace(evt.ID),
		EventType: strings.TrimSpace(string(evt.Type)),
		EntityID:  strings.TrimSpace(evt.EntityID()),
		Detail:    record.detail(),
		Error:     strings.TrimSpace(record.ErrorText),
	}
	emit := func() {
		_ = bus.LogRuntime(ctx, entry)
	}
	if _, txActive := PipelineSQLTxFromContext(ctx); txActive {
		if record.DecisionOutcome == accumulatorCompletionDecisionCompleted {
			if QueuePipelinePostCommitAction(ctx, emit) {
				return
			}
		} else {
			if QueuePipelineAfterPublishAction(ctx, emit) {
				return
			}
		}
	}
	emit()
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
