package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ConversationRuntimeStateDescriptor struct {
	Summary              string                                 `json:"summary,omitempty"`
	LastTurn             *ConversationRuntimeLastTurnDescriptor `json:"last_turn,omitempty"`
	ProviderSessionID    string                                 `json:"provider_session_id,omitempty"`
	RetryReason          string                                 `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                                 `json:"retries_from_session_id,omitempty"`
	Watchdog             *ConversationRuntimeWatchdogDescriptor `json:"watchdog,omitempty"`
}

type ConversationRuntimeLastTurnDescriptor struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok"`
}

type ConversationRuntimeWatchdogDescriptor struct {
	State         string `json:"state,omitempty"`
	BlockingLayer string `json:"blocking_layer,omitempty"`
	Action        string `json:"action,omitempty"`
	Outcome       string `json:"outcome,omitempty"`
	LastOutputAt  string `json:"last_output_at,omitempty"`
	RecordedAt    string `json:"recorded_at,omitempty"`
}

func DecodeConversationRuntimeStateDescriptor(raw []byte) (ConversationRuntimeStateDescriptor, error) {
	var payload ConversationRuntimeStateDescriptor
	if len(raw) == 0 {
		return payload, nil
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ConversationRuntimeStateDescriptor{}, err
	}
	payload.Summary = strings.TrimSpace(payload.Summary)
	payload.ProviderSessionID = strings.TrimSpace(payload.ProviderSessionID)
	payload.RetryReason = strings.TrimSpace(payload.RetryReason)
	payload.RetriesFromSessionID = strings.TrimSpace(payload.RetriesFromSessionID)
	if payload.LastTurn != nil {
		payload.LastTurn.TaskID = strings.TrimSpace(payload.LastTurn.TaskID)
	}
	if payload.Watchdog != nil {
		payload.Watchdog.State = strings.TrimSpace(payload.Watchdog.State)
		payload.Watchdog.BlockingLayer = strings.TrimSpace(payload.Watchdog.BlockingLayer)
		payload.Watchdog.Action = strings.TrimSpace(payload.Watchdog.Action)
		payload.Watchdog.Outcome = strings.TrimSpace(payload.Watchdog.Outcome)
		payload.Watchdog.LastOutputAt = strings.TrimSpace(payload.Watchdog.LastOutputAt)
		payload.Watchdog.RecordedAt = strings.TrimSpace(payload.Watchdog.RecordedAt)
		if err := ValidateConversationRuntimeWatchdogDescriptor(*payload.Watchdog); err != nil {
			return ConversationRuntimeStateDescriptor{}, err
		}
	}
	return payload, nil
}

func ValidateConversationRuntimeWatchdogDescriptor(payload ConversationRuntimeWatchdogDescriptor) error {
	switch payload.State {
	case "healthy_long_running", "no_output":
	default:
		return fmt.Errorf("canonical runtime_state watchdog.state %q is invalid", payload.State)
	}
	switch payload.BlockingLayer {
	case "session_execution":
	default:
		return fmt.Errorf("canonical runtime_state watchdog.blocking_layer %q is invalid", payload.BlockingLayer)
	}
	switch payload.Action {
	case "turn_long_running", "session_no_output":
	default:
		return fmt.Errorf("canonical runtime_state watchdog.action %q is invalid", payload.Action)
	}
	switch payload.Outcome {
	case "observed", "warning_emitted":
	default:
		return fmt.Errorf("canonical runtime_state watchdog.outcome %q is invalid", payload.Outcome)
	}
	if payload.State == "healthy_long_running" && payload.LastOutputAt == "" {
		return fmt.Errorf("canonical runtime_state watchdog.last_output_at is required for healthy_long_running state")
	}
	if payload.RecordedAt == "" {
		return fmt.Errorf("canonical runtime_state watchdog.recorded_at is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, payload.RecordedAt); err != nil {
		return fmt.Errorf("canonical runtime_state watchdog.recorded_at is invalid: %w", err)
	}
	if payload.LastOutputAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, payload.LastOutputAt); err != nil {
			return fmt.Errorf("canonical runtime_state watchdog.last_output_at is invalid: %w", err)
		}
	}
	return nil
}
