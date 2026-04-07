package store

import (
	"encoding/json"
	"strings"
)

type ConversationRuntimeStateDescriptor struct {
	Summary              string                                 `json:"summary,omitempty"`
	LastTurn             *ConversationRuntimeLastTurnDescriptor `json:"last_turn,omitempty"`
	ProviderSessionID    string                                 `json:"provider_session_id,omitempty"`
	RetryReason          string                                 `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                                 `json:"retries_from_session_id,omitempty"`
}

type ConversationRuntimeLastTurnDescriptor struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok"`
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
	return payload, nil
}
