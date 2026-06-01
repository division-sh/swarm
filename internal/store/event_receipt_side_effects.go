package store

import (
	"encoding/json"
	"fmt"
	"strings"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

type agentReceiptSideEffects struct {
	ManagerStatus runtimemanager.ReceiptStatus `json:"manager_status"`
	ReasonCode    string                       `json:"reason_code,omitempty"`
	RetryCount    int                          `json:"retry_count"`
	Error         string                       `json:"error,omitempty"`
}

type pipelineReceiptSideEffects struct {
	ManagerStatus string `json:"manager_status"`
	ReasonCode    string `json:"reason_code,omitempty"`
	Error         string `json:"error,omitempty"`
}

func newAgentReceiptSideEffects(status runtimemanager.ReceiptStatus, reasonCode string, retryCount int, errText string) agentReceiptSideEffects {
	return agentReceiptSideEffects{
		ManagerStatus: runtimemanager.ReceiptStatus(strings.TrimSpace(string(status))),
		ReasonCode:    strings.TrimSpace(reasonCode),
		RetryCount:    retryCount,
		Error:         strings.TrimSpace(errText),
	}
}

func (p agentReceiptSideEffects) validate() error {
	switch p.ManagerStatus {
	case runtimemanager.ReceiptStatusProcessed, runtimemanager.ReceiptStatusError, runtimemanager.ReceiptStatusDeadLetter:
	default:
		if strings.TrimSpace(string(p.ManagerStatus)) == "" {
			return fmt.Errorf("manager_status is required")
		}
		return fmt.Errorf("invalid manager_status %q", p.ManagerStatus)
	}
	if p.RetryCount < 0 {
		return fmt.Errorf("retry_count must be >= 0")
	}
	return nil
}

func marshalAgentReceiptSideEffects(payload agentReceiptSideEffects) ([]byte, error) {
	if err := payload.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeAgentReceiptSideEffects(raw []byte) (agentReceiptSideEffects, error) {
	var payload agentReceiptSideEffects
	if len(raw) == 0 {
		return payload, fmt.Errorf("agent receipt side effects are required")
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, err
	}
	payload.ReasonCode = strings.TrimSpace(payload.ReasonCode)
	payload.Error = strings.TrimSpace(payload.Error)
	if err := payload.validate(); err != nil {
		return payload, err
	}
	return payload, nil
}

func newPipelineReceiptSideEffects(status, reasonCode, errText string) pipelineReceiptSideEffects {
	return pipelineReceiptSideEffects{
		ManagerStatus: strings.TrimSpace(status),
		ReasonCode:    strings.TrimSpace(reasonCode),
		Error:         strings.TrimSpace(errText),
	}
}

func (p pipelineReceiptSideEffects) validate() error {
	if strings.TrimSpace(p.ManagerStatus) == "" {
		return fmt.Errorf("manager_status is required")
	}
	return nil
}

func marshalPipelineReceiptSideEffects(payload pipelineReceiptSideEffects) ([]byte, error) {
	if err := payload.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
