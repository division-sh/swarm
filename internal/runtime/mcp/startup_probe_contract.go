package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	StartupProbeContractManagedAgentCallable = "managed_agent_callable_proof_v1"
	startupProbeParamKey                     = "swarmProbe"
	startupProbeResultKey                    = "swarmStartupProbe"
)

type StartupProbeRequest struct {
	Contract string `json:"contract"`
}

type StartupProbeOutcome string

const (
	StartupProbeOutcomeSuccess          StartupProbeOutcome = "success"
	StartupProbeOutcomeValidationOnly   StartupProbeOutcome = "validation_only"
	StartupProbeOutcomeExecutionFailure StartupProbeOutcome = "execution_failure"
)

type StartupProbeResult struct {
	Contract      string              `json:"contract"`
	Outcome       StartupProbeOutcome `json:"outcome"`
	ToolName      string              `json:"tool_name,omitempty"`
	FailureClass  string              `json:"failure_class,omitempty"`
	FailureDetail string              `json:"failure_detail,omitempty"`
	Message       string              `json:"message,omitempty"`
}

func DecodeStartupProbeRequest(raw any) (*StartupProbeRequest, error) {
	if raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode startup probe request: %w", err)
	}
	var req StartupProbeRequest
	if err := json.Unmarshal(encoded, &req); err != nil {
		return nil, fmt.Errorf("decode startup probe request: %w", err)
	}
	req.Contract = strings.TrimSpace(req.Contract)
	if req.Contract == "" {
		return nil, fmt.Errorf("startup probe contract is required")
	}
	if req.Contract != StartupProbeContractManagedAgentCallable {
		return nil, fmt.Errorf("unsupported startup probe contract %q", req.Contract)
	}
	return &req, nil
}

func DecodeStartupProbeResult(raw any) (*StartupProbeResult, error) {
	if raw == nil {
		return nil, fmt.Errorf("startup probe result is required")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode startup probe result: %w", err)
	}
	var result StartupProbeResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("decode startup probe result: %w", err)
	}
	result.Contract = strings.TrimSpace(result.Contract)
	result.ToolName = strings.TrimSpace(result.ToolName)
	result.FailureClass = strings.TrimSpace(result.FailureClass)
	result.FailureDetail = strings.TrimSpace(result.FailureDetail)
	result.Message = strings.TrimSpace(result.Message)
	if result.Contract != StartupProbeContractManagedAgentCallable {
		return nil, fmt.Errorf("unsupported startup probe result contract %q", result.Contract)
	}
	switch result.Outcome {
	case StartupProbeOutcomeSuccess, StartupProbeOutcomeValidationOnly, StartupProbeOutcomeExecutionFailure:
	default:
		return nil, fmt.Errorf("startup probe outcome is required")
	}
	return &result, nil
}

func StartupProbeSuccessResult(contract, toolName string) *StartupProbeResult {
	return &StartupProbeResult{
		Contract: strings.TrimSpace(contract),
		Outcome:  StartupProbeOutcomeSuccess,
		ToolName: strings.TrimSpace(toolName),
	}
}

func StartupProbeResultForRuntimeError(contract, toolName string, runtimeErr *RuntimeErrorPayload) (*StartupProbeResult, error) {
	contract = strings.TrimSpace(contract)
	if contract != StartupProbeContractManagedAgentCallable {
		return nil, fmt.Errorf("unsupported startup probe contract %q", contract)
	}
	if runtimeErr == nil {
		return nil, fmt.Errorf("runtime error payload is required")
	}
	if runtimeErr.Failure == nil {
		return nil, fmt.Errorf("startup probe execution failure requires canonical failure envelope")
	}
	result := &StartupProbeResult{
		Contract:      contract,
		Outcome:       StartupProbeOutcomeExecutionFailure,
		ToolName:      strings.TrimSpace(toolName),
		FailureClass:  string(runtimeErr.Failure.Class),
		FailureDetail: strings.TrimSpace(runtimeErr.Failure.Detail.Code),
		Message:       strings.TrimSpace(runtimeErr.Failure.Message),
	}
	if result.FailureClass == "platform.schema_invalid" {
		result.Outcome = StartupProbeOutcomeValidationOnly
	}
	return result, nil
}
