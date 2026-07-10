package mcp

import "testing"

func TestDecodeStartupProbeResultRejectsEveryRetiredFailureProjection(t *testing.T) {
	for _, key := range []string{"runtime_error_code", "cause_code", "message", "failure_class", "failure_detail"} {
		t.Run(key, func(t *testing.T) {
			_, err := DecodeStartupProbeResult(map[string]any{
				"contract":  StartupProbeContractManagedAgentCallable,
				"outcome":   StartupProbeOutcomeExecutionFailure,
				"tool_name": "health_check",
				key:         "forged",
			})
			if err == nil {
				t.Fatalf("retired startup field %q was accepted", key)
			}
		})
	}
}

func TestDecodeRuntimeErrorPayloadRejectsUnknownDualAndTrailingShapes(t *testing.T) {
	validProtocol := map[string]any{
		"protocol_error": map[string]any{"code": "mcp_invalid_request", "message": "invalid"},
	}
	for _, raw := range []any{
		map[string]any{"unknown": true},
		map[string]any{
			"failure":        map[string]any{},
			"protocol_error": map[string]any{"code": "mcp_invalid_request", "message": "invalid"},
		},
		map[string]any{"protocol_error": map[string]any{"code": "mcp_invalid_request", "message": "invalid", "extra": true}},
	} {
		if _, err := DecodeRuntimeErrorPayload(raw); err == nil {
			t.Fatalf("invalid runtimeError payload was accepted: %#v", raw)
		}
	}
	if _, err := DecodeRuntimeErrorPayload(validProtocol); err != nil {
		t.Fatalf("valid protocol payload rejected: %v", err)
	}
}
