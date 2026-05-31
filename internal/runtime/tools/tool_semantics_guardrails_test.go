package tools

import (
	"reflect"
	"testing"

	models "swarm/internal/runtime/core/actors"
)

func TestNormalizeNativeToolNameCanonicalAliases(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"":                                   "",
		"bash":                               "bash",
		"Bash":                               "bash",
		"web_search":                         "web_search",
		"WebFetch":                           "web_search",
		"WebSearch":                          "web_search",
		"Read":                               "read_file",
		"read_file":                          "read_file",
		"Write":                              "write_file",
		"Edit":                               "write_file",
		"mcp__runtime-tools__read_file":      "read_file",
		"mcp__runtime-tools__write_file":     "write_file",
		"mcp__runtime-tools__emit_scan_done": "emit_scan_done",
	}

	for raw, want := range tests {
		if got := normalizeNativeToolName(raw); got != want {
			t.Fatalf("normalizeNativeToolName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestRuntimeAndValidatorNormalizationStayAligned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		tool  string
		input any
	}{
		{
			name:  "read_file_alias_path",
			tool:  "read_file",
			input: map[string]any{"file_path": "/tmp/a.txt"},
		},
		{
			name:  "write_file_alias_path",
			tool:  "write_file",
			input: map[string]any{"file_path": "/tmp/a.txt", "content": "x"},
		},
		{
			name: "agent_message_target_aliases",
			tool: "agent_message",
			input: map[string]any{
				"to":              "agent-b",
				"payload":         map[string]any{"message": "hello"},
				"target_agent_id": "",
				"message":         "",
			},
		},
		{
			name: "schedule_event_aliases",
			tool: "schedule",
			input: map[string]any{
				"action":        "scan.requested",
				"at":            "2026-01-02T03:04:05Z",
				"context":       map[string]any{"mode": "corpus"},
				"delay_seconds": 0,
			},
		},
		{
			name: "agent_hire_embedded_config",
			tool: "agent_hire",
			input: map[string]any{
				"config": map[string]any{
					"id":   "worker-1",
					"role": "analysis",
				},
			},
		},
		{
			name: "agent_fire_default_reason",
			tool: "agent_fire",
			input: map[string]any{
				"agent_id": "worker-1",
			},
		},
		{
			name: "agent_reconfigure_config_projection",
			tool: "agent_reconfigure",
			input: map[string]any{
				"model":              "fast",
				"system_prompt":      "be concise",
				"max_turns_per_task": 5,
			},
		},
		{
			name: "mailbox_send_aliases",
			tool: "mailbox_send",
			input: map[string]any{
				"type":     "approval",
				"priority": "critical",
				"subject":  "Need review",
				"payload":  map[string]any{"x": 1},
			},
		},
		{
			name: "human_task_request_explicit_deadline",
			tool: "human_task_request",
			input: map[string]any{
				"entity_id":   "entity-1",
				"deadline_at": "2026-01-02T03:04:05Z",
			},
		},
		{
			name: "human_task_decide_alias",
			tool: "human_task_decide",
			input: map[string]any{
				"decision": "approve",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runtimeNormalized := normalizeRuntimeToolInput(tt.tool, tt.input)
			validatorNormalized := validatorNormalizeRuntimeToolInput(tt.tool, tt.input)
			if !reflect.DeepEqual(runtimeNormalized, validatorNormalized) {
				t.Fatalf("normalization mismatch for %s\nruntime:   %#v\nvalidator: %#v", tt.tool, runtimeNormalized, validatorNormalized)
			}
		})
	}
}

func TestToolDefinitionsForActor_IncludesEnabledNativeTools(t *testing.T) {
	t.Parallel()

	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{
		ID:          "analysis-agent",
		NativeTools: models.NativeToolConfig{FileIO: true},
	})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	foundRead := false
	foundWrite := false
	for _, name := range names {
		if name == "read_file" {
			foundRead = true
		}
		if name == "write_file" {
			foundWrite = true
		}
	}
	if !foundRead || !foundWrite {
		t.Fatalf("expected native file tools in actor definitions, got %v", names)
	}
}
