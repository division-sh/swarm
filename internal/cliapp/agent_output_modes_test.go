package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAgentViewJSONPreservesAPIResultShape(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, request.ID, map[string]any{
			"agent":               agentSummaryResult("agent-1", "reviewer", "running"),
			"current_session_ref": map[string]any{"session_id": "session-1", "started_at": "2026-05-18T03:00:00Z"},
			"last_turn_ref":       map[string]any{"turn_id": "turn-1", "completed_at": "2026-05-18T03:05:00Z", "parse_ok": true},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "agent-1", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	agent, ok := decoded["agent"].(map[string]any)
	if !ok || agent["agent_id"] != "agent-1" || agent["status"] != "running" || agent["memory"] != false || agent["memory_source"] != "platform_default" {
		t.Fatalf("agent machine shape = %#v", decoded["agent"])
	}
	if _, ok := decoded["current_session_ref"].(map[string]any); !ok {
		t.Fatalf("current_session_ref = %#v", decoded["current_session_ref"])
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAgentDetailCommandsShareOutputModes(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		switch request.Method {
		case "agent.get":
			writeJSONRPCResult(t, w, request.ID, map[string]any{"agent": agentSummaryResult("agent-1", "reviewer", "running")})
		case "agent.diagnose":
			writeJSONRPCResult(t, w, request.ID, validAgentDiagnosisResult())
		case "agent.delivery_lifecycle":
			writeJSONRPCResult(t, w, request.ID, validAgentDeliveryLifecycleResult())
		default:
			t.Errorf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	tests := []struct {
		name      string
		args      []string
		quietWant string
	}{
		{name: "view", args: []string{"agent", "view", "agent-1"}, quietWant: "agent-1\n"},
		{name: "diagnose", args: []string{"agent", "diagnose", "agent-1"}, quietWant: "agent-1\nrunning\n"},
		{name: "deliveries", args: []string{"agent", "deliveries", "agent-1"}, quietWant: "delivery-1\ndelivery-2\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defaultOut, _, defaultCode := runAgentOutputModeCommand(t, server, tc.args)
			noColorOut, noColorErr, noColorCode := runAgentOutputModeCommand(t, server, append(append([]string(nil), tc.args...), "--no-color"))
			if defaultCode != 0 || noColorCode != 0 {
				t.Fatalf("default/no-color codes = %d/%d no-color stderr=%s", defaultCode, noColorCode, noColorErr)
			}
			if defaultOut != noColorOut {
				t.Fatalf("--no-color changed non-TTY output:\ndefault: %q\nno-color: %q", defaultOut, noColorOut)
			}

			quietOut, quietErr, quietCode := runAgentOutputModeCommand(t, server, append(append([]string(nil), tc.args...), "--quiet"))
			if quietCode != 0 || quietOut != tc.quietWant || strings.TrimSpace(quietErr) != "" {
				t.Fatalf("quiet result code=%d stdout=%q stderr=%q, want %q", quietCode, quietOut, quietErr, tc.quietWant)
			}

			calls.Store(0)
			_, collisionErr, collisionCode := runAgentOutputModeCommand(t, server, append(append([]string(nil), tc.args...), "--json", "--quiet"))
			if collisionCode != CLIExitValidation || !strings.Contains(collisionErr, "--json and --quiet are mutually exclusive") {
				t.Fatalf("collision code=%d stderr=%q", collisionCode, collisionErr)
			}
			if calls.Load() != 0 {
				t.Fatalf("collision RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func runAgentOutputModeCommand(t *testing.T, server *httptest.Server, args []string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
	return stdout.String(), stderr.String(), code
}

func TestAgentMachineOutputRetainsCanonicalCodeValues(t *testing.T) {
	result := validAgentDiagnosisResult()
	want := map[string]any{"state": "retrying", "blocking_layer": "delivery_retry"}
	if got := result["delivery_lifecycle"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("diagnosis lifecycle fixture = %#v, want canonical machine pair %#v", got, want)
	}
}
