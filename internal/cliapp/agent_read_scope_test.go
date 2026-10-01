package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAgentReadScopeRejectsBeforeClientConstruction(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, command := range [][]string{
		{"agent", "view", "agent-1"}, {"agent", "diagnose", "agent-1"},
		{"agent", "deliveries", "agent-1"}, {"conversation", "list", "--agent-id", "agent-1"},
	} {
		for _, scope := range [][]string{nil, {"--run-id", ""}, {"--run-id", "  "}, {"--run-id", "bad scope!"}, {"--run-id", strings.Repeat("r", 257)}} {
			for _, mode := range []string{"", "--json", "--quiet"} {
				t.Run(strings.Join(command[:2], "-")+"/"+strings.Join(scope, " ")+mode, func(t *testing.T) {
					args := append(append([]string{}, command...), scope...)
					// If validation constructs a client first, the missing token file
					// returns an auth error instead of the required local refusal.
					args = append(args, "--api-token-file", filepath.Join(t.TempDir(), "absent-token"))
					if mode != "" {
						args = append(args, mode)
					}
					var out, errOut bytes.Buffer
					code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
					if code != CLIExitValidation || out.Len() != 0 || !strings.Contains(errOut.String(), "--run-id") || calls.Load() != 0 {
						t.Fatalf("scope refusal: code=%d stdout=%s stderr=%s calls=%d", code, &out, &errOut, calls.Load())
					}
				})
			}
		}
	}
}

func TestAgentReadScopePreservesExactRunAndFlowOnPrefixRetry(t *testing.T) {
	for _, tc := range []struct {
		command string
		method  string
		result  map[string]any
	}{
		{"view", "agent.get", map[string]any{"agent": agentSummaryResult("agent-one", "reviewer", "running")}},
		{"diagnose", "agent.diagnose", validAgentDiagnosisResult()},
		{"deliveries", "agent.delivery_lifecycle", validAgentDeliveryLifecycleResult()},
	} {
		t.Run(tc.command, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var requests []jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				requests = append(requests, req)
				if req.Method == "agent.list" {
					writeJSONRPCResult(t, w, req.ID, map[string]any{"agents": []any{agentSummaryResult("agent-one", "reviewer", "running")}})
					return
				}
				if req.Params["agent_id"] == "agent-o" {
					writeIdentifierRPCError(t, w, req.ID, "AGENT_NOT_FOUND")
					return
				}
				writeJSONRPCResult(t, w, req.ID, tc.result)
			}))
			defer server.Close()
			var out, errOut bytes.Buffer
			args := []string{"agent", tc.command, "agent-o", "--run-id", " exact-run ", "--flow-instance", "/flow/one/"}
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
			if code != 0 || len(requests) != 3 {
				t.Fatalf("prefix retry: code=%d requests=%#v stderr=%s", code, requests, &errOut)
			}
			for i, id := range map[int]string{0: "agent-o", 2: "agent-one"} {
				want := map[string]any{"agent_id": id, "run_id": "exact-run", "flow_instance": "flow/one"}
				if requests[i].Method != tc.method || !reflect.DeepEqual(requests[i].Params, want) {
					t.Fatalf("request %d = %#v; want %s %#v", i, requests[i], tc.method, want)
				}
			}
			if requests[1].Method != "agent.list" || len(requests[1].Params) != 0 {
				t.Fatalf("declared inventory must not choose run authority: %#v", requests[1])
			}
		})
	}
}
