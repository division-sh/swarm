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

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestAgentFrameCLIUsesExactAPISelectorsOnly(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	for _, tc := range []struct {
		name       string
		args       []string
		wantParams map[string]any
		scope      agentframe.InspectionScope
	}{
		{name: "static", args: []string{"agent", "frame", "reviewer", "--scope", "static", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "review", "--json"}, wantParams: map[string]any{"scope": "static", "agent_id": "reviewer", "bundle_hash": "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "flow": "review"}, scope: agentframe.InspectionStatic},
		{name: "effective", args: []string{"agent", "frame", "reviewer", "--scope", "effective", "--flow-instance", "review/one", "--json"}, wantParams: map[string]any{"scope": "effective", "agent_id": "reviewer", "flow_instance": "review/one"}, scope: agentframe.InspectionEffective},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writeJSONRPCResult(t, w, captured.ID, agentFrameCLIInspectionResult(t, tc.scope))
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code = %d stderr=%s", code, stderr.String())
			}
			if captured.Method != "agent.frame" || !reflect.DeepEqual(captured.Params, tc.wantParams) {
				t.Fatalf("request = %s %#v, want agent.frame %#v", captured.Method, captured.Params, tc.wantParams)
			}
			if !strings.Contains(stdout.String(), `"turn_context"`) || !strings.Contains(stdout.String(), `"status":"unresolved"`) {
				t.Fatalf("stdout does not preserve unresolved facts: %s", stdout.String())
			}
		})
	}
}

func TestAgentFrameCLIRejectsSelectorConflictsBeforeAPIRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()
	for _, args := range [][]string{
		{"agent", "frame", "reviewer", "--scope", "static", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "review", "--root"},
		{"agent", "frame", "reviewer", "--scope", "effective", "--root", "--flow-instance", "review/one"},
		{"agent", "frame", "reviewer", "--scope", "effective", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--root"},
		{"agent", "frame", "reviewer", "--scope", "static", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "/review/"},
		{"agent", "frame", "reviewer", "--scope", "static", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", " review "},
		{"agent", "frame", "reviewer", "--scope", "effective", "--flow-instance", "/review/one/"},
		{"agent", "frame", "reviewer", "--scope", "effective", "--flow-instance", "review/one/"},
		{"agent", "frame", " reviewer ", "--scope", "static", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "review"},
		{"agent", "frame", "reviewer", "--scope", " static ", "--bundle-hash", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "review"},
		{"agent", "frame", "reviewer", "--scope", "static", "--bundle-hash", " bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--flow", "review"},
		{"agent", "frame", "reviewer", "--scope", "effective", "--root", "--bundle-hash", " "},
		{"agent", "frame", "reviewer", "--scope", "effective", "--root", "--flow", " "},
		{"agent", "frame", "reviewer", "--scope", "effective", "--flow-instance", " review/one "},
	} {
		var stdout, stderr bytes.Buffer
		if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server)); code != 2 {
			t.Fatalf("args %v code = %d, want 2; stderr=%s", args, code, stderr.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("API calls = %d, want 0", calls.Load())
	}
}

func agentFrameCLIInspection(scope agentframe.InspectionScope) agentframe.Inspection {
	return agentframe.Inspection{
		Version: agentframe.Version, Scope: scope,
		Selector: agentframe.InspectionSelector{AgentID: "reviewer"},
		Session: agentframe.InspectionSession{
			BundleHash:   "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			BundleSource: agentframe.Presence[string]{Status: "unresolved"}, AgentID: "reviewer", AuthoredFlow: "review",
			AgentIdentity: agentframe.Presence[agentidentity.Identity]{Status: "unresolved"},
			Provider:      agentframe.Presence[agentframe.Provider]{Status: "unresolved"},
		},
		Turn: agentframe.InspectionTurn{
			FrameID: agentframe.Presence[string]{Status: "unresolved"}, ContentHash: agentframe.Presence[string]{Status: "unresolved"},
			Kind: agentframe.Presence[agentframe.TurnKind]{Status: "unresolved"}, ParentFrame: agentframe.Presence[string]{Status: "unresolved"},
			Event: agentframe.Presence[agentframe.Event]{Status: "unresolved"}, Capability: agentframe.Presence[agentframe.CapabilityPlan]{Status: "unresolved"},
			Directive: agentframe.Presence[agentframe.Directive]{Status: "unresolved"}, Remediation: agentframe.Presence[agentframe.Remediation]{Status: "unresolved"},
			Lifecycle: agentframe.Lifecycle{Stage: agentframe.Unresolved(), LoopRevision: agentframe.Unresolved()}, PackProvenance: agentframe.Unresolved(),
		},
	}
}

func agentFrameCLIInspectionResult(t testing.TB, scope agentframe.InspectionScope) map[string]any {
	t.Helper()
	raw, err := json.Marshal(agentFrameCLIInspection(scope))
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
