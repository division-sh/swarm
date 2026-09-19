package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutCLIExactConnectionFlags(t *testing.T) {
	for _, flag := range []string{"api-server", "api-token-file", "context"} {
		for _, equals := range []bool{false, true} {
			name := flag + "/separate"
			if equals {
				name = flag + "/equals"
			}
			t.Run(name, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				page := fanOutCLIPage()
				wantToken := apiv1.DefaultLoopbackAPIToken
				if flag == "api-token-file" {
					wantToken = "explicit-fan-out-token"
				}
				listCalls, identityCalls := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/v1/rpc" || r.Header.Get("Authorization") != "Bearer "+wantToken {
						t.Errorf("wrong transport/auth: %s %q", r.URL.Path, r.Header.Get("Authorization"))
					}
					var req jsonRPCRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						return
					}
					switch req.Method {
					case "runtime.identity":
						identityCalls++
						writeJSONRPCResult(t, w, req.ID, map[string]any{
							"runtime_instance_id": "fan-out-runtime", "started_at": "2026-07-02T00:00:00Z",
							"api_version": "v1", "supported_transports": []string{"tcp"},
						})
					case "run.fan_out.list":
						listCalls++
						if req.Params["run_id"] != page.RunID {
							t.Errorf("params=%v", req.Params)
						}
						raw, _ := json.Marshal(page)
						var result map[string]any
						_ = json.Unmarshal(raw, &result)
						writeJSONRPCResult(t, w, req.ID, result)
					default:
						t.Errorf("unexpected method %q", req.Method)
					}
				}))
				defer server.Close()
				swarmDir := t.TempDir()
				value := server.URL
				switch flag {
				case "api-token-file":
					value = writeCLIAPITokenFile(t, wantToken)
					t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{"api_server": server.URL}))
				case "context":
					value = "fan-out-target"
					writeCLIAPITestContext(t, newLocalContextRegistry(swarmDir), value, "fan-out-runtime", server.URL, "")
				}
				args := []string{"--swarm-dir", swarmDir, "run", "fan-out", "list", page.RunID, "--limit", "1", "--json"}
				if equals {
					args = append(args, "--"+flag+"="+value)
				} else {
					args = append(args, "--"+flag, value)
				}
				var stdout, stderr bytes.Buffer
				if code := Execute(context.Background(), args, &stdout, &stderr, nil, nil); code != 0 {
					t.Fatalf("exit=%d stderr=%s", code, stderr.String())
				}
				var got fanoutobligation.ListPage
				if err := json.Unmarshal(stdout.Bytes(), &got); err != nil || !reflect.DeepEqual(got, page) {
					t.Fatalf("page=%s err=%v", stdout.String(), err)
				}
				if listCalls != 1 || (flag == "context" && identityCalls != 1) || (flag != "context" && identityCalls != 0) {
					t.Fatalf("list calls=%d identity calls=%d", listCalls, identityCalls)
				}
			})
		}
	}
}

func TestFanOutCLIConnectionFlagsStillRequireLeaf(t *testing.T) {
	for _, prefix := range [][]string{nil, {"run"}, {"run", "fan-out"}, {"run", "fan-out", "other"}} {
		if cliAPIConnectionFlagAfterLeafCommand(prefix) {
			t.Fatalf("non-leaf classified as an API leaf: %v", prefix)
		}
	}
	for _, flag := range []string{"--api-server", "--api-token-file", "--context"} {
		if err := validateCLIAPIConnectionFlagPlacement([]string{flag, "value", "run", "fan-out", "list"}); err == nil || err.Error() != "unknown flag: "+flag {
			t.Fatalf("accepted root placement %s: %v", flag, err)
		}
	}
}

func TestStatusProjectsCanonicalBlockedFanOutEvidence(t *testing.T) {
	runID := "11111111-1111-4111-8111-111111111111"
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(
		runtimefailures.ClassSchemaInvalid, "fan_out_evaluate_ordinal_failed", "runtime.fan_out", "evaluate", nil,
	))
	if !ok {
		t.Fatal("construct typed blocked failure")
	}
	result := validDiagnosticRunDiagnosis(runID, "stalled", "delivery_lifecycle", "no_active_deliveries", []any{})
	summary := fanoutobligation.RunSummary{
		RunID: runID, Intents: 2, Blocked: 2, Cardinality: 6, Cursor: 1, Owed: 5, Committed: 1, Settled: 1,
		MinNextChunk: 4, MaxNextChunk: 4,
		BlockedIntents: []fanoutobligation.BlockedIntentDiagnosis{
			{TriggeringDeliveryID: "22222222-2222-4222-8222-222222222222", FlowPath: "portfolio", Family: "fan_out", SemanticPath: "registration.first", Cursor: 0, Owed: 2, Failure: failure},
			{TriggeringDeliveryID: "33333333-3333-4333-8333-333333333333", FlowPath: "portfolio", Family: "fan_out", SemanticPath: "registration.second", Cursor: 1, Owed: 3, Failure: failure},
		},
	}
	if err := summary.Validate(); err != nil {
		t.Fatal(err)
	}
	result["fan_out"] = summary
	for _, format := range []string{"human", "json", "unblocked"} {
		t.Run(format, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			response := make(map[string]any, len(result))
			for key, value := range result {
				response[key] = value
			}
			if format == "unblocked" {
				response["fan_out"] = validDiagnosticFanOutSummary(runID)
			}
			before, _ := json.Marshal(response)
			server, _ := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any { return response })
			defer server.Close()
			args := []string{"run", "status", runID}
			if format == "json" {
				args = append(args, "--json")
			}
			var stdout, stderr bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server)); code != 0 {
				t.Fatalf("exit=%d stderr=%s", code, stderr.String())
			}
			after, _ := json.Marshal(response)
			if !bytes.Equal(before, after) {
				t.Fatal("status mutated canonical diagnosis")
			}
			if format == "json" {
				var got, want any
				if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				_ = json.Unmarshal(before, &want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("serialized owner changed: %s", stdout.String())
				}
				return
			}
			text := strings.Join(strings.Fields(stdout.String()), " ")
			for _, want := range []string{"Run " + runID + " stalled", "health settled", "0 unsettled items", "blocker delivery lifecycle, no active deliveries"} {
				if !strings.Contains(text, want) {
					t.Fatalf("canonical state/health changed; missing %q: %s", want, stdout.String())
				}
			}
			if format == "unblocked" {
				if strings.Contains(text, "blocked fan-out intents") || strings.Contains(text, failure.Detail.Code) {
					t.Fatalf("invented blocked evidence: %s", stdout.String())
				}
				return
			}
			for _, want := range []string{"blocked fan-out intents", "cursor 0, owed 2", "cursor 1, owed 3", string(failure.Class) + "/" + failure.Detail.Code} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing %q: %s", want, stdout.String())
				}
			}
			for _, blocked := range summary.BlockedIntents {
				for _, want := range []string{blocked.TriggeringDeliveryID, blocked.FlowPath + "/" + blocked.Family + "/" + blocked.SemanticPath} {
					if !strings.Contains(text, want) {
						t.Fatalf("missing blocked identity %q: %s", want, stdout.String())
					}
				}
			}
		})
	}
}
