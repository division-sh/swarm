package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func fanOutCLIPage() fanoutobligation.ListPage {
	runID := "4ca1069b-b1e5-450c-8122-eccfb3b790cf"
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	return fanoutobligation.ListPage{RunID: runID, RunStatus: "paused", ObservedAt: at, Order: fanoutobligation.ListIdentityOrder,
		Intents: []fanoutobligation.IntentReadback{{Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(), ElementRef: runtimecontracts.FanOutElementRef{FlowPath: "root", Family: "fan_out", SemanticPath: `handlers["ready"].rules[0]`}}, BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), Status: fanoutobligation.StatusOpen, DurableState: "eligible", Cardinality: 64, Cursor: 32, Owed: 32, NextChunkSize: 32, CreatedAt: at, UpdatedAt: at, Runtime: fanoutobligation.UnavailableRuntimeReadback()}}, NextCursor: "next-page"}
}

func TestFanOutReadCLIPageAndFilters(t *testing.T) {
	for _, format := range []string{"human", "json", "quiet", "root", "live", "live_no_commit", "live_json", "live_quiet"} {
		t.Run(format, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			page := fanOutCLIPage()
			if strings.HasPrefix(format, "live") {
				at := page.ObservedAt.Add(time.Second)
				eligible, workers, active, latency := false, 8, 3, 2.5
				page.Intents[0].Runtime = fanoutobligation.RuntimeReadback{ObservedAt: &at, Availability: "available", Reason: "run_paused", Eligible: &eligible, Workers: &workers, ActiveWorkers: &active, LastCommitMS: &latency}
				if format == "live_no_commit" {
					page.Intents[0].Runtime.LastCommitMS = nil
				}
			}
			flowPath := "root"
			if format == "root" {
				flowPath = "."
				page.Intents[0].Key.ElementRef.FlowPath = flowPath
			}
			var captured jsonRPCRequest
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/v1/rpc" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("wrong transport/auth")
				}
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Error(err)
				}
				raw, _ := json.Marshal(page)
				var result map[string]any
				_ = json.Unmarshal(raw, &result)
				writeJSONRPCResult(t, w, captured.ID, result)
			}))
			defer server.Close()
			args := []string{"run", "fan-out", "list", page.RunID, "--status", "open", "--flow-path", flowPath, "--limit", "1", "--cursor", "previous-page"}
			if format == "json" || format == "live_json" {
				args = append(args, "--json")
			}
			if format == "quiet" || format == "live_quiet" {
				args = append(args, "--quiet")
			}
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code=%d stderr=%s", code, errOut.String())
			}
			if calls != 1 || captured.Method != "run.fan_out.list" {
				t.Fatalf("calls=%d method=%s", calls, captured.Method)
			}
			params := captured.Params
			filter := params["filter"].(map[string]any)
			if params["run_id"] != page.RunID || params["limit"] != float64(1) || params["cursor"] != "previous-page" || filter["status"] != "open" || filter["flow_path"] != flowPath {
				t.Fatalf("params=%v", params)
			}
			switch format {
			case "live", "live_no_commit":
				latency := "2.5"
				if format == "live_no_commit" {
					latency = "unavailable"
				}
				for _, want := range []string{"runtime=available (run_paused)", "runtime_observed_at=2026-09-14T12:00:01Z", "eligible=false", "workers=8 active_workers=3", "last_commit_ms=" + latency} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("missing %s: %s", want, out.String())
					}
				}
			case "human", "root":
				for _, want := range []string{"run_status=paused", "state=eligible", "cursor=32/64", "owed=32", "chunk=32", "runtime=unavailable", "next_cursor=next-page"} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("missing %s: %s", want, out.String())
					}
				}
			case "json", "live_json":
				var got fanoutobligation.ListPage
				if err := json.Unmarshal(out.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(out.Bytes(), &wire); err != nil {
					t.Fatal(err)
				}
				runtime := wire["intents"].([]any)[0].(map[string]any)["runtime"].(map[string]any)
				if format == "live_json" {
					if runtime["observed_at"] != "2026-09-14T12:00:01Z" || runtime["last_commit_ms"] != 2.5 || runtime["eligible"] != false || runtime["workers"] != float64(8) || runtime["active_workers"] != float64(3) {
						t.Fatalf("live metrics=%s", out.String())
					}
					break
				}
				value, present := runtime["last_commit_ms"]
				if got.Intents[0].Runtime.LastCommitMS != nil || !present || value != nil {
					t.Fatalf("metrics=%s", out.String())
				}
			case "quiet", "live_quiet":
				if strings.TrimSpace(out.String()) != page.Intents[0].Key.String() {
					t.Fatalf("quiet=%s", out.String())
				}
			}
		})
	}
}

func TestFanOutReadCLIRejectsInvalidInputsAndMalformedPage(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	page := fanOutCLIPage()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONRPCResult(t, w, req.ID, map[string]any{"run_id": page.RunID, "intents": []any{}})
	}))
	defer server.Close()
	for _, flags := range [][]string{{"--limit", "0"}, {"--limit", "501"}, {"--limit", "-1"}, {"--status", "leased"}, {"--triggering-delivery-id", "not-a-uuid"}, {"--flow-path", ""}, {"--flow-path", "./"}} {
		var out, errOut bytes.Buffer
		args := append([]string{"run", "fan-out", "list", page.RunID}, flags...)
		if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server)); code == 0 {
			t.Fatalf("accepted %v", flags)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid inputs reached API: %d", calls)
	}
	var out, errOut bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "fan-out", "list", page.RunID}, &out, &errOut, testRootCommandOptions(server)); code == 0 {
		t.Fatal("malformed response accepted")
	}
}
