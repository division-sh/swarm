package serveapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestQAUnapprovedRecordedHTTPWriteForkBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			prior := buildSelectedAPICapabilities
			buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
				caps, err := prior(owner, req)
				if err == nil && caps.RunFork != nil {
					caps.RunFork = numericForkErrorProbe{caps.RunFork, t}
				}
				return caps, err
			}
			t.Cleanup(func() { buildSelectedAPICapabilities = prior })
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost {
					t.Errorf("write method=%s, want POST", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"value":7,"nested":{"numbers":[7],"fraction":7.5}}`)
			}))
			t.Cleanup(provider.Close)
			root := semanticNumericIngressFixture(t)
			nodeFile := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodeFile)
			if err != nil {
				t.Fatal(err)
			}
			body := strings.ReplaceAll(string(nodes), "numeric.requested", "fetch.succeeded")
			body = strings.ReplaceAll(body, "payload.value", "payload.result.value")
			body = strings.ReplaceAll(body, "payload.nested", "payload.result.nested")
			body += `requester:
  execution_type: system_node
  subscribes_to: [numeric.requested]
  event_handlers:
    numeric.requested:
      activity:
        id: fetch
        tool: recorded_write
        input: {}
`
			if err := os.WriteFile(nodeFile, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			tool := fmt.Sprintf(`recorded_write:
  description: Loopback HTTP write with a recorded result and no approval.
  handler_type: http
  http: {method: POST, url: %q}
  output_schema:
    type: object
    required: [value, nested]
    properties:
      value: {type: integer}
      nested:
        type: object
        required: [numbers, fraction]
        properties:
          numbers: {type: array, items: {type: integer}}
          fraction: {type: number}
  response_success: {kind: http_status_2xx}
`, provider.URL)
			if err := os.WriteFile(filepath.Join(root, "tools.yaml"), []byte(tool), 0600); err != nil {
				t.Fatal(err)
			}
			rt, restart := startSemanticNumericRuntime(t, backend, root, false)
			first := semanticNumericRPC(t, rt.Endpoint, "http", semanticNumericRequest(rt.BundleHash, "event.publish", "", "qa-unapproved-write", "7"))
			if first.Error != nil {
				t.Fatalf("source event: %+v", first.Error)
			}
			var started struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(first.Result, &started); err != nil || started.RunID == "" {
				t.Fatalf("source run: %s err=%v", first.Result, err)
			}
			semanticNumericOutput(t, rt.Endpoint, rt.DB, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			var requestID, rawRequest string
			if err := rt.DB.QueryRow(`SELECT CAST(event_id AS TEXT), CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='platform.activity_requested'`, started.RunID).Scan(&requestID, &rawRequest); err != nil {
				t.Fatal(err)
			}
			var request map[string]json.RawMessage
			if err := json.Unmarshal([]byte(rawRequest), &request); err != nil {
				t.Fatal(err)
			}
			for _, retired := range []string{"effect_class", "fork_policy", "retry_max_attempts", "retry_backoff"} {
				if _, present := request[retired]; present {
					t.Fatalf("source request restored retired policy carrier %s", retired)
				}
			}
			var proposals int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM proposed_effect_continuations WHERE CAST(request_event_id AS TEXT)=$1`, requestID).Scan(&proposals); err != nil || proposals != 0 {
				t.Fatalf("unapproved control: proposals=%d err=%v", proposals, err)
			}
			var sourceResult string
			if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(request_event_id AS TEXT)=$1 AND status='succeeded' AND completed_at IS NOT NULL`, requestID).Scan(&sourceResult); err != nil || sourceResult == "" || calls.Load() != 1 {
				t.Fatalf("recorded source result=%s calls=%d err=%v", sourceResult, calls.Load(), err)
			}
			t.Logf("QA source_run=%s request=%s proposals=%d calls=%d result=%s", started.RunID, requestID, proposals, calls.Load(), sourceResult)
			response := requestServedJSONRPC(t, rt.Endpoint, "run.fork", map[string]any{
				"source_run_id": started.RunID, "fork_event_id": requestID,
				"bundle_hash": rt.BundleHash, "allow_source_freeze": true, "idempotency_key": "qa-recorded-write-fork",
			})
			if response.Error != nil {
				t.Errorf("recorded unapproved HTTP write must reuse its result, run.fork refused: %+v", response.Error)
			} else {
				var fork apiv1.RunForkExecutionResult
				if err := json.Unmarshal(response.Result, &fork); err != nil || fork.ForkRunID == "" {
					t.Fatalf("fork result=%s err=%v", response.Result, err)
				}
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
				requireQARecordedWriteForkConsumption(t, rt, fork.ForkRunID)
				var copied string
				if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1 AND status='succeeded'`, fork.ForkRunID).Scan(&copied); err != nil || copied == "" {
					t.Fatalf("copied result=%s err=%v", copied, err)
				}
				var sourcePayload, childPayload struct {
					Result json.RawMessage `json:"result"`
				}
				if err := json.Unmarshal([]byte(sourceResult), &sourcePayload); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(copied), &childPayload); err != nil {
					t.Fatal(err)
				}
				sourceValue, err := canonicaljson.Decode(sourcePayload.Result)
				if err != nil {
					t.Fatal(err)
				}
				childValue, err := canonicaljson.Decode(childPayload.Result)
				if err != nil {
					t.Fatal(err)
				}
				if !sourceValue.Equal(childValue) {
					t.Fatalf("recorded result was not reused: source=%s child=%s", sourceResult, copied)
				}
				var preserved any
				if err := canonicaljson.DecodePreservingNumberLexemes(childPayload.Result, &preserved); err != nil {
					t.Fatal(err)
				}
				projected, err := workflowexpr.ProjectCELValue(preserved)
				want := map[string]any{"value": int64(7), "nested": map[string]any{"numbers": []any{int64(7)}, "fraction": float64(7.5)}}
				if err != nil || !reflect.DeepEqual(projected, want) {
					t.Fatalf("recorded result changed numeric kinds: got=%#v want=%#v err=%v", projected, want, err)
				}
				previous := fork.ForkRunID
				for hop := 1; hop <= 2; hop++ {
					var childInput string
					if err := rt.DB.QueryRow(`SELECT CAST(event_id AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='platform.activity_requested'`, previous).Scan(&childInput); err != nil {
						t.Fatal(err)
					}
					reforked := requestServedJSONRPC(t, rt.Endpoint, "run.fork", map[string]any{
						"source_run_id": previous, "fork_event_id": childInput, "bundle_hash": rt.BundleHash,
						"allow_source_freeze": true, "idempotency_key": fmt.Sprintf("qa-recorded-write-refork-%d", hop),
					})
					var next apiv1.RunForkExecutionResult
					if reforked.Error != nil {
						t.Fatalf("child-input refork hop %d: %+v", hop, reforked.Error)
					}
					if err := json.Unmarshal(reforked.Result, &next); err != nil || next.ForkRunID == "" || next.ForkRunID == previous {
						t.Fatalf("child-input refork identity: %s error=%v", reforked.Result, err)
					}
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, next.ForkRunID)
					requireQARecordedWriteForkConsumption(t, rt, next.ForkRunID)
					if calls.Load() != 1 {
						t.Fatalf("child-input refork hop %d redispatched HTTP: %d", hop, calls.Load())
					}
					previous = next.ForkRunID
				}
				beforeReplay := qaRecordedWritePublicationCounts(t, rt, fork.ForkRunID)
				// Reopen and reread the immutable ACK rather than materializing
				// another child or borrowing the retired execution context.
				rt = restart()
				replayed := requestServedJSONRPC(t, rt.Endpoint, "run.fork", map[string]any{
					"source_run_id": started.RunID, "fork_event_id": requestID, "bundle_hash": rt.BundleHash,
					"allow_source_freeze": true, "idempotency_key": "qa-recorded-write-fork",
				})
				var recovered apiv1.RunForkExecutionResult
				if replayed.Error != nil {
					t.Fatalf("reopen original operation ACK: %+v", replayed.Error)
				}
				if err := json.Unmarshal(replayed.Result, &recovered); err != nil || recovered.ForkRunID != fork.ForkRunID || calls.Load() != 1 || qaRecordedWritePublicationCounts(t, rt, fork.ForkRunID) != beforeReplay {
					t.Fatalf("recovered ACK rematerialized or redispatched: %s calls=%d error=%v", replayed.Result, calls.Load(), err)
				}
			}
			var after string
			if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(request_event_id AS TEXT)=$1 AND status='succeeded'`, requestID).Scan(&after); err != nil || after != sourceResult || calls.Load() != 1 {
				t.Fatalf("fork changed recorded source or redispatched HTTP: result=%s calls=%d err=%v", after, calls.Load(), err)
			}
		})
	}
}

func requireQARecordedWriteForkConsumption(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	var consumed string
	if err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='numeric.completed'`, runID).Scan(&consumed); err != nil {
		t.Fatalf("fork did not actually consume the recorded activity result: %v", err)
	}
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(consumed), &value); err != nil {
		t.Fatal(err)
	}
	projected, err := workflowexpr.ProjectCELValue(value)
	want := map[string]any{"value": int64(15), "fraction": float64(8), "explicit_double": float64(8)}
	if err != nil || !reflect.DeepEqual(projected, want) {
		t.Fatalf("fork consumer changed numeric execution: got=%#v want=%#v error=%v", projected, want, err)
	}
}

func qaRecordedWritePublicationCounts(t *testing.T, rt servedControlProofRuntime, runID string) [2]int {
	t.Helper()
	var counts [2]int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name IN ('platform.activity_requested','fetch.succeeded','numeric.completed')`, runID).Scan(&counts[0]); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1`, runID).Scan(&counts[1]); err != nil {
		t.Fatal(err)
	}
	return counts
}
