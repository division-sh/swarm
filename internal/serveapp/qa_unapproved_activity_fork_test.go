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
  effect_class: non_idempotent_write
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
			rt, _ := startSemanticNumericRuntime(t, backend, root, false)
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
			var request struct {
				Policy string `json:"fork_policy"`
				Effect string `json:"effect_class"`
			}
			if err := json.Unmarshal([]byte(rawRequest), &request); err != nil {
				t.Fatal(err)
			}
			var proposals int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM proposed_effect_continuations WHERE CAST(request_event_id AS TEXT)=$1`, requestID).Scan(&proposals); err != nil || proposals != 0 {
				t.Fatalf("unapproved control: proposals=%d err=%v", proposals, err)
			}
			var sourceResult string
			if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(request_event_id AS TEXT)=$1 AND status='succeeded' AND completed_at IS NOT NULL`, requestID).Scan(&sourceResult); err != nil || sourceResult == "" || calls.Load() != 1 {
				t.Fatalf("recorded source result=%s calls=%d err=%v", sourceResult, calls.Load(), err)
			}
			t.Logf("QA source_run=%s request=%s policy=%s effect=%s proposals=%d calls=%d result=%s", started.RunID, requestID, request.Policy, request.Effect, proposals, calls.Load(), sourceResult)
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
				var copied string
				if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1 AND status='succeeded'`, fork.ForkRunID).Scan(&copied); err != nil || copied == "" {
					t.Fatalf("copied result=%s err=%v", copied, err)
				}
				var sourcePayload, childPayload map[string]any
				if err := json.Unmarshal([]byte(sourceResult), &sourcePayload); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(copied), &childPayload); err != nil {
					t.Fatal(err)
				}
				if sourcePayload["result"] == nil || !reflect.DeepEqual(sourcePayload["result"], childPayload["result"]) {
					t.Fatalf("recorded result was not reused: source=%s child=%s", sourceResult, copied)
				}
			}
			var after string
			if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE CAST(request_event_id AS TEXT)=$1 AND status='succeeded'`, requestID).Scan(&after); err != nil || after != sourceResult || calls.Load() != 1 {
				t.Fatalf("fork changed recorded source or redispatched HTTP: result=%s calls=%d err=%v", after, calls.Load(), err)
			}
		})
	}
}
