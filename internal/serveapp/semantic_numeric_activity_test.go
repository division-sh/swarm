package serveapp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestActivitySemanticResultExecutionParity(t *testing.T) {
	proveActivitySemanticResultExecutionParity(t, false)
}

func TestMockActivitySemanticResultExecutionParity(t *testing.T) {
	proveActivitySemanticResultExecutionParity(t, true)
}

func proveActivitySemanticResultExecutionParity(t *testing.T, mock bool) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"value":7e0,"nested":{"numbers":[7.0],"fraction":7.5}}`)
			}))
			defer provider.Close()
			root := semanticNumericIngressFixture(t)
			nodeFile := filepath.Join(root, "nodes.yaml")
			nodes, err := os.ReadFile(nodeFile)
			if err != nil {
				t.Fatal(err)
			}
			numeric := strings.ReplaceAll(string(nodes), "numeric.requested", "fetch.succeeded")
			numeric = strings.ReplaceAll(numeric, "payload.value", "payload.result.value")
			numeric = strings.ReplaceAll(numeric, "payload.nested", "payload.result.nested")
			numeric += `requester:
  execution_type: system_node
  subscribes_to: [numeric.requested]
  event_handlers:
    numeric.requested:
      activity:
        id: fetch
        tool: numeric_provider
        input: {}
`
			if mock {
				numeric = strings.ReplaceAll(numeric, "tool: numeric_provider", "tool: numeric.fetch")
			}
			if err := os.WriteFile(nodeFile, []byte(numeric), 0600); err != nil {
				t.Fatal(err)
			}
			tool := fmt.Sprintf(`numeric_provider:
  description: Numeric result integration fixture.
  handler_type: http
  effect_class: non_idempotent_write
  http: {method: POST, url: %q}
  input_schema: {type: object}
  output_schema:
    type: object
    required: [value, nested]
    properties:
      value: {type: integer, enum: [7]}
      nested:
        type: object
        required: [numbers, fraction]
        properties:
          numbers: {type: array, minItems: 1, maxItems: 1, items: {type: integer, enum: [7]}}
          fraction: {type: number, enum: [7.5]}
  response_success: {kind: http_status_2xx}
`, provider.URL)
			if mock {
				tool = strings.Replace(tool, "numeric_provider:", "numeric.fetch:\n  category: provider_connector\n  credentials: [numeric_mock_secret]", 1)
			}
			if err := os.WriteFile(filepath.Join(root, "tools.yaml"), []byte(tool), 0600); err != nil {
				t.Fatal(err)
			}
			rt, restart := startSemanticNumericRuntime(t, backend, root, mock)
			key := uuid.NewString()
			first := semanticNumericRPC(t, rt.Endpoint, "http", semanticNumericRequest(rt.BundleHash, "event.publish", "", key, "7"))
			if first.Error != nil {
				t.Fatalf("activity ingress: %#v", first.Error)
			}
			var run string
			if err := rt.DB.QueryRow(`SELECT CAST(run_id AS TEXT) FROM events WHERE event_name='numeric.requested'`).Scan(&run); err != nil {
				t.Fatal(err)
			}
			semanticNumericOutput(t, rt.Endpoint, rt.DB, run)
			requireSemanticReplay(t, first.Result, semanticNumericRPC(t, rt.Endpoint, "http", semanticNumericRequest(rt.BundleHash, "event.publish", "", key, "7e0")))
			semanticNumericOutput(t, rt.Endpoint, rt.DB, run)
			rt = restart()
			requireSemanticReplay(t, first.Result, semanticNumericRPC(t, rt.Endpoint, "http", semanticNumericRequest(rt.BundleHash, "event.publish", "", key, "7.0")))
			semanticNumericOutput(t, rt.Endpoint, rt.DB, run)
			wantCalls := int32(1)
			mode := "live"
			if mock {
				wantCalls, mode = 0, "mock"
			}
			if calls.Load() != wantCalls {
				t.Fatalf("provider dispatches=%d", calls.Load())
			}
			var journaled int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1 AND status='succeeded' AND execution_mode=$2`, run, mode).Scan(&journaled); err != nil || journaled != 1 {
				t.Fatalf("journaled=%d err=%v", journaled, err)
			}
		})
	}
}
