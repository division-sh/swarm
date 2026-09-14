package serveapp

import (
	"context"
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
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

type numericForkErrorProbe struct {
	apiv1.RunForkExecutor
	t *testing.T
}

func (p numericForkErrorProbe) ExecuteRunFork(ctx context.Context, req apiv1.RunForkExecutionRequest) (apiv1.RunForkExecutionResult, error) {
	result, err := p.RunForkExecutor.ExecuteRunFork(ctx, req)
	if err != nil {
		p.t.Logf("original numeric fork failure: %v", err)
	}
	return result, err
}

func TestActivitySemanticResultExecutionParity(t *testing.T) {
	proveActivitySemanticResultExecutionParity(t, false, false)
}

func TestMockActivitySemanticResultExecutionParity(t *testing.T) {
	proveActivitySemanticResultExecutionParity(t, true, false)
}

func TestMockActivitySelectedForkSemanticResultExecutionParity(t *testing.T) {
	proveActivitySemanticResultExecutionParity(t, true, true)
}

func proveActivitySemanticResultExecutionParity(t *testing.T, mock, fork bool) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			if fork {
				prior := buildSelectedAPICapabilities
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					selected = owner
					caps, err := prior(owner, req)
					if err == nil && caps.RunFork != nil {
						caps.RunFork = numericForkErrorProbe{caps.RunFork, t}
					}
					return caps, err
				}
				t.Cleanup(func() { buildSelectedAPICapabilities = prior })
			}
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
			if fork {
				waitForkReceiverSourceCompletion(t, rt, run)
				var frontier string
				if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='numeric.completed'`, run).Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				before := readServedForkRecipientSourceDomain(t, rt, run)
				targetRoot := t.TempDir()
				copyReleaseFixtureTree(t, root, targetRoot)
				targetTool := strings.ReplaceAll(tool, "enum: [7]", "enum: [11]")
				targetNodes := strings.Split(string(nodes), "collector:")[0] + `requester:
  execution_type: system_node
  subscribes_to: [numeric.completed]
  event_handlers:
    numeric.completed:
      activity: {id: fetch, tool: numeric.fetch, input: {}}
collector:
  execution_type: system_node
  subscribes_to: [fetch.succeeded]
  event_handlers:
    fetch.succeeded:
      data_accumulation:
        source_event: fetch.succeeded
        writes:
          - target_field: score
            expression: 'payload.result.value + payload.result.nested.numbers[?0].value() + 2'
`
				for name, raw := range map[string]string{"nodes.yaml": targetNodes, "tools.yaml": targetTool} {
					if err := os.WriteFile(filepath.Join(targetRoot, name), []byte(raw), 0600); err != nil {
						t.Fatal(err)
					}
				}
				target := loadWorkflowValidationBundleAt(t, targetRoot)
				fact, err := prepareServeSourceArtifact(servedControlProofAuthorActivityContext(t, rt), selected.SourceArtifactWriter(), target)
				if err != nil || fact.BundleHash() == rt.BundleHash {
					t.Fatalf("selected numeric artifact: %v", err)
				}
				params := map[string]any{"source_run_id": run, "fork_event_id": frontier, "bundle_hash": fact.BundleHash(), "allow_source_freeze": true, "idempotency_key": "numeric-mock-fork"}
				// Selected execution must compile the retained artifact, not reread
				// this distinguishable current-directory responder declaration.
				if err := os.WriteFile(filepath.Join(targetRoot, "tools.yaml"), []byte(strings.ReplaceAll(targetTool, "enum: [11]", "enum: [99]")), 0600); err != nil {
					t.Fatal(err)
				}
				var child, replay apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &child)
				if child.ForkRunID == "" || child.ForkRunID == run || child.SourceRunID != run {
					t.Fatalf("fork identity: %+v", child)
				}
				requireNumericMockForkOutput(t, rt, child.ForkRunID)
				var attempts int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1 AND status='succeeded' AND execution_mode='mock'`, child.ForkRunID).Scan(&attempts); err != nil || attempts != 1 {
					t.Fatalf("fork mock attempts=%d err=%v", attempts, err)
				}
				rt = restart()
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
				requireNumericMockForkOutput(t, rt, child.ForkRunID)
				if !reflect.DeepEqual(child, replay) || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, run)) {
					t.Fatal("fork replay changed result or source")
				}
			}
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

func requireNumericMockForkOutput(t *testing.T, rt servedControlProofRuntime, run string) {
	t.Helper()
	waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, run)
	var entityID string
	if err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1`, run).Scan(&entityID); err != nil {
		t.Fatal(err)
	}
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": run, "entity_id": entityID}, &entity)
	if entity.Fields["score"] != float64(24) {
		t.Fatalf("selected mock arithmetic: %+v", entity.Fields)
	}
	var count int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1`, run).Scan(&count); err != nil || count != 1 {
		t.Fatalf("selected attempts=%d err=%v", count, err)
	}
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(result_payload AS TEXT) FROM activity_attempts WHERE run_id=$1 AND status='succeeded' AND execution_mode='mock'`, run).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	projected, err := workflowexpr.ProjectCELValue(decoded)
	if err != nil {
		t.Fatal(err)
	}
	result := projected.(map[string]any)["result"]
	want := map[string]any{"value": int64(11), "nested": map[string]any{"numbers": []any{int64(11)}, "fraction": float64(7.5)}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("selected journal result=%#v want=%#v", result, want)
	}
}
