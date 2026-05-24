package apiv1

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	bundle := runCompletionSystemNodeBundle(t)
	source := semanticview.Wrap(bundle)
	var coordinator *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle:    source,
		BundleFingerprint: runStartTestFingerprint,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := eventPublishTestHandler(t, pg, bus, source)

	module := newRunCompletionSystemNodeModule(t, source)
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  module,
		EventReceiptsCapability: pg.CanonicalEventReceiptsCapability,
		BundleFingerprint:       runStartTestFingerprint,
	})

	runID := "11111111-1111-4111-8111-111111111111"
	started := rpcCall(t, handler, runStartBody(runID, runStartTestFingerprint, "flow.started", `{"topic":"supported-surfaces"}`, "idem-system-node-run"))
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	if result := asMap(t, started.Result); result["run_id"] != runID || result["status"] != "running" {
		t.Fatalf("run.start result = %#v, want running run %s", result, runID)
	}

	run := waitForRunGetStatus(t, handler, runID, "completed")
	if run["run_id"] != runID {
		t.Fatalf("run.get run_id = %#v, want %s", run["run_id"], runID)
	}

	eventID := triggerEventIDForRun(t, db, runID)
	assertPipelineReceiptSucceeded(t, db, eventID)
	assertSystemNodeReceiptPersisted(t, db, eventID, "pipeline")
	assertSystemNodeDeliverySettled(t, db, eventID, "pipeline")
	assertRunEntityTerminal(t, db, runID, "done")

	diagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"diagnose","method":"run.diagnose","params":{"run_id":%q}}`, runID))
	if diagnose.Error != nil {
		t.Fatalf("run.diagnose error = %#v", diagnose.Error)
	}
	diagnosis := asMap(t, diagnose.Result)
	if diagnosis["operational_state"] != "completed" {
		t.Fatalf("run.diagnose operational_state = %#v, want completed", diagnosis["operational_state"])
	}
	diagnosisRun := asMap(t, diagnosis["run"])
	if diagnosisRun["status"] != "completed" {
		t.Fatalf("run.diagnose run.status = %#v, want completed", diagnosisRun["status"])
	}

	terminalPublish := rpcCall(t, handler, eventPublishBody(runID, runStartTestFingerprint, "flow.started", `{"topic":"after-terminal"}`, "", "idem-after-terminal"))
	if terminalPublish.Error == nil {
		t.Fatal("event.publish --run-id terminal error = nil")
	}
	if data := asMap(t, terminalPublish.Error.Data); data["code"] != RunAlreadyTerminalCode {
		t.Fatalf("event.publish --run-id terminal data = %#v, want %s", data, RunAlreadyTerminalCode)
	}
	if count := countEventsByName(t, db, "flow.started"); count != 1 {
		t.Fatalf("flow.started event count after terminal publish = %d, want 1", count)
	}
}

type runCompletionSystemNodeModule struct {
	source   semanticview.Source
	workflow *runtimepipeline.WorkflowDefinition
	nodes    []runtimepipeline.WorkflowNode
	guards   runtimepipeline.GuardRegistry
	actions  runtimepipeline.ActionRegistry
}

func newRunCompletionSystemNodeModule(t *testing.T, source semanticview.Source) runtimepipeline.WorkflowModule {
	t.Helper()
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return runCompletionSystemNodeModule{
		source:   source,
		workflow: workflow,
		nodes:    nodes,
		guards:   runtimepipeline.NewContractGuardRegistry(source),
		actions:  runtimepipeline.NewContractActionRegistry(source),
	}
}

func (m runCompletionSystemNodeModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m runCompletionSystemNodeModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m runCompletionSystemNodeModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m runCompletionSystemNodeModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m runCompletionSystemNodeModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

func waitForRunGetStatus(t *testing.T, handler *Handler, runID, wantStatus string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastStatus any
	for time.Now().Before(deadline) {
		get := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"get","method":"run.get","params":{"run_id":%q}}`, runID))
		if get.Error != nil {
			t.Fatalf("run.get error = %#v", get.Error)
		}
		run := asMap(t, asMap(t, get.Result)["run"])
		lastStatus = run["status"]
		if run["status"] == wantStatus {
			return run
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run.get status for %s = %#v, want %s", runID, lastStatus, wantStatus)
	return nil
}

func triggerEventIDForRun(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()
	var eventID string
	if err := db.QueryRow(`SELECT trigger_event_id::text FROM runs WHERE run_id = $1::uuid`, runID).Scan(&eventID); err != nil {
		t.Fatalf("load trigger event id: %v", err)
	}
	if strings.TrimSpace(eventID) == "" {
		t.Fatal("trigger_event_id is empty")
	}
	return eventID
}

func assertSystemNodeDeliverySettled(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	var status, reason string
	var deliveredAt sql.NullTime
	if err := db.QueryRow(`
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), delivered_at
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&status, &reason, &deliveredAt); err != nil {
		t.Fatalf("load system-node delivery: %v", err)
	}
	if status != "delivered" || reason != "node_processed" || !deliveredAt.Valid {
		t.Fatalf("system-node delivery = status:%q reason:%q delivered:%v, want delivered/node_processed/delivered_at", status, reason, deliveredAt.Valid)
	}
}

func assertSystemNodeReceiptPersisted(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	var outcome, reason string
	if err := db.QueryRow(`
		SELECT COALESCE(outcome, ''), COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load system-node receipt: %v", err)
	}
	if outcome != "no_op" || reason != "idempotent_no_op" {
		t.Fatalf("system-node receipt = outcome:%q reason:%q, want no_op/idempotent_no_op", outcome, reason)
	}
}

func assertPipelineReceiptSucceeded(t *testing.T, db *sql.DB, eventID string) {
	t.Helper()
	var outcome string
	if err := db.QueryRow(`
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome); err != nil {
		t.Fatalf("load pipeline receipt: %v", err)
	}
	if outcome != "success" {
		t.Fatalf("pipeline receipt outcome = %q, want success", outcome)
	}
}

func assertRunEntityTerminal(t *testing.T, db *sql.DB, runID, wantState string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`
		SELECT COALESCE(current_state, '')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $1::uuid
	`, runID).Scan(&state); err != nil {
		t.Fatalf("load run entity terminal state: %v", err)
	}
	if state != wantState {
		t.Fatalf("run entity current_state = %q, want %q", state, wantState)
	}
}

func runCompletionSystemNodeBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeRunCompletionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: review
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: discovery
    flow: discovery
    mode: static
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "entities.yaml"), `
run:
  topic: string
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: active
terminal_states:
  - done
states:
  - active
  - done
pins:
  inputs:
    events:
      - flow.started
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeRunCompletionFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeRunCompletionFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeRunCompletionFixtureFile(t, filepath.Join(root, "events.yaml"), `
{}
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
{}
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "flows", "discovery", "schema.yaml"), `
name: discovery
initial_state: active
terminal_states:
  - done
states:
  - active
  - done
pins:
  inputs:
    events:
      - flow.started
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "flows", "discovery", "events.yaml"), `
flow.started:
  entity_id:
    type: string
  topic:
    type: string
  required:
    - entity_id
`)
	writeRunCompletionFixtureFile(t, filepath.Join(root, "flows", "discovery", "nodes.yaml"), `
pipeline:
  id: pipeline
  execution_type: system_node
  subscribes_to:
    - flow.started
  event_handlers:
    flow.started:
      advances_to: done
`)
	repoRoot := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func runCompletionRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func writeRunCompletionFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
