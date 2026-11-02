package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"

	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	bundle := runCompletionSystemNodeBundle(t)
	source := semanticview.Wrap(bundle)
	bundleFact := sourceArtifactFactForTestBundle(t, bundle)
	bundleHash := bundleFact.BundleHash()
	var coordinator *runtimepipeline.PipelineCoordinator
	bus, err := newScopedAPITestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: bundleFact,
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
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, completeAPITestDurableWorkflowOptions(t, pg, bus, runtimepipeline.PipelineCoordinatorOptions{
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		DeliveryStore:       pg,
		DeliveryRuntime:     bus,
		PipelineObligations: pg.PipelineObligations(),
		RunLifecycle:        pg,
		SourceArtifactFact:  bundleFact,
	}))

	runID := "11111111-1111-4111-8111-111111111111"
	started := rpcCall(t, handler, runStartBody(runID, bundleHash, "flow.started", `{"topic":"supported-surfaces"}`, "idem-system-node-run"))
	if started.Error != nil {
		t.Fatalf("run.start error = %#v", started.Error)
	}
	if result := asMap(t, started.Result); result["run_id"] != runID || result["status"] != "live" {
		t.Fatalf("run.start result = %#v, want live run %s", result, runID)
	}
	requireAPIV1Convergence(t, "live instance final stage and settled work", func() (bool, error) {
		owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute("discovery", "discovery", "discovery")}
		instance, found, err := pg.LoadWorkflowInstance(context.Background(), owner)
		if err != nil {
			return false, err
		}
		settled, err := pg.LoadRunTestQuiescence(context.Background(), runID, time.Now().UTC())
		if err != nil {
			return false, err
		}
		return found && instance.StageDefined && instance.CurrentState == "done" && settled.Ready, nil
	})

	eventID := triggerEventIDForRun(t, db, runID)
	pipelineNodeID := identitytest.FlowNode(t, "discovery", "pipeline").Key()
	assertPipelineReceiptSucceeded(t, db, eventID)
	assertSystemNodeOutcomePersisted(t, db, eventID, pipelineNodeID)
	assertSystemNodeDeliverySettled(t, db, eventID, pipelineNodeID)
	rootNodeID := identitytest.FlowNode(t, ".", "root-completion").Key()
	assertSystemNodeOutcomePersisted(t, db, eventID, rootNodeID)
	assertSystemNodeDeliverySettled(t, db, eventID, rootNodeID)
	assertFlowEntityTerminal(t, pg, runID, runtimepipeline.FlowInstanceEntityID("discovery"), "discovery", "done")

	diagnose := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"diagnose","method":"run.diagnose","params":{"run_id":%q}}`, runID))
	if diagnose.Error != nil {
		t.Fatalf("run.diagnose error = %#v", diagnose.Error)
	}
	diagnosis := asMap(t, diagnose.Result)
	if settled := asMap(t, diagnosis["test_quiescence"]); settled["ready"] != true {
		t.Fatalf("run.diagnose work is not settled: %#v", settled)
	}
	diagnosisRun := asMap(t, diagnosis["run"])
	if diagnosisRun["run_id"] != runID || diagnosisRun["status"] != "live" {
		t.Fatalf("run.diagnose lost the live run: %#v", diagnosisRun)
	}
	if count := countEventsByName(t, pg, "flow.started"); count != 1 {
		t.Fatalf("flow.started event count after instance finish = %d, want 1", count)
	}
}

type runCompletionSystemNodeModule struct {
	source semanticview.Source
	nodes  []runtimepipeline.WorkflowNode
	guards runtimepipeline.GuardRegistry
}

func newRunCompletionSystemNodeModule(t *testing.T, source semanticview.Source) runtimepipeline.WorkflowModule {
	t.Helper()
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return runCompletionSystemNodeModule{
		source: source,
		nodes:  nodes,
		guards: runtimepipeline.NewContractGuardRegistry(source),
	}
}

func (m runCompletionSystemNodeModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m runCompletionSystemNodeModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m runCompletionSystemNodeModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
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
	var settledAt sql.NullTime
	if err := db.QueryRow(`
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), settled_at
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodeID).Scan(&status, &reason, &settledAt); err != nil {
		t.Fatalf("load system-node delivery: %v", err)
	}
	if status != "delivered" || reason != "" || !settledAt.Valid {
		t.Fatalf("system-node delivery = status:%q reason:%q settled:%v, want canonical delivered settlement", status, reason, settledAt.Valid)
	}
}

func assertSystemNodeOutcomePersisted(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	var outcome, reason string
	if err := db.QueryRow(`
		SELECT COALESCE(o.outcome, ''), COALESCE(o.reason_code, '')
		FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = $2
	`, eventID, nodeID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load system-node outcome: %v", err)
	}
	if outcome != "delivered" || reason != "" {
		t.Fatalf("system-node outcome = outcome:%q reason:%q, want delivered with no failure reason", outcome, reason)
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

func assertFlowEntityTerminal(t *testing.T, selected runtimepipeline.WorkflowInstancePersistenceReader, runID, entityID, flowInstance, wantState string) {
	t.Helper()
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(flowInstance, flowInstance, flowInstance)}
	instance, found, err := selected.LoadWorkflowInstance(context.Background(), owner)
	if err != nil || !found || instance.EntityID != entityID || instance.StorageRef != flowInstance || instance.WorkflowName != flowInstance || instance.EntityType != "" {
		t.Fatalf("fieldless flow identity = %+v, found=%t err=%v, want %s/%s/%s", instance, found, err, runID, entityID, flowInstance)
	}
	if !instance.StageDefined || instance.CurrentState != wantState || instance.Revision < 1 || instance.EnteredStageAt.IsZero() || instance.CreatedAt.IsZero() || instance.UpdatedAt.IsZero() {
		t.Fatalf("fieldless flow terminal lifecycle = %+v, want present named stage %q", instance, wantState)
	}
}

func runCompletionSystemNodeBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := canonicalrouting.CopyRunCompletionSystemNode(t)
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
	if isRunCompletionOptionalDeclarationFixture(path) && strings.TrimSpace(contents) == "{}" {
		t.Fatalf("optional declaration fixture %s must be omitted instead of written as {}", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func isRunCompletionOptionalDeclarationFixture(path string) bool {
	switch filepath.Base(path) {
	case "agents.yaml", "state.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml", "types.yaml":
		return true
	default:
		return false
	}
}
