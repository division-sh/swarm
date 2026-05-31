package runtime_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

const templateInstanceDeliveryRunID = "99999999-9999-4999-8999-999999999901"

func TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately(t *testing.T) {
	bundle := loadRuntimeTempBundle(t, templateInstanceDeliveryFixtureFiles())
	source := semanticview.Wrap(bundle)
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	ctx := seedRuntimeTestRun(t, db)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module: module,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	subscribed := make(chan struct{}, 1)
	pc.SetTestSubscribeHook(func() { subscribed <- struct{}{} })
	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go pc.Run(runCtx)
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow runtime did not subscribe")
	}
	if err := bus.AddFlowInstanceRoute(runtimecontracts.SystemNodeContract{}, runtimeflowidentity.DeriveRoute("operating", "inst-1")); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	eventID := "99999999-9999-4999-8999-999999999902"
	evt := (events.Event{
		ID:        eventID,
		RunID:     templateInstanceDeliveryRunID,
		Type:      events.EventType("operating/inst-1/opco.product_initialization_requested"),
		Payload:   []byte(`{"entity_id":"11111111-1111-4111-8111-111111111111"}`),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("11111111-1111-4111-8111-111111111111").WithFlowInstance("operating/inst-1")
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator' AND outcome = 'no_op'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'workflow-runtime'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'lifecycle-orchestrator'
	`, 1, eventID)
	assertRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM event_deliveries
		WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = '__runtime_replay_scope__'
	`, 1, eventID)
	waitRuntimeDBCount(t, ctx, db, `
		SELECT COUNT(*) FROM events
		WHERE event_name = 'operating/opco.ceo_ready'
	`, 1)
}

type runtimeTestWorkflowModule struct {
	source       semanticview.Source
	workflow     *runtimepipeline.WorkflowDefinition
	workflowNode []runtimepipeline.WorkflowNode
	guards       runtimepipeline.GuardRegistry
	actions      runtimepipeline.ActionRegistry
}

func newRuntimeTestWorkflowModule(t *testing.T, source semanticview.Source) runtimepipeline.WorkflowModule {
	t.Helper()
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	return &runtimeTestWorkflowModule{
		source:       source,
		workflow:     workflow,
		workflowNode: nodes,
		guards:       runtimepipeline.NewContractGuardRegistry(source),
		actions:      runtimepipeline.NewContractActionRegistry(source),
	}
}

func (m *runtimeTestWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *runtimeTestWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *runtimeTestWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.workflowNode...)
}
func (m *runtimeTestWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}
func (m *runtimeTestWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

func loadRuntimeTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load temp bundle: %v", err)
	}
	return bundle
}

func templateInstanceDeliveryFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
flows:
  - id: operating
    flow: operating
    mode: template
`,
		"flows/operating/schema.yaml": `name: operating
initial_state: initializing
terminal_states: [ready]
states: [initializing, ready]
auto_emit_on_create:
  event: opco.product_initialization_requested
`,
		"flows/operating/events.yaml": `opco.product_initialization_requested:
  entity_id: string
opco.ceo_ready:
  entity_id: string
`,
		"flows/operating/nodes.yaml": `lifecycle-orchestrator:
  id: lifecycle-orchestrator
  execution_type: system_node
  subscribes_to: [opco.product_initialization_requested]
  produces: [opco.ceo_ready]
  event_handlers:
    opco.product_initialization_requested:
      emit: opco.ceo_ready
`,
	}
}

func seedRuntimeTestRun(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(context.Background(), templateInstanceDeliveryRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, templateInstanceDeliveryRunID); err != nil {
		t.Fatalf("seed runtime test run: %v", err)
	}
	return ctx
}

func waitRuntimeDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var got int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d for query %s", got, want, strings.TrimSpace(query))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func assertRuntimeDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d for query %s", got, want, strings.TrimSpace(query))
	}
}
