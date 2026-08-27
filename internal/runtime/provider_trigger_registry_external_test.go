package runtime_test

import (
	"context"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	stdruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type boundedProviderCredentialStore struct{}

func (boundedProviderCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	return key, key != "", nil
}
func (boundedProviderCredentialStore) Set(context.Context, string, string) error { return nil }
func (boundedProviderCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (boundedProviderCredentialStore) Delete(context.Context, string) error      { return nil }
func (s boundedProviderCredentialStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	value, present, err := s.Get(ctx, key)
	return runtimecredentials.NewAtomicSnapshot(runtimecredentials.Metadata{Key: key, Present: present}, value), err
}

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	return embeddedTriggerCatalog(t)
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *runtimepkg.RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...runtimepkg.InboundPersistence) *runtimepkg.InboundGateway {
	t.Helper()
	if bus != nil {
		bus.SetProviderOutputAuthorizationVerifier(testProviderTriggerCatalog(t))
	}
	gateway := runtimepkg.NewInboundGateway(bus, logger, shutdownAdmissionClosed, executionposture.Live, stores...)
	gateway.SetCredentialStore(boundedProviderCredentialStore{})
	return gateway
}

// handleBoundedProviderDelivery exercises provider parsing through the real
// standing-service inbound publication operation.
func handleBoundedProviderDelivery(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, target runtimepkg.InboundTarget, w http.ResponseWriter, r *http.Request, provider, signingSecret string) {
	t.Helper()
	_ = bus
	plan, err := testProviderTriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: target.Alias, Provider: provider, SigningSecret: signingSecret,
	})
	if err != nil {
		t.Fatalf("compile provider admission: %v", err)
	}
	target.Provider = provider
	target.SigningSecret = signingSecret
	target.AdmissionPlan = plan
	gateway.HandleResolvedWebhook(w, r, target, nil)
}

func TestBoundedProviderDeliveryRequiresPreResolvedStandingTarget(t *testing.T) {
	_, path, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("resolve bounded provider helper source")
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse bounded provider helper source: %v", err)
	}
	var handler *ast.FuncDecl
	for _, declaration := range file.Decls {
		candidate, ok := declaration.(*ast.FuncDecl)
		if ok && candidate.Name.Name == "handleBoundedProviderDelivery" {
			handler = candidate
			break
		}
	}
	if handler == nil {
		t.Fatal("bounded provider delivery helper is missing")
	}
	forbidden := map[string]struct{}{
		"seedBoundedStandingTarget":     {},
		"insertPostgresStandingFixture": {},
		"insertSQLiteStandingFixture":   {},
		"BeginTx":                       {},
		"Exec":                          {},
		"ExecContext":                   {},
	}
	ast.Inspect(handler.Body, func(node ast.Node) bool {
		switch expression := node.(type) {
		case *ast.Ident:
			if _, found := forbidden[expression.Name]; found {
				t.Errorf("admitted bounded provider handler contains forbidden live setup operation %s", expression.Name)
			}
		case *ast.SelectorExpr:
			if _, found := forbidden[expression.Sel.Name]; found {
				t.Errorf("admitted bounded provider handler contains forbidden live mutation %s", expression.Sel.Name)
			}
		}
		return true
	})

	hasResolvedTarget := false
	for _, field := range handler.Type.Params.List {
		for _, name := range field.Names {
			switch name.Name {
			case "runID", "entityID", "flowInstance", "persistence", "store":
				t.Errorf("admitted bounded provider handler retains mutable setup coordinate %q", name.Name)
			case "target":
				selector, ok := field.Type.(*ast.SelectorExpr)
				hasResolvedTarget = ok && selector.Sel.Name == "InboundTarget"
			}
		}
	}
	if !hasResolvedTarget {
		t.Error("admitted bounded provider handler does not require a pre-resolved inbound target")
	}
}

func seedBoundedStandingTarget(t *testing.T, ctx context.Context, persistence runtimepkg.InboundPersistence, runID, entityID, flowInstance, provider string) runtimepkg.InboundTarget {
	t.Helper()
	packageKey := "test.provider." + strings.ToLower(strings.TrimSpace(provider))
	flowID := "bounded-inbound"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	const bundleHash = "bundle-v1:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const bundleSource = "ephemeral"

	switch selected := persistence.(type) {
	case *storepkg.PostgresStore:
		insertPostgresStandingFixture(t, ctx, storetest.DatabaseForTest(selected), serviceID, packageKey, flowID, flowInstance, entityID, runID, bundleHash, bundleSource)
	case *storepkg.SQLiteRuntimeStore:
		insertSQLiteStandingFixture(t, ctx, selected, serviceID, packageKey, flowID, flowInstance, entityID, runID, bundleHash, bundleSource)
	default:
		t.Fatalf("unsupported bounded provider persistence %T", persistence)
	}

	return runtimepkg.InboundTarget{
		BundleHash: bundleHash, ServiceID: serviceID, PackageKey: packageKey,
		FlowID: flowID, RunID: runID, Generation: 1, PublicationSequence: 1,
		InstanceID: flowInstance, FlowInstance: flowInstance, EntityID: entityID,
		EntitySlug: entityID, Alias: entityID,
	}
}

const boundedProviderFlowID = "bounded_inbound"

// boundedStandingConnectorBundle puts connector consumers in the exact static
// flow path targeted by the bounded standing-ingress fixture. Process-served
// tests separately prove real standing singleton materialization.
func boundedStandingConnectorBundle(t *testing.T, flowInstance string, bundle *runtimecontracts.WorkflowContractBundle) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if bundle == nil || flowInstance == "" {
		return bundle
	}
	inputs := []runtimecontracts.FlowInputEventPin(nil)
	if bundle.RootSchema != nil {
		inputs = append(inputs, bundle.RootSchema.Pins.Inputs.EventPins...)
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: boundedProviderFlowID, Flow: boundedProviderFlowID},
		Path:  flowInstance,
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeStatic,
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: inputs}},
		},
		Nodes:  bundle.Nodes,
		Events: bundle.Events,
		Agents: bundle.Agents,
		Tools:  bundle.Tools,
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle.Nodes = nil
	bundle.Events = nil
	bundle.Agents = nil
	bundle.Tools = nil
	bundle.FlowTree = runtimecontracts.FlowTree{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			boundedProviderFlowID: &root.Children[0],
		},
	}
	bundle.FlowSchemas = map[string]runtimecontracts.FlowSchemaDocument{
		boundedProviderFlowID: flow.Schema,
	}
	admitted := loadRuntimeTempBundle(t, map[string]string{
		"package.yaml":                        "name: bounded-standing-connector\nversion: 1.0.0\nflows:\n  - id: bounded_inbound\n    flow: bounded_inbound\n    mode: static\n",
		"flows/bounded_inbound/schema.yaml":   "name: bounded_inbound\nmode: static\ninitial_state: active\nstates: [active]\n",
		"flows/bounded_inbound/entities.yaml": "bounded_entity: {}\n",
	})
	admitted.RootSchema = bundle.RootSchema
	admitted.Nodes = bundle.Nodes
	admitted.Events = bundle.Events
	admitted.Agents = bundle.Agents
	admitted.Tools = bundle.Tools
	admitted.FlowTree = bundle.FlowTree
	admitted.FlowSchemas = bundle.FlowSchemas
	if err := runtimecontracts.CompileWorkflowSemantics(admitted); err != nil {
		t.Fatalf("compile bounded standing connector semantics: %v", err)
	}
	return admitted
}

func insertPostgresStandingFixture(t *testing.T, ctx context.Context, db *sql.DB, serviceID, packageKey, flowID, instanceID, entityID, runID, bundleHash, bundleSource string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_services (
			service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
			operator_override, effective_state, current_bundle_hash, current_bundle_source,
			revision_sequence, current_generation, current_run_id, publication_state,
			publication_sequence, created_at, updated_at
		) VALUES ($1::uuid, $2, $3, $4, $5::uuid, TRUE, 'none', 'active', $6, $7, 1, 1, $8::uuid, 'published', 1, now(), now())
		ON CONFLICT (service_id) DO NOTHING
	`, serviceID, packageKey, flowID, instanceID, entityID, bundleHash, bundleSource, runID); err != nil {
		t.Fatalf("seed postgres standing service: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO standing_service_generations (service_id, generation, run_id, created_at)
		VALUES ($1::uuid, 1, $2::uuid, now())
		ON CONFLICT (service_id, generation) DO NOTHING
	`, serviceID, runID); err != nil {
		t.Fatalf("seed postgres standing generation: %v", err)
	}
}

func insertSQLiteStandingFixture(t *testing.T, ctx context.Context, selected *storepkg.SQLiteRuntimeStore, serviceID, packageKey, flowID, instanceID, entityID, runID, bundleHash, bundleSource string) {
	t.Helper()
	now := time.Now().UTC()
	db := storetest.DatabaseForTest(selected)
	if db == nil {
		t.Fatal("sqlite standing fixture database is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin sqlite standing fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_services (
				service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
				operator_override, effective_state, current_bundle_hash, current_bundle_source,
				revision_sequence, current_generation, current_run_id, publication_state,
				publication_sequence, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, TRUE, 'none', 'active', ?, ?, 1, 1, ?, 'published', 1, ?, ?)
			ON CONFLICT(service_id) DO NOTHING
		`, serviceID, packageKey, flowID, instanceID, entityID, bundleHash, bundleSource, runID, now, now); err != nil {
		t.Fatalf("seed sqlite standing service: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_generations (service_id, generation, run_id, created_at)
			VALUES (?, 1, ?, ?)
			ON CONFLICT(service_id, generation) DO NOTHING
		`, serviceID, runID, now); err != nil {
		t.Fatalf("seed sqlite standing generation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit sqlite standing fixture: %v", err)
	}
}
