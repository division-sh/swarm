package runtime_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	stdruntime "runtime"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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
	owner, err := runtimecredentials.NewSnapshotOwner(boundedProviderCredentialStore{})
	if err != nil {
		t.Fatal(err)
	}
	gateway.SetCredentialAdmission(func(ctx context.Context, target runtimepkg.InboundTarget) (runtimecredentials.SecretBinding, func(context.Context) error, error) {
		projection := owner.BeginSecretBindingProjection()
		var binding runtimecredentials.SecretBinding
		if target.AdmissionPlan.RequiresSecret() && target.SigningSecret != "" {
			var err error
			binding, err = projection.ObserveSecretBinding(ctx, target.SigningSecret)
			if err != nil {
				return binding, nil, err
			}
		}
		return binding, projection.ValidateCurrent, nil
	})
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

func seedBoundedStandingTarget(t *testing.T, ctx context.Context, persistence runtimepkg.InboundPersistence, alias string) runtimepkg.InboundTarget {
	t.Helper()
	flowPath := boundedProviderFlowID
	serviceID := runtimeflowidentity.StandingServiceID(flowPath)
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || source.Validate() != nil {
		t.Fatal("bounded provider target requires an admitted source artifact fact")
	}
	bundleHash := source.BundleHash()
	writer, ok := persistence.(sourceartifactfixture.Writer)
	if !ok || bundleHash != sourceartifactfixture.BundleHash {
		t.Fatal("bounded component binding requires its exact admitted fixture artifact writer")
	}
	sourceartifactfixture.Require(t, ctx, writer)

	owner, ok := persistence.(runtimepipeline.StandingServicePersistence)
	if !ok {
		t.Fatalf("bounded provider persistence %T lacks the standing owner", persistence)
	}
	standing, err := owner.ReconcileStandingService(ctx, runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, FlowPath: flowPath, Source: source, BindingEnabled: true,
	})
	if err != nil {
		t.Fatalf("reconcile bounded standing binding: %v", err)
	}
	sequence, err := owner.PublishStandingService(ctx, standing.ServiceID, standing.RunID, standing.Generation)
	if err != nil {
		t.Fatalf("publish bounded standing binding: %v", err)
	}

	return runtimepkg.InboundTarget{
		BundleHash: bundleHash, ServiceID: serviceID, FlowPath: flowPath,
		RunID: standing.RunID, Generation: standing.Generation, PublicationSequence: sequence,
		Alias: alias,
	}
}

func boundedInboundTestCoordinates() (string, string) {
	return runtimeflowidentity.StandingGenerationRunID(runtimeflowidentity.StandingServiceID(boundedProviderFlowID), 1),
		runtimeflowidentity.EntityID(boundedProviderFlowID)
}

const boundedProviderFlowID = "bounded_inbound"

// boundedStandingConnectorBundle puts connector consumers in the exact static
// flow path targeted by the bounded standing-ingress fixture. Process-served
// tests separately prove real standing singleton materialization.
func boundedStandingConnectorBundle(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	if bundle == nil {
		return bundle
	}
	inputs := []runtimecontracts.FlowInputEventPin(nil)
	if bundle.RootSchema != nil {
		inputs = append(inputs, bundle.RootSchema.Pins.Inputs.EventPins...)
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: boundedProviderFlowID},
		Schema: runtimecontracts.FlowSchemaDocument{

			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: inputs}},
		},
		Nodes:  bundle.Nodes,
		Events: bundle.Events,
		Agents: bundle.Agents,
		Tools:  bundle.Tools,
	}
	admitted := loadRuntimeTempBundle(t, map[string]string{
		"schema.yaml":                   "name: bounded-standing-connector\n",
		"bounded_inbound/schema.yaml":   "name: bounded_inbound\nstages:\n  active: {}\n",
		"bounded_inbound/entities.yaml": "bounded_entity: {}\n",
	})
	admitted.RootSchema = bundle.RootSchema
	admitted.FlowTree.Root.Schema = *bundle.RootSchema
	admittedFlow, ok := admitted.FlowTree.ByPath[boundedProviderFlowID]
	if !ok {
		t.Fatalf("admitted bounded connector source omitted flow %q", boundedProviderFlowID)
	}
	// Keep the admitted lifecycle used by the shared inbound state fixture.
	flow.Schema = admittedFlow.Schema
	flow.Schema.Pins.Inputs.EventPins = inputs
	admittedFlow.Schema = flow.Schema
	admittedFlow.Nodes = flow.Nodes
	admittedFlow.Events = flow.Events
	admittedFlow.Agents = flow.Agents
	admittedFlow.Tools = flow.Tools
	admitted.FlowSchemas[boundedProviderFlowID] = flow.Schema
	if err := runtimecontracts.CompileWorkflowSemantics(admitted); err != nil {
		t.Fatalf("compile bounded standing connector semantics: %v", err)
	}
	return admitted
}
