package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/accprojection"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"gopkg.in/yaml.v3"
)

func stubSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

func requiredEventPayload(fields map[string]runtimecontracts.EventFieldSpec) runtimecontracts.EventCatalogEntry {
	required := make([]string, 0, len(fields))
	for field := range fields {
		required = append(required, field)
	}
	sort.Strings(required)
	return runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: fields, Required: required}}
}

func sourceWithEvents(entries map[string]runtimecontracts.EventCatalogEntry) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Events: entries})
}

func mustCompileEngineSource(bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(fmt.Sprintf("compile engine test semantics: %v", err))
	}
	return semanticview.Wrap(bundle)
}

func fanOutPayloadSource(t testing.TB, eventTypes ...string) semanticview.Source {
	t.Helper()
	events := make(map[string]runtimecontracts.EventCatalogEntry, len(eventTypes))
	for _, eventType := range eventTypes {
		events[eventType] = runtimecontracts.EventCatalogEntry{
			Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"items": {Type: "[json]"},
			}},
		}
	}
	return fanOutSourceWithBundleIdentity(t, &runtimecontracts.WorkflowContractBundle{Events: events})
}

func fanOutEntitySource(t testing.TB) semanticview.Source {
	t.Helper()
	return fanOutSourceWithBundleIdentity(t, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root", Version: "v-test"},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {Fields: map[string]runtimecontracts.EntityFieldDecl{
				"items": {Type: "[text]"},
			}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"replacement": {Type: "[text]"},
			}, Required: []string{"replacement"}}},
			"item.requested": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"item": {Type: "text"},
			}, Required: []string{"item"}}},
		},
	})
}

func fanOutSourceWithBundleIdentity(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	packageFile := filepath.Join(root, "package.yaml")
	platformFile := filepath.Join(root, "platform-spec.yaml")
	if err := os.WriteFile(packageFile, []byte("name: engine-fan-out-test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(platformFile, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle.Paths = runtimecontracts.ContractPaths{
		ContractsRoot:      root,
		ProjectPackageFile: packageFile,
		PlatformSpecFile:   platformFile,
	}
	return semanticview.Wrap(bundle)
}

func sourceWithStructuredRendererModule(t *testing.T) (semanticview.Source, runtimecontracts.PolicyModule) {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	modulePath := filepath.Join(root, "modules", "structured_renderer.wasm")
	if err := os.MkdirAll(filepath.Dir(modulePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modulePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	module := runtimecontracts.PolicyModule{
		Path:   "modules/structured_renderer.wasm",
		ABI:    "core-json-v1",
		Entry:  "compute",
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		InputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"component", "owner", "language", "files"},
			"properties": map[string]any{
				"component": map[string]any{"type": "string"},
				"owner":     map[string]any{"type": "string"},
				"language":  map[string]any{"type": "string"},
				"files": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
		},
		OutputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"content", "format", "line_count"},
			"properties": map[string]any{
				"content":    map[string]any{"type": "string"},
				"format":     map[string]any{"type": "string"},
				"line_count": map[string]any{"type": "integer"},
			},
		},
		Limits: runtimecontracts.PolicyModuleLimits{
			Gas:         5_000_000,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "render", Flow: "render"},
		Policy: runtimecontracts.PolicyDocument{Modules: map[string]runtimecontracts.PolicyModule{
			"structured_renderer": module,
		}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{ContractsRoot: root},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &flow,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"render": &flow,
			},
		},
	}
	return semanticview.Wrap(bundle), module
}

func sourceWithPythonRendererModule(t *testing.T) (semanticview.Source, runtimecontracts.PolicyModule) {
	t.Helper()
	source := []byte(`def handle(input):
    lines = [
        "component: " + input["component"],
        "owner: " + input["owner"],
        "language: " + input["language"],
    ]
    for name in input["files"]:
        if name.endswith(".yaml"):
            lines.append("- deploy/" + name)
        elif name.endswith(".go"):
            lines.append("- src/" + name)
        else:
            lines.append("- " + name)
    return {"content": "\n".join(lines), "format": "yaml", "line_count": len(lines)}
`)
	return sourceWithPythonRendererSource(t, source)
}

func sourceWithPythonRendererSource(t *testing.T, source []byte) (semanticview.Source, runtimecontracts.PolicyModule) {
	t.Helper()
	root := t.TempDir()
	modulePath := filepath.Join(root, "modules", "python_renderer.py")
	if err := os.MkdirAll(filepath.Dir(modulePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modulePath, source, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(source)
	module := runtimecontracts.PolicyModule{
		Path:   "modules/python_renderer.py",
		Kind:   pythonmodule.Kind,
		ABI:    pythonmodule.ABI,
		Entry:  pythonmodule.DefaultEntry,
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		InputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"component", "owner", "language", "files"},
			"properties": map[string]any{
				"component": map[string]any{"type": "string"},
				"owner":     map[string]any{"type": "string"},
				"language":  map[string]any{"type": "string"},
				"files": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
		},
		OutputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"content", "format", "line_count"},
			"properties": map[string]any{
				"content":    map[string]any{"type": "string"},
				"format":     map[string]any{"type": "string"},
				"line_count": map[string]any{"type": "integer"},
			},
		},
		Limits: runtimecontracts.PolicyModuleLimits{
			Gas:         2_500_000_000,
			MemoryPages: 8192,
			OutputBytes: 4096,
		},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "render", Flow: "render"},
		Policy: runtimecontracts.PolicyDocument{Modules: map[string]runtimecontracts.PolicyModule{
			"python_renderer": module,
		}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{ContractsRoot: root},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &flow,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"render": &flow,
			},
		},
	}
	return semanticview.Wrap(bundle), module
}

func newStructuredRendererExecutor(t *testing.T, source semanticview.Source) *Executor {
	t.Helper()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`computed.rendered_bundle.format == "yaml"`: func(base BaseContext) (bool, error) {
			rendered, _ := base.Computed.Raw()["rendered_bundle"].(map[string]any)
			return rendered["format"] == "yaml", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	return exec
}

func structuredRendererModuleSpec() *runtimecontracts.ComputeModuleSpec {
	return &runtimecontracts.ComputeModuleSpec{
		RowID:  "render_bundle",
		Module: "structured_renderer",
		Into:   "computed.rendered_bundle",
		Input: map[string]string{
			"component": "payload.component",
			"owner":     "payload.owner",
			"language":  "payload.language",
			"files":     "payload.files",
		},
		InputPaths: map[string]runtimepaths.Path{
			"component": runtimepaths.Parse("payload.component"),
			"owner":     runtimepaths.Parse("payload.owner"),
			"language":  runtimepaths.Parse("payload.language"),
			"files":     runtimepaths.Parse("payload.files"),
		},
	}
}

func pythonRendererModuleSpec() *runtimecontracts.ComputeModuleSpec {
	spec := structuredRendererModuleSpec()
	spec.Module = "python_renderer"
	return spec
}

func structuredRendererExecutionRequest(t *testing.T, moduleSpec *runtimecontracts.ComputeModuleSpec) ExecutionRequest {
	t.Helper()
	return ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testFlowExecutableNode(t, "render", "render-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("render.requested"),
			"",
			"",
			mustEncodeJSON(t, map[string]any{
				"component": "api",
				"owner":     "platform",
				"language":  "go",
				"files":     []any{"main.go", "README.md", "service.yaml"},
			}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "render_bundle",
					PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindModule, Module: moduleSpec},
					Compute: &runtimecontracts.ComputeSpec{
						Operation: runtimecontracts.ComputeOpModule,
						StoreAs:   "computed.rendered_bundle",
						Module:    moduleSpec,
					},
				},
				{
					ID:        "rendered_yaml",
					Condition: `computed.rendered_bundle.format == "yaml"`,
					Emit: runtimecontracts.EmitSpec{Event: "bundle.rendered", Fields: map[string]runtimecontracts.ExpressionValue{
						"content": runtimecontracts.RefExpression("computed.rendered_bundle.content"),
					}},
				},
			},
		},
	}
}

func sourceWithDeclarativeEmitExternalizationFlows() semanticview.Source {
	component := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "component-scaffold", Flow: "component-scaffold", Mode: "template", PackageKey: "."},
		Path:  "component-scaffold",
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: runtimecontracts.FlowModeTemplate,
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "component.scaffolded"}}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"component.scaffolded": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"component-node": {ID: "component-node"},
		},
	}
	repo := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "repo-scaffold", Flow: "repo-scaffold", PackageKey: "."},
		Path:  "repo-scaffold",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "repo_scaffold.repo_scaffolded"}}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo_scaffold.repo_scaffolded": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"items": {Type: "[json]"},
				}},
			},
		},
	}
	operating := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating", PackageKey: "."},
		Path:  "operating",
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{PackageKey: "."}, Children: []runtimecontracts.FlowContractView{component, repo, operating}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"component.scaffolded":          component.Events["component.scaffolded"],
			"repo_scaffold.repo_scaffolded": repo.Events["repo_scaffold.repo_scaffolded"],
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"component-scaffold": component.Schema,
			"repo-scaffold":      repo.Schema,
			"operating":          operating.Schema,
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"component-scaffold": &root.Children[0],
				"repo-scaffold":      &root.Children[1],
				"operating":          &root.Children[2],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"component-scaffold": &root.Children[0],
				"repo-scaffold":      &root.Children[1],
				"operating":          &root.Children[2],
			},
		},
	}
	return mustCompileEngineSource(bundle)
}

func sourceWithPolicy(values map[string]any) semanticview.Source {
	policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{}}
	for key, value := range values {
		policy.Values[key] = runtimecontracts.PolicyValue{Value: value}
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: policy,
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"ScoredItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{
				"score":    {Type: "integer"},
				"active":   {Type: "boolean"},
				"status":   {Type: "text"},
				"name":     {Type: "text"},
				"category": {Type: "text"},
			}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"digest.requested": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"score": {Type: "integer"}, "items": {Type: "[ScoredItem]"},
			}),
			"items.submitted": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"category": {Type: "text"}, "items": {Type: "[ScoredItem]"},
			}),
		},
	})
}

func stubSourceWithRootEntityContract() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"items": {Type: "[json]"},
				}},
			},
		},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Analysis": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"summary":      {Type: "text"},
						"report_count": {Type: "integer"},
					},
				},
				"VerticalState": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"status":      {Type: "text"},
						"active_jobs": {Type: "[Job]"},
					},
				},
				"Job": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"id":    {Type: "text"},
						"title": {Type: "text"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"analysis":  {Type: "Analysis"},
					"verticals": {Type: "map[text]VerticalState"},
					"tags":      {Type: "[text]"},
				},
			},
		},
	})
}

func sourceWithKilledState() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Stages: []runtimecontracts.WorkflowStageContract{
				{ID: "pending"},
				{ID: "killed"},
			},
			TerminalStages: []string{"killed"},
		},
	})
}

type stubStateRepo struct{}
type recordingStateRepo struct {
	saves int
}
type stubEntityCollectionReader struct {
	entityType string
	flowID     string
	rows       []map[string]any
	calls      int
	err        error
}

func (r *stubEntityCollectionReader) QueryEntityCollection(_ context.Context, flowID, entityType string) ([]map[string]any, error) {
	r.calls++
	r.flowID = flowID
	r.entityType = entityType
	return r.rows, r.err
}

type testPublicationCommitter interface {
	CommitPublications(context.Context, []EmitIntent) error
}
type stubMutationOwner struct {
	state StateRepository
}
type composedMutationOwner struct {
	state        StateRepository
	verifier     EmitPersistenceVerifier
	lifecycle    WorkflowLifecycleEffectOwner
	activities   ActivityIntentWriter
	publications testPublicationCommitter
	order        *[]string
}
type stubLocker struct{}
type recordingPublicationCommitter struct {
	intents []EmitIntent
	err     error
}
type stubDispatcher struct{}
type stubActionRegistry struct {
	entries map[identity.ActionKey]runtimeregistry.ActionInstruction
}
type stubActionRunner struct {
	called []string
}
type lockOrderStateRepo struct {
	order *[]string
}
type lockOrderLocker struct {
	order *[]string
}
type stubEvaluator struct {
	bools map[string]bool
	errs  map[string]error
}
type contextualBoolEvaluator struct {
	bools map[string]func(BaseContext) (bool, error)
}
type stubGuardRegistry struct {
	entries map[identity.GuardKey]runtimeregistry.GuardInstruction
}
type stubPayloadShaper struct{}
type recordingTransitionValidator struct {
	calls   int
	current string
	next    string
}
type recordingPayloadShaper struct {
	lastReq     ExecutionRequest
	lastPayload map[string]any
	lastSurface EmitSurface
	err         error
}
type eventErrPayloadShaper struct {
	failEvent string
	shaped    []string
}

func (stubStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (stubStateRepo) SaveState(context.Context, StateAddress, StateMutation) error { return nil }
func (r *recordingStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, nil
}
func (r *recordingStateRepo) SaveState(context.Context, StateAddress, StateMutation) error {
	r.saves++
	return nil
}
func (o stubMutationOwner) CommitEngineMutation(ctx context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	if o.state != nil {
		if err := o.state.SaveState(ctx, mutation.Address, mutation.State); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	return CommittedEngineMutation{
		ActivityIntents: append([]ActivityIntent(nil), mutation.ActivityIntents...),
		EmitIntents:     append([]EmitIntent(nil), mutation.EmitIntents...),
	}, nil
}
func (o composedMutationOwner) CommitEngineMutation(ctx context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	if o.order != nil {
		*o.order = append(*o.order, "tx")
	}
	if o.state != nil {
		if err := o.state.SaveState(ctx, mutation.Address, mutation.State); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	if o.lifecycle != nil && len(mutation.LifecycleEffects) > 0 {
		if err := o.lifecycle.ApplyWorkflowLifecycleEffects(ctx, mutation.LifecycleEffects); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	if o.activities != nil && len(mutation.ActivityIntents) > 0 {
		if err := o.activities.WriteActivityIntents(ctx, mutation.ActivityIntents); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	if o.verifier != nil && len(mutation.EmitPrerequisites.Fields) > 0 {
		if err := o.verifier.VerifyEmitPersistence(ctx, mutation.Address, mutation.EmitPrerequisites); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	if o.publications != nil && len(mutation.EmitIntents) > 0 {
		if err := o.publications.CommitPublications(ctx, mutation.EmitIntents); err != nil {
			return CommittedEngineMutation{}, err
		}
	}
	return CommittedEngineMutation{
		ActivityIntents: append([]ActivityIntent(nil), mutation.ActivityIntents...),
		EmitIntents:     append([]EmitIntent(nil), mutation.EmitIntents...),
	}, nil
}
func (stubLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}
func (r lockOrderStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	if r.order != nil {
		*r.order = append(*r.order, "load")
	}
	return testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}), true, nil
}
func (lockOrderStateRepo) SaveState(context.Context, StateAddress, StateMutation) error {
	return nil
}
func (l lockOrderLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	if l.order != nil {
		*l.order = append(*l.order, "lock")
	}
	return fn(ctx)
}
func (o *recordingPublicationCommitter) CommitPublications(_ context.Context, intents []EmitIntent) error {
	if o.err != nil {
		return o.err
	}
	o.intents = append(o.intents, intents...)
	return nil
}
func (stubDispatcher) DispatchPostCommit(context.Context, []EmitIntent) error { return nil }
func (s stubEvaluator) EvalBool(expression string, _ BaseContext) (bool, error) {
	if err := s.errs[expression]; err != nil {
		return false, err
	}
	return s.bools[expression], nil
}
func (s stubEvaluator) EvalValue(string, BaseContext) (any, error) { return nil, ErrNotImplemented }
func (s contextualBoolEvaluator) EvalBool(expression string, base BaseContext) (bool, error) {
	if fn, ok := s.bools[expression]; ok {
		return fn(base)
	}
	return false, nil
}
func (s contextualBoolEvaluator) EvalValue(string, BaseContext) (any, error) {
	return nil, ErrNotImplemented
}
func (r stubGuardRegistry) HasGuard(id identity.GuardKey) bool     { _, ok := r.entries[id]; return ok }
func (r stubGuardRegistry) IsExecutable(id identity.GuardKey) bool { _, ok := r.entries[id]; return ok }
func (r stubGuardRegistry) Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool) {
	entry, ok := r.entries[id]
	return entry, ok
}
func (r stubActionRegistry) HasAction(id identity.ActionKey) bool { _, ok := r.entries[id]; return ok }
func (r stubActionRegistry) IsExecutable(id identity.ActionKey) bool {
	_, ok := r.entries[id]
	return ok
}
func (r stubActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	entry, ok := r.entries[id]
	return entry, ok
}
func (r *stubActionRunner) ExecuteAction(_ context.Context, action runtimecontracts.ActionSpec, _ runtimeregistry.ActionInstruction, _ ExecutionContext) (ActionExecution, error) {
	r.called = append(r.called, action.ID)
	return ActionExecution{Handled: true}, nil
}
func (stubPayloadShaper) ShapeEmitPayload(_ context.Context, _ ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	out := cloneStringAnyMap(payload)
	out["shaped_for"] = eventType
	return out, nil
}
func (v *recordingTransitionValidator) ValidateTransition(currentState, nextState string) error {
	v.calls++
	v.current = currentState
	v.next = nextState
	return nil
}
func (s *recordingPayloadShaper) ShapeEmitPayload(ctx context.Context, req ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	s.lastReq = req
	s.lastPayload = cloneStringAnyMap(payload)
	s.lastSurface = EmitSurfaceFromContext(ctx)
	if s.err != nil {
		return nil, s.err
	}
	out := cloneStringAnyMap(payload)
	out["shaped_for"] = eventType
	return out, nil
}
func (s *eventErrPayloadShaper) ShapeEmitPayload(_ context.Context, _ ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	s.shaped = append(s.shaped, eventType)
	if eventType == s.failEvent {
		return nil, errors.New("payload shape failed")
	}
	out := cloneStringAnyMap(payload)
	out["shaped_for"] = eventType
	return out, nil
}

type testWorkflowLifecycleOwner struct {
	effects []runtimeworkflowlifecycle.Effect
	order   *[]string
}

func (o *testWorkflowLifecycleOwner) AcceptedEventEffect(route runtimeflowidentity.Route, entityID identity.EntityID, event events.Event, fromState, toState string) (runtimeworkflowlifecycle.Effect, error) {
	var transition *runtimeworkflowlifecycle.Transition
	if strings.TrimSpace(toState) != "" && strings.TrimSpace(toState) != strings.TrimSpace(fromState) {
		value, err := runtimeworkflowlifecycle.NewTransition(fromState, toState, "test-transition")
		if err != nil {
			return runtimeworkflowlifecycle.Effect{}, err
		}
		transition = &value
	}
	return runtimeworkflowlifecycle.NewAcceptedEvent(route, entityID, event.ID(), string(event.Type()), event.ExecutionMode(), event.CreatedAt(), transition)
}

func (o *testWorkflowLifecycleOwner) ApplyWorkflowLifecycleEffects(_ context.Context, effects []runtimeworkflowlifecycle.Effect) error {
	o.effects = append(o.effects, effects...)
	if o.order != nil {
		*o.order = append(*o.order, "lifecycle")
	}
	return nil
}

func TestExecutorTimerReconciliationRequiresExactEventAuthority(t *testing.T) {
	owner := &testWorkflowLifecycleOwner{}
	executor := &Executor{deps: RuntimeDependencies{Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Stage: "waiting", StageOwned: true, Event: "timer.timeout", Delay: "1h",
		}}},
	}), WorkflowLifecycle: owner}}
	frame, err := executor.newExecutionFrame(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Route:    runtimeflowidentity.RouteForInstancePath("flow-1"),
		Event: eventtest.RunCreatingRootIngress(
			"event-1", "work.received", "", "", json.RawMessage(`{}`), 0, "", "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, "flow-1"), time.Time{},
		),
		State: StateSnapshot{CurrentState: "waiting"},
	})
	if err != nil {
		t.Fatalf("newExecutionFrame: %v", err)
	}
	if _, _, err := executor.buildWorkflowLifecycleEffect(&frame); err == nil || !strings.Contains(err.Error(), "exact event identity") {
		t.Fatalf("buildWorkflowLifecycleEffect error = %v, want exact-authority refusal", err)
	}
}

func TestExecutorTimerReconciliationCarriesOnlyActualTransitionTarget(t *testing.T) {
	owner := &testWorkflowLifecycleOwner{}
	executor := &Executor{deps: RuntimeDependencies{Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "waiting.timeout", Event: "timer.timeout", StartOn: "state:waiting", Delay: "1h",
		}}},
	}), WorkflowLifecycle: owner}}
	createdAt := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	frame, err := executor.newExecutionFrame(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Route:    runtimeflowidentity.RouteForInstancePath("flow-1"),
		Event: eventtest.RunCreatingRootIngress(
			"event-1", "work.noted", "", "", json.RawMessage(`{}`), 0, "", "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, "flow-1"), createdAt,
		),
		State: StateSnapshot{CurrentState: "waiting"},
	})
	if err != nil {
		t.Fatalf("newExecutionFrame: %v", err)
	}

	effect, ok, err := executor.buildWorkflowLifecycleEffect(&frame)
	if err != nil {
		t.Fatalf("build event-only lifecycle effect: %v", err)
	}
	if !ok {
		t.Fatal("event-only lifecycle effect missing")
	}
	if _, hasTransition := effect.Transition(); hasTransition {
		t.Fatalf("event-only lifecycle effect = %#v, want absent transition", effect)
	}

	frame.result.NextState = "done"
	frame.result.StateMutation.NextState = "done"
	effect, ok, err = executor.buildWorkflowLifecycleEffect(&frame)
	if err != nil {
		t.Fatalf("build transition lifecycle effect: %v", err)
	}
	transition, hasTransition := effect.Transition()
	if !ok || !hasTransition || transition.From() != "waiting" || transition.To() != "done" {
		t.Fatalf("transition lifecycle effect = %#v, want waiting -> done", effect)
	}
}

func testStateSnapshot(currentState string, metadata map[string]any, gates map[string]bool, buckets map[string]map[string]any) StateSnapshot {
	return StateSnapshot{
		CurrentState: currentState,
		StateCarrier: NewStateCarrier(metadata, gates, buckets),
	}
}

func eventPayloadMap(t *testing.T, evt events.Event) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(evt.Payload(), &out); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	return out
}

func TestNewExecutor_DefaultsMaxChainDepth(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	if got := exec.MaxChainDepth(); got != DefaultMaxChainDepth {
		t.Fatalf("MaxChainDepth = %d, want %d", got, DefaultMaxChainDepth)
	}
}

func TestExecutor_ValidateRequestAllowsDeepInboundChainDepth(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		MaxChainDepth: 2,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	if err := exec.ValidateRequest(ExecutionRequest{Node: testRootExecutableNode(t, "validation-node"), ChainDepth: 3}); err != nil {
		t.Fatalf("ValidateRequest error = %v, want nil", err)
	}
}

func TestExecutionRequestStateAddressUsesExplicitExecutionFlowForRootDeclaration(t *testing.T) {
	req := ExecutionRequest{
		EntityID:        identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:            testRootExecutableNode(t, "root-node"),
		ExecutionFlowID: identity.NormalizeFlowID("root-workflow"),
		Route:           runtimeflowidentity.StoredRoute("root-workflow", "instance", "root-workflow/instance"),
	}
	address := req.StateAddress()
	if got := address.FlowID.String(); got != "root-workflow" {
		t.Fatalf("state address flow_id = %q, want explicit root execution flow", got)
	}
	if req.Node.FlowID() != "" {
		t.Fatalf("root declaration flow_id = %q, want empty declaration coordinate", req.Node.FlowID())
	}
}

func TestExecutorValidateRequestRejectsDurableRouteWithoutExecutionFlow(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Node:  testRootExecutableNode(t, "root-node"),
		Route: runtimeflowidentity.StoredRoute("root-workflow", "instance", "root-workflow/instance"),
	})
	if err == nil || !strings.Contains(err.Error(), "exact execution flow identity is required") {
		t.Fatalf("ValidateRequest error = %v, want missing execution-flow authority", err)
	}
}

func TestExecutor_ValidateRequestRejectsConflictingCompletionDialect(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnComplete: []runtimecontracts.HandlerRuleEntry{{Condition: "true"}},
			Rules:      []runtimecontracts.HandlerRuleEntry{{Condition: "else"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both on_complete and rules") {
		t.Fatalf("ValidateRequest error = %v, want conflicting completion error", err)
	}
}

func TestExecutor_ValidateRequestRejectsPlatformEntityWriteTargets(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	cases := map[string]runtimecontracts.SystemNodeEventHandler{
		"data accumulation": {
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetPathRef: "_entity.current_state",
					Value:         runtimecontracts.CELExpression(`"done"`),
				}},
			},
		},
		"shared store_as": {
			Count: &runtimecontracts.CountSpec{
				Source:  "payload.items",
				StoreAs: "_entity.current_state",
			},
		},
	}
	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			err := exec.ValidateRequest(ExecutionRequest{Node: testRootExecutableNode(t, "validation-node"), Handler: handler})
			if err == nil || !strings.Contains(err.Error(), "_entity is read-only platform entity metadata") {
				t.Fatalf("ValidateRequest error = %v, want _entity read-only write-target rejection", err)
			}
		})
	}
}

func TestExecutionScopeResolveOperand_AllowsSupportedEventRouteRoot(t *testing.T) {
	scope := newExecutionScope(
		nil,
		map[string]any{"entity_id": "payload-entity"},
		map[string]any{"source": map[string]any{"entity_id": "source-entity"}},
		nil,
		nil,
		nil,
	)

	got, err := scope.resolveOperand("event.source.entity_id", executionOperandDefaultNone)
	if err != nil {
		t.Fatalf("resolveOperand(event.source.entity_id) error: %v", err)
	}
	if got != "source-entity" {
		t.Fatalf("resolveOperand(event.source.entity_id) = %#v, want source-entity", got)
	}
}

func TestExecutionScopeResolveOperand_RejectsLegacyEventReceiverProjection(t *testing.T) {
	scope := newExecutionScope(
		nil,
		nil,
		map[string]any{"entity_id": "legacy-entity"},
		nil,
		nil,
		nil,
	)

	_, err := scope.resolveOperand("event.entity_id", executionOperandDefaultNone)
	if err == nil {
		t.Fatal("expected event.entity_id to fail closed")
	}
	if !strings.Contains(err.Error(), "event.entity_id is unsupported") {
		t.Fatalf("resolveOperand error = %q, want event.entity_id unsupported", err.Error())
	}
}

func TestCompiledExecutionCondition_AllowsSupportedEventRouteRoot(t *testing.T) {
	compiled, err := compileExecutionCondition(`event.source.entity_id == "source-entity"`, workflowexpr.ValueExpressionOptions{})
	if err != nil {
		t.Fatalf("compileExecutionCondition error: %v", err)
	}

	scope := newExecutionScope(
		nil,
		map[string]any{"entity_id": "payload-entity"},
		map[string]any{"source": map[string]any{"entity_id": "source-entity"}},
		nil,
		nil,
		nil,
	)

	ok, err := compiled.Eval(scope)
	if err != nil {
		t.Fatalf("compiled condition Eval error: %v", err)
	}
	if !ok {
		t.Fatal("compiled condition evaluated false, want true")
	}
}

func TestCompiledExecutionCondition_RejectsLegacyEventReceiverProjection(t *testing.T) {
	for _, expression := range []string{
		`event.flow_instance == "legacy-flow"`,
		`event["flow_instance"] == "legacy-flow"`,
	} {
		t.Run(expression, func(t *testing.T) {
			_, err := compileExecutionCondition(expression, workflowexpr.ValueExpressionOptions{})
			if err == nil {
				t.Fatalf("expected %q to fail closed", expression)
			}
			if !strings.Contains(err.Error(), "event.flow_instance is unsupported") {
				t.Fatalf("compileExecutionCondition error = %q, want event.flow_instance unsupported", err.Error())
			}
		})
	}
}

func TestExecutionScopeResolveOperand_AllowsPlatformEntityRoot(t *testing.T) {
	scope := newExecutionScope(
		nil,
		nil,
		nil,
		map[string]any{"id": "business-id"},
		map[string]any{"id": "platform-id"},
		nil,
	)

	got, err := scope.resolveOperand("_entity.id", executionOperandDefaultNone)
	if err != nil {
		t.Fatalf("resolveOperand(_entity.id) error: %v", err)
	}
	if got != "platform-id" {
		t.Fatalf("resolveOperand(_entity.id) = %#v, want platform-id", got)
	}
}

func TestExecutor_ValidateRequestRejectsCreateEntityWithAccumulate(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			Accumulate:   &runtimecontracts.AccumulateSpec{},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/accumulate error", err)
	}
}

func TestExecutor_ValidateRequestRejectsCreateEntityWithSelectEntity(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			SelectEntity: &runtimecontracts.SelectEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "vertical_id", Ref: "payload.vertical_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and select_entity") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/select_entity error", err)
	}
}

func TestExecutor_ValidateRequestRejectsCreateEntityWithSelectOrCreateEntity(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			CreateEntity: true,
			SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and select_or_create_entity") {
		t.Fatalf("ValidateRequest error = %v, want create_entity/select_or_create_entity error", err)
	}
}

func TestExecutor_ValidateRequestRejectsSelectEntityWithSelectOrCreateEntity(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			SelectEntity: &runtimecontracts.SelectEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
			SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
				Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "repo_id", Ref: "payload.repo_id"}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "declares both select_entity and select_or_create_entity") {
		t.Fatalf("ValidateRequest error = %v, want select_entity/select_or_create_entity error", err)
	}
}

func TestExecutor_ValidateRequestRejectsTieredWeightedAverageWithoutDimensionKey(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpWeightedAverage,
				Keys: runtimecontracts.ComputeKeyConfig{
					ScoreKeys: []string{"score"},
				},
				Tiers: []runtimecontracts.ComputeTier{{Dimensions: []string{"build_complexity"}, Weight: 1}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "keys.dimension_key") {
		t.Fatalf("ValidateRequest error = %v, want keys.dimension_key error", err)
	}
}

func TestExecutor_ValidateRequestRejectsTieredWeightedAverageWithoutScoreKeys(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpWeightedAverage,
				Keys: runtimecontracts.ComputeKeyConfig{
					DimensionKey: "dimension",
				},
				Tiers: []runtimecontracts.ComputeTier{{Dimensions: []string{"build_complexity"}, Weight: 1}},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "keys.score_keys") {
		t.Fatalf("ValidateRequest error = %v, want keys.score_keys error", err)
	}
}

func TestExecutor_LoadsStateInsideEntityLock(t *testing.T) {
	order := []string{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     lockOrderStateRepo{order: &order},
		MutationOwner: stubMutationOwner{},
		Locker:        lockOrderLocker{order: &order},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("test.event"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
			time.Now().UTC(),
		),

		State: StateSnapshot{StateCarrier: NewStateCarrier(map[string]any{}, nil, map[string]map[string]any{})},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got, want := order, []string{"lock", "load"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestExecutor_StepOrderIsStable(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	steps := exec.Steps()
	if len(steps) != 23 {
		t.Fatalf("step count = %d, want 23", len(steps))
	}
	if steps[0] != StepLoop || steps[1] != StepQuery || steps[len(steps)-1] != StepClear {
		t.Fatalf("unexpected step order: %v", steps)
	}
	if steps[6] != StepFilter {
		t.Fatalf("expected filter at index 6, got order %v", steps)
	}
	if steps[17] != StepProjection {
		t.Fatalf("expected projection after data_writes at index 17, got order %v", steps)
	}
	if steps[21] != StepActivity {
		t.Fatalf("expected activity after action at index 21, got order %v", steps)
	}
}

func TestExecutor_ShapeEmitPayloadUsesUpdatedState(t *testing.T) {
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	req := ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testFlowExecutableNode(t, "scoring", "scoring-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("scoring/score.dimension_complete"),
			"",
			"",
			[]byte(`{"dimension":"build_complexity","score":80}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "11111111-1111-1111-1111-111111111111"),
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),

		State: StateSnapshot{
			EntityID:     identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
			CurrentState: "discovered",
			StateCarrier: NewStateCarrier(map[string]any{
				"composite_score": 0,
			}, nil, map[string]map[string]any{}),
		},
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					SourceField: "score",
					TargetField: "composite_score",
				}},
			},
			OnComplete: []runtimecontracts.HandlerRuleEntry{
				{Condition: "else", Emit: runtimecontracts.EmitSpec{Event: "vertical.rejected"}},
			},
		},
	}
	req.State.SetField("dimensions_requested", []any{"build_complexity"})
	result, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emit intents = %d, want 1", len(result.EmitIntents))
	}
	if got := shaper.lastReq.State.StateCarrier.Fields["composite_score"]; got != 80.0 && got != 80 {
		t.Fatalf("payload shaper saw composite_score = %#v, want 80", got)
	}
}

func accumulatorProjectionTestSource(t testing.TB) semanticview.Source {
	t.Helper()
	return fanOutSourceWithBundleIdentity(t, &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root", Version: "v-test"},
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension":  {Type: "text"},
						"tier":       {Type: "integer"},
						"score":      {Type: "integer"},
						"evidence":   {Type: "text"},
						"confidence": {Type: "text"},
					},
				},
				"DimensionSummary": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension":  {Type: "text"},
						"score":      {Type: "integer"},
						"confidence": {Type: "text"},
						"source":     {Type: "text"},
					},
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"projection": {Value: map[string]any{"default_confidence": "medium"}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"vertical": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scores": {
						Type:            "[DimensionScore]",
						Initial:         []any{},
						MaterializeFrom: "scoring-node.dimensions_received",
					},
					"summary": {
						Type:            "[DimensionSummary]",
						Initial:         []any{},
						MaterializeFrom: "scoring-node.dimensions_received",
						Project: map[string]any{
							"dimension":  "source.dimension",
							"score":      "source.score",
							"confidence": "policy.projection.default_confidence",
							"source":     "scoring-node",
						},
					},
					"unrelated_invalid_scores": {
						Type:            "[DimensionScore]",
						Initial:         []any{},
						MaterializeFrom: "other-node.missing_buffer",
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scoring-node": {
				StateSchema: runtimecontracts.NodeStateSchema{
					Fields: []runtimecontracts.NodeStateField{{Name: "dimensions_received", Type: "[DimensionScore]"}},
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.dimension_complete": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "dimensions_received"},
					},
				},
			},
			"other-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"score.unrelated": {
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "missing_buffer"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"score.dimension_complete": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"vertical_id": {Type: "uuid"},
					"dimension":   {Type: "text"},
					"tier":        {Type: "integer"},
					"score":       {Type: "integer"},
					"evidence":    {Type: "text"},
					"confidence":  {Type: "text"},
					"targets":     {Type: "[text]"},
				}, Required: []string{"vertical_id", "dimension", "tier", "score", "evidence", "confidence", "targets"}},
			},
		},
	})
}

func TestExecutor_AccumulatorProjectionMaterializesTypedEntityFieldBeforeEmit(t *testing.T) {
	source := accumulatorProjectionTestSource(t)
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		Emit: runtimecontracts.EmitSpec{
			Event: "vertical.scored",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"scores": runtimecontracts.RefExpression("entity.scores"),
			},
		},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "scoring-node"),
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	scores, ok := result.StateMutation.Fields["scores"].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected scores = %#v", result.StateMutation.Fields["scores"])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected score item = %#v", scores[0])
	}
	if _, exists := score["event_id"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
	if _, exists := score["vertical_id"]; exists {
		t.Fatalf("projected score leaked payload extra field: %#v", score)
	}
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	summaries, ok := result.StateMutation.Fields["summary"].([]any)
	if !ok || len(summaries) != 1 {
		t.Fatalf("projected summary = %#v", result.StateMutation.Fields["summary"])
	}
	summary, ok := summaries[0].(map[string]any)
	if !ok {
		t.Fatalf("projected summary item = %#v", summaries[0])
	}
	if got := summary["confidence"]; got != "medium" {
		t.Fatalf("projected summary confidence = %#v", got)
	}
	if got := summary["source"]; got != "scoring-node" {
		t.Fatalf("projected summary literal source = %#v", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	emittedScores, ok := emitted["scores"].([]any)
	if !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
}

func TestAccumulatorProjection_OmitsAbsentOptionalNamedFields(t *testing.T) {
	binding := accprojection.Binding{
		SourceType: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
			{Name: "id", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
			{Name: "note", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}, IsOptional: true},
		}},
		TargetType: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
			{Name: "id", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
			{Name: "note", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}, IsOptional: true},
			{Name: "summary", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}, IsOptional: true},
		}},
		Project: map[string]any{
			"id":   "source.id",
			"note": "source.note",
		},
	}
	typed, err := accumulatorTypedView(binding, map[string]any{"id": "item-1"})
	if err != nil {
		t.Fatalf("accumulatorTypedView: %v", err)
	}
	if !reflect.DeepEqual(typed, map[string]any{"id": "item-1"}) {
		t.Fatalf("typed view = %#v, want absent optional field omitted", typed)
	}

	projected, err := (&Executor{}).projectAccumulatorItem(nil, binding, typed)
	if err != nil {
		t.Fatalf("projectAccumulatorItem: %v", err)
	}
	if !reflect.DeepEqual(projected, map[string]any{"id": "item-1"}) {
		t.Fatalf("projected item = %#v, want absent optional fields omitted", projected)
	}
}

func TestAccumulatorTypedView_RejectsAbsentRequiredNamedField(t *testing.T) {
	_, err := accumulatorTypedView(accprojection.Binding{
		SourceType: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
			{Name: "id", TypeRef: "text", Type: runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}},
		}},
	}, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), `missing required typed-view field "id"`) {
		t.Fatalf("error = %v, want missing required field", err)
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWithoutOnComplete(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		Emit: runtimecontracts.EmitSpec{
			Event: "vertical.scored",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"scores": runtimecontracts.RefExpression("entity.scores"),
			},
		},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	score := requireProjectedScore(t, result, "scores")
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	emittedScores, ok := emitted["scores"].([]any)
	if !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
}

func TestExecutor_AuthoredEmptyRuleCannotReachSelection(t *testing.T) {
	for _, raw := range []string{
		"rules:\n  - {}\n",
		"rules:\n  selected: {}\n",
		"on_complete:\n  - {}\n",
	} {
		var handler runtimecontracts.SystemNodeEventHandler
		err := yaml.Unmarshal([]byte(raw), &handler)
		if err == nil || !strings.Contains(err.Error(), "EMPTY-AUTHORED-RULE") {
			t.Fatalf("yaml.Unmarshal error = %v, want pre-execution authored-row rejection for %s", err, raw)
		}
		if len(handler.Rules) != 0 || len(handler.OnComplete) != 0 {
			t.Fatalf("rejected authored row reached executable handler: %#v", handler)
		}
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWithRulesBeforeEmitFields(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "metadata.handler_marker",
				Value:       runtimecontracts.LiteralExpression("top-level"),
			}},
		},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "matched",
			Condition: "else",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.rule_marker",
					Value:       runtimecontracts.LiteralExpression("rule"),
				}},
			},
			Emit: runtimecontracts.EmitSpec{
				Event: "vertical.scored",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"scores":         runtimecontracts.RefExpression("entity.scores"),
					"handler_marker": runtimecontracts.RefExpression("metadata.handler_marker"),
					"rule_marker":    runtimecontracts.RefExpression("metadata.rule_marker"),
				},
			},
		}},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	requireProjectedScore(t, result, "scores")
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "matched" {
		t.Fatalf("RuleID = %q, want matched", got)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextRules || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionSelected || !result.HandlerRuleSelection.Ref().Valid() {
		t.Fatalf("selected rules fact = %#v", result.HandlerRuleSelection)
	}
	if got := result.StateMutation.Fields["handler_marker"]; got != "top-level" {
		t.Fatalf("handler_marker = %#v, want top-level", got)
	}
	if got := result.StateMutation.Fields["rule_marker"]; got != "rule" {
		t.Fatalf("rule_marker = %#v, want rule", got)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", len(result.EmitIntents))
	}
	var emitted map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &emitted); err != nil {
		t.Fatalf("emit payload json: %v", err)
	}
	if emittedScores, ok := emitted["scores"].([]any); !ok || len(emittedScores) != 1 {
		t.Fatalf("emit payload scores = %#v", emitted["scores"])
	}
	if got := emitted["handler_marker"]; got != "top-level" {
		t.Fatalf("emit handler_marker = %#v, want top-level", got)
	}
	if got := emitted["rule_marker"]; got != "rule" {
		t.Fatalf("emit rule_marker = %#v, want rule", got)
	}
}

func TestExecutor_AccumulatorProjectionMaterializesWhenRulesDoNotMatch(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{bools: map[string]bool{
		"payload.score > 100": false,
	}})
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		Rules: []runtimecontracts.HandlerRuleEntry{{
			ID:        "too-high",
			Condition: "payload.score > 100",
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.rule_marker",
					Value:       runtimecontracts.LiteralExpression("unexpected"),
				}},
			},
		}},
	}
	result := executeAccumulatorProjectionTestEvent(t, exec, handler, testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}))
	requireProjectedScore(t, result, "scores")
	if got := strings.TrimSpace(result.HandlerRuleSelection.DisplayLabel()); got != "" {
		t.Fatalf("RuleID = %q, want empty when rules do not match", got)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextRules || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionNoMatch || result.HandlerRuleSelection.Ref().Valid() {
		t.Fatalf("no-match rules fact = %#v", result.HandlerRuleSelection)
	}
	if _, ok := result.StateMutation.Fields["rule_marker"]; ok {
		t.Fatalf("rule_marker unexpectedly written: %#v", result.StateMutation.Fields)
	}
}

func TestExecutor_RuleEvaluationFailureCarriesExactAttemptedIdentity(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  - element_id: 00000000-0000-4000-8000-000000000425
    id: attempted-rule
    condition: evaluator.failure
`), &handler); err != nil {
		t.Fatal(err)
	}
	node := testRootExecutableNode(t, "scoring-node")
	qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("condition evaluator failed")
	exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{errs: map[string]error{"evaluator.failure": wantErr}})
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     node,
		Event: eventtest.RunCreatingRootIngress(
			"evt-evaluation-failure", "score.dimension_complete", "", "", json.RawMessage(`{"score":87}`),
			0, "", "", events.EventEnvelope{}, time.Time{},
		),
		Handler: qualified,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Execute error = %v, want %v", err, wantErr)
	}
	fact := result.HandlerRuleSelection
	if fact.Context() != handlerselection.ContextRules || fact.Disposition() != handlerselection.DispositionEvaluationFailed || fact.DisplayLabel() != "attempted-rule" {
		t.Fatalf("failed evaluation fact = %#v", fact)
	}
	if got := fact.Ref().ElementID().String(); got != "00000000-0000-4000-8000-000000000425" {
		t.Fatalf("attempted element ID = %q", got)
	}
}

func TestExecutor_UnsupportedConditionIsExactFailedEvaluation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		field       string
		elementID   string
		wantContext handlerselection.Context
	}{
		{name: "rules", field: "rules", elementID: "00000000-0000-4000-8000-000000000427", wantContext: handlerselection.ContextRules},
		{name: "on_complete", field: "on_complete", elementID: "00000000-0000-4000-8000-000000000428", wantContext: handlerselection.ContextOnComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler runtimecontracts.SystemNodeEventHandler
			raw := fmt.Sprintf(`%s:
  - element_id: %s
    id: unsupported-condition
    condition: unsupported.condition
`, tc.field, tc.elementID)
			if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
				t.Fatal(err)
			}
			node := testRootExecutableNode(t, "unsupported-condition-node")
			qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
			if err != nil {
				t.Fatal(err)
			}
			exec := newAccumulatorProjectionTestExecutor(t, stubEvaluator{errs: map[string]error{"unsupported.condition": ErrNotImplemented}})
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1",
				Node:     node,
				Event: eventtest.RunCreatingRootIngress(
					"evt-unsupported-"+tc.name, "score.dimension_complete", "", "", json.RawMessage(`{"score":87}`),
					0, "", "", events.EventEnvelope{}, time.Time{},
				),
				Handler: qualified,
				State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if !errors.Is(err, ErrNotImplemented) {
				t.Fatalf("Execute error = %v, want %v", err, ErrNotImplemented)
			}
			fact := result.HandlerRuleSelection
			if fact.Context() != tc.wantContext || fact.Disposition() != handlerselection.DispositionEvaluationFailed || fact.DisplayLabel() != "unsupported-condition" {
				t.Fatalf("unsupported evaluation fact = %#v", fact)
			}
			if got := fact.Ref().ElementID().String(); got != tc.elementID {
				t.Fatalf("attempted element ID = %q, want %q", got, tc.elementID)
			}
		})
	}
}

func TestExecutor_AccumulatorProjectionMaterializesBeforeTopLevelFanOutEmitFields(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "metadata.handler_marker",
				Value:       runtimecontracts.LiteralExpression("top-level"),
			}},
		},
		FanOut: &runtimecontracts.FanOutSpec{
			ItemsFrom: "payload.targets",
			As:        "target_item",
			Identity:  "target_item",
			Emit: runtimecontracts.EmitSpec{
				Event: "vertical.scored",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"handler_marker": runtimecontracts.RefExpression("metadata.handler_marker"),
					"scores":         runtimecontracts.RefExpression("entity.scores"),
					"target":         runtimecontracts.CELExpression("target_item"),
				},
			},
		},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "scoring-node"),
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-accumulator"),
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high","targets":["agent-a"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	requireProjectedScore(t, result, "scores")
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeFannedOut)
	}
	if got := result.StateMutation.Fields["handler_marker"]; got != "top-level" {
		t.Fatalf("handler_marker state mutation = %#v, want top-level", got)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("trigger transaction created eager fan-out emits: %#v", result.EmitIntents)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 1 {
		t.Fatalf("durable fan-out intent = %#v, want cardinality 1", result.FanOutIntent)
	}
	if got := result.FanOutIntent.Capsule.StateFields["handler_marker"]; got != "top-level" {
		t.Fatalf("fan-out capsule handler_marker = %#v, want top-level", got)
	}
	if scores, ok := result.FanOutIntent.Capsule.Entity["scores"].([]any); !ok || len(scores) != 1 {
		t.Fatalf("fan-out capsule projected scores = %#v, want one score", result.FanOutIntent.Capsule.Entity["scores"])
	}
}

func TestExecutor_AccumulatorProjectionBindsEntityFanOutSourceAfterProjection(t *testing.T) {
	exec := newAccumulatorProjectionTestExecutor(t, nil)
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			Into:      "dimensions_received",
			DedupBy:   "payload.dimension",
			DedupPath: runtimecontracts.RefExpression("payload.dimension").RefPath,
		},
		FanOut: &runtimecontracts.FanOutSpec{
			ItemsFrom: "entity.scores",
			As:        "score_item",
			Identity:  "score_item.dimension",
			Emit: runtimecontracts.EmitSpec{
				Event: "vertical.scored",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"dimension": runtimecontracts.CELExpression("score_item.dimension"),
				},
			},
		},
	}
	node := testRootExecutableNode(t, "scoring-node")
	qualified, err := completeSemanticFixtureHandlerRuleIdentity(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := semanticview.Bundle(exec.deps.Source)
	if !ok || bundle == nil {
		t.Fatal("fan-out source has no contract bundle")
	}
	if err := bundle.CompileFanOutHandlerPlans(node, "score.dimension_complete", qualified); err != nil {
		t.Fatal(err)
	}
	plans := bundle.FanOutPlansForHandler(node, "score.dimension_complete")
	if len(plans) != 1 {
		t.Fatalf("compiled fan-out plans = %d, want 1", len(plans))
	}
	plan := plans[0]
	if !plan.SourceAfterWrites {
		t.Fatal("compiled fan-out plan did not classify accumulator projection as a source mutation")
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("00000000-0000-4000-8000-000000002276"),
		Node:     node,
		Event: eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-projected-source"),
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{"scores": []any{}}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	requireProjectedScore(t, result, "scores")
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 1 {
		t.Fatalf("projected fan-out intent = %#v, want cardinality 1", result.FanOutIntent)
	}
	if result.FanOutIntent.Source.Kind != "entity_field_revision" || result.FanOutIntent.Source.Field != "scores" {
		t.Fatalf("projected fan-out source = %#v", result.FanOutIntent.Source)
	}
	if _, copied := result.FanOutIntent.Capsule.Entity["scores"]; copied {
		t.Fatalf("projected source was redundantly copied into capsule: %#v", result.FanOutIntent.Capsule.Entity)
	}
}

func newAccumulatorProjectionTestExecutor(t *testing.T, evaluator Evaluator) *Executor {
	t.Helper()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        accumulatorProjectionTestSource(t),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, evaluator)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	return exec
}

func executeAccumulatorProjectionTestEvent(t *testing.T, exec *Executor, handler runtimecontracts.SystemNodeEventHandler, state StateSnapshot) ExecutionResult {
	t.Helper()
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "scoring-node"),
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"score.dimension_complete", "", "", json.RawMessage(`{"vertical_id":"11111111-1111-1111-1111-111111111111","dimension":"market","tier":2,"score":87,"evidence":"strong","confidence":"high"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: handler,
		State:   state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	return result
}

func requireProjectedScore(t *testing.T, result ExecutionResult, field string) map[string]any {
	t.Helper()
	scores, ok := result.StateMutation.Fields[field].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected %s = %#v", field, result.StateMutation.Fields[field])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected %s item = %#v", field, scores[0])
	}
	if _, exists := score["event_id"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
	if _, exists := score["vertical_id"]; exists {
		t.Fatalf("projected score leaked payload extra field: %#v", score)
	}
	return score
}

func TestExecutor_AccumulatorProjectionMaterializesForQualifiedRuntimeEvent(t *testing.T) {
	source := semanticview.Wrap(loadEngineProjectionFlowBundle(t))
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	scoringNode := testFlowExecutableNode(t, "scoring", "scoring-node")
	handler, ok := source.ExecutableNodeEventHandler(scoringNode, "scoring/score.dimension_complete")
	if !ok {
		t.Fatal("expected qualified runtime event to resolve to authored local handler")
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     scoringNode,
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"scoring/score.dimension_complete", "", "", json.RawMessage(`{"dimension":"market","score":87}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		HandlerEventKey: "score.dimension_complete",
		Handler:         handler,
		State:           testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if _, ok := loadAccumulatorForBucket(StateSnapshot{StateCarrier: result.StateMutation.StateCarrier}, accumulatorBucketRef(scoringNode, "score.dimension_complete")); !ok {
		t.Fatalf("logical accumulator bucket missing from state mutation: %#v", result.StateMutation.StateCarrier.StateBuckets)
	}
	if _, ok := loadAccumulatorForBucket(StateSnapshot{StateCarrier: result.StateMutation.StateCarrier}, accumulatorBucketRef(scoringNode, "scoring/score.dimension_complete")); ok {
		t.Fatalf("concrete runtime event bucket survived in state mutation: %#v", result.StateMutation.StateCarrier.StateBuckets)
	}
	scores, ok := result.StateMutation.Fields["scores"].([]any)
	if !ok || len(scores) != 1 {
		t.Fatalf("projected scores = %#v", result.StateMutation.Fields["scores"])
	}
	score, ok := scores[0].(map[string]any)
	if !ok {
		t.Fatalf("projected score item = %#v", scores[0])
	}
	if got := score["dimension"]; got != "market" {
		t.Fatalf("projected dimension = %#v", got)
	}
	if _, exists := score["event_type"]; exists {
		t.Fatalf("projected score leaked accumulator metadata: %#v", score)
	}
}

func TestExecutor_AccumulatorBucketUsesMatchedHandlerEventKeyForScopedConcreteEvents(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{
			DedupBy:   "payload.component_id",
			DedupPath: runtimecontracts.RefExpression("payload.component_id").RefPath,
		},
	}
	lifecycleNode := testFlowExecutableNode(t, "operating", "lifecycle-orchestrator")
	firstState := testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{})
	first, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     lifecycleNode,
		Event: eventtest.RunCreatingRootIngress(
			"evt-a",
			"component-scaffold/a/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"a"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),

		HandlerEventKey: "component.scaffolded",
		Handler:         handler,
		State:           firstState,
	})
	if err != nil {
		t.Fatalf("first Execute error: %v", err)
	}
	firstAccumulator, ok := loadAccumulatorForBucket(StateSnapshot{StateCarrier: first.StateMutation.StateCarrier}, accumulatorBucketRef(lifecycleNode, "component.scaffolded"))
	if !ok || len(firstAccumulator.Items) != 1 {
		t.Fatalf("first stream accumulator = %#v, want one item", firstAccumulator)
	}
	secondState := testStateSnapshot("pending", map[string]any{}, nil, first.StateMutation.StateCarrier.StateBuckets)
	second, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     lifecycleNode,
		Event: eventtest.RunCreatingRootIngress(
			"evt-b",
			"component-scaffold/b/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"b"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Date(2026, time.January, 1, 0, 0, 1, 0, time.UTC),
		),

		HandlerEventKey: "component.scaffolded",
		Handler:         handler,
		State:           secondState,
	})
	if err != nil {
		t.Fatalf("second Execute error: %v", err)
	}
	state := StateSnapshot{StateCarrier: second.StateMutation.StateCarrier}
	acc, ok := loadAccumulatorForBucket(state, accumulatorBucketRef(lifecycleNode, "component.scaffolded"))
	if !ok {
		t.Fatalf("logical accumulator bucket missing: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
	if got := len(acc.Items); got != 2 {
		t.Fatalf("accumulator items = %d, want 2", got)
	}
	if got := acc.Items[0]["event_type"]; got != "component-scaffold/a/component.scaffolded" {
		t.Fatalf("first item event_type = %#v", got)
	}
	if got := acc.Items[1]["event_type"]; got != "component-scaffold/b/component.scaffolded" {
		t.Fatalf("second item event_type = %#v", got)
	}
	if _, ok := loadAccumulatorForBucket(state, accumulatorBucketRef(lifecycleNode, "component-scaffold/a/component.scaffolded")); ok {
		t.Fatalf("first concrete event bucket survived: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
	if _, ok := loadAccumulatorForBucket(state, accumulatorBucketRef(lifecycleNode, "component-scaffold/b/component.scaffolded")); ok {
		t.Fatalf("second concrete event bucket survived: %#v", second.StateMutation.StateCarrier.StateBuckets)
	}
}

func TestExecutor_JoinUsesPersistedActivationAndMembershipOrder(t *testing.T) {
	resultType := runtimecontracts.CatalogTypeReference{Type: "jsonb"}
	spec := runtimecontracts.JoinSpec{
		ID: "line_items", Stage: "awaiting", Output: "payload.result", OutputPath: runtimepaths.Parse("payload.result"),
		Members:    runtimecontracts.JoinMembersSpec{From: "entity.expected", FromPath: runtimepaths.Parse("entity.expected"), By: "payload.member_id", ByPath: runtimepaths.Parse("payload.member_id")},
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready"}, OnCompleteFound: true,
		Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention"}}, TimeoutFound: true,
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "orders", Joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival, Node: testFlowExecutableNode(t, "orders", "join-node"), HandlerEvent: "item.completed", Spec: spec, ResultType: resultType}},
		}}), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	joinNode := testFlowExecutableNode(t, "orders", "join-node")
	activation, err := newEngineTestJoinActivation(joinNode, "item.completed", spec, "", []string{"a", "b"}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{Join: &spec}
	first, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: testFlowExecutableNode(t, "orders", "join-node"), HandlerEventKey: "item.completed", Handler: handler,
		JoinDeclaration: activation.JoinRef().Declaration(),
		Event:           eventtest.RunCreatingRootIngress("evt-b", "item.completed", "", "", json.RawMessage(`{"member_id":"b","result":{"score":2}}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), now),
		State:           testStateSnapshot("awaiting", map[string]any{"expected": []any{"a", "b"}}, nil, buckets),
	})
	if err != nil {
		t.Fatalf("first arrival: %v", err)
	}
	if first.Status != OutcomeWaiting {
		t.Fatalf("first status = %s, want waiting", first.Status)
	}
	second, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: testFlowExecutableNode(t, "orders", "join-node"), HandlerEventKey: "item.completed", Handler: handler,
		JoinDeclaration: activation.JoinRef().Declaration(),
		Event:           eventtest.RunCreatingRootIngress("evt-a", "item.completed", "", "", json.RawMessage(`{"member_id":"a","result":{"score":1}}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), now.Add(time.Second)),
		State:           testStateSnapshot("awaiting", map[string]any{"expected": []any{"a", "b"}}, nil, first.StateMutation.StateCarrier.StateBuckets),
	})
	if err != nil {
		t.Fatalf("second arrival: %v", err)
	}
	if second.StateMutation.NextState != "ready" {
		t.Fatalf("next state = %q, want ready", second.StateMutation.NextState)
	}
	closed, ok, err := joinruntime.Load(second.StateMutation.StateCarrier.StateBuckets, joinNode, activation.Key())
	if err != nil || !ok {
		t.Fatalf("load closed activation = %#v, %v, %v", closed, ok, err)
	}
	if closed.Status != joinruntime.StatusClosed || closed.CloseReason != joinruntime.CloseReasonComplete {
		t.Fatalf("closed activation = %#v", closed)
	}
	results := closed.Results()
	if len(results) != 2 || results[0].(map[string]any)["score"] != float64(1) || results[1].(map[string]any)["score"] != float64(2) {
		t.Fatalf("results = %#v, want membership order a,b", results)
	}
}

func TestFanInBarrierExecutorConsumesEffectiveJoinPlan(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)
	portfolioNode := testFlowExecutableNode(t, "portfolio", "portfolio-collector")
	plan, ok := semanticview.WorkflowJoinPlanForHandler(source, portfolioNode, "operating.reported")
	if !ok || plan.Spec.Members.By != "payload.operating_id" || plan.Spec.Window == nil || plan.Spec.Window.By != "payload.period_id" {
		t.Fatalf("effective barrier plan = %#v", plan)
	}
	rawHandler, ok := source.ExecutableNodeEventHandler(portfolioNode, "operating.reported")
	if !ok || rawHandler.Join == nil {
		t.Fatal("authored barrier handler is unavailable")
	}
	if rawHandler.Join.Members.By != "" || rawHandler.Join.Window == nil || rawHandler.Join.Window.By != "" {
		t.Fatalf("authored handler contains derived identity: %#v", rawHandler.Join)
	}

	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	activation, err := newEngineTestJoinActivation(plan.Node, plan.HandlerEvent, plan.Spec, "2026-Q3", []string{"operating-a"}, now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "portfolio/portfolio", Node: portfolioNode, HandlerEventKey: "operating.reported", Handler: rawHandler,
		JoinDeclaration: activation.JoinRef().Declaration(),
		Event:           eventtest.RunCreatingRootIngress("evt-operating-a", "operating.reported", "", "", json.RawMessage(`{"operating_id":"operating-a","period_id":"2026-Q3","revenue":42}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "portfolio/portfolio"), now),
		State:           testStateSnapshot("awaiting", map[string]any{"expected_operating_ids": []any{"operating-a"}, "period_id": "2026-Q3"}, nil, buckets),
	})
	if err != nil {
		t.Fatalf("execute barrier arrival: %v", err)
	}
	if result.StateMutation.NextState != "complete" {
		t.Fatalf("barrier next state = %q, want complete", result.StateMutation.NextState)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextJoinComplete || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionSelected || !result.HandlerRuleSelection.Ref().Valid() {
		t.Fatalf("join completion selection = %#v", result.HandlerRuleSelection)
	}
	if got := result.HandlerRuleSelection.Ref().ElementID().String(); got != "445e8fbd-e8f7-4b4b-81f0-08ebec2e1b70" {
		t.Fatalf("join completion element ID = %q", got)
	}
}

func TestFanInBarrierExecutorRecordsAuthoredJoinTimeoutSelection(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	node := testFlowExecutableNode(t, "portfolio", "portfolio-collector")
	plan, ok := semanticview.WorkflowJoinPlanForHandler(source, node, "operating.reported")
	if !ok {
		t.Fatal("effective join plan is unavailable")
	}
	handler, ok := source.ExecutableNodeEventHandler(node, "operating.reported")
	if !ok {
		t.Fatal("authored join handler is unavailable")
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 23, 15, 30, 0, 0, time.UTC)
	activation, err := newEngineTestJoinActivation(node, "operating.reported", plan.Spec, "2026-Q3", []string{"operating-a"}, now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(activation.TimerHandle().PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "portfolio/portfolio", Node: node, HandlerEventKey: "operating.reported", Handler: handler,
		JoinDeclaration: activation.JoinRef().Declaration(),
		Event: eventtest.RunCreatingRootIngress(
			"evt-join-timeout", events.EventType(activation.TimerEventType()), "runtime", activation.TimerTaskID(), payload, 0,
			"", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "portfolio/portfolio"), now.Add(5*time.Minute),
		),
		State: testStateSnapshot("awaiting", map[string]any{"expected_operating_ids": []any{"operating-a"}, "period_id": "2026-Q3"}, nil, buckets),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.StateMutation.NextState != "failed" {
		t.Fatalf("timeout next state = %q, want failed", result.StateMutation.NextState)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextJoinTimeout || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionSelected || !result.HandlerRuleSelection.Ref().Valid() {
		t.Fatalf("join timeout selection = %#v", result.HandlerRuleSelection)
	}
	if got := result.HandlerRuleSelection.Ref().ElementID().String(); got != "cc68292e-a6af-47bc-8785-472465db0d81" {
		t.Fatalf("join timeout element ID = %q", got)
	}
}

func TestExecutor_JoinCompletionConsumesCatalogResultType(t *testing.T) {
	for _, tc := range []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{name: "named field", expression: `join.results[0].score > 0`},
		{name: "named value is not scalar", expression: `join.results[0] > 1`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := runtimecontracts.JoinSpec{
				ID: "line_items", Stage: "awaiting", Output: "payload.result", OutputPath: runtimepaths.Parse("payload.result"),
				Members:      runtimecontracts.JoinMembersSpec{From: "entity.expected", FromPath: runtimepaths.Parse("entity.expected"), By: "payload.member_id", ByPath: runtimepaths.Parse("payload.member_id")},
				CompleteWhen: tc.expression,
				Remaining:    runtimecontracts.JoinRemainingIgnore,
				OnComplete:   runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready"}, OnCompleteFound: true,
				Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention"}}, TimeoutFound: true,
			}
			resultType := runtimecontracts.CatalogTypeReference{
				Type: "JoinResult",
				Catalog: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
					"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"score": {Type: "integer"}}},
				}},
			}
			joinNode := testFlowExecutableNode(t, "orders", "join-node")
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
				Name:  "orders",
				Joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival, Node: joinNode, HandlerEvent: "item.completed", Spec: spec, ResultType: resultType}},
			}})
			exec, err := NewExecutor(RuntimeDependencies{
				Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
			activation, err := newEngineTestJoinActivation(joinNode, "item.completed", spec, "", []string{"a"}, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			buckets := map[string]map[string]any{}
			if err := joinruntime.Store(buckets, activation); err != nil {
				t.Fatal(err)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", Node: testFlowExecutableNode(t, "orders", "join-node"), HandlerEventKey: "item.completed", Handler: runtimecontracts.SystemNodeEventHandler{Join: &spec},
				JoinDeclaration: activation.JoinRef().Declaration(),
				Event:           eventtest.RunCreatingRootIngress("evt-a", "item.completed", "", "", json.RawMessage(`{"member_id":"a","result":{"score":1}}`), 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), now),
				State:           testStateSnapshot("awaiting", map[string]any{"expected": []any{"a"}}, nil, buckets),
			})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "no matching overload") {
					t.Fatalf("Execute error = %v, want catalog-backed typed rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute error = %v", err)
			}
			if result.StateMutation.NextState != "ready" {
				t.Fatalf("next state = %q, want ready", result.StateMutation.NextState)
			}
		})
	}
}

func newEngineTestJoinActivation(node identity.ExecutableNode, handlerEvent string, spec runtimecontracts.JoinSpec, window string, members []string, armedAt, fireAt time.Time) (joinruntime.Activation, error) {
	ref, err := timeridentity.NewJoinRef(node, handlerEvent, spec.Stage, spec.EffectiveID(), window)
	if err != nil {
		return joinruntime.Activation{}, err
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		return joinruntime.Activation{}, err
	}
	return joinruntime.NewActivation(handle, members, armedAt, fireAt)
}

func TestExecutor_ComputeReadsAccumulatorByMatchedHandlerEventKey(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	state := testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{})
	lifecycleNode := testFlowExecutableNode(t, "operating", "lifecycle-orchestrator")
	storeAccumulator(&state, lifecycleNode, "component.scaffolded", &Accumulator{
		Items: []map[string]any{
			{"component_id": "a"},
			{"component_id": "b"},
		},
	})
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     lifecycleNode,
		Event: eventtest.RunCreatingRootIngress(
			"evt-b",
			"component-scaffold/b/component.scaffolded",
			"",
			"",
			json.RawMessage(`{"component_id":"b"}`),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"),
			time.Time{},
		),

		HandlerEventKey: "component.scaffolded",
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpCount,
				StoreAs:   "entity.component_count",
			},
		},
		State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Fields["component_count"]; got != 2 {
		t.Fatalf("component_count = %#v, want 2", got)
	}
}

func TestExecutor_PolicySheetLookupRowFeedsSelectionRow(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`computed.template_path == "templates/service/go"`: func(base BaseContext) (bool, error) {
			return base.Computed.Raw()["template_path"] == "templates/service/go", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	lookup := &runtimecontracts.ComputeLookupSpec{
		RowID: "scaffold_paths",
		On:    []string{"payload.scaffold_type", "payload.language"},
		OnPaths: []runtimepaths.Path{
			runtimepaths.Parse("payload.scaffold_type"),
			runtimepaths.Parse("payload.language"),
		},
		DefaultDeclared: true,
		DefaultFail:     true,
		Entries: []runtimecontracts.ComputeLookupEntry{{
			Key: []runtimecontracts.ComputeLookupLiteral{
				{Value: "service", Kind: "string", Canonical: "string:\"service\"", Summary: `"service"`},
				{Value: "go", Kind: "string", Canonical: "string:\"go\"", Summary: `"go"`},
			},
			Value:        "templates/service/go",
			ValueKind:    "string",
			ValueSummary: `"templates/service/go"`,
		}},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testRootExecutableNode(t, "repo-scaffold"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("repo.scaffold_requested"),
			"",
			"",
			mustEncodeJSON(t, map[string]any{"scaffold_type": "service", "language": "go"}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "scaffold_paths",
					PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindLookup, Lookup: lookup},
					Compute: &runtimecontracts.ComputeSpec{
						Operation: runtimecontracts.ComputeOpLookup,
						StoreAs:   "computed.template_path",
						Lookup:    lookup,
					},
				},
				{
					ID:        "service_route",
					Condition: `computed.template_path == "templates/service/go"`,
					Emit:      runtimecontracts.EmitSpec{Event: "repo.service_template_selected"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.Computed["template_path"]; got != "templates/service/go" {
		t.Fatalf("result computed template_path = %#v, want templates/service/go", got)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("emit intents = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "repo.service_template_selected" {
		t.Fatalf("emit event = %q, want repo.service_template_selected", got)
	}
}

func TestExecutor_PolicySheetComputeModuleRowFeedsSelectionRow(t *testing.T) {
	source, module := sourceWithStructuredRendererModule(t)
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`computed.rendered_bundle.format == "yaml"`: func(base BaseContext) (bool, error) {
			rendered, _ := base.Computed.Raw()["rendered_bundle"].(map[string]any)
			return rendered["format"] == "yaml", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	moduleSpec := &runtimecontracts.ComputeModuleSpec{
		RowID:  "render_bundle",
		Module: "structured_renderer",
		Into:   "computed.rendered_bundle",
		Input: map[string]string{
			"component": "payload.component",
			"owner":     "payload.owner",
			"language":  "payload.language",
			"files":     "payload.files",
		},
		InputPaths: map[string]runtimepaths.Path{
			"component": runtimepaths.Parse("payload.component"),
			"owner":     runtimepaths.Parse("payload.owner"),
			"language":  runtimepaths.Parse("payload.language"),
			"files":     runtimepaths.Parse("payload.files"),
		},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testFlowExecutableNode(t, "render", "render-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("render.requested"),
			"",
			"",
			mustEncodeJSON(t, map[string]any{
				"component": "api",
				"owner":     "platform",
				"language":  "go",
				"files":     []any{"main.go", "README.md", "service.yaml"},
			}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "render_bundle",
					PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindModule, Module: moduleSpec},
					Compute: &runtimecontracts.ComputeSpec{
						Operation: runtimecontracts.ComputeOpModule,
						StoreAs:   "computed.rendered_bundle",
						Module:    moduleSpec,
					},
				},
				{
					ID:        "rendered_yaml",
					Condition: `computed.rendered_bundle.format == "yaml"`,
					Emit: runtimecontracts.EmitSpec{Event: "bundle.rendered", Fields: map[string]runtimecontracts.ExpressionValue{
						"content": runtimecontracts.RefExpression("computed.rendered_bundle.content"),
					}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rendered, _ := result.Computed["rendered_bundle"].(map[string]any)
	content, _ := rendered["content"].(string)
	for _, want := range []string{"component: api", "owner: platform", "- deploy/service.yaml"} {
		if !strings.Contains(content, want) {
			t.Fatalf("rendered content missing %q: %s", want, content)
		}
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "rendered_yaml" {
		t.Fatalf("selected rule = %q, want rendered_yaml", got)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("emit intents = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "bundle.rendered" {
		t.Fatalf("emit event = %q, want bundle.rendered", got)
	}
	if _, ok := result.StateMutation.Fields["rendered_bundle"]; ok {
		t.Fatalf("module result leaked into state mutation metadata: %#v", result.StateMutation.Fields)
	}
	if got := len(result.ComputeModuleTraces); got != 1 {
		t.Fatalf("module traces = %d, want 1", got)
	}
	trace := result.ComputeModuleTraces[0]
	if trace.ModuleID != "structured_renderer" || trace.RowID != "render_bundle" || trace.Digest != module.Digest || trace.FuelConsumed == 0 || trace.OutputHash == "" || trace.Engine == "" {
		t.Fatalf("trace = %#v", trace)
	}
}

func TestExecutor_PolicySheetPythonModuleRowFeedsSelectionRow(t *testing.T) {
	source, module := sourceWithPythonRendererModule(t)
	exec := newStructuredRendererExecutor(t, source)
	req := structuredRendererExecutionRequest(t, pythonRendererModuleSpec())
	result, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rendered, _ := result.Computed["rendered_bundle"].(map[string]any)
	content, _ := rendered["content"].(string)
	for _, want := range []string{"component: api", "owner: platform", "- src/main.go", "- deploy/service.yaml"} {
		if !strings.Contains(content, want) {
			t.Fatalf("rendered content missing %q: %s", want, content)
		}
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "rendered_yaml" {
		t.Fatalf("selected rule = %q, want rendered_yaml", got)
	}
	if got := len(result.ComputeModuleTraces); got != 1 {
		t.Fatalf("module traces = %d, want 1", got)
	}
	trace := result.ComputeModuleTraces[0]
	if trace.ModuleID != "python_renderer" ||
		trace.RowID != "render_bundle" ||
		trace.Kind != pythonmodule.Kind ||
		trace.Digest != module.Digest ||
		trace.Interpreter != pythonmodule.Interpreter ||
		trace.InterpreterDigest != pythonmodule.InterpreterDigest ||
		trace.SnapshotDigest == "" ||
		trace.HarnessABI != pythonmodule.HarnessABI ||
		trace.SourceHash != module.Digest ||
		trace.FuelConsumed == 0 ||
		trace.OutputHash == "" ||
		trace.Engine == "" {
		t.Fatalf("trace = %#v", trace)
	}

	req.ExpectedComputeModuleTraces = append([]ComputeModuleTrace(nil), result.ComputeModuleTraces...)
	replayed, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("replay Execute with matching trace error: %v", err)
	}
	if got := len(replayed.ComputeModuleTraces); got != 1 {
		t.Fatalf("replay traces = %d, want 1", got)
	}
	req.ExpectedComputeModuleTraces[0].FuelConsumed++
	if _, err := exec.ExecuteSemanticFixture(context.Background(), req); err != nil {
		t.Fatalf("python replay with fuel evidence drift error = %v, want fuel evidence-only acceptance", err)
	}
	req.ExpectedComputeModuleTraces[0].SourceHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	_, err = exec.ExecuteSemanticFixture(context.Background(), req)
	if err == nil {
		t.Fatal("replay Execute error = nil, want source hash divergence")
	}
	var typed *computemodule.Error
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeReplay {
		t.Fatalf("error = %#v, want code %s", err, computemodule.CodeReplay)
	}
	if !strings.Contains(err.Error(), "source_hash") {
		t.Fatalf("error = %v, want source hash diagnostic", err)
	}
}

func TestExecutor_PythonModuleOutputSchemaFailureStopsBeforeEmit(t *testing.T) {
	source, _ := sourceWithPythonRendererSource(t, []byte(`def handle(input):
    return {"content": "ok", "format": "yaml", "line_count": "three"}
`))
	exec := newStructuredRendererExecutor(t, source)
	req := structuredRendererExecutionRequest(t, pythonRendererModuleSpec())
	result, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err == nil {
		t.Fatal("Execute error = nil, want output schema violation")
	}
	var typed *computemodule.Error
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeABI {
		t.Fatalf("error = %#v, want code %s", err, computemodule.CodeABI)
	}
	if !strings.Contains(err.Error(), "output schema violation") {
		t.Fatalf("error = %v, want output schema diagnostic", err)
	}
	if got := len(result.ComputeModuleTraces); got != 1 {
		t.Fatalf("failure traces = %d, want 1", got)
	}
	trace := result.ComputeModuleTraces[0]
	if trace.Outcome != computemodule.ReplayOutcomeFailure || trace.ErrorCode != string(computemodule.CodeABI) || trace.OutputHash != "" {
		t.Fatalf("failure trace = %#v, want ABI failure outcome without output hash", trace)
	}
	req.ExpectedComputeModuleTraces = append([]ComputeModuleTrace(nil), result.ComputeModuleTraces...)
	_, err = exec.ExecuteSemanticFixture(context.Background(), req)
	if err == nil {
		t.Fatal("replay Execute error = nil, want reproduced deterministic failure")
	}
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeABI {
		t.Fatalf("replay error = %#v, want original deterministic code %s", err, computemodule.CodeABI)
	}
	req.ExpectedComputeModuleTraces[0].ErrorCode = string(computemodule.CodeTrap)
	_, err = exec.ExecuteSemanticFixture(context.Background(), req)
	if err == nil {
		t.Fatal("replay Execute error = nil, want error-code divergence")
	}
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeReplay {
		t.Fatalf("error-code divergence = %#v, want replay code", err)
	}
	if typed.Finding == nil || typed.Finding.Kind != computemodule.ReplayFindingResultDivergence || typed.Finding.Field != "error_code" {
		t.Fatalf("finding = %#v, want result divergence on error_code", typed.Finding)
	}
}

func TestExecutor_ComputeModuleReplayTraceComparison(t *testing.T) {
	source, _ := sourceWithStructuredRendererModule(t)
	exec := newStructuredRendererExecutor(t, source)
	req := structuredRendererExecutionRequest(t, structuredRendererModuleSpec())

	first, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("initial Execute error: %v", err)
	}
	if got := len(first.ComputeModuleTraces); got != 1 {
		t.Fatalf("initial traces = %d, want 1", got)
	}

	req.ExpectedComputeModuleTraces = append([]ComputeModuleTrace(nil), first.ComputeModuleTraces...)
	replayed, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("replay Execute with matching trace error: %v", err)
	}
	if got := len(replayed.ComputeModuleTraces); got != 1 {
		t.Fatalf("replay traces = %d, want 1", got)
	}

	zeroExpected := structuredRendererExecutionRequest(t, structuredRendererModuleSpec())
	zeroExpected.ExpectedComputeModuleTraces = []ComputeModuleTrace{}
	_, err = exec.ExecuteSemanticFixture(context.Background(), zeroExpected)
	if err == nil {
		t.Fatal("replay Execute with zero expected traces error = nil, want unexpected trace divergence")
	}
	var typed *computemodule.Error
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeReplay {
		t.Fatalf("zero expected error = %#v, want code %s", err, computemodule.CodeReplay)
	}
	if !strings.Contains(err.Error(), "unexpected compute_module trace") {
		t.Fatalf("zero expected error = %v, want unexpected trace diagnostic", err)
	}

	req.ExpectedComputeModuleTraces[0].OutputHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	_, err = exec.ExecuteSemanticFixture(context.Background(), req)
	if err == nil {
		t.Fatal("replay Execute error = nil, want divergence")
	}
	if !errors.As(err, &typed) || typed.Code != computemodule.CodeReplay {
		t.Fatalf("error = %#v, want code %s", err, computemodule.CodeReplay)
	}
	if typed.Finding == nil || typed.Finding.Kind != computemodule.ReplayFindingResultDivergence || typed.Finding.Field != "output_hash" {
		t.Fatalf("finding = %#v, want result divergence on output_hash", typed.Finding)
	}
	if !strings.Contains(err.Error(), "output_hash") {
		t.Fatalf("error = %v, want output hash diagnostic", err)
	}
}

func TestExecutor_ComputeModuleReplayEnvelopeClassifiesDivergenceKinds(t *testing.T) {
	source, _ := sourceWithStructuredRendererModule(t)
	exec := newStructuredRendererExecutor(t, source)
	req := structuredRendererExecutionRequest(t, structuredRendererModuleSpec())
	first, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatalf("initial Execute error: %v", err)
	}
	if got := len(first.ComputeModuleTraces); got != 1 {
		t.Fatalf("initial traces = %d, want 1", got)
	}
	base := first.ComputeModuleTraces[0]
	if base.InputHash == "" || base.ABI != computemodule.ABI || base.Entry != computemodule.DefaultEntry || base.Limits.Fuel == 0 || base.Arch == "" || base.Outcome != computemodule.ReplayOutcomeSuccess {
		t.Fatalf("base envelope missing replay identity: %#v", base)
	}
	tests := []struct {
		name      string
		mutate    func(*ComputeModuleTrace)
		wantKind  computemodule.ReplayFindingKind
		wantField string
	}{
		{
			name: "identity_input_hash",
			mutate: func(trace *ComputeModuleTrace) {
				trace.InputHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
			},
			wantKind:  computemodule.ReplayFindingIdentityDivergence,
			wantField: "input_hash",
		},
		{
			name: "unsupported_engine_profile",
			mutate: func(trace *ComputeModuleTrace) {
				trace.Engine = "wasmtime-go:v0.unsupported"
			},
			wantKind:  computemodule.ReplayFindingUnsupportedProfile,
			wantField: "engine",
		},
		{
			name: "unsupported_arch_profile",
			mutate: func(trace *ComputeModuleTrace) {
				trace.Arch = "unsupported-arch"
			},
			wantKind:  computemodule.ReplayFindingUnsupportedProfile,
			wantField: "arch",
		},
		{
			name: "result_output_hash",
			mutate: func(trace *ComputeModuleTrace) {
				trace.OutputHash = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
			},
			wantKind:  computemodule.ReplayFindingResultDivergence,
			wantField: "output_hash",
		},
		{
			name: "resource_fuel",
			mutate: func(trace *ComputeModuleTrace) {
				trace.FuelConsumed++
			},
			wantKind:  computemodule.ReplayFindingResourceDivergence,
			wantField: "fuel_consumed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			replayReq := structuredRendererExecutionRequest(t, structuredRendererModuleSpec())
			trace := base
			tc.mutate(&trace)
			replayReq.ExpectedComputeModuleTraces = []ComputeModuleTrace{trace}
			_, err := exec.ExecuteSemanticFixture(context.Background(), replayReq)
			if err == nil {
				t.Fatal("replay Execute error = nil, want replay finding")
			}
			var typed *computemodule.Error
			if !errors.As(err, &typed) || typed.Code != computemodule.CodeReplay {
				t.Fatalf("error = %#v, want code %s", err, computemodule.CodeReplay)
			}
			if typed.Finding == nil || typed.Finding.Kind != tc.wantKind || typed.Finding.Field != tc.wantField {
				t.Fatalf("finding = %#v, want %s on %s", typed.Finding, tc.wantKind, tc.wantField)
			}
		})
	}
}

func TestDecodeComputeModuleOutputRejectsTrailingJSON(t *testing.T) {
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"content"},
		"properties": map[string]any{
			"content": map[string]any{"type": "string"},
		},
	}
	for _, raw := range [][]byte{
		[]byte(`{"content":"ok"} trailing`),
		[]byte(`{"content":"ok"} {"content":"again"}`),
	} {
		t.Run(string(raw), func(t *testing.T) {
			_, err := decodeComputeModuleOutput("structured_renderer", "render_bundle", raw, schema)
			if err == nil {
				t.Fatal("decodeComputeModuleOutput error = nil, want strict JSON failure")
			}
			var typed *computemodule.Error
			if !errors.As(err, &typed) || typed.Code != computemodule.CodeABI {
				t.Fatalf("error = %#v, want code %s", err, computemodule.CodeABI)
			}
		})
	}
}

func TestExecutor_PolicySheetValidateRowFeedsSelectionRow(t *testing.T) {
	pinCandidate := true
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Validation: map[string]runtimecontracts.PolicyValidationSet{
			"deploy_manifest": {
				Classes: map[string]runtimecontracts.PolicyValidationClass{
					"invalid": {Disposition: "deploy.manifest_invalid"},
				},
				Inputs: map[string]string{
					"source_ref":          "string",
					"manifest_source_ref": "string",
				},
				Rules: []runtimecontracts.PolicyValidationRule{{
					ID:           "VR-001",
					Class:        "invalid",
					Text:         "Manifest source ref must match request source ref.",
					PinCandidate: &pinCandidate,
					Check: runtimecontracts.PolicyValidationCheck{
						Equal: &runtimecontracts.PolicyValidationEqualCheck{
							Left:  "input.source_ref",
							Right: "input.manifest_source_ref",
						},
					},
				}},
			},
		}},
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`computed.validation.deploy_manifest.valid == false`: func(base BaseContext) (bool, error) {
			validation, _ := base.Computed.Raw()["validation"].(map[string]any)
			deploy, _ := validation["deploy_manifest"].(map[string]any)
			return deploy["valid"] == false, nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	validation := &runtimecontracts.ComputeValidationSpec{
		RowID: "validate_manifest",
		Set:   "deploy_manifest",
		Into:  "computed.validation.deploy_manifest",
		Input: map[string]string{
			"source_ref":          "payload.source_ref",
			"manifest_source_ref": "payload.file_manifest.source_ref",
		},
		InputPaths: map[string]runtimepaths.Path{
			"source_ref":          runtimepaths.Parse("payload.source_ref"),
			"manifest_source_ref": runtimepaths.Parse("payload.file_manifest.source_ref"),
		},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testRootExecutableNode(t, "deploy-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("deploy.requested"),
			"",
			"",
			mustEncodeJSON(t, map[string]any{
				"source_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"file_manifest": map[string]any{
					"source_ref": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
			}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "validate_manifest",
					PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindValidate, Validation: validation},
					Compute: &runtimecontracts.ComputeSpec{
						Operation:  runtimecontracts.ComputeOpValidate,
						StoreAs:    "computed.validation.deploy_manifest",
						Validation: validation,
					},
				},
				{
					ID:        "invalid_manifest",
					Condition: `computed.validation.deploy_manifest.valid == false`,
					Emit:      runtimecontracts.EmitSpec{Event: "deploy.manifest_invalid"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	validationResult, _ := result.Computed["validation"].(map[string]any)
	deployResult, _ := validationResult["deploy_manifest"].(map[string]any)
	if got := deployResult["valid"]; got != false {
		t.Fatalf("validation valid = %#v, want false", got)
	}
	violations, _ := deployResult["violations"].([]any)
	if len(violations) != 1 {
		t.Fatalf("violations len = %d, want 1: %#v", len(violations), deployResult["violations"])
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "invalid_manifest" {
		t.Fatalf("selected rule = %q, want invalid_manifest", got)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("emit intents = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "deploy.manifest_invalid" {
		t.Fatalf("emit event = %q, want deploy.manifest_invalid", got)
	}
	if _, ok := result.StateMutation.Fields["validation"]; ok {
		t.Fatalf("validation result leaked into state mutation metadata: %#v", result.StateMutation.Fields)
	}
}

func TestExecutor_PolicySheetValidateNumericEqualityCanonicalizesRuntimeValues(t *testing.T) {
	pinCandidate := true
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Validation: map[string]runtimecontracts.PolicyValidationSet{
			"count_match": {
				Classes: map[string]runtimecontracts.PolicyValidationClass{
					"invalid": {Disposition: "deploy.count_mismatch"},
				},
				Inputs: map[string]string{
					"payload_count": "number",
					"entity_count":  "number",
				},
				Rules: []runtimecontracts.PolicyValidationRule{{
					ID:           "VR-COUNT-001",
					Class:        "invalid",
					Text:         "Payload count must match entity count.",
					PinCandidate: &pinCandidate,
					Check: runtimecontracts.PolicyValidationCheck{
						Equal: &runtimecontracts.PolicyValidationEqualCheck{
							Left:  "input.payload_count",
							Right: "input.entity_count",
						},
					},
				}},
			},
		}},
	})
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`computed.validation.count_match.valid == true`: func(base BaseContext) (bool, error) {
			validation, _ := base.Computed.Raw()["validation"].(map[string]any)
			count, _ := validation["count_match"].(map[string]any)
			return count["valid"] == true, nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	validation := &runtimecontracts.ComputeValidationSpec{
		RowID: "validate_count",
		Set:   "count_match",
		Into:  "computed.validation.count_match",
		Input: map[string]string{
			"payload_count": "payload.count",
			"entity_count":  "entity.expected_count",
		},
		InputPaths: map[string]runtimepaths.Path{
			"payload_count": runtimepaths.Parse("payload.count"),
			"entity_count":  runtimepaths.Parse("entity.expected_count"),
		},
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		Node:     testRootExecutableNode(t, "deploy-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			events.EventType("deploy.requested"),
			"",
			"",
			mustEncodeJSON(t, map[string]any{"count": 1}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		),
		State: testStateSnapshot("pending", map[string]any{"expected_count": int(1)}, nil, map[string]map[string]any{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "validate_count",
					PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindValidate, Validation: validation},
					Compute: &runtimecontracts.ComputeSpec{
						Operation:  runtimecontracts.ComputeOpValidate,
						StoreAs:    "computed.validation.count_match",
						Validation: validation,
					},
				},
				{
					ID:        "valid_count",
					Condition: `computed.validation.count_match.valid == true`,
					Emit:      runtimecontracts.EmitSpec{Event: "deploy.count_matched"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	validationResult, _ := result.Computed["validation"].(map[string]any)
	countResult, _ := validationResult["count_match"].(map[string]any)
	if got := countResult["valid"]; got != true {
		t.Fatalf("validation valid = %#v, want true; result %#v", got, countResult)
	}
	violations, _ := countResult["violations"].([]any)
	if len(violations) != 0 {
		t.Fatalf("violations len = %d, want 0: %#v", len(violations), countResult["violations"])
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "valid_count" {
		t.Fatalf("selected rule = %q, want valid_count", got)
	}
}

func TestExecutor_AccumulatorProjectionFailsClosedWhenDeclaredBindingDoesNotResolveAtRuntime(t *testing.T) {
	source := semanticview.Wrap(loadEngineProjectionFlowBundle(t))
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	scoringNode := testFlowExecutableNode(t, "scoring", "scoring-node")
	handler, ok := source.ExecutableNodeEventHandler(scoringNode, "scoring/score.dimension_complete")
	if !ok {
		t.Fatal("expected qualified runtime event to resolve to authored local handler")
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     scoringNode,
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"scoring/score.unregistered_dimension_complete", "", "", json.RawMessage(`{"dimension":"market","score":87}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: handler,
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "runtime_invariant_violation") {
		t.Fatalf("Execute error = %v, want runtime_invariant_violation", err)
	}
}

type orderedStateRepo struct {
	order    *[]string
	mutation StateMutation
}

func (r *orderedStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	return testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}), true, nil
}

func (r *orderedStateRepo) SaveState(_ context.Context, _ StateAddress, mutation StateMutation) error {
	*r.order = append(*r.order, "save")
	r.mutation = mutation
	return nil
}

type orderedLocker struct{ order *[]string }

func (l orderedLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	*l.order = append(*l.order, "lock")
	return fn(ctx)
}

type orderedPublicationCommitter struct{ order *[]string }

func (o orderedPublicationCommitter) CommitPublications(context.Context, []EmitIntent) error {
	*o.order = append(*o.order, "publications")
	return nil
}

type orderedDispatcher struct{ order *[]string }

func (d orderedDispatcher) DispatchPostCommit(context.Context, []EmitIntent) error {
	*d.order = append(*d.order, "dispatch")
	return nil
}

type orderedActivityWriter struct {
	order   *[]string
	intents []ActivityIntent
	err     error
}

func (w *orderedActivityWriter) WriteActivityIntents(_ context.Context, intents []ActivityIntent) error {
	*w.order = append(*w.order, "activity_intents")
	w.intents = append(w.intents, intents...)
	return w.err
}

type orderedActivityDispatcher struct {
	order   *[]string
	intents []ActivityIntent
}

func (d *orderedActivityDispatcher) DispatchActivities(_ context.Context, intents []ActivityIntent) error {
	*d.order = append(*d.order, "activity_dispatch")
	d.intents = append(d.intents, intents...)
	return nil
}

func sourceWithActivityTool() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"source.requested": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{"url": {Type: "text"}}),
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"url": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}), runtimecontracts.ToolSchemaRequired("url")),

				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
					"title": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
				}))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "GET",
				URL:    "https://example.test/source?url={{input.url}}",
			})),
		},
	})
}

func TestExecutor_ActivityIntentPersistsBeforePostCommitDispatch(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
	writer := &orderedActivityWriter{order: &order}
	dispatcher := &orderedActivityDispatcher{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:             sourceWithActivityTool(),
		StateRepo:          repo,
		MutationOwner:      composedMutationOwner{state: repo, activities: writer, order: &order},
		Locker:             orderedLocker{order: &order},
		Dispatcher:         orderedDispatcher{order: &order},
		ActivityDispatcher: dispatcher,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:        identity.NormalizeEntityID("entity-1"),
		Node:            testFlowExecutableNode(t, "research", "scanner"),
		HandlerEventKey: "source.requested",
		Event:           eventtest.RunCreatingRootIngress("evt-1", "source.requested", "", "task-1", json.RawMessage(`{"url":"https://example.com"}`), 2, "run-1", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Activity: runtimecontracts.ActivitySpec{
				Tool: "source_scrape",
				Input: map[string]runtimecontracts.ExpressionValue{
					"url": runtimecontracts.CELExpression("payload.url"),
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got, want := order, []string{"lock", "tx", "save", "activity_intents", "activity_dispatch"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if len(writer.intents) != 1 || len(dispatcher.intents) != 1 {
		t.Fatalf("activity intents writer=%d dispatcher=%d, want 1/1", len(writer.intents), len(dispatcher.intents))
	}
	intent := writer.intents[0]
	if got, ok := intent.Input.Lookup("url"); !ok || got.Interface() != "https://example.com" {
		t.Fatalf("activity input url = %#v, present=%v", got.Interface(), ok)
	}
	if intent.SuccessEvent != "research/scanner_source_requested_source_scrape.succeeded" {
		t.Fatalf("success event = %q", intent.SuccessEvent)
	}
	if result.Failure != nil || result.FailureDisposition != FailureDispositionNone {
		t.Fatalf("failure = %#v disposition=%q", result.Failure, result.FailureDisposition)
	}
}

func TestExecutor_ActivityDispatchDoesNotRunWhenIntentPersistenceFails(t *testing.T) {
	order := []string{}
	writer := &orderedActivityWriter{order: &order, err: errors.New("intent store failed")}
	dispatcher := &orderedActivityDispatcher{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:             sourceWithActivityTool(),
		StateRepo:          &orderedStateRepo{order: &order},
		MutationOwner:      composedMutationOwner{state: &orderedStateRepo{order: &order}, activities: writer, order: &order},
		Locker:             orderedLocker{order: &order},
		Dispatcher:         orderedDispatcher{order: &order},
		ActivityDispatcher: dispatcher,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:        identity.NormalizeEntityID("entity-1"),
		Node:            testFlowExecutableNode(t, "research", "scanner"),
		HandlerEventKey: "source.requested",
		Event:           eventtest.RunCreatingRootIngress("evt-1", "source.requested", "", "", json.RawMessage(`{"url":"https://example.com"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Activity: runtimecontracts.ActivitySpec{
				Tool: "source_scrape",
				Input: map[string]runtimecontracts.ExpressionValue{
					"url": runtimecontracts.CELExpression("payload.url"),
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "intent store failed") {
		t.Fatalf("Execute error = %v, want intent store failure", err)
	}
	for _, step := range order {
		if step == "activity_dispatch" {
			t.Fatalf("activity dispatched despite failed intent persistence; order=%v", order)
		}
	}
}

func TestExecutor_ExecuteUsesAtomicEnvelopeAndOrderedSteps(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
	lifecycle := &testWorkflowLifecycleOwner{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:            stubSource(),
		StateRepo:         repo,
		MutationOwner:     composedMutationOwner{state: repo, lifecycle: lifecycle, publications: orderedPublicationCommitter{order: &order}, order: &order},
		Locker:            orderedLocker{order: &order},
		WorkflowLifecycle: lifecycle,
		Dispatcher:        orderedDispatcher{order: &order},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Route:    runtimeflowidentity.RouteForInstancePath("flow-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EnvelopeForFlowInstance(events.EventEnvelope{}, "flow-1"), time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			ClearGates: []string{"gate_a"},
			Emit:       runtimecontracts.EmitSpec{Event: "task.recorded"},
			Action:     runtimecontracts.ActionSpec{ID: "record"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"lock", "tx", "save", "lifecycle", "publications", "dispatch"}) {
		t.Fatalf("unexpected envelope order: %v", order)
	}
	if len(result.ExecutedSteps) != len(OrderedSteps) {
		t.Fatalf("executed step count = %d, want %d", len(result.ExecutedSteps), len(OrderedSteps))
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if result.ChainDepth != 1 || len(result.EmitIntents) != 1 {
		t.Fatalf("emit chain depth wrong: depth=%d intents=%d", result.ChainDepth, len(result.EmitIntents))
	}
	if !reflect.DeepEqual(repo.mutation.ClearGates, []string{"gate_a"}) {
		t.Fatalf("clear gates mutation = %#v", repo.mutation.ClearGates)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{
		"record_state_change",
		"update_stage",
		"cancel_stage_timers",
		"start_stage_timers",
		"record",
	}) {
		t.Fatalf("actions executed = %#v", got)
	}
}

func TestExecutor_ListPrimitivesMutateState(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(nil),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	initial := StateSnapshot{
		CurrentState: "pending",
		StateCarrier: NewStateCarrier(map[string]any{
			"dedup_key": "dup-1",
		}, nil, map[string]map[string]any{}),
	}
	node := testFlowExecutableNode(t, "flow-1", "node-1")
	storeAccumulator(&initial, node, "items.submitted", &Accumulator{
		Received: map[string]bool{"seed": true},
		Items:    []map[string]any{{"seed": true}},
	})

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     node,
		Event:    eventtest.RunCreatingRootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"items":[{"score":60,"active":true},{"score":40,"active":true},{"score":60,"active":false}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				StoreAs: "entity.query_rows",
			},
			Filter: &runtimecontracts.FilterSpec{
				ItemsFrom: "entity.query_rows",
				Condition: "item.score > 50",
				StoreAs:   "entity.filtered",
			},
			Reduce: &runtimecontracts.ReduceSpec{
				ItemsFrom: "entity.filtered",
				Operation: "sum",
				StoreAs:   "entity.total",
			},
			Count: &runtimecontracts.CountSpec{
				ItemsFrom: "entity.filtered",
				Condition: "item.active == true",
				StoreAs:   "entity.active_count",
			},
			Clear: &runtimecontracts.ClearSpec{
				Targets: []string{"pending_dedup", "accumulator_state"},
			},
			AdvancesTo: "done",
		},
		State: initial,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	filtered, ok := repo.mutation.Fields["filtered"].([]any)
	if !ok || len(filtered) != 2 {
		t.Fatalf("filtered = %#v", repo.mutation.Fields["filtered"])
	}
	if got := repo.mutation.Fields["total"]; got != 120 {
		t.Fatalf("total = %#v, want 120", got)
	}
	if got := repo.mutation.Fields["active_count"]; got != 1 {
		t.Fatalf("active_count = %#v, want 1", got)
	}
	if _, ok := repo.mutation.Fields["dedup_key"]; ok {
		t.Fatalf("expected dedup_key to be cleared, metadata=%#v", repo.mutation.Fields)
	}
	if nodeBucket, ok := repo.mutation.StateBuckets[node.Key()]; ok {
		if _, ok := nodeBucket[handlerAccumulatorBucketKey]; ok {
			t.Fatalf("expected accumulator state to be cleared, state_buckets=%#v", repo.mutation.StateBuckets)
		}
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q, want done", result.NextState)
	}
}

func TestExecutor_QueryGroupByStoresCounts(t *testing.T) {
	order := []string{}
	repo := &orderedStateRepo{order: &order}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(nil),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-2", "digest.requested", "", "", json.RawMessage(`{"items":[{"status":"queued"},{"status":"queued"},{"status":"done"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				GroupBy: "item.status",
				Count:   true,
				StoreAs: "entity.grouped",
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := repo.mutation.Fields["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped = %#v", repo.mutation.Fields["grouped"])
	}
	if grouped["queued"] != 2 || grouped["done"] != 1 {
		t.Fatalf("grouped counts = %#v", grouped)
	}
}

func TestExecutor_QueryEntityTableUsesAdmittedSourceExactlyOnce(t *testing.T) {
	reader := &stubEntityCollectionReader{rows: []map[string]any{
		{"id": "a", "status": "queued"},
		{"id": "b", "status": "queued"},
		{"id": "c", "status": "done"},
	}}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: collectionExecutionSource(), EntityCollections: reader,
		StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", ExecutionFlowID: identity.NormalizeFlowID("root"), Node: identitytest.RootNode(t, "worker"), HandlerEventKey: "work.received",
		Event: eventtest.RunCreatingRootIngress("evt-query-entities", "work.received", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{
			Entities: "items", Filter: `item.status != "done"`, GroupBy: "status", Count: true,
		}},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if reader.calls != 1 || reader.flowID != "root" || reader.entityType != "items" {
		t.Fatalf("entity reader = calls %d flow %q type %q", reader.calls, reader.flowID, reader.entityType)
	}
	grouped, ok := result.Computed["query"].(map[string]any)
	if !ok || grouped["queued"] != 2 || len(grouped) != 1 {
		t.Fatalf("computed.query = %#v", result.Computed["query"])
	}
}

func TestExecutor_QueryRejectsDualSourceBeforeReading(t *testing.T) {
	reader := &stubEntityCollectionReader{rows: []map[string]any{{"id": "a", "status": "queued"}}}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: collectionExecutionSource(), EntityCollections: reader,
		StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: identitytest.RootNode(t, "worker"), HandlerEventKey: "work.received",
		Event:   eventtest.RunCreatingRootIngress("evt-query-dual", "work.received", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", Entities: "items"}},
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "exactly one collection source") {
		t.Fatalf("Execute error = %v", err)
	}
	if reader.calls != 0 {
		t.Fatalf("entity reader calls = %d, want zero", reader.calls)
	}
}

func TestExecutor_QuerySelectionPreservesOptionalOmission(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: collectionExecutionSource(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: identitytest.RootNode(t, "worker"), HandlerEventKey: "work.received",
		Event:   eventtest.RunCreatingRootIngress("evt-query-select", "work.received", "", "", json.RawMessage(`{"items":[{"id":"a","status":"queued"},{"id":"b","status":"done","note":"kept"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", Select: []string{"id", "note"}}},
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rows, ok := result.Computed["query"].([]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("computed.query = %#v", result.Computed["query"])
	}
	first, _ := rows[0].(map[string]any)
	second, _ := rows[1].(map[string]any)
	if _, present := first["note"]; present || second["note"] != "kept" {
		t.Fatalf("selected rows = %#v", rows)
	}
}

func TestExecutor_QuerySelectionRejectsMissingRequiredField(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: collectionExecutionSource(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: identitytest.RootNode(t, "worker"), HandlerEventKey: "work.received",
		Event:   eventtest.RunCreatingRootIngress("evt-query-select-missing", "work.received", "", "", json.RawMessage(`{"items":[{"status":"queued"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Source: "payload.items", Select: []string{"id"}}},
		State:   testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "missing required field id") {
		t.Fatalf("Execute error = %v", err)
	}
}

func collectionExecutionSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"WorkItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{
				"id": {Type: "text"}, "status": {Type: "text"}, "note": {Type: "text", IsOptional: true},
			}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"items": {Fields: map[string]runtimecontracts.EntityFieldDecl{"id": {Type: "text"}, "status": {Type: "text"}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"work.received": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{"items": {Type: "[WorkItem]"}}),
		},
	})
}

func TestExecutor_QueryFilterUsesExplicitCollidingScopes(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(map[string]any{"score": 6}),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-2", "digest.requested", "", "", json.RawMessage(`{"score":5,"items":[{"score":7},{"score":5}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{
				Source:  "payload.items",
				Filter:  "item.score > payload.score && item.score > entity.score && item.score > policy.score",
				StoreAs: "entity.query_rows",
			},
		},
		State: testStateSnapshot("pending", map[string]any{"score": 4}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rows, ok := result.StateMutation.Fields["query_rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("query_rows = %#v", result.StateMutation.Fields["query_rows"])
	}
	item, _ := rows[0].(map[string]any)
	if item["score"] != 7.0 {
		t.Fatalf("query_rows[0] = %#v", item)
	}
}

func TestExecutor_FilterRejectsUnqualifiedConditionField(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(map[string]any{"score": 1}),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"score":5,"items":[{"score":7}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Filter: &runtimecontracts.FilterSpec{
				ItemsFrom: "payload.items",
				Condition: "score > 5",
				StoreAs:   "entity.filtered",
			},
		},
		State: testStateSnapshot("pending", map[string]any{"score": 4}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "undeclared reference") {
		t.Fatalf("Execute error = %v, want undeclared reference", err)
	}
}

func TestExecutorEntityCollectionConditionUsesCompiledItemType(t *testing.T) {
	repo := &orderedStateRepo{order: &[]string{}}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        entityCollectionExpressionSource(),
		StateRepo:     repo,
		MutationOwner: stubMutationOwner{state: repo},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:        "entity-1",
		Node:            identitytest.RootNode(t, "filter-node"),
		HandlerEventKey: "filter.requested",
		Event:           eventtest.RunCreatingRootIngress("evt-entity-filter", "filter.requested", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Filter: &runtimecontracts.FilterSpec{
			ItemsFrom: "entity.items",
			Condition: "item.score > 5",
			StoreAs:   "entity.filtered",
		}},
		State: testStateSnapshot("pending", map[string]any{
			"items": []any{map[string]any{"score": 7}, map[string]any{"score": 3}},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rows, ok := result.StateMutation.Fields["filtered"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("filtered rows = %#v, want one exact entity item", result.StateMutation.Fields["filtered"])
	}
}

func TestExecutorChainedCollectionConditionUsesCompiledItemType(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(nil),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:        "entity-1",
		Node:            identitytest.RootNode(t, "collection-node"),
		HandlerEventKey: "digest.requested",
		Event: eventtest.RunCreatingRootIngress(
			"evt-chained-filter", "digest.requested", "", "",
			json.RawMessage(`{"score":0,"items":[{"score":7,"active":true},{"score":3,"active":true}]}`),
			0, "", "", events.EventEnvelope{}, time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Query: &runtimecontracts.QuerySpec{Source: "payload.items", StoreAs: "computed.queried"},
			Filter: &runtimecontracts.FilterSpec{
				ItemsFrom: "computed.queried", Condition: "item.score > 5", StoreAs: "computed.filtered",
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	rows, ok := result.Computed["filtered"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("computed.filtered = %#v, want one chained item", result.Computed["filtered"])
	}
}

func entityCollectionExpressionSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"ScoredItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{"score": {Type: "integer"}}},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"work_state": {Fields: map[string]runtimecontracts.EntityFieldDecl{
				"items":    {Type: "[ScoredItem]"},
				"filtered": {Type: "[ScoredItem]"},
			}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"filter.requested": {Payload: runtimecontracts.EventPayloadSpec{}},
		},
	})
}

func TestExecutor_GuardRecursesAndUsesRegistryCheck(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		GuardRegistry: stubGuardRegistry{entries: map[identity.GuardKey]runtimeregistry.GuardInstruction{
			identity.NormalizeGuardKey("registry_guard"): {
				Key:   identity.NormalizeGuardKey("registry_guard"),
				Check: "entity.allowed == true",
			},
		}},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5":      true,
		"entity.allowed == true": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check: "shadowed.top.level.check",
				Checks: []runtimecontracts.GuardCheck{
					{ID: "payload_score", Check: "payload.score > 5"},
					{ID: "registry_guard"},
				},
			},
		},
		State: StateSnapshot{
			StateCarrier: NewStateCarrier(map[string]any{"allowed": true}, nil, map[string]map[string]any{}),
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.GuardsEvaluated; !reflect.DeepEqual(got, []string{"payload_score", "registry_guard"}) {
		t.Fatalf("GuardsEvaluated = %#v", got)
	}
}

func TestExecutor_RulesUseFirstMatchAndSkipLaterEntries(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "default",
			Rules: []runtimecontracts.HandlerRuleEntry{
				{ID: "rule-1", Condition: "payload.score > 5", AdvancesTo: "approved"},
				{ID: "rule-2", Condition: "else", AdvancesTo: "rejected"},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.HandlerRuleSelection.DisplayLabel() != "rule-1" {
		t.Fatalf("RuleID = %q", result.HandlerRuleSelection.DisplayLabel())
	}
	if result.NextState != "approved" {
		t.Fatalf("NextState = %q", result.NextState)
	}
}

func TestExecutor_PolicySheetRowsExecuteThroughRules(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  - id: deep_scan
    case:
      selector: payload.mode
      equals: deep
    advances_to: deep_scan
  - id: fallback
    default: true
    advances_to: fallback
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		`payload.mode == "deep"`: true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "scan.requested", "", "", json.RawMessage(`{"mode":"deep"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler:  handler,
		State:    testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "deep_scan" {
		t.Fatalf("RuleID = %q, want deep_scan", got)
	}
	if got := result.NextState; got != "deep_scan" {
		t.Fatalf("NextState = %q, want deep_scan", got)
	}
}

func TestExecutor_RulesUseHandlerAdvancesToDefaultWhenRuleOmitsTarget(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "default",
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.HandlerRuleSelection.DisplayLabel() != "rule-1" {
		t.Fatalf("RuleID = %q", result.HandlerRuleSelection.DisplayLabel())
	}
	if result.NextState != "default" {
		t.Fatalf("NextState = %q, want handler-level default", result.NextState)
	}
}

func TestExecutor_HandlerSetsGateAppliesWithMatchedRule(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"payload.score > 5": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			SetsGate: &runtimecontracts.GateSpec{Name: "approved", Value: true},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.HandlerRuleSelection.DisplayLabel() != "rule-1" {
		t.Fatalf("RuleID = %q", result.HandlerRuleSelection.DisplayLabel())
	}
	if result.SetsGate != "approved" {
		t.Fatalf("SetsGate = %q, want handler-level gate with matched rule", result.SetsGate)
	}
	if result.StateMutation.SetGate != "approved" {
		t.Fatalf("StateMutation.SetGate = %q, want approved", result.StateMutation.SetGate)
	}
	if got := result.StateMutation.Gates["approved"]; !got {
		t.Fatalf("StateMutation.Gates[approved] = %v, want true", got)
	}
}

func TestExecutor_RejectsAmbiguousHandlerTopLevelEmitWithRules(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "handler.emitted"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level emit config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RejectsAmbiguousHandlerTopLevelEmitWithRulesWithoutRuleEmit(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "handler.emitted"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:         "rule-1",
				Condition:  "payload.score > 5",
				AdvancesTo: "approved",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level emit config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RulesEmitTemplateSpecializationQueuesOneMergedEvent(t *testing.T) {
	source := sourceWithEvents(map[string]runtimecontracts.EventCatalogEntry{
		"account.scored": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
			"account_id": {Type: "text"}, "score": {Type: "integer"},
		}),
		"account.bucketed": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
			"account_id": {Type: "text"}, "score": {Type: "integer"}, "bucket": {Type: "text"},
		}),
	})
	cases := []struct {
		name   string
		score  int
		bools  map[string]bool
		bucket string
		ruleID string
	}{
		{
			name:   "high_first_match",
			score:  91,
			bools:  map[string]bool{"payload.score >= 80": true, "payload.score >= 40": true},
			bucket: "high",
			ruleID: "high",
		},
		{
			name:   "medium_after_high_fails",
			score:  52,
			bools:  map[string]bool{"payload.score >= 80": false, "payload.score >= 40": true},
			bucket: "medium",
			ruleID: "medium",
		},
		{
			name:   "else_low",
			score:  12,
			bools:  map[string]bool{"payload.score >= 80": false, "payload.score >= 40": false},
			bucket: "low",
			ruleID: "low",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			publications := &recordingPublicationCommitter{}
			exec, err := NewExecutor(RuntimeDependencies{
				Source:        source,
				StateRepo:     stubStateRepo{},
				MutationOwner: composedMutationOwner{publications: publications},
				Locker:        stubLocker{},
				Dispatcher:    stubDispatcher{},
				PayloadShaper: stubPayloadShaper{},
				MaxChainDepth: 5,
			}, stubEvaluator{bools: tc.bools})
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}

			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID:   "entity-1",
				Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
				ChainDepth: 1,
				Event: eventtest.RunCreatingRootIngress(
					"evt-1",
					"account.scored",
					"",
					"",
					mustEncodeJSON(t, map[string]any{"account_id": "acct-1", "score": tc.score}),
					0,
					"",
					"",
					events.EventEnvelope{},
					time.Time{},
				),
				Handler: runtimecontracts.SystemNodeEventHandler{
					Emit: runtimecontracts.EmitSpec{
						Event: "account.bucketed",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"account_id": runtimecontracts.CELExpression("payload.account_id"),
							"score":      runtimecontracts.CELExpression("payload.score"),
						},
					},
					Rules: []runtimecontracts.HandlerRuleEntry{
						{
							ID:        "high",
							Condition: "payload.score >= 80",
							Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
								"bucket": runtimecontracts.CELExpression(`"high"`),
							}},
						},
						{
							ID:        "medium",
							Condition: "payload.score >= 40",
							Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
								"bucket": runtimecontracts.CELExpression(`"medium"`),
							}},
						},
						{
							ID:        "low",
							Condition: "else",
							Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
								"bucket": runtimecontracts.CELExpression(`"low"`),
							}},
						},
					},
				},
				State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if got := result.HandlerRuleSelection.DisplayLabel(); got != tc.ruleID {
				t.Fatalf("RuleID = %q, want %q", got, tc.ruleID)
			}
			if got := len(result.EmitIntents); got != 1 {
				t.Fatalf("EmitIntents len = %d, want 1", got)
			}
			if got := string(result.EmitIntents[0].Event.Type()); got != "account.bucketed" {
				t.Fatalf("emit event = %q, want account.bucketed", got)
			}
			payload := eventPayloadMap(t, result.EmitIntents[0].Event)
			if got := payload["account_id"]; got != "acct-1" {
				t.Fatalf("account_id = %#v, want acct-1", got)
			}
			if got := payload["bucket"]; got != tc.bucket {
				t.Fatalf("bucket = %#v, want %s", got, tc.bucket)
			}
			if got := int(payload["score"].(float64)); got != tc.score {
				t.Fatalf("score = %#v, want %d", payload["score"], tc.score)
			}
			if got := len(publications.intents); got != 1 {
				t.Fatalf("publications intents len = %d, want 1", got)
			}
		})
	}
}

func TestExecutor_EmitFromLoweringQueuesCanonicalPayload(t *testing.T) {
	publications := &recordingPublicationCommitter{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        emitFromExecutorSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: composedMutationOwner{publications: publications},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testRootExecutableNode(t, "bucket-node"),
		ChainDepth: 1,
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"account.scored",
			"",
			"",
			mustEncodeJSON(t, map[string]any{"interest_score": 0.91, "computed_tier": "gold"}),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{
				Event: "account.bucketed",
				From:  "entity",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"interest_score": runtimecontracts.CELExpression("payload"),
					"tier":           runtimecontracts.CELExpression("payload.computed_tier"),
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"account_id": "acct-1",
			"bucket":     "vip",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents len = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "account.bucketed" {
		t.Fatalf("emit event = %q, want account.bucketed", got)
	}
	payload := eventPayloadMap(t, result.EmitIntents[0].Event)
	if got := payload["account_id"]; got != "acct-1" {
		t.Fatalf("account_id = %#v, want acct-1", got)
	}
	if got := payload["bucket"]; got != "vip" {
		t.Fatalf("bucket = %#v, want vip", got)
	}
	if got := payload["interest_score"]; got != 0.91 {
		t.Fatalf("interest_score = %#v, want 0.91", got)
	}
	if got := payload["tier"]; got != "gold" {
		t.Fatalf("tier = %#v, want gold", got)
	}
	if got := payload["shaped_for"]; got != "account.bucketed" {
		t.Fatalf("shaped_for = %#v, want account.bucketed", got)
	}
	if got := len(publications.intents); got != 1 {
		t.Fatalf("publications intents len = %d, want 1", got)
	}
}

func TestExecutor_EmitFromLoweringRequiresCanonicalBundleAtRuntime(t *testing.T) {
	exec := &Executor{}
	_, err := exec.lowerEmitSpecForFrame(&executionFrame{}, "handler.emit", runtimecontracts.EmitSpec{
		Event: "account.bucketed",
		From:  "entity",
	})
	if err == nil || !strings.Contains(err.Error(), "runtime has no contract source for canonical lowering") {
		t.Fatalf("lowerEmitSpecForFrame error = %v, want missing contract source failure", err)
	}
}

func TestExecutor_OnSuccessEmitWithMatchedRuleQueuesRuleThenSuccess(t *testing.T) {
	publications := &recordingPublicationCommitter{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: composedMutationOwner{publications: publications},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{
				Event: "handler.succeeded",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"audit": runtimecontracts.LiteralExpression("ok"),
				},
			}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit: runtimecontracts.EmitSpec{
					Event: "rule.emitted",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"score": runtimecontracts.RefExpression("payload.score"),
					},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "rule-1" {
		t.Fatalf("RuleID = %q, want rule-1", got)
	}
	if got := len(result.EmitIntents); got != 2 {
		t.Fatalf("EmitIntents len = %d, want 2", got)
	}
	if got := []string{string(result.EmitIntents[0].Event.Type()), string(result.EmitIntents[1].Event.Type())}; !reflect.DeepEqual(got, []string{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("emit order = %#v", got)
	}
	if got := len(publications.intents); got != 2 {
		t.Fatalf("publications intents len = %d, want 2", got)
	}
	rulePayload := eventPayloadMap(t, result.EmitIntents[0].Event)
	if got := rulePayload["score"]; got != float64(9) {
		t.Fatalf("rule payload score = %#v, want 9", got)
	}
	successPayload := eventPayloadMap(t, result.EmitIntents[1].Event)
	if got := successPayload["audit"]; got != "ok" {
		t.Fatalf("success payload audit = %#v, want ok", got)
	}
}

func emitFromExecutorSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"account": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"account_id": {Type: "string"},
					"bucket":     {Type: "string"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"account.scored": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"interest_score": {Type: "number"},
						"computed_tier":  {Type: "string"},
					},
					Required: []string{"interest_score", "computed_tier"},
				},
			},
			"account.bucketed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"account_id":     {Type: "string"},
						"bucket":         {Type: "string"},
						"interest_score": {Type: "number"},
						"tier":           {Type: "string"},
					},
					Required: []string{"account_id", "bucket", "interest_score"},
				},
			},
		},
	})
}

func TestExecutor_OnSuccessEmitFiresWhenRulesDoNotMatch(t *testing.T) {
	publications := &recordingPublicationCommitter{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: composedMutationOwner{publications: publications},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": false}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":3}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "" {
		t.Fatalf("RuleID = %q, want empty on no-match success", got)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextRules || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionNoMatch {
		t.Fatalf("rules no-match selection = %#v", result.HandlerRuleSelection)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents len = %d, want 1", got)
	}
	if got := string(result.EmitIntents[0].Event.Type()); got != "handler.succeeded" {
		t.Fatalf("emit event = %q, want handler.succeeded", got)
	}
	if got := len(publications.intents); got != 1 {
		t.Fatalf("publications intents len = %d, want 1", got)
	}
}

func TestExecutor_OnSuccessEmitFailsClosedWhenRuleEventMatchesSuccessEvent(t *testing.T) {
	publications := &recordingPublicationCommitter{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: composedMutationOwner{publications: publications},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "shared.event"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "shared.event"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate declarative emit event") {
		t.Fatalf("Execute error = %v, want duplicate declarative emit event", err)
	}
	if got := len(publications.intents); got != 0 {
		t.Fatalf("publications intents len = %d, want 0 after duplicate failure", got)
	}
}

func TestExecutor_RejectsOnSuccessEmitWithRuleFanOut(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, stubEvaluator{})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	err = exec.ValidateRequest(ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{
			OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "else",
				FanOut: &runtimecontracts.FanOutSpec{
					ItemsFrom: "payload.items",
					As:        "fan_item",
					Identity:  "fan_item",
					Emit:      runtimecontracts.EmitSpec{Event: "item.done"},
				},
			}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "rules[0].fan_out") {
		t.Fatalf("ValidateRequest error = %v, want rules[0].fan_out rejection", err)
	}
}

func TestExecutor_OnSuccessSecondEmitFailureDoesNotCommitFirstEmitOrState(t *testing.T) {
	stateRepo := &recordingStateRepo{}
	publications := &recordingPublicationCommitter{}
	shaper := &eventErrPayloadShaper{failEvent: "handler.succeeded"}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stateRepo,
		MutationOwner: composedMutationOwner{publications: publications},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			OnSuccess:  runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "rule-1",
				Condition: "payload.score > 5",
				Emit:      runtimecontracts.EmitSpec{Event: "rule.emitted"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "payload shape failed") {
		t.Fatalf("Execute error = %v, want payload shape failed", err)
	}
	if got := shaper.shaped; !reflect.DeepEqual(got, []string{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("payload shaper order = %#v", got)
	}
	if got := len(publications.intents); got != 0 {
		t.Fatalf("publications intents len = %d, want 0 after second emit failure", got)
	}
	if got := stateRepo.saves; got != 0 {
		t.Fatalf("state saves = %d, want 0 after second emit failure", got)
	}
}

func TestExecutor_RuleDataAccumulationRunsBeforeTopLevelWrites(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.score > 5": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "metadata.final_source",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				Condition: "payload.score > 5",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
					Writes: []runtimecontracts.WorkflowDataWrite{{
						TargetField: "metadata.final_source",
						Value:       runtimecontracts.LiteralExpression("rule"),
					}, {
						TargetField: "metadata.rule_only",
						Value:       runtimecontracts.LiteralExpression("applied"),
					}},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.StateMutation.Fields["final_source"]; got != "handler" {
		t.Fatalf("final_source = %#v, want handler", got)
	}
	if got := result.StateMutation.Fields["rule_only"]; got != "applied" {
		t.Fatalf("rule_only = %#v, want applied", got)
	}
}

func TestExecutor_RulesDoNotSeeCurrentHandlerTopLevelWritesBeforeSelection(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`entity.branch_target == "handler"`: func(base BaseContext) (bool, error) {
			return base.Entity.Raw()["branch_target"] == "handler", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "branch_target",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "too-early",
				Condition: `entity.branch_target == "handler"`,
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
					Writes: []runtimecontracts.WorkflowDataWrite{{
						TargetField: "rule_selected",
						Value:       runtimecontracts.LiteralExpression(true),
					}},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := strings.TrimSpace(result.HandlerRuleSelection.DisplayLabel()); got != "" {
		t.Fatalf("rule_id = %q, want empty when branch selection cannot see top-level writes", got)
	}
	if _, exists := result.StateMutation.Fields["rule_selected"]; exists {
		t.Fatalf("rule_selected unexpectedly present after rules evaluated before top-level writes: %#v", result.StateMutation.Fields["rule_selected"])
	}
	if got := result.StateMutation.Fields["branch_target"]; got != "handler" {
		t.Fatalf("branch_target = %#v, want handler after data_accumulation step", got)
	}
}

func TestExecutor_OnCompleteDoesNotSeeCurrentHandlerTopLevelWritesBeforeSelection(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, contextualBoolEvaluator{bools: map[string]func(BaseContext) (bool, error){
		`entity.branch_target == "handler"`: func(base BaseContext) (bool, error) {
			return base.Entity.Raw()["branch_target"] == "handler", nil
		},
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetField: "branch_target",
					Value:       runtimecontracts.LiteralExpression("handler"),
				}},
			},
			OnComplete: []runtimecontracts.HandlerRuleEntry{{
				ID:        "too-early",
				Condition: `entity.branch_target == "handler"`,
				Emit:      runtimecontracts.EmitSpec{Event: "branch.selected"},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := strings.TrimSpace(result.HandlerRuleSelection.DisplayLabel()); got != "" {
		t.Fatalf("rule_id = %q, want empty when on_complete selection cannot see top-level writes", got)
	}
	if result.HandlerRuleSelection.Context() != handlerselection.ContextOnComplete || result.HandlerRuleSelection.Disposition() != handlerselection.DispositionNoMatch {
		t.Fatalf("on_complete no-match selection = %#v", result.HandlerRuleSelection)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("emit intents = %d, want 0 when on_complete branch is not selected early", got)
	}
	if got := result.StateMutation.Fields["branch_target"]; got != "handler" {
		t.Fatalf("branch_target = %#v, want handler after data_accumulation step", got)
	}
}

func TestExecutor_ChainDepthOverflowInterceptsEmitsButSucceeds(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		MaxChainDepth: 1,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			AdvancesTo: "done",
			Emit:       runtimecontracts.EmitSpec{Event: "task.followup"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.Status; got != OutcomeCompleted {
		t.Fatalf("Status = %q, want completed", got)
	}
	if got := result.NextState; got != "done" {
		t.Fatalf("NextState = %q, want done", got)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("EmitIntents count = %d, want 0", got)
	}
	if got := len(result.DeadLetterIntents); got != 1 {
		t.Fatalf("DeadLetterIntents count = %d, want 1", got)
	}
	if got := result.DeadLetterIntents[0].DeadLetterHint; got != "chain_depth_exceeded" {
		t.Fatalf("DeadLetterHint = %q", got)
	}
}

func TestExecutor_FanOutCreatesShapedEmitIntentsAndStopsLoop(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutPayloadSource(t, "task.completed"),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-shaped"), "task.completed", "", "", json.RawMessage(`{"items":["a","b"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "fan_item",
				Identity:  "fan_item",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
			},
			AdvancesTo: "processing",
			Action:     runtimecontracts.ActionSpec{ID: "should_not_run"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q", result.Status)
	}
	if result.NextState != "processing" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if result.FanOutCount != 2 || result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 2 {
		t.Fatalf("fan_out results wrong: count=%d intent=%#v", result.FanOutCount, result.FanOutIntent)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("trigger transaction created eager fan-out emits: %#v", result.EmitIntents)
	}
	if _, found := result.StateMutation.Bookkeeping["fan_out_count"]; found {
		t.Fatalf("retired fan_out_count metadata survived: %#v", result.StateMutation.Bookkeeping)
	}
	if result.FanOutIntent.Capsule.ChainDepth != 1 {
		t.Fatalf("capsule chain depth = %d, want trigger depth 1", result.FanOutIntent.Capsule.ChainDepth)
	}
	if got := result.ActionsExecuted; len(got) != 4 {
		t.Fatalf("fan-out transition actions = %#v, want four lifecycle actions and no authored action", got)
	} else {
		for _, action := range got {
			if action == "should_not_run" {
				t.Fatalf("post-fan-out authored action executed: %#v", got)
			}
		}
	}
}

func TestExecutor_FanOutDeliveryBarrierTriggerAndCompletionUseDisjointExecutionPaths(t *testing.T) {
	node := testRootExecutableNode(t, "dispatcher")
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
fan_out:
  element_id: a1111111-1111-4111-8111-111111111111
  items_from: payload.items
  as: fan_item
  identity: fan_item
  emit:
    event: item.requested
    fields:
      item: fan_item
join:
  id: all-items-delivered
  members:
    from_fan_out: a1111111-1111-4111-8111-111111111111
  on_complete:
    element_id: b1111111-1111-4111-8111-111111111111
    emit:
      event: batch.completed
      fields:
        total: join.total
        succeeded: join.dispositions.succeeded
`), &handler); err != nil {
		t.Fatal(err)
	}
	qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {ID: "dispatcher", ExecutionType: runtimecontracts.SystemNodeExecutionType, EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"batch.requested": qualified}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"batch.requested": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"items": {Type: "[text]"}}}},
			"item.requested":  {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"item": {Type: "text"}}}},
			"batch.completed": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"total": {Type: "integer"}, "succeeded": {Type: "integer"}}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root", Version: "v-test"},
	}
	source := fanOutSourceWithBundleIdentity(t, bundle)
	if err := bundle.CompileFanOutHandlerPlans(node, "batch.requested", qualified); err != nil {
		t.Fatal(err)
	}
	plans := bundle.FanOutPlansForHandler(node, "batch.requested")
	if len(plans) != 1 {
		t.Fatalf("compiled fan-out plans = %#v", plans)
	}
	bundle.Semantics.Joins = []runtimecontracts.WorkflowJoinPlan{{
		Node: node, HandlerEvent: "batch.requested", Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery,
		Spec: *qualified.Join, FanOut: runtimecontracts.WorkflowFanOutDeliveryJoinPlan{FanOut: plans[0].Ref},
	}}
	declaration, err := timeridentity.NewFanOutDeliveryJoinRef(
		node, "batch.requested", qualified.Join.EffectiveID(),
		plans[0].Ref.ElementRef.PackageKey, plans[0].Ref.ElementRef.ElementID,
		plans[0].Ref.BundleHash, plans[0].Ref.SemanticDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
		Dispatcher: stubDispatcher{}, PayloadShaper: stubPayloadShaper{}, MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC()
	trigger := eventtest.RunCreatingRootIngress(
		eventtest.UUID("fan-out-delivery-trigger"), "batch.requested", "operator", "",
		json.RawMessage(`{"items":["a","b"]}`), 0, semanticExecutionFixtureRunID, "", events.EventEnvelope{}, createdAt,
	)
	request := ExecutionRequest{
		EntityID: "entity-1", Node: node, HandlerEventKey: "batch.requested", Event: trigger,
		Handler: qualified, JoinDeclaration: declaration,
		State: testStateSnapshot("active", map[string]any{}, nil, map[string]map[string]any{}),
	}
	triggerResult, err := exec.ExecuteSemanticFixture(context.Background(), request)
	if err != nil {
		t.Fatalf("execute barrier trigger: %v", err)
	}
	if triggerResult.Status != OutcomeFannedOut || triggerResult.FanOutIntent == nil || triggerResult.FanOutBarrier == nil || triggerResult.FanOutBarrierCompletion != nil {
		t.Fatalf("barrier trigger result = %#v", triggerResult)
	}
	if triggerResult.FanOutIntent.Cardinality != 2 || triggerResult.FanOutBarrier.IntentKey != triggerResult.FanOutIntent.Key || len(triggerResult.EmitIntents) != 0 {
		t.Fatalf("barrier trigger ownership = intent:%#v barrier:%#v emits:%#v", triggerResult.FanOutIntent, triggerResult.FanOutBarrier, triggerResult.EmitIntents)
	}

	bound, err := declaration.BindFanOutIntent(triggerResult.FanOutIntent.Key.TriggeringDeliveryID, declaration.Generation())
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(bound)
	if err != nil {
		t.Fatal(err)
	}
	payload := handle.PayloadMetadata()
	payload["join"] = map[string]any{
		"total": 2,
		"dispositions": map[string]any{
			"succeeded": 2, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	completionEvent := eventtest.ExistingRunRootIngress(
		eventtest.UUID("fan-out-delivery-completion"), events.EventType(handle.EventType()), "platform", handle.TaskID(), raw, 0,
		semanticExecutionFixtureRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), createdAt.Add(time.Second),
	)
	request.Event = completionEvent
	completionResult, err := exec.ExecuteSemanticFixture(context.Background(), request)
	if err != nil {
		t.Fatalf("execute barrier completion: %v", err)
	}
	if completionResult.Status == OutcomeFannedOut || completionResult.FanOutIntent != nil || completionResult.FanOutBarrier != nil || completionResult.FanOutBarrierCompletion == nil {
		t.Fatalf("completion re-entered trigger path = %#v", completionResult)
	}
	if len(completionResult.EmitIntents) != 1 || completionResult.EmitIntents[0].Event.Type() != "batch.completed" {
		t.Fatalf("completion output = %#v", completionResult.EmitIntents)
	}
	var completed map[string]any
	if err := json.Unmarshal(completionResult.EmitIntents[0].Event.Payload(), &completed); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(completed["total"]) != "2" || fmt.Sprint(completed["succeeded"]) != "2" {
		t.Fatalf("completion summary payload = %#v", completed)
	}
}

func TestExecutor_FanOutDeliveryBarrierStaleGenerationIsMutationFreeDiscard(t *testing.T) {
	node := testRootExecutableNode(t, "dispatcher")
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
fan_out:
  element_id: a1111111-1111-4111-8111-111111111111
  items_from: payload.items
  as: fan_item
  identity: fan_item
  emit:
    event: item.requested
join:
  id: all-items-delivered
  members:
    from_fan_out: a1111111-1111-4111-8111-111111111111
  on_complete:
    element_id: b1111111-1111-4111-8111-111111111111
    advances_to: complete
    emit:
      event: batch.completed
`), &handler); err != nil {
		t.Fatal(err)
	}
	qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {ID: "dispatcher", ExecutionType: runtimecontracts.SystemNodeExecutionType, EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"batch.requested": qualified}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"batch.requested": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"items": {Type: "[text]"}}}},
			"item.requested":  {},
			"batch.completed": {},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root", Version: "v-test"},
	}
	source := fanOutSourceWithBundleIdentity(t, bundle)
	if err := bundle.CompileFanOutHandlerPlans(node, "batch.requested", qualified); err != nil {
		t.Fatal(err)
	}
	plan := bundle.FanOutPlansForHandler(node, "batch.requested")[0]
	bundle.Semantics.Joins = []runtimecontracts.WorkflowJoinPlan{{
		Node: node, HandlerEvent: "batch.requested", Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery,
		Spec: *qualified.Join, FanOut: runtimecontracts.WorkflowFanOutDeliveryJoinPlan{FanOut: plan.Ref},
	}}
	declaration, err := timeridentity.NewFanOutDeliveryJoinRef(
		node, "batch.requested", qualified.Join.EffectiveID(), plan.Ref.ElementRef.PackageKey,
		plan.Ref.ElementRef.ElementID, plan.Ref.BundleHash, plan.Ref.SemanticDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	activation, err := loopruntime.New(semanticExecutionFixtureRunID, "entity-1", "", "revision", "revision_id", eventtest.UUID("loop-start"), "active", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	staleGeneration := activation.Generation()
	if _, err := activation.Repeat("active", eventtest.UUID("loop-repeat"), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	bound, err := declaration.BindFanOutIntent(eventtest.UUID("fan-out-trigger"), staleGeneration)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(bound)
	if err != nil {
		t.Fatal(err)
	}
	payload := handle.PayloadMetadata()
	payload["join"] = map[string]any{"total": 1, "dispositions": map[string]any{
		"succeeded": 1, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0,
	}}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{},
		Dispatcher: stubDispatcher{}, PayloadShaper: stubPayloadShaper{}, MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: node, HandlerEventKey: "batch.requested", Handler: qualified,
		JoinDeclaration: declaration,
		Event: eventtest.ExistingRunRootIngress(
			eventtest.UUID("stale-fan-out-completion"), events.EventType(handle.EventType()), "platform", handle.TaskID(), raw, 0,
			semanticExecutionFixtureRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), now.Add(2*time.Second),
		),
		State: testStateSnapshot("active", map[string]any{"sentinel": "unchanged"}, nil, buckets),
	})
	if err != nil {
		t.Fatalf("execute stale completion: %v", err)
	}
	if result.Status != OutcomeDiscarded || result.FanOutBarrierCompletion != nil || len(result.EmitIntents) != 0 || result.StateMutation.NextState != "" || len(result.ActionsExecuted) != 0 {
		t.Fatalf("stale completion result = %#v, want mutation-free discard", result)
	}
	if !reflect.DeepEqual(result.StateMutation.StateBuckets, map[string]map[string]any(nil)) || len(result.StateMutation.Fields) != 0 {
		t.Fatalf("stale completion mutated state = %#v", result.StateMutation)
	}
}

func TestExecutor_DeferredFanOutRejectsUndeclaredBusinessPayload(t *testing.T) {
	node := testRootExecutableNode(t, "fan-out-node")
	elementID, err := contractelementidentity.ParseContractElementID("418dadf9-0ebd-418d-a904-53d3a849b7df")
	if err != nil {
		t.Fatal(err)
	}
	handler := runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{
		ElementID: elementID,
		ItemsFrom: "payload.items", As: "fan_item", Identity: "fan_item.label",
		Emit: runtimecontracts.EmitSpec{Event: "item.emitted", Fields: map[string]runtimecontracts.ExpressionValue{
			"label": runtimecontracts.CELExpression("fan_item.label"),
			"extra": runtimecontracts.CELExpression(`"not-declared"`),
		}},
	}}
	qualified, err := runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"fan-out-node": {ID: "fan-out-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"batch.ready": qualified}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root", Version: "v-test", NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
			"fan-out-node": {"batch.ready": qualified},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"batch.ready": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"items": {Type: "[json]"}}}},
		},
	}
	shaper := &recordingPayloadShaper{err: errors.Join(ErrEmitPayloadContractViolation, errors.New("undeclared fan-out field"))}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: fanOutSourceWithBundleIdentity(t, bundle), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{},
		Locker: stubLocker{}, Dispatcher: stubDispatcher{}, PayloadShaper: shaper,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	trigger := eventtest.RunCreatingRootIngress(
		eventtest.UUID("deferred-fan-out-payload"), "batch.ready", "", "", json.RawMessage(`{"items":[{"label":"x"}]}`), 0,
		semanticExecutionFixtureRunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1", Node: node, Event: trigger, Handler: qualified,
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 1 {
		t.Fatalf("durable fan-out intent = %#v, want one item", result.FanOutIntent)
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{
		Request: *result.FanOutIntent, Source: result.FanOutIntent.Source,
		Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: now, UpdatedAt: now,
	}
	_, err = exec.EvaluateFanOutOrdinal(context.Background(), intent, trigger, map[string]any{"label": "x"}, 0)
	if !errors.Is(err, ErrEmitPayloadContractViolation) {
		t.Fatalf("deferred fan-out payload error = %v, want %v", err, ErrEmitPayloadContractViolation)
	}
}

func TestExecutor_FanOutCountPreservesRuleThenTopLevelWriteSnapshots(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutEntitySource(t),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.enabled": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("00000000-0000-4000-8000-000000002274"),
		Node:     testRootExecutableNode(t, "dispatcher"),
		Event:    eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-count-order"), "task.completed", "", "", json.RawMessage(`{"enabled":true}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetRef: "metadata.top_count", Value: runtimecontracts.CELExpression("fan_out.count")},
				{TargetRef: "metadata.top_saw_rule", Value: runtimecontracts.CELExpression("entity.rule_count")},
			}},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				Condition: "payload.enabled",
				DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetRef: "metadata.rule_count", Value: runtimecontracts.CELExpression("fan_out.count")},
				}},
				FanOut: &runtimecontracts.FanOutSpec{
					ItemsFrom: "entity.items", As: "fan_item", Identity: "fan_item",
					Emit: runtimecontracts.EmitSpec{Event: "item.requested", Fields: map[string]runtimecontracts.ExpressionValue{
						"item": runtimecontracts.CELExpression("fan_item"),
					}},
				},
			}},
		},
		State: testStateSnapshot("pending", map[string]any{"items": []any{"a", "b", "c"}}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 3 {
		t.Fatalf("fan-out intent = %#v, want cardinality 3", result.FanOutIntent)
	}
	if got := result.StateMutation.Fields["rule_count"]; got != int64(3) && got != 3 {
		t.Fatalf("rule_count = %#v, want 3", got)
	}
	if got := result.StateMutation.Fields["top_count"]; got != int64(3) && got != 3 {
		t.Fatalf("top_count = %#v, want 3", got)
	}
	if got := result.StateMutation.Fields["top_saw_rule"]; got != int64(3) && got != 3 {
		t.Fatalf("top_saw_rule = %#v, want rule-layer value 3", got)
	}
}

func TestExecutor_FanOutEntitySourceBindsAfterSameHandlerMutation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutEntitySource(t),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: identity.NormalizeEntityID("00000000-0000-4000-8000-000000002275"),
		Node:     testRootExecutableNode(t, "dispatcher"),
		Event:    eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-post-write-source"), "task.completed", "", "", json.RawMessage(`{"replacement":["new-a","new-b","new-c"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetRef: "entity.items", Value: runtimecontracts.CELExpression("payload.replacement"),
			}}},
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "entity.items", As: "fan_item", Identity: "fan_item",
				Emit: runtimecontracts.EmitSpec{Event: "item.requested", Fields: map[string]runtimecontracts.ExpressionValue{
					"item": runtimecontracts.CELExpression("fan_item"),
				}},
			},
		},
		State: testStateSnapshot("pending", map[string]any{"items": []any{"old"}}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 3 || result.FanOutCount != 3 {
		t.Fatalf("post-write fan-out = count:%d intent:%#v, want cardinality 3", result.FanOutCount, result.FanOutIntent)
	}
	if got := result.StateMutation.Fields["items"]; !reflect.DeepEqual(got, []any{"new-a", "new-b", "new-c"}) {
		t.Fatalf("persisted source = %#v", got)
	}
	if result.FanOutIntent.Source.Kind != "entity_field_revision" || result.FanOutIntent.Source.Field != "items" {
		t.Fatalf("fan-out source = %#v", result.FanOutIntent.Source)
	}
	if _, copied := result.FanOutIntent.Capsule.Entity["items"]; copied {
		t.Fatalf("entity source was redundantly copied into capsule: %#v", result.FanOutIntent.Capsule.Entity)
	}
}

func TestExecutor_FanOutBoundExceededFailsClosedBeforeEmit(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutPayloadSource(t, "task.completed"),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-bound"), "task.completed", "", "", json.RawMessage(`{"items":["a","b"]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "fan_item",
				Identity:  "fan_item",
				MaxItems:  1,
				Emit: runtimecontracts.EmitSpec{
					Event: "item.process",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"item_id": runtimecontracts.CELExpression("fan_item"),
					},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected fan_out bound exceeded error")
	}
	if !errors.Is(err, ErrFanOutBoundExceeded) {
		t.Fatalf("Execute error = %v, want ErrFanOutBoundExceeded", err)
	}
	runtimeErr, ok := failures.As(err)
	if !ok {
		t.Fatalf("Execute error = %T %v, want canonical failure", err, err)
	}
	if runtimeErr.Failure.Class != failures.ClassFanOutBoundExceeded || runtimeErr.Failure.Retryable {
		t.Fatalf("runtime error = %#v, want non-retryable %s", runtimeErr, failures.ClassFanOutBoundExceeded)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("bound failure emitted partial prefix: %#v", result.EmitIntents)
	}
	attributes := runtimeErr.Failure.Detail.Attributes
	if fmt.Sprint(attributes["actual"]) != "2" || fmt.Sprint(attributes["authored_limit"]) != "1" || fmt.Sprint(attributes["effective_limit"]) != "1" {
		t.Fatalf("bound failure attributes = %#v, want actual/authored/effective 2/1/1", attributes)
	}
	if got := fmt.Sprint(attributes["remediation"]); got != "keep source cardinality within the effective max_items bound" {
		t.Fatalf("bound failure remediation attribute = %q", got)
	}
	if got := FailureDispositionFor(err); got != FailureDispositionTerminal {
		t.Fatalf("FailureDispositionFor = %v, want terminal", got)
	}
}

func TestExecutor_FanOutRuleContextsPreserveOrderMultiplicityAndBounds(t *testing.T) {
	type fanOutContextCase struct {
		name            string
		eventType       events.EventType
		payload         json.RawMessage
		handlerEventKey string
		handler         func(*runtimecontracts.FanOutSpec) runtimecontracts.SystemNodeEventHandler
	}
	cases := []fanOutContextCase{
		{
			name:      "rules",
			eventType: "batch.ready",
			handler: func(spec *runtimecontracts.FanOutSpec) runtimecontracts.SystemNodeEventHandler {
				return runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{
					ID: "dispatch", Condition: "else", FanOut: spec, AdvancesTo: "dispatched",
				}}}
			},
		},
		{
			name:      "on_complete",
			eventType: "batch.ready",
			handler: func(spec *runtimecontracts.FanOutSpec) runtimecontracts.SystemNodeEventHandler {
				return runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID: "dispatch", Condition: "else", FanOut: spec, AdvancesTo: "dispatched",
				}}}
			},
		},
	}

	newSpec := func(maxItems int) *runtimecontracts.FanOutSpec {
		return &runtimecontracts.FanOutSpec{
			ItemsFrom:   "payload.items",
			As:          "line_item",
			Identity:    "line_item.id",
			MaxItems:    maxItems,
			MaxItemsSet: maxItems > 0,
			Emit: runtimecontracts.EmitSpec{
				Event: "item.process",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"item_id":    runtimecontracts.CELExpression("line_item.id"),
					"item_index": runtimecontracts.CELExpression("fan_out.index"),
				},
			},
		}
	}
	payloadItems := json.RawMessage(`{"items":[{"id":"item-b"},{"id":"item-a"},{"id":"item-b"}]}`)
	wantItemOrder := []string{"item-b", "item-a", "item-b"}
	fixedEmitNow := time.Date(2026, time.July, 12, 12, 0, 0, 1, time.UTC)
	state := func() StateSnapshot {
		return testStateSnapshot("ready", map[string]any{}, nil, map[string]map[string]any{})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			transition := &recordingTransitionValidator{}
			exec, err := NewExecutor(RuntimeDependencies{
				Source:              fanOutPayloadSource(t, "batch.ready"),
				StateRepo:           stubStateRepo{},
				MutationOwner:       stubMutationOwner{},
				Locker:              stubLocker{},
				Dispatcher:          stubDispatcher{},
				PayloadShaper:       stubPayloadShaper{},
				TransitionValidator: transition,
				EmitNow:             func() time.Time { return fixedEmitNow },
			}, nil)
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			payload := tc.payload
			if len(payload) == 0 {
				payload = payloadItems
			} else {
				var timerPayload map[string]any
				if err := json.Unmarshal(payload, &timerPayload); err != nil {
					t.Fatalf("decode timer payload: %v", err)
				}
				timerPayload["items"] = []any{map[string]any{"id": "item-b"}, map[string]any{"id": "item-a"}, map[string]any{"id": "item-b"}}
				payload, _ = json.Marshal(timerPayload)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID:        "entity-1",
				Node:            testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:           eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-"+tc.name), tc.eventType, "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{}),
				HandlerEventKey: tc.handlerEventKey,
				Handler:         tc.handler(newSpec(0)),
				State:           state(),
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if result.FanOutCount != len(wantItemOrder) || result.FanOutIntent == nil || result.FanOutIntent.Cardinality != len(wantItemOrder) {
				t.Fatalf("fan_out result count=%d intent=%#v, want cardinality %d", result.FanOutCount, result.FanOutIntent, len(wantItemOrder))
			}
			if len(result.EmitIntents) != 0 {
				t.Fatalf("trigger transaction created eager fan-out emits: %#v", result.EmitIntents)
			}
			if got := result.NextState; got != "dispatched" {
				t.Fatalf("NextState = %q, want dispatched", got)
			}
			if transition.calls != 1 || transition.current != "ready" || transition.next != "dispatched" {
				t.Fatalf("transition = calls:%d %q->%q, want one ready->dispatched", transition.calls, transition.current, transition.next)
			}

			transition = &recordingTransitionValidator{}
			exec.deps.TransitionValidator = transition
			result, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID:        "entity-1",
				Node:            testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:           eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-bound-"+tc.name), tc.eventType, "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{}),
				HandlerEventKey: tc.handlerEventKey,
				Handler:         tc.handler(newSpec(1)),
				State:           state(),
			})
			if err == nil || !errors.Is(err, ErrFanOutBoundExceeded) {
				t.Fatalf("bounded Execute result=%#v error=%v, want ErrFanOutBoundExceeded", result, err)
			}
			if transition.calls != 0 {
				t.Fatalf("bounded transition calls = %d, want 0", transition.calls)
			}
		})
	}
}

func TestExecutor_FanOutRejectsInvalidSourceAndExplicitZeroBound(t *testing.T) {
	tests := []struct {
		name string
		spec runtimecontracts.FanOutSpec
		want string
	}{
		{
			name: "nested items source",
			spec: runtimecontracts.FanOutSpec{ItemsFrom: "payload.items.missing", As: "line_item", Identity: "line_item.id", Emit: runtimecontracts.EmitSpec{Event: "item.process"}},
			want: "must reference exactly one declared top-level collection field",
		},
		{
			name: "explicit zero bound",
			spec: runtimecontracts.FanOutSpec{ItemsFrom: "payload.items", As: "line_item", Identity: "line_item.id", MaxItemsSet: true, Emit: runtimecontracts.EmitSpec{Event: "item.process"}},
			want: "fan_out.max_items must be a positive integer when set",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := NewExecutor(RuntimeDependencies{
				Source: stubSource(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1", Node: testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:   eventtest.RunCreatingRootIngress("evt-1", "batch.ready", "", "", json.RawMessage(`{"items":[{"id":"item-a"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{FanOut: &tc.spec},
				State:   testStateSnapshot("ready", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if err == nil || !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Execute result=%#v error=%v, want ErrInvalidConfig containing %q", result, err, tc.want)
			}
			if len(result.EmitIntents) != 0 || result.StateMutation.NextState != "" {
				t.Fatalf("invalid fan_out produced effects: %#v", result)
			}
		})
	}
}

func TestActiveFanOutPlanEmitSourceNamesOwningSite(t *testing.T) {
	tests := []struct {
		name string
		kind runtimecontracts.FanOutSiteKind
		want string
	}{
		{name: "handler", want: "handler.fan_out.emit"},
		{name: "rules", kind: runtimecontracts.FanOutSiteRule, want: "handler.rules.fan_out.emit"},
		{name: "on_complete", kind: runtimecontracts.FanOutSiteOnComplete, want: "handler.on_complete.fan_out.emit"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (activeFanOutPlan{Plan: runtimecontracts.FanOutCompiledPlan{Site: runtimecontracts.FanOutSiteRef{Kind: tc.kind}}}).EmitSource(); got != tc.want {
				t.Fatalf("EmitSource = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSelectedFanOutPlanIgnoresContradictoryRawHandlerSpec(t *testing.T) {
	canonical := runtimecontracts.FanOutCompiledPlan{
		Site:      runtimecontracts.FanOutSiteRef{Kind: runtimecontracts.FanOutSiteHandler, Index: -1},
		ItemsFrom: "payload.canonical", Identity: "item.id", Emit: runtimecontracts.EmitSpec{Event: "canonical.requested"},
	}
	frame := &executionFrame{req: ExecutionRequest{
		Handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{
			ItemsFrom: "entity.hostile", Identity: "hostile", Emit: runtimecontracts.EmitSpec{Event: "hostile.requested"},
		}},
		FanOutPlans: []runtimecontracts.FanOutCompiledPlan{canonical},
	}}
	selected := selectedFanOutPlan(frame)
	if !selected.Found || selected.Plan.ItemsFrom != canonical.ItemsFrom || selected.Plan.Emit.EventType() != "canonical.requested" {
		t.Fatalf("selected plan = %#v, want canonical compiled owner", selected)
	}
}

func TestExecutor_PayloadTransformSeesDataAccumulationWrites(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithEvents(map[string]runtimecontracts.EventCatalogEntry{
			"vertical.discovered": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"mode": {Type: "text"}, "discovery_context": {Type: "object"},
			}),
			"scoring.requested": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"vertical_name": {Type: "text"}, "rubric": {Type: "text"}, "dimensions_requested": {Type: "[text]"}, "discovery_context": {Type: "object"},
			}),
		}),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.mode == 'corpus'": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "vertical-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"vertical.discovered", "", "", json.RawMessage(`{"mode":"corpus","discovery_context":{"source":"corpus"}}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{TargetField: "name", Value: runtimecontracts.LiteralExpression("Test Vertical")},
					{TargetField: "dimensions_requested", Value: runtimecontracts.LiteralExpression([]string{"a", "b"})},
				},
			},
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					Condition: "payload.mode == 'corpus'",
					DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
						Writes: []runtimecontracts.WorkflowDataWrite{
							{TargetField: "scoring_rubric", Value: runtimecontracts.LiteralExpression("corpus_rubric")},
						},
					},
					Emit: runtimecontracts.EmitSpec{
						Event: "scoring.requested",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"vertical_name":        runtimecontracts.CELExpression("entity.name"),
							"rubric":               runtimecontracts.CELExpression("entity.scoring_rubric"),
							"dimensions_requested": runtimecontracts.CELExpression("entity.dimensions_requested"),
							"discovery_context":    runtimecontracts.CELExpression("payload.discovery_context"),
						},
					},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(result.EmitIntents[0].Event.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := payload["vertical_name"]; got != "Test Vertical" {
		t.Fatalf("vertical_name = %#v", got)
	}
	if got := payload["rubric"]; got != "corpus_rubric" {
		t.Fatalf("rubric = %#v", got)
	}
	dims, ok := payload["dimensions_requested"].([]any)
	if !ok || len(dims) != 2 || dims[0] != "a" || dims[1] != "b" {
		t.Fatalf("dimensions_requested = %#v", payload["dimensions_requested"])
	}
	ctx, ok := payload["discovery_context"].(map[string]any)
	if !ok || ctx["source"] != "corpus" {
		t.Fatalf("discovery_context = %#v", payload["discovery_context"])
	}
}

func TestExecutor_EmitIntentUsesTargetStateFlowIdentityBeforeInboundSource(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	const (
		targetEntityID     = "validation-entity"
		targetFlowInstance = "validation/inst-1"
		sourceEntityID     = "scoring-entity"
		sourceFlowInstance = "scoring/inst-1"
	)
	manifestations := []string{
		"validation.started",
		"validation.package_ready",
		"brand.requested",
		"cto.spec_review_requested",
		"spec.revision_requested",
	}
	for _, eventType := range manifestations {
		t.Run(eventType, func(t *testing.T) {
			state := testStateSnapshot("researching", map[string]any{
				"flow_path": targetFlowInstance,
			}, nil, map[string]map[string]any{})
			state.EntityID = identity.NormalizeEntityID(targetEntityID)
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: targetEntityID,
				Node:     testFlowExecutableNode(t, "validation", "validation-router"),
				Event: eventtest.RunCreatingRootIngress(
					"evt-1",
					"scoring/vertical.resumed",
					"",
					"",
					json.RawMessage(`{"vertical_id":"`+sourceEntityID+`"}`),
					0,
					"",
					"",
					events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
						EntityID:     sourceEntityID,
						FlowInstance: sourceFlowInstance,
						FlowID:       "scoring",
					}),
					time.Time{},
				),

				Handler: runtimecontracts.SystemNodeEventHandler{
					Emit: runtimecontracts.EmitSpec{
						Event: eventType,
						Fields: map[string]runtimecontracts.ExpressionValue{
							"source_entity_id":     runtimecontracts.CELExpression("event.source.entity_id"),
							"source_flow_instance": runtimecontracts.CELExpression("event.source.flow_instance"),
						},
					},
				},
				State: state,
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if got := len(result.EmitIntents); got != 1 {
				t.Fatalf("EmitIntents count = %d, want 1", got)
			}
			emitted := result.EmitIntents[0].Event
			if got := emitted.EntityID(); got != targetEntityID {
				t.Fatalf("emitted entity_id = %q, want target validation entity %q", got, targetEntityID)
			}
			if got := emitted.FlowInstance(); got != targetFlowInstance {
				t.Fatalf("emitted flow_instance = %q, want target validation flow %q", got, targetFlowInstance)
			}
			var payload map[string]any
			if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if got := payload["source_entity_id"]; got != sourceEntityID {
				t.Fatalf("source_entity_id = %#v, want explicit source entity %q", got, sourceEntityID)
			}
			if got := payload["source_flow_instance"]; got != sourceFlowInstance {
				t.Fatalf("source_flow_instance = %#v, want explicit source flow %q", got, sourceFlowInstance)
			}
		})
	}
}

func TestExecutor_EmitIntentUsesAdmittedProducerSourceBeforeStateMetadata(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	admitted := events.RouteIdentity{
		FlowID:       "validation",
		FlowInstance: "validation",
		EntityID:     "validation-entity",
	}
	producerSource, err := events.NewStaticFlowRoutingSource(admitted)
	if err != nil {
		t.Fatalf("NewStaticFlowRoutingSource: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:       identity.NormalizeEntityID(admitted.EntityID),
		Node:           testFlowExecutableNode(t, "validation", "validation-router"),
		ProducerSource: producerSource,
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"validation.started",
			"",
			"",
			json.RawMessage(`{}`),
			0,
			"",
			"",
			events.EnvelopeForFlowInstance(events.EventEnvelope{}, "validation/validation"),
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "validation.completed"},
		},
		State: testStateSnapshot("researching", map[string]any{
			"flow_path": "validation/validation",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	if got := result.EmitIntents[0].Event.SourceRoute(); got != admitted.Normalized() {
		t.Fatalf("emitted source route = %#v, want admitted %#v", got, admitted.Normalized())
	}
}

func TestExecutor_EmitIntentUsesExplicitProducerSourceWhenStateFlowPathNormalizesEmpty(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	producerSource, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: "root", FlowInstance: "source/inst-1", EntityID: "entity-1",
	})
	if err != nil {
		t.Fatalf("NewConcreteTemplateInstanceRoutingSource: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:       "entity-1",
		Node:           testFlowExecutableNode(t, "root", "node-1"),
		ProducerSource: producerSource,
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"root.started",
			"",
			"",
			json.RawMessage(`{}`),
			0,
			"",
			"",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "entity-1"), "source/inst-1"),
			time.Time{},
		),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "root.done"},
		},
		State: testStateSnapshot("pending", map[string]any{
			"flow_path": "/",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	if got := result.EmitIntents[0].Event.FlowInstance(); got != "source/inst-1" {
		t.Fatalf("emitted flow_instance = %q, want explicit producer source/inst-1", got)
	}
}

func TestExecutor_DeclarativeEmitSurfacesUseProducerSourceRouteNamespace(t *testing.T) {
	source := sourceWithDeclarativeEmitExternalizationFlows()
	parentRoute := events.RouteIdentity{
		FlowID:       "operating",
		FlowInstance: "operating/opco-1",
		EntityID:     "opco-entity",
	}.Normalized()
	cases := []struct {
		name      string
		eventType string
		payload   json.RawMessage
		handler   runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "top-level emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
			},
		},
		{
			name: "rule emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					Condition: "else",
					Emit:      runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
				}},
			},
		},
		{
			name: "on-complete emit",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					Emit: runtimecontracts.EmitSpec{Event: "component-scaffold/component.scaffolded"},
				}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := NewExecutor(RuntimeDependencies{
				Source:        source,
				StateRepo:     stubStateRepo{},
				MutationOwner: stubMutationOwner{},
				Locker:        stubLocker{},
				Dispatcher:    stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			eventType := tc.eventType
			if eventType == "" {
				eventType = "repo-scaffold/repo_scaffold.repo_scaffolded"
			}
			payload := tc.payload
			if len(payload) == 0 {
				payload = json.RawMessage(`{}`)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "component-entity",
				Node:     testFlowExecutableNode(t, "component-scaffold", "component-node"),
				Event:    eventtest.RunCreatingRootIngress("evt-1", events.EventType(eventType), "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler:  tc.handler,
				State: testStateSnapshot("ready", map[string]any{
					"flow_path":            "component-scaffold/component-1",
					"parent_flow_id":       parentRoute.FlowID,
					"parent_flow_instance": parentRoute.FlowInstance,
					"parent_entity_id":     parentRoute.EntityID,
				}, nil, map[string]map[string]any{}),
			})
			if err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if got := len(result.EmitIntents); got != 1 {
				t.Fatalf("EmitIntents count = %d, want 1", got)
			}
			emitted := result.EmitIntents[0].Event
			if got, want := string(emitted.Type()), "component-scaffold/component-1/component.scaffolded"; got != want {
				t.Fatalf("emitted type = %q, want %q", got, want)
			}
			if got := emitted.SourceRoute().FlowInstance; got != "component-scaffold/component-1" {
				t.Fatalf("source flow_instance = %q, want component-scaffold/component-1", got)
			}
			if got, want := emitted.SourceAgent(), testFlowExecutableNode(t, "component-scaffold", "component-node").Key(); got != want {
				t.Fatalf("source agent = %q, want %s", got, want)
			}
			if got := emitted.ProducerType(); got != events.EventProducerNode {
				t.Fatalf("producer type = %q, want node", got)
			}
			if got := emitted.TargetRoute().FlowInstance; got != parentRoute.FlowInstance {
				t.Fatalf("target flow_instance = %q, want %s", got, parentRoute.FlowInstance)
			}
		})
	}
}

func TestExecutor_FanOutEmitUsesProducerSourceRouteNamespace(t *testing.T) {
	source := sourceWithDeclarativeEmitExternalizationFlows()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fan-out route namespace fixture has no contract bundle")
	}
	source = fanOutSourceWithBundleIdentity(t, bundle)
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:        "component-entity",
		Node:            testFlowExecutableNode(t, "component-scaffold", "component-node"),
		HandlerEventKey: "repo_scaffold.repo_scaffolded",
		Event:           eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-route-namespace"), "repo-scaffold/repo_scaffold.repo_scaffolded", "", "", json.RawMessage(`{"items":[{"id":"a"},{"id":"b"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "component_item",
				Identity:  "component_item.id",
				Emit: runtimecontracts.EmitSpec{
					Event: "component-scaffold/component.scaffolded",
				},
			},
		},
		State: testStateSnapshot("ready", map[string]any{
			"flow_path":            "component-scaffold/component-1",
			"parent_flow_id":       "operating",
			"parent_flow_instance": "operating/opco-1",
			"parent_entity_id":     "opco-entity",
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("trigger transaction created %d eager fan-out emits", got)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 2 {
		t.Fatalf("durable fan-out intent = %#v, want cardinality 2", result.FanOutIntent)
	}
	if got := result.FanOutIntent.Capsule.ProducerSource.Route().FlowInstance; got != "component-scaffold/component-1" {
		t.Fatalf("pinned producer source flow_instance = %q, want component-scaffold/component-1", got)
	}
}

func TestExecutor_ChildPinOutputTargetsStoredParentRoute(t *testing.T) {
	source := sourceWithChildOutputPin()
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	parentRoute := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root/inst-1",
		EntityID:     "parent-ent",
	}
	state := testStateSnapshot("running", map[string]any{
		"flow_path":            "child/inst-1",
		"parent_flow_id":       parentRoute.FlowID,
		"parent_flow_instance": parentRoute.FlowInstance,
		"parent_entity_id":     parentRoute.EntityID,
	}, nil, map[string]map[string]any{})
	state.EntityID = identity.NormalizeEntityID("child-ent")

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "child-ent",
		Node:     testFlowExecutableNode(t, "child", "child-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"child/requested",
			"",
			"",
			json.RawMessage(`{}`),
			0,
			"",
			"",
			events.EnvelopeForSourceRoute(events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "wrong-parent"), "wrong/root"), events.RouteIdentity{
				FlowID:       "wrong",
				FlowInstance: "wrong/root",
				EntityID:     "wrong-parent",
			}),
			time.Time{},
		),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{Event: "child.done"},
		},
		State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 1 {
		t.Fatalf("EmitIntents count = %d, want 1", got)
	}
	if got := result.EmitIntents[0].Event.TargetRoute(); got != parentRoute {
		t.Fatalf("target route = %#v, want %#v", got, parentRoute)
	}
}

func TestExecutor_LoweredConnectEmissionRemainsTargetlessBeforeEventBus(t *testing.T) {
	source := sourceWithChildOutputPinAndRootConnect()
	graph := runtimepinrouting.CompileConnectGraph(source)
	if len(graph.Plans()) != 1 || len(graph.Issues()) != 0 {
		t.Fatalf("compiled plans/issues = %#v/%#v, want one valid child-to-root connect", graph.Plans(), graph.Issues())
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	structuralParent := events.RouteIdentity{FlowID: "root", FlowInstance: "root/run-1", EntityID: "stored-parent"}
	currentOwner := events.RouteIdentity{FlowID: "root", FlowInstance: "root/run-1", EntityID: "current-owner"}
	producerSource, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: "child", FlowInstance: "child/inst-1", EntityID: "child-source",
	})
	if err != nil {
		t.Fatalf("producer source: %v", err)
	}
	state := testStateSnapshot("running", map[string]any{
		"flow_path": "child/inst-1", "parent_flow_id": structuralParent.FlowID,
		"parent_flow_instance": structuralParent.FlowInstance, "parent_entity_id": structuralParent.EntityID,
	}, nil, map[string]map[string]any{})
	state.EntityID = identity.NormalizeEntityID("child-source")
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testFlowExecutableNode(t, "child", "child-node")), Target: events.MustExistingEntityTarget(currentOwner),
	})
	result, err := exec.ExecuteSemanticFixture(ctx, ExecutionRequest{
		EntityID: "child-source", Node: testFlowExecutableNode(t, "child", "child-node"), ProducerSource: producerSource,
		Event: eventtest.RunCreatingRootIngress("evt-connect", "child/requested", "", "", json.RawMessage(`{}`), 0, "", "",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "inbound", FlowInstance: "inbound/one", EntityID: "inbound-owner"}), time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "child/inst-1/child.done"}}, State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emit intents = %#v, want one", result.EmitIntents)
	}
	emitted := result.EmitIntents[0].Event
	if !emitted.TargetRoute().Empty() || len(emitted.TargetRoutes()) != 0 {
		t.Fatalf("lowered-connect emission target = %#v/%#v, want targetless before EventBus", emitted.TargetRoute(), emitted.TargetRoutes())
	}
	if got := emitted.SourceRoute(); got != producerSource.Route() {
		t.Fatalf("emitted source = %#v, want admitted source %#v", got, producerSource.Route())
	}
}

func TestExecutor_NestedStaticOutputUsesExactCurrentDeliveryTarget(t *testing.T) {
	source := sourceWithNestedStaticOutputPin()
	exec, err := NewExecutor(RuntimeDependencies{
		Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	currentOwner := events.RouteIdentity{FlowID: "root", FlowInstance: "root/run-1", EntityID: "current-owner"}
	inboundOwner := events.RouteIdentity{FlowID: "inbound", FlowInstance: "inbound/one", EntityID: "inbound-owner"}
	producerSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: "child", FlowInstance: "root/child", EntityID: "source-owner",
	})
	if err != nil {
		t.Fatalf("producer source: %v", err)
	}
	state := testStateSnapshot("running", map[string]any{"flow_path": "root/child"}, nil, map[string]map[string]any{})
	state.EntityID = identity.NormalizeEntityID(currentOwner.EntityID)
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(testFlowExecutableNode(t, "child", "child-node")), Target: events.MustExistingEntityTarget(currentOwner),
	})
	result, err := exec.ExecuteSemanticFixture(ctx, ExecutionRequest{
		EntityID: identity.NormalizeEntityID(currentOwner.EntityID), Node: testFlowExecutableNode(t, "child", "child-node"), ProducerSource: producerSource,
		Event: eventtest.RunCreatingRootIngress("evt-static", "child/requested", "", "", json.RawMessage(`{}`), 0, "", "",
			events.EnvelopeForTargetRoute(events.EventEnvelope{}, inboundOwner), time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "child.done"}}, State: state,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emit intents = %#v, want one", result.EmitIntents)
	}
	if got := result.EmitIntents[0].Event.TargetRoute(); got != currentOwner {
		t.Fatalf("target = %#v, want exact current delivery %#v", got, currentOwner)
	}
	if currentOwner.EntityID == inboundOwner.EntityID || currentOwner.EntityID == producerSource.Route().EntityID {
		t.Fatal("test identities must be distinguishable")
	}
}

func TestExecutor_NestedStaticOutputRejectsMissingOrEntitylessCurrentDelivery(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "entityless", ctx: runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(testFlowExecutableNode(t, "child", "child-node")),
			Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root/run-1"}),
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := NewExecutor(RuntimeDependencies{
				Source: sourceWithNestedStaticOutputPin(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			producerSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "root/child", EntityID: "source-owner"})
			if err != nil {
				t.Fatalf("producer source: %v", err)
			}
			state := testStateSnapshot("running", map[string]any{"flow_path": "root/child"}, nil, map[string]map[string]any{})
			state.EntityID = identity.NormalizeEntityID("state-owner")
			result, err := exec.ExecuteSemanticFixture(tc.ctx, ExecutionRequest{
				EntityID: "state-owner", Node: testFlowExecutableNode(t, "child", "child-node"), ProducerSource: producerSource,
				Event:   eventtest.RunCreatingRootIngress("evt-static-hostile", "child/requested", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "child.done"}}, State: state,
			})
			if err == nil || !strings.Contains(err.Error(), "target_required_missing") {
				t.Fatalf("Execute error = %v, want target_required_missing", err)
			}
			if len(result.EmitIntents) != 0 {
				t.Fatalf("emit intents = %#v, want none", result.EmitIntents)
			}
		})
	}
}

func TestExecutor_ChildPinOutputRejectsIncompleteStoredParentRoute(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithChildOutputPin(), StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	state := testStateSnapshot("running", map[string]any{
		"flow_path": "child/inst-1", "parent_flow_id": "root", "parent_entity_id": "parent-ent",
	}, nil, map[string]map[string]any{})
	state.EntityID = identity.NormalizeEntityID("child-ent")
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "child-ent", Node: testFlowExecutableNode(t, "child", "child-node"),
		Event:   eventtest.RunCreatingRootIngress("evt-partial-parent", "child/requested", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "child.done"}}, State: state,
	})
	if err == nil || !strings.Contains(err.Error(), "parent_route_incomplete") {
		t.Fatalf("Execute error = %v, want parent_route_incomplete", err)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("emit intents = %#v, want none", result.EmitIntents)
	}
}

func sourceWithChildOutputPin() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:         "child",
			Flow:       "child",
			PackageKey: ".",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.done": {},
		},
		Path: "child",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events:      map[string]runtimecontracts.EventCatalogEntry{"child.done": {}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Paths:    runtimecontracts.FlowContractPaths{PackageKey: "."},
				Children: []runtimecontracts.FlowContractView{child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &child,
			},
		},
	}
	return mustCompileEngineSource(bundle)
}

func sourceWithNestedStaticOutputPin() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "."},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic, Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{
			EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done"}},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"child.done": {}},
		Path:   "root/child",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events:      map[string]runtimecontracts.EventCatalogEntry{"child.done": {}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{PackageKey: "."}, Children: []runtimecontracts.FlowContractView{child}},
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &child},
		},
	}
	return mustCompileEngineSource(bundle)
}

func sourceWithChildOutputPinAndRootConnect() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "."},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate, Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{
			EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done"}},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"child.done": {}},
		Path:   "child",
	}
	rootInput := runtimecontracts.FlowInputEventPin{Event: "child.done"}
	connect := runtimecontracts.FlowPackageConnect{Event: "child.done", From: "child", To: ".", SourceFile: "package.yaml", SourceLine: 1}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Package:     runtimecontracts.ProjectPackageDocument{Name: "engine-test", Version: "1.0.0", Connect: []runtimecontracts.FlowPackageConnect{connect}},
		PackageTree: []runtimecontracts.LoadedProjectPackage{{Key: ".", Paths: runtimecontracts.ProjectPackagePaths{PackageFile: "package.yaml"}, Manifest: runtimecontracts.ProjectPackageDocument{Connect: []runtimecontracts.FlowPackageConnect{connect}}}},
		RootSchema:  &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{rootInput}}}},
		Events:      map[string]runtimecontracts.EventCatalogEntry{"child.done": {}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{PackageKey: "."}, Children: []runtimecontracts.FlowContractView{child}},
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &child},
		},
	}
	return mustCompileEngineSource(bundle)
}

func TestExecutor_DataAccumulationTargetPathWritesNestedEntityLeaf(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"summary":"ready"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					SourceField:   "summary",
					TargetPathRef: "entity.analysis.summary",
				}},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"analysis": map[string]any{
				"summary":      "stale",
				"report_count": 2,
			},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	analysis, ok := result.StateMutation.Fields["analysis"].(map[string]any)
	if !ok {
		t.Fatalf("analysis = %#v", result.StateMutation.Fields["analysis"])
	}
	if got := analysis["summary"]; got != "ready" {
		t.Fatalf("analysis.summary = %#v, want ready", got)
	}
	if got := analysis["report_count"]; got != 2 {
		t.Fatalf("analysis.report_count = %#v, want 2", got)
	}
}

func TestExecutor_DataAccumulationAppliesTypedContainedOperations(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "node-1"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"job.received",
			"",
			"",
			json.RawMessage(`{"vertical_id":"north","job":{"id":"job-1","title":"Build"}}`),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{
						Operation: runtimecontracts.WorkflowDataOperationSet,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("north"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status":      "active",
							"active_jobs": []any{},
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationMerge,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("north"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status": "busy",
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.verticals.active_jobs",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value:     runtimecontracts.RefExpression("payload.job"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationUpdate,
						TargetRef: "entity.tags",
						Index:     runtimecontracts.LiteralExpression(1),
						Value:     runtimecontracts.LiteralExpression("gold"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.tags",
						Value:     runtimecontracts.LiteralExpression("vip"),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationDelete,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.LiteralExpression("obsolete"),
					},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"verticals": map[string]any{
				"obsolete": map[string]any{"status": "old", "active_jobs": []any{}},
			},
			"tags": []any{"new", "silver"},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	verticals, ok := result.StateMutation.Fields["verticals"].(map[string]any)
	if !ok {
		t.Fatalf("verticals = %#v", result.StateMutation.Fields["verticals"])
	}
	if _, exists := verticals["obsolete"]; exists {
		t.Fatalf("obsolete key survived delete: %#v", verticals)
	}
	north, ok := verticals["north"].(map[string]any)
	if !ok {
		t.Fatalf("verticals.north = %#v", verticals["north"])
	}
	if got := north["status"]; got != "busy" {
		t.Fatalf("verticals.north.status = %#v, want busy", got)
	}
	jobs, ok := north["active_jobs"].([]any)
	if !ok || len(jobs) != 1 {
		t.Fatalf("verticals.north.active_jobs = %#v", north["active_jobs"])
	}
	job, ok := jobs[0].(map[string]any)
	if !ok || job["id"] != "job-1" || job["title"] != "Build" {
		t.Fatalf("active job = %#v", jobs[0])
	}
	if !reflect.DeepEqual(result.StateMutation.Fields["tags"], []any{"new", "gold", "vip"}) {
		t.Fatalf("tags = %#v", result.StateMutation.Fields["tags"])
	}
}

func TestExecutor_SingletonCoordinatorAppliesContainedStateThroughLoadedContract(t *testing.T) {
	bundle := loadEngineSingletonCoordinatorFlowBundle(t)
	if _, err := bundle.ResolveFlowSingletonCoordinator("coordinator"); err != nil {
		t.Fatalf("ResolveFlowSingletonCoordinator: %v", err)
	}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        semanticview.Wrap(bundle),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}

	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "coordinator-1",
		Node:     testFlowExecutableNode(t, "coordinator", "coordinator-node"),
		Event: eventtest.RunCreatingRootIngress(
			"evt-1",
			"job.received",
			"",
			"",
			json.RawMessage(`{"vertical_id":"north","job":{"id":"job-1","title":"Build"}}`),
			0,
			"",
			"",
			events.EventEnvelope{},
			time.Time{},
		),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{
					{
						Operation: runtimecontracts.WorkflowDataOperationSet,
						TargetRef: "entity.verticals",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value: runtimecontracts.LiteralExpression(map[string]any{
							"status":      "active",
							"active_jobs": []any{},
						}),
					},
					{
						Operation: runtimecontracts.WorkflowDataOperationAppend,
						TargetRef: "entity.verticals.active_jobs",
						Key:       runtimecontracts.RefExpression("payload.vertical_id"),
						Value:     runtimecontracts.RefExpression("payload.job"),
					},
				},
			},
		},
		State: testStateSnapshot("active", map[string]any{
			"verticals": map[string]any{},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	verticals, ok := result.StateMutation.Fields["verticals"].(map[string]any)
	if !ok {
		t.Fatalf("verticals = %#v", result.StateMutation.Fields["verticals"])
	}
	north, ok := verticals["north"].(map[string]any)
	if !ok {
		t.Fatalf("verticals.north = %#v", verticals["north"])
	}
	if got := north["status"]; got != "active" {
		t.Fatalf("verticals.north.status = %#v, want active", got)
	}
	jobs, ok := north["active_jobs"].([]any)
	if !ok || len(jobs) != 1 {
		t.Fatalf("verticals.north.active_jobs = %#v", north["active_jobs"])
	}
	job, ok := jobs[0].(map[string]any)
	if !ok || job["id"] != "job-1" || job["title"] != "Build" {
		t.Fatalf("active job = %#v", jobs[0])
	}
}

func TestExecutor_DataAccumulationContainedOperationRejectsMissingMapKey(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "job.received", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					Operation: runtimecontracts.WorkflowDataOperationDelete,
					TargetRef: "entity.verticals",
					Key:       runtimecontracts.LiteralExpression("missing"),
				}},
			},
		},
		State: testStateSnapshot("pending", map[string]any{
			"verticals": map[string]any{},
		}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected missing map key failure")
	}
	if !strings.Contains(err.Error(), `map key "missing" does not exist`) {
		t.Fatalf("error = %v, want missing-key context", err)
	}
}

func TestExecutor_DataAccumulationRejectsContainedSetOrMergeIndex(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	tests := []struct {
		name string
		op   runtimecontracts.WorkflowDataOperation
	}{
		{name: "set", op: runtimecontracts.WorkflowDataOperationSet},
		{name: "merge", op: runtimecontracts.WorkflowDataOperationMerge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1",
				Node:     testRootExecutableNode(t, "node-1"),
				Event:    eventtest.RunCreatingRootIngress("evt-1", "job.received", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler: runtimecontracts.SystemNodeEventHandler{
					DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
						Writes: []runtimecontracts.WorkflowDataWrite{{
							Operation: tc.op,
							TargetRef: "entity.verticals",
							Key:       runtimecontracts.LiteralExpression("north"),
							Index:     runtimecontracts.LiteralExpression(0),
							Value: runtimecontracts.LiteralExpression(map[string]any{
								"status": "active",
							}),
						}},
					},
				},
				State: testStateSnapshot("pending", map[string]any{
					"verticals": map[string]any{},
				}, nil, map[string]map[string]any{}),
			})
			if err == nil {
				t.Fatal("expected contained operation index rejection")
			}
			if !strings.Contains(err.Error(), "must not declare index") {
				t.Fatalf("error = %v, want index rejection", err)
			}
		})
	}
}

func TestExecutor_RejectsUndeclaredNestedEntityWriteBeforeExecution(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpCount,
				StoreAs:   "entity.analysis.missing",
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected invalid config error")
	}
	if !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("error = %v, want invalid config", err)
	}
	if !strings.Contains(err.Error(), "entity.analysis.missing") {
		t.Fatalf("error = %v, want target path context", err)
	}
}

func TestExecutor_ClearRemovesNestedEntityLeaf(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testRootExecutableNode(t, "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"entity.analysis.summary"}},
		},
		State: testStateSnapshot("pending", map[string]any{
			"analysis": map[string]any{
				"summary":      "stale",
				"report_count": 2,
			},
		}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	analysis, ok := result.StateMutation.Fields["analysis"].(map[string]any)
	if !ok {
		t.Fatalf("analysis = %#v", result.StateMutation.Fields["analysis"])
	}
	if _, exists := analysis["summary"]; exists {
		t.Fatalf("analysis.summary unexpectedly present: %#v", analysis)
	}
	if got := analysis["report_count"]; got != 2 {
		t.Fatalf("analysis.report_count = %#v, want 2", got)
	}
}

func TestExecutor_ClearSpecialTargetsBypassContractValidation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSourceWithRootEntityContract(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	node := testFlowExecutableNode(t, "root", "node-1")
	initial := testStateSnapshot("pending", map[string]any{
		"dedup_key":         "dup-1",
		"accumulated_total": 5,
		"received_items":    []any{"a"},
	}, nil, map[string]map[string]any{
		node.Key(): {
			handlerAccumulatorBucketKey: map[string]any{"items": []any{"a"}},
		},
	})
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     node,
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Clear: &runtimecontracts.ClearSpec{Targets: []string{"pending_dedup", "accumulator_state"}},
		},
		State: initial,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if _, ok := result.StateMutation.Fields["dedup_key"]; ok {
		t.Fatalf("expected dedup_key to be cleared, metadata=%#v", result.StateMutation.Fields)
	}
	if _, ok := result.StateMutation.Fields["received_items"]; ok {
		t.Fatalf("expected received_items to be cleared, metadata=%#v", result.StateMutation.Fields)
	}
	if nodeBucket, ok := result.StateMutation.StateBuckets[node.Key()]; ok {
		if _, ok := nodeBucket[handlerAccumulatorBucketKey]; ok {
			t.Fatalf("expected accumulator bucket to be cleared, state_buckets=%#v", result.StateMutation.StateBuckets)
		}
	}
}

func TestExecutor_EmitFieldsCELFailureReturnsError(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	_, err = exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "vertical-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event: eventtest.RunCreatingRootIngress("evt-1",
			"vertical.discovered", "", "", json.RawMessage(`{"mode":"corpus"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),

		Handler: runtimecontracts.SystemNodeEventHandler{
			Emit: runtimecontracts.EmitSpec{
				Event: "scoring.requested",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"missing": runtimecontracts.CELExpression("payload.discovery_context.source +"),
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatal("expected emit.fields CEL failure to return an error")
	}
}

func TestExecutor_FanOutEmptyPersistsCountAndContinues(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutPayloadSource(t, "task.completed"),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-empty"), "task.completed", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "fan_item",
				Identity:  "fan_item",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
			},
			AdvancesTo: "scanning",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeFannedOut {
		t.Fatalf("Status = %q", result.Status)
	}
	if result.NextState != "scanning" {
		t.Fatalf("NextState = %q", result.NextState)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 0 {
		t.Fatalf("empty fan-out intent = %#v, want closed cardinality 0", result.FanOutIntent)
	}
	if _, found := result.StateMutation.Bookkeeping["fan_out_count"]; found {
		t.Fatalf("retired fan_out_count metadata survived: %#v", result.StateMutation.Bookkeeping)
	}
}

func TestExecutor_FanOutDoesNotPersistHiddenCountInEntityBookkeeping(t *testing.T) {
	source := stubSourceWithRootEntityContract()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fan-out hidden-count fixture has no contract bundle")
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"task.completed": {
			Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"items": {Type: "[json]"},
			}},
		},
	}
	source = fanOutSourceWithBundleIdentity(t, bundle)
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        source,
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "root", "node-1"),
		Event:    eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-no-hidden-count"), "task.completed", "", "", json.RawMessage(`{"items":[]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "fan_item",
				Identity:  "fan_item",
				Emit:      runtimecontracts.EmitSpec{Event: "item.process"},
			},
			AdvancesTo: "scanning",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 0 {
		t.Fatalf("empty fan-out intent = %#v, want cardinality 0", result.FanOutIntent)
	}
	if _, found := result.StateMutation.Bookkeeping["fan_out_count"]; found {
		t.Fatalf("retired fan_out_count metadata survived: %#v", result.StateMutation.Bookkeeping)
	}
}

func TestExecutor_FanOutUsesExplicitEmitEvent(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        fanOutPayloadSource(t, "batch.submitted"),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: stubPayloadShaper{},
		MaxChainDepth: 5,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress(eventtest.UUID("fan-out-explicit-event"), "batch.submitted", "", "", json.RawMessage(`{"items":[{"kind":"a"},{"kind":"b"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			FanOut: &runtimecontracts.FanOutSpec{
				ItemsFrom: "payload.items",
				As:        "routed_item",
				Identity:  "routed_item.kind",
				Emit:      runtimecontracts.EmitSpec{Event: "routed.item"},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := len(result.EmitIntents); got != 0 {
		t.Fatalf("trigger transaction created %d eager fan-out emits", got)
	}
	if result.FanOutIntent == nil || result.FanOutIntent.Cardinality != 2 || result.FanOutIntent.PlanRef.SemanticDigest == "" {
		t.Fatalf("explicit-event fan-out intent = %#v", result.FanOutIntent)
	}
}

func TestExecutor_GuardKillTransitionsToKilledStateWhenDeclared(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithKilledState(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{"payload.score >= policy.threshold": false}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "check.requested", "", "", json.RawMessage(`{"score":50}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "payload.score >= policy.threshold",
				OnFail: "kill",
			},
			AdvancesTo: "done",
			Emit:       runtimecontracts.EmitSpec{Event: "check.passed"},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.Status; got != OutcomeKilled {
		t.Fatalf("Status = %q", got)
	}
	if got := result.NextState; got != "killed" {
		t.Fatalf("NextState = %q", got)
	}
	if got := result.StateMutation.NextState; got != "killed" {
		t.Fatalf("StateMutation.NextState = %q", got)
	}
}

func TestExecutor_GroupByStoresGroupedItems(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(nil),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			GroupBy: &runtimecontracts.GroupBySpec{
				ItemsFrom: "payload.items",
				Key:       "category",
				StoreAs:   "entity.grouped",
			},
			AdvancesTo: "done",
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := result.StateMutation.Fields["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped metadata = %#v", result.StateMutation.Fields["grouped"])
	}
	xItems, _ := grouped["x"].([]any)
	yItems, _ := grouped["y"].([]any)
	if len(xItems) != 2 || len(yItems) != 1 {
		t.Fatalf("grouped metadata = %#v", grouped)
	}
	if result.NextState != "done" {
		t.Fatalf("NextState = %q", result.NextState)
	}
}

func TestExecutor_GroupByBareKeyUsesItemScopeWithoutFallbackAcrossRoots(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        sourceWithPolicy(map[string]any{"category": "policy"}),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "items.submitted", "", "", json.RawMessage(`{"category":"payload","items":[{"name":"a","category":"x"},{"name":"b","category":"y"},{"name":"c","category":"x"}]}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			GroupBy: &runtimecontracts.GroupBySpec{
				ItemsFrom: "payload.items",
				Key:       "category",
				StoreAs:   "entity.grouped",
			},
			AdvancesTo: "done",
		},
		State: testStateSnapshot("pending", map[string]any{"category": "entity"}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	grouped, ok := result.StateMutation.Fields["grouped"].(map[string]any)
	if !ok {
		t.Fatalf("grouped metadata = %#v", result.StateMutation.Fields["grouped"])
	}
	xItems, _ := grouped["x"].([]any)
	yItems, _ := grouped["y"].([]any)
	if len(xItems) != 2 || len(yItems) != 1 {
		t.Fatalf("grouped metadata = %#v", grouped)
	}
	if _, ok := grouped["payload"]; ok {
		t.Fatalf("grouped metadata unexpectedly used payload scope: %#v", grouped)
	}
	if _, ok := grouped["entity"]; ok {
		t.Fatalf("grouped metadata unexpectedly used entity scope: %#v", grouped)
	}
	if _, ok := grouped["policy"]; ok {
		t.Fatalf("grouped metadata unexpectedly used policy scope: %#v", grouped)
	}
}

func TestExecutor_ClearGatesWildcardUsesNodeGateSchema(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			ClearGates: []string{"*"},
		},
		State: StateSnapshot{
			StateCarrier: NewStateCarrier(map[string]any{"note": "keep"}, map[string]bool{"gate_a": true, "gate_b": true}, nil),
		},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.ClearGates; !reflect.DeepEqual(got, []string{"gate_a", "gate_b"}) {
		t.Fatalf("ClearGates = %#v", got)
	}
	if result.StateMutation.Gates["gate_a"] != false || result.StateMutation.Gates["gate_b"] != false {
		t.Fatalf("typed gates not cleared: %#v", result.StateMutation.Gates)
	}
	if result.StateMutation.Gates["gate_a"] != false || result.StateMutation.Gates["gate_b"] != false {
		t.Fatalf("typed gate state not cleared: %#v", result.StateMutation.Gates)
	}
	if result.StateMutation.Fields["note"] != "keep" {
		t.Fatalf("non-gate metadata changed: %#v", result.StateMutation.Fields)
	}
}

func TestExecutor_ClearGatesRunsBeforeGuardEvaluation(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
	}, stubEvaluator{bools: map[string]bool{
		"_entity.gates.review == false": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			ClearGates: []string{"review"},
			Guard: &runtimecontracts.GuardSpec{
				Check: "_entity.gates.review == false",
			},
		},
		State: StateSnapshot{StateCarrier: NewStateCarrier(nil, map[string]bool{"review": true}, nil)},
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeCompleted {
		t.Fatalf("Status = %q", result.Status)
	}
}

func TestExecutor_ActionRegistryEmitsAndRunsActionRunner(t *testing.T) {
	runner := &stubActionRunner{}
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("notify"): {
				Key:   identity.NormalizeActionKey("notify"),
				Emits: "action.emitted",
			},
		}},
		ActionRunner:  runner,
		PayloadShaper: shaper,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Action: runtimecontracts.ActionSpec{ID: "notify"},
		},
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := runner.called; !reflect.DeepEqual(got, []string{"notify"}) {
		t.Fatalf("action runner calls = %#v", got)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{"notify"}) {
		t.Fatalf("ActionsExecuted = %#v", got)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "action.emitted" {
		t.Fatalf("unexpected action emit intents: %#v", result.EmitIntents)
	}
	if got := shaper.lastPayload["score"]; got != float64(9) {
		t.Fatalf("action emit payload score = %#v, want 9", got)
	}
	if shaper.lastSurface != EmitSurfaceAction {
		t.Fatalf("action emit surface = %q, want %q", shaper.lastSurface, EmitSurfaceAction)
	}
}

func TestExecutor_RuleActionRunsOnlyForSelectedRule(t *testing.T) {
	runner := &stubActionRunner{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("auto_action"): {
				Key: identity.NormalizeActionKey("auto_action"),
			},
			identity.NormalizeActionKey("human_action"): {
				Key: identity.NormalizeActionKey("human_action"),
			},
		}},
		ActionRunner: runner,
	}, stubEvaluator{bools: map[string]bool{
		"payload.amount < 100":  false,
		"payload.amount >= 100": true,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "refund.requested", "", "", json.RawMessage(`{"amount":250}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Rules: []runtimecontracts.HandlerRuleEntry{
				{
					ID:        "auto",
					Condition: "payload.amount < 100",
					Action:    runtimecontracts.ActionSpec{ID: "auto_action"},
				},
				{
					ID:        "needs-human",
					Condition: "payload.amount >= 100",
					Action:    runtimecontracts.ActionSpec{ID: "human_action"},
				},
			},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := result.HandlerRuleSelection.DisplayLabel(); got != "needs-human" {
		t.Fatalf("RuleID = %q, want needs-human", got)
	}
	if got := runner.called; !reflect.DeepEqual(got, []string{"human_action"}) {
		t.Fatalf("action runner calls = %#v, want only selected rule action", got)
	}
	if got := result.ActionsExecuted; !reflect.DeepEqual(got, []string{"human_action"}) {
		t.Fatalf("ActionsExecuted = %#v, want only selected rule action", got)
	}
}

func TestExecutor_RejectsAmbiguousHandlerTopLevelActionWithRules(t *testing.T) {
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("handler_action"): {
				Key: identity.NormalizeActionKey("handler_action"),
			},
		}},
		ActionRunner: &stubActionRunner{},
	}, stubEvaluator{bools: map[string]bool{"payload.amount >= 100": true}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "refund.requested", "", "", json.RawMessage(`{"amount":250}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Action: runtimecontracts.ActionSpec{ID: "handler_action"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:        "needs-human",
				Condition: "payload.amount >= 100",
			}},
		},
		State: testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err == nil {
		t.Fatalf("expected ambiguous handler-level action config to be rejected, got %+v", result)
	}
	if !strings.Contains(err.Error(), "handler-top-level action is only allowed on handlers without rules") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecutor_RejectsUnsupportedRuleActionContextsBeforeExecution(t *testing.T) {
	cases := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    string
	}{
		{
			name: "on_complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID:        "complete",
					Condition: "else",
					Action:    runtimecontracts.ActionSpec{ID: "notify"},
				}},
			},
			want: "handler.on_complete[complete] action is unsupported",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := &stubActionRunner{}
			exec, err := NewExecutor(RuntimeDependencies{
				Source:        stubSource(),
				StateRepo:     stubStateRepo{},
				MutationOwner: stubMutationOwner{},
				Locker:        stubLocker{},
				Dispatcher:    stubDispatcher{},
				ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
					identity.NormalizeActionKey("notify"): {
						Key: identity.NormalizeActionKey("notify"),
					},
				}},
				ActionRunner: runner,
			}, stubEvaluator{bools: map[string]bool{"payload.ok": true}})
			if err != nil {
				t.Fatalf("NewExecutor error: %v", err)
			}
			result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
				EntityID: "entity-1",
				Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
				Handler:  tc.handler,
				State:    testStateSnapshot("pending", map[string]any{}, nil, map[string]map[string]any{}),
			})
			if err == nil {
				t.Fatalf("expected unsupported action context rejection, got result %+v", result)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if len(runner.called) != 0 {
				t.Fatalf("action runner calls = %#v, want none", runner.called)
			}
			if len(result.ActionsExecuted) != 0 {
				t.Fatalf("ActionsExecuted = %#v, want none", result.ActionsExecuted)
			}
		})
	}
}

func TestSelectedActionSpecConsumesRuleActionOnlyFromHandlerRules(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{Action: runtimecontracts.ActionSpec{ID: "handler_action"}}
	rule := &runtimecontracts.HandlerRuleEntry{Action: runtimecontracts.ActionSpec{ID: "rule_action"}}
	cases := []struct {
		name   string
		source handlerRuleSource
		want   string
	}{
		{name: "handler rules", source: handlerRuleSourceRules, want: "rule_action"},
		{name: "on complete", source: handlerRuleSourceOnComplete, want: "handler_action"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectedActionSpec(handler, rule, tc.source).ID; got != tc.want {
				t.Fatalf("selectedActionSpec = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExecutor_MergeActionStatePreservesInMemoryWrites(t *testing.T) {
	entityID := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	baseline := testStateSnapshot("ready", map[string]any{
		"same":           "unchanged",
		"in_memory_only": "frame-write",
	}, map[string]bool{
		"g_frame": true,
	}, map[string]map[string]any{
		"bucket": {"in_memory_only": "frame-write"},
	})
	baseline.StateCarrier.Bookkeeping = map[string]any{
		"in_memory_only": "frame-write",
	}
	projected := testStateSnapshot("ready", map[string]any{
		"same":          "unchanged",
		"action_output": "persisted-output",
	}, nil, map[string]map[string]any{
		"bucket": {"action_output": "persisted-output"},
	})
	projected.StateCarrier.Bookkeeping = map[string]any{
		"action_output": "persisted-output",
	}
	exec := &Executor{}
	frame := &executionFrame{
		ctx: context.Background(),
		req: ExecutionRequest{EntityID: entityID},
		state: ExecutionState{
			State: baseline,
		},
	}

	mutation := StateMutation{StateCarrier: projected.StateCarrier}
	if err := exec.mergeActionState(frame, baseline, &mutation); err != nil {
		t.Fatalf("mergeActionState: %v", err)
	}

	if got := frame.state.State.StateCarrier.Fields["in_memory_only"]; got != "frame-write" {
		t.Fatalf("in_memory_only = %#v, want preserved frame-write", got)
	}
	if got := frame.state.State.StateCarrier.Fields["action_output"]; got != "persisted-output" {
		t.Fatalf("action_output = %#v, want persisted-output", got)
	}
	if got := frame.state.State.StateCarrier.Bookkeeping["in_memory_only"]; got != "frame-write" {
		t.Fatalf("bookkeeping in_memory_only = %#v, want preserved frame-write", got)
	}
	if got := frame.state.State.StateCarrier.Bookkeeping["action_output"]; got != "persisted-output" {
		t.Fatalf("bookkeeping action_output = %#v, want persisted-output", got)
	}
	if got := frame.state.State.StateCarrier.StateBuckets["bucket"]["in_memory_only"]; got != "frame-write" {
		t.Fatalf("bucket in_memory_only = %#v, want preserved frame-write", got)
	}
	if got := frame.state.State.StateCarrier.StateBuckets["bucket"]["action_output"]; got != "persisted-output" {
		t.Fatalf("bucket action_output = %#v, want persisted-output", got)
	}
}

func TestExecutor_ActionRegistryEmitContractViolationRejectsHandler(t *testing.T) {
	runner := &stubActionRunner{}
	shaper := &recordingPayloadShaper{err: errors.Join(ErrEmitPayloadContractViolation, errors.New("wrapped payload contract failure"))}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		ActionRegistry: stubActionRegistry{entries: map[identity.ActionKey]runtimeregistry.ActionInstruction{
			identity.NormalizeActionKey("notify"): {
				Key:   identity.NormalizeActionKey("notify"),
				Emits: "action.emitted",
			},
		}},
		ActionRunner:  runner,
		PayloadShaper: shaper,
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID: "entity-1",
		Node:     testFlowExecutableNode(t, "flow-1", "node-1"),
		Event:    eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"score":9}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Action: runtimecontracts.ActionSpec{ID: "notify"},
		},
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if !errors.Is(err, ErrEmitPayloadContractViolation) {
		t.Fatalf("Execute error = %v, want %v", err, ErrEmitPayloadContractViolation)
	}
	if result.Status != OutcomeRejected {
		t.Fatalf("Status = %q, want %q", result.Status, OutcomeRejected)
	}
	if result.Failure == nil || result.Failure.Class != failures.ClassSchemaInvalid || result.FailureDisposition != FailureDispositionTerminal {
		t.Fatalf("failure = %#v disposition=%q", result.Failure, result.FailureDisposition)
	}
	if len(result.EmitIntents) != 0 {
		t.Fatalf("EmitIntents = %#v, want none", result.EmitIntents)
	}
	if len(result.ActionsExecuted) != 0 {
		t.Fatalf("ActionsExecuted = %#v, want none", result.ActionsExecuted)
	}
	if len(runner.called) != 0 {
		t.Fatalf("action runner calls = %#v, want none", runner.called)
	}
	if shaper.lastSurface != EmitSurfaceAction {
		t.Fatalf("action emit surface = %q, want %q", shaper.lastSurface, EmitSurfaceAction)
	}
}

func TestExecutor_GuardOnFailEscalateCreatesEmitIntent(t *testing.T) {
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source:        stubSource(),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{
		"payload.ok == true": false,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":false}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check:  "payload.ok == true",
				OnFail: "escalate:guard.failed",
			},
		},
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeEscalated {
		t.Fatalf("Status = %q", result.Status)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "guard.failed" {
		t.Fatalf("unexpected escalation intents: %#v", result.EmitIntents)
	}
	if result.ChainDepth != 2 {
		t.Fatalf("ChainDepth = %d", result.ChainDepth)
	}
	if len(shaper.lastPayload) != 0 {
		t.Fatalf("guard escalation payload = %#v, want empty explicit business payload", shaper.lastPayload)
	}
}

func TestExecutor_GuardOnFailEscalateObjectFieldsShapeExplicitPayload(t *testing.T) {
	shaper := &recordingPayloadShaper{}
	exec, err := NewExecutor(RuntimeDependencies{
		Source: sourceWithEvents(map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"ok": {Type: "boolean"}, "score": {Type: "integer"}, "legacy": {Type: "text"},
			}),
			"guard.failed": requiredEventPayload(map[string]runtimecontracts.EventFieldSpec{
				"score": {Type: "integer"}, "reason": {Type: "text"},
			}),
		}),
		StateRepo:     stubStateRepo{},
		MutationOwner: stubMutationOwner{},
		Locker:        stubLocker{},
		Dispatcher:    stubDispatcher{},
		PayloadShaper: shaper,
		MaxChainDepth: 5,
	}, stubEvaluator{bools: map[string]bool{
		"payload.ok == true": false,
	}})
	if err != nil {
		t.Fatalf("NewExecutor error: %v", err)
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{
		EntityID:   "entity-1",
		Node:       testFlowExecutableNode(t, "flow-1", "node-1"),
		ChainDepth: 1,
		Event:      eventtest.RunCreatingRootIngress("evt-1", "task.completed", "", "", json.RawMessage(`{"ok":false,"score":42,"legacy":"should-not-pass"}`), 0, "", "", events.EventEnvelope{}, time.Time{}),
		Handler: runtimecontracts.SystemNodeEventHandler{
			Guard: &runtimecontracts.GuardSpec{
				Check: "payload.ok == true",
				OnFailSpec: runtimecontracts.GuardFailureSpec{
					Action: runtimecontracts.GuardFailureActionEscalate,
					Escalation: runtimecontracts.EmitSpec{
						Event: "guard.failed",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"score":  runtimecontracts.CELExpression("payload.score"),
							"reason": runtimecontracts.CELExpression(`"score_below_threshold"`),
						},
					},
				},
			},
		},
		State: testStateSnapshot("", map[string]any{}, nil, map[string]map[string]any{}),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if result.Status != OutcomeEscalated {
		t.Fatalf("Status = %q", result.Status)
	}
	if len(result.EmitIntents) != 1 || string(result.EmitIntents[0].Event.Type()) != "guard.failed" {
		t.Fatalf("unexpected escalation intents: %#v", result.EmitIntents)
	}
	if got := asInt(shaper.lastPayload["score"]); got != 42 {
		t.Fatalf("guard escalation score payload = %#v, want 42", shaper.lastPayload["score"])
	}
	if got := shaper.lastPayload["reason"]; got != "score_below_threshold" {
		t.Fatalf("guard escalation reason payload = %#v, want score_below_threshold", got)
	}
	if _, ok := shaper.lastPayload["legacy"]; ok {
		t.Fatalf("guard escalation leaked unmapped trigger payload: %#v", shaper.lastPayload)
	}
}

func loadEngineProjectionFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: projection-flow
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: projection-flow\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
initial_state: pending
states: [pending, scored]
terminal_states: [scored]
pins:
  inputs:
    events:
      - score.dimension_complete
  outputs:
    events:
      - event: vertical.scored
        sink: harness
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "types.yaml"), `
types:
  DimensionScore:
    dimension: text
    score: integer
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
vertical:
  scores:
    type: "[DimensionScore]"
    initial: []
    materialize_from: scoring-node.dimensions_received
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), `
score.dimension_complete:
  dimension: text
  score: integer
vertical.scored:
  scores: "[DimensionScore]"
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), `
scoring-node:
  id: scoring-node
  execution_type: system_node
  event_handlers:
    score.dimension_complete:
      accumulate:
        into: dimensions_received
        dedup_by: payload.dimension
      emit:
        event: vertical.scored
        fields:
          scores: entity.scores
  state_schema:
    fields:
      dimensions_received: "[DimensionScore]"
`)

	repoRoot := repoRootForEngineProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadEngineSingletonCoordinatorFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: singleton-coordinator-runtime
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: singleton-coordinator-runtime\n")
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), `
name: coordinator
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - job.received
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), `
types:
  VerticalState:
    status: text
    active_jobs: "[Job]"
  Job:
    id: text
    title: text
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), `
coordinator_state:
  verticals: map[text]VerticalState
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
job.received:
  vertical_id: text
  job: Job
`)
	writeEngineProjectionFixtureFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  event_handlers: {}
`)

	repoRoot := repoRootForEngineProjectionTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writeEngineProjectionFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func repoRootForEngineProjectionTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
