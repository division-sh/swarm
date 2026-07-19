package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/platform"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestExecuteWithPersistedComputeModuleReplayEvidenceLoadsAndFailsClosedOnStoredDivergence(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	sqliteStore := newComputeModuleReplaySQLiteStore(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES (?, 'running', ?, ?, ?, ?)
	`, runID, authorActivityTestBundleSourceFact.BundleHash, authorActivityTestBundleSourceFact.BundleSource, authorActivityTestBundleSourceFact.BundleFingerprint, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}

	source := computeModuleReplaySource(t)
	exec := newComputeModuleReplayExecutor(t, source)
	req := computeModuleReplayExecutionRequest(t)
	first, err := exec.ExecuteSemanticFixture(ctx, req)
	if err != nil {
		t.Fatalf("initial Execute: %v", err)
	}
	if len(first.ComputeModuleTraces) != 1 {
		t.Fatalf("initial traces = %#v, want one", first.ComputeModuleTraces)
	}

	unrelated := first.ComputeModuleTraces[0]
	unrelated.ModuleID = "unrelated_renderer"
	persistComputeModuleReplayEvidenceForExecution(t, ctx, sqliteStore, "evt-unrelated", "other-node", unrelated)

	persisted := first.ComputeModuleTraces[0]
	persisted.OutputHash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	persistComputeModuleReplayEvidenceForExecution(t, ctx, sqliteStore, req.Event.ID(), string(req.NodeID), persisted)

	loaded, err := sqliteStore.LoadComputeModuleReplayEvidenceForExecution(ctx, runID, req.Event.ID(), string(req.NodeID))
	if err != nil {
		t.Fatalf("LoadComputeModuleReplayEvidenceForExecution: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Normalized() != persisted.Normalized() {
		t.Fatalf("scoped replay evidence = %#v, want only matching envelope %#v", loaded, persisted.Normalized())
	}

	_, err = exec.ExecuteWithPersistedComputeModuleReplayEvidence(ctx, sqliteStore, runID, req)
	if err == nil {
		t.Fatal("persisted replay Execute error = nil, want result divergence")
	}
	var moduleErr *computemodule.Error
	if !errors.As(err, &moduleErr) || moduleErr.Code != computemodule.CodeReplay {
		t.Fatalf("persisted replay error = %#v, want compute_module replay error", err)
	}
	if moduleErr.Finding == nil ||
		moduleErr.Finding.Kind != computemodule.ReplayFindingResultDivergence ||
		moduleErr.Finding.Field != "output_hash" {
		t.Fatalf("persisted replay finding = %#v, want result divergence on output_hash", moduleErr.Finding)
	}
}

func persistComputeModuleReplayEvidenceForExecution(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, eventID, nodeID string, envelope computemodule.ReplayEnvelope) {
	t.Helper()
	detail := computemodule.NewReplayEvidenceDetail([]computemodule.ReplayEnvelope{envelope})
	detail["node_id"] = nodeID
	logger := runtimepkg.NewRuntimeLogger(sqliteStore)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Message:   "Compute module replay evidence recorded",
		Component: "compute_module",
		Action:    computemodule.ReplayEvidenceAction,
		EventID:   eventID,
		Detail:    detail,
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log persisted replay evidence: %v", err)
	}
}

func newComputeModuleReplaySQLiteStore(t *testing.T) *store.SQLiteRuntimeStore {
	t.Helper()
	var spec runtimecontracts.PlatformSpecDocument
	source, err := yamlsource.Load(platform.PlatformSpecYAML())
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	if err := source.Decode(&spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	sqliteStore, err := store.NewSQLiteRuntimeStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite runtime store: %v", err)
		}
	})
	if err := sqliteStore.BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        store.RuntimeStoreOrigin{SwarmVersion: "engine-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	return sqliteStore
}

func computeModuleReplaySource(t *testing.T) semanticview.Source {
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
		ABI:    computemodule.ABI,
		Entry:  computemodule.DefaultEntry,
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
	return semanticview.Wrap(bundle)
}

func newComputeModuleReplayExecutor(t *testing.T, source semanticview.Source) *runtimeengine.Executor {
	t.Helper()
	exec, err := runtimeengine.NewExecutor(runtimeengine.RuntimeDependencies{
		Source:     source,
		StateRepo:  replayStateRepo{},
		TxRunner:   replayTxRunner{},
		Locker:     replayLocker{},
		Outbox:     replayOutbox{},
		Dispatcher: replayDispatcher{},
	}, nil)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}
	return exec
}

func computeModuleReplayExecutionRequest(t *testing.T) runtimeengine.ExecutionRequest {
	t.Helper()
	return runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
		NodeID:   identity.NormalizeNodeID("render-node"),
		FlowID:   identity.NormalizeFlowID("render"),
		Event: eventtest.RootIngress(
			"evt-1",
			events.EventType("render.requested"),
			"",
			"",
			mustComputeModuleReplayJSON(t, map[string]any{
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
		Handler: runtimecontracts.SystemNodeEventHandler{
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpModule,
				StoreAs:   "computed.rendered_bundle",
				Module: &runtimecontracts.ComputeModuleSpec{
					RowID:  "render_bundle",
					Module: "structured_renderer",
					Into:   "computed.rendered_bundle",
					Input: map[string]string{
						"component": "payload.component",
						"owner":     "payload.owner",
						"language":  "payload.language",
						"files":     "payload.files",
					},
				},
			},
		},
		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111"),
			CurrentState: "pending",
		},
	}
}

func mustComputeModuleReplayJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return raw
}

type replayStateRepo struct{}

func (replayStateRepo) LoadState(context.Context, identity.EntityID) (runtimeengine.StateSnapshot, bool, error) {
	return runtimeengine.StateSnapshot{}, false, nil
}

func (replayStateRepo) SaveState(context.Context, identity.EntityID, runtimeengine.StateMutation) error {
	return nil
}

type replayTxRunner struct{}

func (replayTxRunner) Run(ctx context.Context, fn func(runtimeengine.Tx) error) error {
	return fn(replayTx{ctx: ctx})
}

type replayTx struct {
	ctx context.Context
}

func (tx replayTx) Context() context.Context {
	return tx.ctx
}

type replayLocker struct{}

func (replayLocker) WithEntityLock(ctx context.Context, _ identity.EntityID, fn func(context.Context) error) error {
	return fn(ctx)
}

type replayOutbox struct{}

func (replayOutbox) WriteOutbox(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type replayDispatcher struct{}

func (replayDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}
