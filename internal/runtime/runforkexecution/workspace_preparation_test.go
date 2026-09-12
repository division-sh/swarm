package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

var errWorkspaceConstruction = errors.New("workspace construction rejected")
var errWorkspaceCleanup = errors.New("workspace cleanup rejected")
var errWorkspaceProbeReached = errors.New("initialized workspace probe reached without provider launch")

type workspacePreparationTrace struct {
	mu             sync.Mutex
	mode           string
	real           bool
	steps          []string
	releases       int
	removeFailures int
	repaired       bool
	started        chan struct{}
	cancelled      chan struct{}
	finish         chan struct{}
	projection     string
	containers     []string
	cancel         context.CancelFunc
}

type preparationWorkspace struct {
	workspace.Lifecycle
	trace    *workspacePreparationTrace
	selected bool
}

func (w *preparationWorkspace) RebindSourceProjection(p *sourceartifact.RuntimeProjection, source semanticview.Source) (workspace.Lifecycle, error) {
	bound, err := workspace.RebindSourceProjection(w.Lifecycle, p, source)
	if err != nil {
		return nil, err
	}
	w.trace.projection = p.PrivateRoot()
	return &preparationWorkspace{Lifecycle: bound, trace: w.trace, selected: true}, nil
}

func (w *preparationWorkspace) step(name string) error {
	if !w.selected {
		return errors.New("source workspace lifecycle was used")
	}
	w.trace.mu.Lock()
	defer w.trace.mu.Unlock()
	w.trace.steps = append(w.trace.steps, name)
	return nil
}

func (w *preparationWorkspace) EnsurePrereqs(ctx context.Context) error {
	if err := w.step("prerequisites"); err != nil {
		return err
	}
	if w.trace.mode == "prerequisite_failure" {
		return errWorkspaceConstruction
	}
	if w.trace.mode == "cancel_after_prerequisites" {
		w.trace.cancel()
	}
	if w.trace.real {
		return w.Lifecycle.EnsurePrereqs(ctx)
	}
	return nil
}

func (w *preparationWorkspace) EnsureSystemWorkspaces(ctx context.Context) error {
	if err := w.step("scaffold_created"); err != nil {
		return err
	}
	switch w.trace.mode {
	case "partial_failure", "cleanup_fail_once", "cleanup_persistent":
		if !w.trace.real {
			return errWorkspaceConstruction
		}
	case "cancellation", "retirement":
		if !w.trace.real {
			close(w.trace.started)
			<-ctx.Done()
			close(w.trace.cancelled)
			<-w.trace.finish
			return ctx.Err()
		}
	}
	if w.trace.real {
		if err := w.Lifecycle.EnsureSystemWorkspaces(ctx); err != nil {
			return err
		}
	}
	err := w.step("system_created")
	if w.trace.mode == "cancel_after_system" {
		w.trace.cancel()
	}
	return err
}

func (w *preparationWorkspace) ResolveClaudeWorkspace(ctx context.Context, actor actors.AgentConfig, req workspace.ClaudeStateRequest, head string) (*workspace.Target, error) {
	if err := w.step("probe"); err != nil {
		return nil, err
	}
	if w.trace.real {
		target, err := w.Lifecycle.(workspace.ClaudeWorkspaceResolver).ResolveClaudeWorkspace(ctx, actor, req, head)
		if err != nil {
			return nil, err
		}
		if err := target.ClaudeState.Release(ctx); err != nil {
			return nil, err
		}
		if err := w.step("private_state_released"); err != nil {
			return nil, err
		}
	}
	return nil, errWorkspaceProbeReached
}

func (w *preparationWorkspace) ReleaseSourceProjection(ctx context.Context) error {
	if err := w.step("release"); err != nil {
		return err
	}
	w.trace.mu.Lock()
	w.trace.releases++
	fail := !w.trace.repaired && (w.trace.mode == "cleanup_persistent" || (w.trace.mode == "cleanup_fail_once" && w.trace.releases == 1))
	w.trace.mu.Unlock()
	if fail && !w.trace.real {
		return errWorkspaceCleanup
	}
	return w.Lifecycle.ReleaseSourceProjection(ctx)
}

func TestSelectedWorkspacePreparationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"probe_order", "prerequisite_failure", "partial_failure", "cancellation", "retirement", "cleanup_fail_once", "cleanup_persistent", "no_probe"} {
			t.Run(backend+"/"+mode, func(t *testing.T) { proveSelectedWorkspacePreparation(t, backend, mode, false) })
		}
	}
}

func TestSelectedWorkspaceInitializationBoundaries(t *testing.T) {
	for _, mode := range []string{"cancel_before", "cancel_after_prerequisites", "cancel_after_system", "idempotent", "released"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			trace := &workspacePreparationTrace{mode: mode, cancel: cancel}
			p := &selectedContractWorkspaceProjection{lifecycle: &preparationWorkspace{Lifecycle: workspace.NewHostManager(), trace: trace, selected: true}}
			if mode == "released" {
				if err := p.Release(); err != nil {
					t.Fatal(err)
				}
				if err := p.Initialize(ctx); err == nil || p.initialized {
					t.Fatal("released projection accepted initialization")
				}
				if !reflect.DeepEqual(trace.steps, []string{"release"}) {
					t.Fatalf("released projection performed work: %v", trace.steps)
				}
				return
			}
			if mode == "cancel_before" {
				cancel()
			}
			err := p.Initialize(ctx)
			if mode == "idempotent" {
				if err != nil || !p.initialized {
					t.Fatalf("initialization: %v", err)
				}
				if err := p.Initialize(ctx); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.Canceled) || p.initialized {
				t.Fatalf("cancellation lost: %v, initialized %v", err, p.initialized)
			}
			want := []string{"prerequisites", "scaffold_created", "system_created", "release"}
			if mode == "cancel_before" {
				want = []string{"release"}
			} else if mode == "cancel_after_prerequisites" {
				want = []string{"prerequisites", "release"}
			}
			if err := p.Release(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(trace.steps, want) {
				t.Fatalf("initialization order %v, want %v", trace.steps, want)
			}
		})
	}
}

func TestSelectedWorkspacePreparationRealDockerBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_REAL_DOCKER") != "1" {
		t.Skip("requires explicit credential-free Docker proof opt-in")
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"probe_order", "partial_failure", "cancellation", "retirement", "cleanup_fail_once", "cleanup_persistent"} {
			t.Run(backend+"/"+mode, func(t *testing.T) { proveSelectedWorkspacePreparation(t, backend, mode, true) })
		}
	}
}

func proveSelectedWorkspacePreparation(t *testing.T, backend, mode string, real bool) {
	t.Helper()
	var selected startupownership.Store
	var db *sql.DB
	var owner SelectedContractExecutionOwner
	if backend == "sqlite" {
		s := storetest.StartSQLiteRuntimeStore(t)
		selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
	} else {
		_, db, _ = testutil.StartPostgres(t)
		s := storetest.AdmitPostgresRuntimeStore(t, db)
		selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
	}
	ctx, cancel := context.WithCancel(runForkTestContext(t))
	defer cancel()
	process, _ := worklifetime.ProcessFromContext(ctx)
	capability := selectedContractTestProcessCapability(t, ctx, selected)
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRun, eventID := uuid.NewString(), uuid.NewString()
	seedSelectedClaudeExecutionSource(t, ctx, backend, db, selected, loaded, sourceRun, uuid.NewString(), eventID, time.Unix(1700002303, 0).UTC())
	trace := &workspacePreparationTrace{mode: mode, real: real, started: make(chan struct{}), cancelled: make(chan struct{}), finish: make(chan struct{})}
	t.Cleanup(func() {
		trace.mu.Lock()
		trace.repaired = true
		trace.mu.Unlock()
	})
	var lifecycle workspace.Lifecycle = workspace.NewHostManager()
	if real {
		docker := workspace.NewDockerManager()
		cfg := workspace.DefaultDockerConfig()
		cfg.WorkspaceImage = os.Getenv("SWARM_TEST_REAL_DOCKER_IMAGE")
		if cfg.WorkspaceImage == "" {
			t.Fatal("real Docker proof requires explicit image")
		}
		cfg.WorkspaceNetwork = "none"
		docker.SetConfig(cfg)
		artifact, err := sourceartifact.AdmitDirectory(loader.SourceRoot)
		if err != nil {
			t.Fatal(err)
		}
		sourceProjection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := sourceProjection.Release(); err != nil {
				t.Error(err)
			}
		})
		sourceWorkspace, err := workspace.RebindSourceProjection(docker, sourceProjection, loaded.Source)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := sourceWorkspace.ReleaseSourceProjection(context.Background()); err != nil {
				t.Error(err)
			}
		})
		if err := sourceWorkspace.EnsurePrereqs(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sourceWorkspace.EnsureSystemWorkspaces(ctx); err != nil {
			t.Fatal(err)
		}
		for _, name := range sourceWorkspace.(*workspace.DockerManager).SystemWorkspaceContainers() {
			before, err := exec.Command("docker", "inspect", "--format", "{{.Id}} {{.State.Running}} {{.State.StartedAt}}", name).CombinedOutput()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				after, err := exec.Command("docker", "inspect", "--format", "{{.Id}} {{.State.Running}} {{.State.StartedAt}}", name).CombinedOutput()
				if err != nil || string(before) != string(after) {
					t.Errorf("source container changed: %s (%v)", name, err)
				}
			})
		}
		transport := workspace.NewDockerManager()
		docker.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
			if len(args) > 0 && args[0] == "rm" {
				trace.mu.Lock()
				fail := !trace.repaired && (mode == "cleanup_persistent" || (mode == "cleanup_fail_once" && trace.removeFailures == 0))
				if fail {
					trace.removeFailures++
				}
				trace.mu.Unlock()
				if fail {
					return "", errWorkspaceCleanup
				}
			}
			for i, arg := range args {
				if arg != "--name" || i+1 == len(args) {
					continue
				}
				trace.mu.Lock()
				trace.containers = append(trace.containers, args[i+1])
				second := len(trace.containers) == 2
				trace.mu.Unlock()
				if second && (mode == "cancellation" || mode == "retirement") {
					close(trace.started)
					<-ctx.Done()
					close(trace.cancelled)
					<-trace.finish
					return "", ctx.Err()
				}
				if second && mode != "probe_order" {
					return "", errWorkspaceConstruction
				}
			}
			return transport.RunDocker(ctx, args...)
		})
		lifecycle = docker
	}
	cfg := &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI,
		Models:    selection.ModelAliases{selection.ModelAliasRegular: {selection.BackendClaudeCLI: "test-model"}},
		ClaudeCLI: config.ClaudeCLIConfig{Command: "must-not-launch-provider", OutputFormat: "stream-json"}}}
	if mode == "no_probe" {
		cfg.LLM.Backend = selection.BackendAnthropic
		cfg.LLM.Models = selection.ModelAliases{selection.ModelAliasRegular: {selection.BackendAnthropic: "test-model"}}
	}
	request := SelectedContractExecutionRequest{Owner: owner, SourceRunID: sourceRun, At: eventID,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
		SourceLoader:      SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected.(SourceArtifactSelectedContractSourceStore)},
		AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, Config: cfg, ProcessCapability: capability, Workspace: &preparationWorkspace{Lifecycle: lifecycle, trace: trace}}}
	before := selectedPreparationDatabaseSnapshot(t, db, backend)
	var sequence int64
	if err := db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	baseline := process.ActiveCount()
	type outcome struct {
		prepared *PreparedSelectedFork
		err      error
	}
	finished := make(chan outcome, 1)
	go func() { p, err := owner.Prepare(ctx, request); finished <- outcome{p, err} }()
	var got outcome
	if mode == "cancellation" || mode == "retirement" {
		select {
		case <-trace.started:
		case got = <-finished:
			t.Fatalf("preparation skipped construction boundary: %v", got.err)
		}
		var retirement chan error
		if mode == "cancellation" {
			cancel()
		} else {
			retirement = make(chan error, 1)
			go func() { retirement <- owner.RetireSelectedContexts(context.Background()) }()
		}
		<-trace.cancelled
		if process.ActiveCount() <= baseline {
			t.Error("construction lost process ownership")
		}
		select {
		case <-finished:
			t.Error("preparation returned before accepted construction settled")
		default:
		}
		if retirement != nil {
			select {
			case <-retirement:
				t.Error("retirement skipped construction")
			default:
			}
		}
		close(trace.finish)
		got = <-finished
		if retirement != nil {
			if err := <-retirement; err != nil {
				t.Fatal(err)
			}
		}
	} else {
		got = <-finished
	}
	if mode == "no_probe" {
		if got.err != nil || got.prepared == nil {
			t.Fatalf("non-probing preparation: %v", got.err)
		}
		if err := got.prepared.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		want := errWorkspaceConstruction
		if mode == "probe_order" {
			want = errWorkspaceProbeReached
		}
		if mode == "cancellation" || mode == "retirement" {
			want = context.Canceled
		}
		if got.prepared != nil || !errors.Is(got.err, want) {
			t.Fatalf("preparation result: %v", got.err)
		}
	}
	if mode == "cleanup_fail_once" || mode == "cleanup_persistent" {
		if !errors.Is(got.err, errWorkspaceCleanup) || process.ActiveCount() <= baseline {
			t.Fatal("failed cleanup lost evidence or process ownership")
		}
		if _, err := os.Stat(trace.projection); err != nil {
			t.Fatalf("failed cleanup discarded selected projection: %v", err)
		}
		if mode == "cleanup_persistent" {
			if err := owner.RetireSelectedContexts(context.Background()); !errors.Is(err, errWorkspaceCleanup) {
				t.Fatalf("persistent cleanup retirement: %v", err)
			}
			if process.ActiveCount() <= baseline {
				t.Fatal("failed retirement settled process possession")
			}
			process.Retire()
			expired, stop := context.WithCancel(context.Background())
			stop()
			if receipt, err := process.Join(expired); err == nil || receipt != nil {
				t.Fatal("failed workspace release permitted a store-close join receipt")
			}
			trace.mu.Lock()
			trace.repaired = true
			trace.mu.Unlock()
		}
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if process.ActiveCount() != baseline {
		t.Fatalf("unsettled work: got %d want %d", process.ActiveCount(), baseline)
	}
	after := selectedPreparationDatabaseSnapshot(t, db, backend)
	// Only the probe-boundary cell may add its exact non-executable failure evidence.
	if mode == "probe_order" {
		var valid, total int
		if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences a
			JOIN runtime_external_effect_attempts e ON a.source_identity=CAST(e.attempt_id AS TEXT)||':'||e.state
			JOIN runtime_external_effect_operations o ON o.operation_id=e.operation_id
			WHERE a.sequence>$1 AND a.source_owner='runtime_external_effect_attempts' AND a.run_id IS NULL
			AND e.state='terminal_failure' AND o.authority_kind='startup_probe' AND o.selected_execution_id IS NULL`, sequence).Scan(&valid); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences WHERE sequence>$1`, sequence).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if valid != 1 || total != valid {
			t.Fatalf("probe author evidence valid/total = %d/%d", valid, total)
		}
		for _, table := range []string{"managed_agent_capability_surfaces", "runtime_external_effect_operations", "runtime_external_effect_attempts", "author_activity_order", "author_activity_occurrences"} {
			delete(before, table)
			delete(after, table)
		}
	}
	if !reflect.DeepEqual(before, after) {
		for table, rows := range before {
			if !reflect.DeepEqual(rows, after[table]) {
				t.Errorf("preparation changed domain %s", table)
			}
		}
	}
	trace.mu.Lock()
	steps := append([]string(nil), trace.steps...)
	containers := append([]string(nil), trace.containers...)
	trace.mu.Unlock()
	for _, name := range containers {
		out, err := exec.Command("docker", "container", "ls", "--all", "--filter", "name=^/"+name+"$", "--format", "{{.Names}}").CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "" {
			t.Errorf("selected container survived preparation cleanup: %s (%v)", name, err)
		}
	}
	want := []string{"prerequisites", "scaffold_created", "release"}
	switch mode {
	case "probe_order":
		want = []string{"prerequisites", "scaffold_created", "system_created", "probe"}
		if real {
			want = append(want, "private_state_released")
		}
		want = append(want, "release")
	case "prerequisite_failure":
		want = []string{"prerequisites", "release"}
	case "cleanup_fail_once":
		want = append(want, "release")
	case "cleanup_persistent":
		want = append(want, "release", "release")
	case "no_probe":
		want = []string{"release"}
	}
	if !reflect.DeepEqual(steps, want) {
		t.Fatalf("selected workspace order %v, want %v", steps, want)
	}
	if _, err := os.Stat(trace.projection); !os.IsNotExist(err) {
		t.Fatalf("selected projection not released: %v", err)
	}
}
