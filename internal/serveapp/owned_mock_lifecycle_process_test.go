package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/devscratch"
	"github.com/google/uuid"
)

// This entry exists only in a go test -c binary. The parent owns retained roots
// and credentials across children; production swarm cannot parse this input.
func TestOwnedMockLifecycleProcessEntry(t *testing.T) {
	raw := os.Getenv("SWARM_INTERNAL_MOCK_LIFECYCLE_REQUEST")
	if raw == "" {
		t.Skip("internal retained lifecycle child; launched by its parent proof")
	}
	var request struct {
		ConfigPath string
		Source     string
		Store      string
		Dev        bool
		APIPort    int
		Token      string
	}
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	if request.Token == "" || request.APIPort == 0 {
		t.Fatal("internal lifecycle child requires exact parent transport and auth facts")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath, opts.StoreMode = request.Source, request.ConfigPath, request.Store
	opts.StoreModeSet = request.Store != ""
	opts.Dev, opts.NoFeed = request.Dev, request.Dev
	opts.APIListenAddr, opts.MCPListenAddr = fmt.Sprintf("127.0.0.1:%d", request.APIPort), "127.0.0.1:0"
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	opts.NoColor, opts.SelfCheck = true, true
	opts.ShutdownGrace = 2 * time.Second
	t.Log("proof_surface=H internal retained mock lifecycle; not public serve or private test")
	code, err := runOwnedMockLifecycle(ctx, root, root, opts,
		apiv1.AuthTokenResolution{Tokens: []string{request.Token}, Explicit: true, Source: "internal-lifecycle-parent"})
	if err != nil || code != 0 {
		t.Fatalf("internal lifecycle exit=%d: %v", code, err)
	}
}

// Only test callers can choose this retained MockOnly composition. The compiled
// child and in-process persistence proofs use the same production constructor.
func runOwnedMockLifecycle(ctx context.Context, root, retainedRoot string, opts cliapp.ServeOptions, auth apiv1.AuthTokenResolution) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runtimeID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeID))
	configResult, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: opts.ConfigPath})
	if err != nil {
		return 1, err
	}
	cfg := configResult.Config
	cfg.Workspace.HostRoot = filepath.Join(retainedRoot, ".swarm", "lifecycle-workspaces")
	paths, err := cliapp.ResolveCLISourcePlatformSpecPaths(root, cliapp.CLISourcePlatformSpecPathOptions{SourceRoot: opts.SourceRoot, ConfigPath: opts.ConfigPath, PlatformSpecPath: opts.PlatformSpecPath})
	if err != nil {
		return 1, err
	}
	base, err := cliapp.LoadConfiguredPlatformPackBase(root, configResult)
	if err != nil {
		return 1, err
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		return 1, err
	}
	swarmDir := cliapp.CLISwarmDirResolution{Path: filepath.Join(retainedRoot, ".swarm"), Source: "internal-retained-lifecycle"}
	local, err := cliapp.ResolveLocalRuntimeState(cliapp.LocalRuntimeStateOptions{
		RepoRoot: root, ResolvedPaths: paths, SwarmDir: swarmDir, Config: cfg,
		StoreMode: opts.StoreMode, StoreModeSet: opts.StoreModeSet, CreateDefaultDataSource: true, EnforceLegacySQLite: true,
	})
	if err != nil {
		return 1, err
	}
	opts.SourceRoot, opts.PlatformSpecPath = paths.SourceRoot, paths.PlatformSpecPath
	if opts.ShutdownGrace == 0 {
		opts.ShutdownGrace = 2 * time.Second
	}
	credentials, err := cliapp.BuildCredentialStore()
	if err != nil {
		return 1, err
	}
	providerCredentials, err := cliapp.BuildProviderCredentialStore()
	if err != nil {
		return 1, err
	}
	managedCredentials, err := cliapp.BuildManagedCredentialStore()
	if err != nil {
		return 1, err
	}
	selection := local.StoreSelection
	var scratch *devscratch.EpochAuthority
	if opts.Dev {
		coordinate, err := cliapp.ResolveDevScratch(local)
		if err != nil {
			return 1, err
		}
		selection = coordinate.Selection
		scratch, err = devscratch.Acquire(coordinate.Coordinate)
		if err != nil {
			return 1, err
		}
		if err := scratch.PrepareFreshStore(); err != nil {
			_ = scratch.AbortBeforeStoreOpen()
			return 1, err
		}
	}
	presenter := newServeLifecyclePresenter(opts)
	defer presenter.finish()
	if os.Getenv("SWARM_INTERNAL_MOCK_LIFECYCLE_REQUEST") != "" {
		stopEvidence := startOwnedLifecycleEvidence(presenter, opts.ErrorOutput)
		defer stopEvidence()
	}
	code := buildRuntimeComposition(ctx, runtimeCompositionRequest{
		Purpose: executionposture.MockOnly, ProviderIngress: true,
		Repo: root, Options: opts, Config: configResult, ResolvedPaths: paths,
		LocalState: local, SwarmDir: swarmDir, StoreSelection: selection, MountSources: local.MountSources,
		PlatformPackBase: base, PlatformPackBases: bases,
		APIAuth:          auth,
		ScratchAuthority: scratch,
		Credentials:      credentials, ProviderCredentials: providerCredentials, ManagedCredentials: managedCredentials,
		Presenter: presenter, NoticePresentation: newServeNoticePresentationSink(presenter), Cancel: cancel,
		BootStartedAt: time.Now().UTC(), RuntimeInstanceID: runtimeID,
		DataProjectionRoot: filepath.Join(retainedRoot, ".lifecycle-data-projections"),
	})
	return code, nil
}

// The compiled proof child exposes bounded evidence only on its parent's signal.
// Snapshot the existing presentation owner before cleanup can block its rendering.
func startOwnedLifecycleEvidence(presenter *serveLifecyclePresenter, out io.Writer) func() {
	requests := make(chan os.Signal, 1)
	signal.Notify(requests, syscall.SIGUSR1)
	fmt.Fprintf(out, "lifecycle evidence available pid=%d\n", os.Getpid())
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-requests:
				writeOwnedLifecycleEvidence(presenter, out)
			}
		}
	}()
	return func() { signal.Stop(requests); close(stop); <-done }
}

func writeOwnedLifecycleEvidence(presenter *serveLifecyclePresenter, out io.Writer) {
	presenter.mu.Lock()
	var startupFailure string
	if failure := presenter.failure; failure != nil {
		startupFailure = fmt.Sprintf("step=%d name=%.256s status=%.256s detail=%.2048s", failure.Step, failure.Name, failure.Status, failure.Detail)
	}
	state := fmt.Sprintf("ready=%t failed=%t startup_failure=%s runtime_failure=%.2048s cleanup_error=%.2048s shutdown_error=%.2048s\n",
		presenter.ready, presenter.failed, startupFailure, fmt.Sprint(presenter.runtimeFailureErr), fmt.Sprint(presenter.cleanupErr), fmt.Sprint(presenter.shutdownErr))
	var phases []runtimepkg.BootProgressEvent
	for step := 1; step <= runtimepkg.BootProgressTotalSteps; step++ {
		if event, ok := presenter.bootEvents[step]; ok {
			phases = append(phases, event)
		}
	}
	presenter.mu.Unlock()
	fmt.Fprintf(out, "lifecycle evidence begin pid=%d\n%s", os.Getpid(), state)
	for _, phase := range phases {
		fmt.Fprintf(out, "phase=%d name=%s status=%s detail=%.2048s\n", phase.Step, phase.Name, phase.Status, phase.Detail)
	}
	stacks := make([]byte, 1<<20)
	n := runtime.Stack(stacks, true)
	_, _ = out.Write(stacks[:n])
	fmt.Fprintf(out, "\nlifecycle evidence end pid=%d stack_bytes=%d\n", os.Getpid(), n)
}

func TestOwnedLifecycleStartupEvidence(t *testing.T) {
	failure := runtimepkg.BootProgressEvent{Step: 1, Name: "controlled-startup", Detail: "original startup cause"}
	presenter := &serveLifecyclePresenter{
		failed: true, failure: &failure,
		bootEvents:        map[int]runtimepkg.BootProgressEvent{1: failure},
		runtimeFailureErr: errors.New("continuation cause"),
		cleanupErr:        errors.New("cleanup cause"), shutdownErr: errors.New("shutdown cause"),
	}
	var out bytes.Buffer
	writeOwnedLifecycleEvidence(presenter, &out)
	for _, want := range []string{"ready=false failed=true", "original startup cause", "continuation cause", "cleanup cause", "shutdown cause", "phase=1 name=controlled-startup", "TestOwnedLifecycleStartupEvidence", fmt.Sprintf("lifecycle evidence end pid=%d", os.Getpid())} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("evidence missing %q", want)
		}
	}
	presenter.cleanupErr = errors.New(strings.Repeat("x", 1<<20))
	out.Reset()
	writeOwnedLifecycleEvidence(presenter, &out)
	if strings.Contains(out.String(), strings.Repeat("x", 2049)) {
		t.Fatal("unbounded error in startup evidence")
	}
}

func startOwnedMockLifecycleFollowUpRuntime(t *testing.T, opts cliapp.ServeOptions) (string, *runtimepkg.Runtime) {
	t.Helper()
	process := startOwnedMockLifecycleTestProcess(t, repoRootForTest(), t.TempDir(), opts)
	process.waitForReadyLine()
	return "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc", servedTestProcessRuntime(t, process)
}

// Retained persistence proofs inject their parent-owned H session explicitly.
// This is not a public connected-test option or T command-lifetime proof.
func executeScenarioInOwnedLifecycle(t *testing.T, root string, args []string, endpoint string, out, errOut io.Writer) int {
	t.Helper()
	t.Log("proof_surface=H scenario protocol on parent-owned lifecycle; not public private-test lifetime")
	return executeCLIFromWithRunners(context.Background(), root, args, out, errOut, nil,
		func(ctx context.Context, _ cliapp.TestSessionRequest, execute func(context.Context, cliapp.TestSessionEndpoint) error) error {
			return execute(ctx, cliapp.TestSessionEndpoint{APIServer: strings.TrimSuffix(endpoint, "/v1/rpc"), Token: apiv1.DefaultLoopbackAPIToken})
		})
}
