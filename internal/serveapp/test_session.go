package serveapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/google/uuid"
)

func loadRuntimeCompositionBundles(ctx context.Context, req runtimeCompositionRequest, artifacts sourceArtifactReader) ([]serveRuntimeBundle, error) {
	if req.AdmittedBundle == nil {
		return loadServeRuntimeBundles(ctx, req.Repo, artifacts, req.ResolvedPaths, req.Options, req.PlatformPackBases)
	}
	module, _, err := cliapp.NewSwarmWorkflowModuleForBundle(req.AdmittedBundle)
	if err != nil {
		return nil, err
	}
	loaded, err := materializeRuntimeBundle(module, req.AdmittedBundle, req.ResolvedPaths.SourceRoot)
	if err != nil {
		return nil, err
	}
	return []serveRuntimeBundle{loaded}, nil
}

func buildPrivateTestComposition(ctx context.Context, req runtimeCompositionRequest) int {
	return buildRuntimeHost(ctx, req, nil)
}

// RunTestSession shares acquisition and joined cleanup, not serve's boot maker.
// Source/scenario admission belongs to the calling CLI before this acquisition.
func RunTestSession(ctx context.Context, in cliapp.TestSessionRequest, execute func(context.Context, cliapp.TestSessionEndpoint) error) (retErr error) {
	if in.Bundle == nil || in.Bundle.SourceArtifact == nil || in.PlatformPackBase == nil || execute == nil {
		return fmt.Errorf("private test session requires admitted source, pack base and execution callback")
	}
	backend := strings.TrimSpace(in.WorkspaceBackend)
	if backend == "" {
		backend = "host"
	}
	preference, err := cliapp.ResolveWorkspaceBackend(backend, true, nil)
	if err != nil {
		return err
	}
	cfg, err := cliapp.DefaultRuntimeConfig()
	if err != nil {
		return err
	}
	cfg.LLM = in.LLM
	if err := cfg.Validate(); err != nil {
		return err
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(in.PlatformPackBase)
	if err != nil {
		return err
	}
	privateRoot, err := os.MkdirTemp("", "swarm-test-session-")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, removePrivateTestRoot(privateRoot)) }()
	cfg.Workspace.HostRoot = filepath.Join(privateRoot, "workspaces")
	cfg.Runtime.RecoveryOnStartup = false
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(privateRoot, "credentials.json"))
	if err != nil {
		return err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return err
	}
	token := hex.EncodeToString(tokenBytes)
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot = in.SourceRoot
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.Output, opts.ErrorOutput = io.Discard, io.Discard
	opts.NoColor = true
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runtimeID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeID))
	ctx, err = scenarioexecution.WithPrivateHostScope(ctx, runtimeID)
	if err != nil {
		return err
	}
	presenter := newServeLifecyclePresenter(opts)
	defer presenter.finish()
	var executionErr error
	called := false
	var scenarioMu sync.Mutex
	scenarioRuns := map[string]struct{}{}
	code := buildPrivateTestComposition(ctx, runtimeCompositionRequest{
		Purpose: executionposture.MockOnly, ProviderIngress: false,
		WorkspacePreference: preference,
		Repo:                privateRoot, Options: opts,
		Config:           cliapp.RuntimeConfigLoadResult{Config: cfg, Source: "private-test-session"},
		AdmittedBundle:   in.Bundle,
		ResolvedPaths:    cliapp.CLISourcePaths{SourceRoot: in.SourceRoot},
		SwarmDir:         cliapp.CLISwarmDirResolution{Path: privateRoot, Source: "private-test-session"},
		StoreSelection:   storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(privateRoot, "state.db")},
		PlatformPackBase: in.PlatformPackBase, PlatformPackBases: bases,
		APIAuth:     apiv1.AuthTokenResolution{Tokens: []string{token}, Explicit: true, Source: "private-test-session"},
		Credentials: credentials, ProviderCredentials: credentials,
		ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(),
		Presenter:          presenter, NoticePresentation: newServeNoticePresentationSink(presenter),
		Cancel: cancel, BootStartedAt: time.Now().UTC(), RuntimeInstanceID: runtimeID,
		DataProjectionRoot: filepath.Join(privateRoot, "data-projections"),
		OnReady: func(readyCtx context.Context, server string, publication apiv1.EventPublicationOptions, rt *runtimepkg.Runtime) error {
			called = true
			executionErr = execute(readyCtx, cliapp.TestSessionEndpoint{APIServer: server, Token: token,
				StartScenario: func(ctx context.Context, params map[string]any) (string, error) {
					var output *providertriggers.PublicationAdmission
					if eventName, _ := params["event_name"].(string); strings.HasPrefix(eventName, "inbound.") {
						payload, ok := params["payload"].(map[string]any)
						if !ok {
							return "", fmt.Errorf("private provider input requires an object payload")
						}
						_, admission, handled, err := rt.Options.ProviderTriggerCatalog.IssuePrivateTestPublication(ctx, publication.Bundle.BundleHash, ".", uuid.NewString(), eventName, privateTestOperatorAccount(in.Bundle, payload), payload, time.Now().UTC())
						if err != nil {
							return "", err
						}
						if handled {
							output = &admission
						}
					}
					runID, err := apiv1.StartPrivateScenario(ctx, publication, params, time.Now().UTC(), output)
					if err == nil {
						scenarioMu.Lock()
						scenarioRuns[runID] = struct{}{}
						scenarioMu.Unlock()
					}
					return runID, err
				},
				PublishProviderInput: func(ctx context.Context, params map[string]any) (cliapp.TestProviderPublication, bool, error) {
					var empty cliapp.TestProviderPublication
					eventName, _ := params["event_name"].(string)
					if !strings.HasPrefix(eventName, "inbound.") {
						return empty, false, nil
					}
					runID, _ := params["run_id"].(string)
					scenarioMu.Lock()
					_, owned := scenarioRuns[runID]
					scenarioMu.Unlock()
					if !owned {
						return empty, true, fmt.Errorf("private provider input requires this test host's scenario run")
					}
					payload, ok := params["payload"].(map[string]any)
					if !ok {
						return empty, true, fmt.Errorf("private provider input requires an object payload")
					}
					key, _ := params["idempotency_key"].(string)
					ctx, release, err := apiv1.AdmitPrivateProviderScenarioRun(ctx, publication, runID, params)
					if err != nil {
						return empty, true, err
					}
					defer release()
					eventID, handled, err := rt.InboundGateway.PublishPrivateTestProviderOutput(ctx, rt.Options.ProviderTriggerCatalog, publication.Bundle.BundleHash, ".", runID, eventName, privateTestOperatorAccount(in.Bundle, payload), payload, key, time.Now().UTC())
					return cliapp.TestProviderPublication{EventID: eventID, RunID: runID}, handled, err
				},
			})
			return executionErr
		},
	})
	presenter.mu.Lock()
	cleanupErr := errors.Join(presenter.cleanupErr, presenter.shutdownErr, presenter.runtimeFailureErr)
	var startupErr error
	if presenter.failure != nil {
		startupErr = fmt.Errorf("%s: %s", presenter.failure.Name, presenter.failure.Detail)
	}
	presenter.mu.Unlock()
	if code != 0 || !called {
		return errors.Join(executionErr, startupErr, cleanupErr, fmt.Errorf("private test session failed (exit %d)", code))
	}
	return errors.Join(executionErr, cleanupErr)
}

func privateTestOperatorAccount(bundle *runtimecontracts.WorkflowContractBundle, payload map[string]any) string {
	if bundle != nil {
		if account, err := bundle.ResolveFlowOperatorAccount("."); err == nil && account.FromPolicy {
			return account.Policy
		}
	}
	value, _ := payload["operator_account"].(string)
	return value
}

func removePrivateTestRoot(root string) error {
	// Data/source projections are immutable while executing. After the joined
	// shutdown, restore directory write permission only inside this owned root.
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("release private test root: %w", err)
	}
	return os.RemoveAll(root)
}
