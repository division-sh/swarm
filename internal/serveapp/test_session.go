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
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
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
	loaded, err := materializeRuntimeBundle(module, req.AdmittedBundle, req.ResolvedPaths.SourceRoot, req.ResolvedPaths.PlatformSpecPath)
	if err != nil {
		return nil, err
	}
	return []serveRuntimeBundle{loaded}, nil
}

// RunTestSession uses the production composition with privately owned resources.
// Source/scenario admission belongs to the calling CLI before this acquisition.
func RunTestSession(ctx context.Context, in cliapp.TestSessionRequest, execute func(context.Context, cliapp.TestSessionEndpoint) error) (retErr error) {
	if in.Bundle == nil || in.Bundle.SourceArtifact == nil || in.PlatformPackBase == nil || execute == nil {
		return fmt.Errorf("private test session requires admitted source, pack base and execution callback")
	}
	cfg, err := cliapp.DefaultRuntimeConfig()
	if err != nil {
		return err
	}
	cfg.LLM.Backend, cfg.LLM.Models = in.LiveBackend, in.ModelAliases
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
	opts.SourceRoot, opts.PlatformSpecPath = in.SourceRoot, in.PlatformSpecPath
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.Output, opts.ErrorOutput = io.Discard, io.Discard
	opts.NoColor = true
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runtimeID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeID))
	presenter := newServeLifecyclePresenter(opts)
	defer presenter.finish()
	var executionErr error
	called := false
	code := buildRuntimeComposition(ctx, runtimeCompositionRequest{
		Purpose: executionposture.MockOnly, ProviderIngress: false,
		Repo: privateRoot, Options: opts,
		Config:           cliapp.RuntimeConfigLoadResult{Config: cfg, Source: "private-test-session"},
		AdmittedBundle:   in.Bundle,
		ResolvedPaths:    cliapp.CLISourcePlatformSpecPaths{SourceRoot: in.SourceRoot, PlatformSpecPath: in.PlatformSpecPath},
		SwarmDir:         cliapp.CLISwarmDirResolution{Path: privateRoot, Source: "private-test-session"},
		StoreSelection:   storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(privateRoot, "state.db")},
		PlatformPackBase: in.PlatformPackBase, PlatformPackBases: bases,
		APIAuth:     apiv1.AuthTokenResolution{Tokens: []string{token}, Explicit: true, Source: "private-test-session"},
		Credentials: credentials, ProviderCredentials: credentials,
		ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(),
		Presenter:          presenter, NoticePresentation: newServeNoticePresentationSink(presenter),
		Cancel: cancel, BootStartedAt: time.Now().UTC(), RuntimeInstanceID: runtimeID,
		DataProjectionRoot: filepath.Join(privateRoot, "data-projections"),
		OnReady: func(readyCtx context.Context, server string) error {
			called = true
			executionErr = execute(readyCtx, cliapp.TestSessionEndpoint{APIServer: server, Token: token})
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
