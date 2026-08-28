package cliapp

import (
	"context"
	"io"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/spf13/cobra"
)

func RepoRoot() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		panic("resolve CLI test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func mustInvocationRootForTest(raw string) InvocationRoot {
	if raw == "" {
		root, err := captureInvocationRoot()
		if err != nil {
			panic(err)
		}
		return root
	}
	root, err := NewInvocationRoot(raw)
	if err != nil {
		panic(err)
	}
	return root
}

func executeRootCommand(ctx context.Context, root string, args []string, out, errOut io.Writer) int {
	return executeRootCommandWithOptions(ctx, root, args, out, errOut, defaultRootCommandOptions())
}

func executeRootCommandWithOptions(ctx context.Context, root string, args []string, out, errOut io.Writer, opts rootCommandOptions) int {
	return executeRootCommandAtInvocation(ctx, mustInvocationRootForTest(root), args, out, errOut, opts)
}

func newRootCommand(ctx context.Context, root string, out, errOut io.Writer) *cobra.Command {
	return newRootCommandWithOptions(ctx, root, out, errOut, defaultRootCommandOptions())
}

func newRootCommandWithOptions(ctx context.Context, root string, out, errOut io.Writer, opts rootCommandOptions) *cobra.Command {
	return newRootCommandAtInvocation(ctx, mustInvocationRootForTest(root), out, errOut, opts)
}

func loadCLICommandConfig() (cliCommandConfig, error) {
	root, err := captureInvocationRoot()
	if err != nil {
		return cliCommandConfig{}, err
	}
	return loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: root.Path()})
}

func loadRuntimeConfig(path string) (*config.Config, error) {
	root, err := captureInvocationRoot()
	if err != nil {
		return nil, err
	}
	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root.Path(), ExplicitPath: path})
	if err != nil {
		return nil, err
	}
	return result.Config, nil
}

func resolveCLISwarmDir(opts cliSwarmDirOptions) (CLISwarmDirResolution, error) {
	root, err := captureInvocationRoot()
	if err != nil {
		return CLISwarmDirResolution{}, err
	}
	return resolveCLISwarmDirAt(root, opts)
}

func rootCommandOptionsForTest(t testing.TB, opts rootCommandOptions) rootCommandOptions {
	t.Helper()
	if opts.invocationRoot.Path() == "" {
		opts.invocationRoot = mustInvocationRootForTest(t.TempDir())
	}
	return opts
}

func newCLIAPIClientForTest(t testing.TB, opts rootCommandOptions) (*cliAPIClient, error) {
	t.Helper()
	return newCLIAPIClient(rootCommandOptionsForTest(t, opts))
}

func resolveCLIAPISettingsForTest(t testing.TB, opts rootCommandOptions) (cliAPISettings, error) {
	t.Helper()
	return resolveCLIAPISettings(rootCommandOptionsForTest(t, opts))
}

func unifiedConfigOptionsForTest(t testing.TB, opts unifiedConfigLoadOptions) unifiedConfigLoadOptions {
	t.Helper()
	if opts.RepoRoot == "" {
		opts.RepoRoot = t.TempDir()
	}
	return opts
}

func loadUnifiedConfigForTest(t testing.TB, opts unifiedConfigLoadOptions) (unifiedConfigLoadResult, error) {
	t.Helper()
	return loadUnifiedConfig(unifiedConfigOptionsForTest(t, opts))
}

func loadUnifiedConfigAllowDiagnosticsForTest(t testing.TB, opts unifiedConfigLoadOptions) (unifiedConfigLoadResult, error) {
	t.Helper()
	return loadUnifiedConfigAllowDiagnostics(unifiedConfigOptionsForTest(t, opts))
}
