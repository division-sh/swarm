package tools

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

type providerHostWorkspace struct{ target *workspace.Target }

// Only transport is real in this fixture; it does not qualify workspace adoption.
func mockProviderHostWorkspace(t *testing.T) providerHostWorkspace {
	t.Helper()
	return providerHostWorkspace{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}}
}

func (w providerHostWorkspace) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func (w providerHostWorkspace) ResolveWorkspaceForCapabilityAdmission(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}
