package serveapp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/division-sh/swarm/internal/cliapp"
)

var cliInvocationTestMu sync.Mutex

func repoRootForTest() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		panic("resolve serveapp test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func executeCLIFrom(ctx context.Context, root string, args []string, out, errOut io.Writer, runServe cliapp.ServeRunner) int {
	cliInvocationTestMu.Lock()
	defer cliInvocationTestMu.Unlock()

	previous, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(root); err != nil {
		panic(err)
	}
	defer func() {
		if err := os.Chdir(previous); err != nil {
			panic(err)
		}
	}()
	return cliapp.Execute(ctx, args, out, errOut, runServe)
}

func executeCLI(ctx context.Context, args []string, out, errOut io.Writer, runServe cliapp.ServeRunner) int {
	return cliapp.Execute(ctx, args, out, errOut, runServe)
}

func invocationRootForTest(root string) cliapp.InvocationRoot {
	captured, err := cliapp.NewInvocationRoot(root)
	if err != nil {
		panic(err)
	}
	return captured
}

func runFrom(ctx context.Context, root string, opts cliapp.ServeOptions) int {
	return Run(ctx, invocationRootForTest(root), opts)
}
