package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/division-sh/swarm/internal/serveapp"
)

var (
	binaryVersion = "dev"
	binaryCommit  = "unknown"
	binaryDate    = "unknown"
)

func main() {
	worker.ConfigureBuildMetadata(binaryVersion, binaryCommit)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 2 {
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	cliapp.ConfigureBuildMetadata(cliapp.BuildMetadata{
		Version: binaryVersion,
		Commit:  binaryCommit,
		Date:    binaryDate,
	})
	os.Exit(cliapp.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr, runServingProcess, serveapp.RunTestSession))
}

func runServingProcess(ctx context.Context, root cliapp.InvocationRoot, opts cliapp.ServeOptions) int {
	grace, err := manager.ResolveShutdownGrace(opts.ShutdownGrace)
	if err != nil {
		return serveapp.Run(ctx, root, opts)
	}
	return withServingExitDeadline(ctx, grace, func() int { return serveapp.Run(ctx, root, opts) })
}

// The deadline bounds actual process death, never an internal join or lease
// release. A stuck native waiter or accepted producer keeps ownership until exit.
func withServingExitDeadline(ctx context.Context, grace time.Duration, run func() int) int {
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			os.Exit(1)
		}
	}()
	code := run()
	close(done)
	<-joined
	return code
}
