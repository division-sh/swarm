package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/division-sh/swarm/internal/cliapp"
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
	os.Exit(cliapp.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr, serveapp.Run, serveapp.RunTestSession))
}
