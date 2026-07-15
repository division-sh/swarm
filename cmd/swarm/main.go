package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/serveapp"
)

var (
	binaryVersion = "dev"
	binaryCommit  = "unknown"
	binaryDate    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cliapp.ConfigureBuildMetadata(cliapp.BuildMetadata{
		Version: binaryVersion,
		Commit:  binaryCommit,
		Date:    binaryDate,
	})
	os.Exit(cliapp.Execute(ctx, "", os.Args[1:], os.Stdout, os.Stderr, serveapp.Run))
}
