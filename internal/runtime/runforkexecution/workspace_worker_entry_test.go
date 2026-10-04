package runforkexecution

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}
