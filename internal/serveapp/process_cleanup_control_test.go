package serveapp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestServeRuntimeTestProcessCleanupRequiresSuccessfulExit(t *testing.T) {
	const key = "SWARM_TEST_CLEANUP_EXIT_CONTROL"
	if mode := os.Getenv(key); mode != "" {
		if mode != "zero" && mode != "failed" && mode != "already_joined" {
			t.Fatal("unknown cleanup control")
		}
		code := 0
		if mode != "zero" {
			code = 23
		}
		done := make(chan int, 1)
		cancelled := false
		process := &serveRuntimeTestProcess{
			t: t, done: done, out: &lockedBuffer{},
			cancel: func() { cancelled = true; done <- code },
		}
		if mode == "already_joined" {
			process.recordStopped(code)
		}
		process.cleanup()
		if !process.stopped || process.code != code || cancelled != (mode != "already_joined") {
			t.Fatalf("cleanup did not preserve cancellation/join: stopped=%v code=%d cancelled=%v", process.stopped, process.code, cancelled)
		}
		return
	}
	for _, mode := range []string{"zero", "failed", "already_joined"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServeRuntimeTestProcessCleanupRequiresSuccessfulExit$", "-test.timeout=5s", "-test.v")
			command.Env = append(os.Environ(), key+"="+mode)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("cleanup control did not join: %v\n%s", ctx.Err(), output)
			}
			if mode != "failed" {
				if err != nil {
					t.Fatalf("successful/consumed exit was refused: %v\n%s", err, output)
				}
				return
			}
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 1 || !strings.Contains(string(output), "Run exited during cleanup with code 23") {
				t.Fatalf("cleanup accepted or misclassified nonzero exit: %v\n%s", err, output)
			}
		})
	}
}
