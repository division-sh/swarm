package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestServingStopDeadlineExitsProcessWithoutFakingJoin(t *testing.T) {
	if mode := os.Getenv("SWARM_TEST_PROCESS_STOP_MODE"); mode != "" {
		ctx, cancel := context.WithCancel(context.Background())
		code := withServingExitDeadline(ctx, 50*time.Millisecond, func() int {
			cancel()
			fmt.Println("draining")
			if mode == "stuck" {
				select {}
			}
			fmt.Println("joined")
			return 0
		})
		os.Exit(code)
	}
	for _, mode := range []string{"graceful", "stuck"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServingStopDeadlineExitsProcessWithoutFakingJoin$")
			cmd.Env = append(os.Environ(), "SWARM_TEST_PROCESS_STOP_MODE="+mode)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil || !strings.Contains(string(output), "draining\n") {
				t.Fatalf("process did not reach the stop boundary: %v: %s", err, output)
			}
			if mode == "graceful" {
				if err != nil || !strings.Contains(string(output), "joined\n") {
					t.Fatalf("graceful join: %v: %s", err, output)
				}
			} else {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 1 || strings.Contains(string(output), "joined\n") {
					t.Fatalf("stuck process reported join or survived: %v: %s", err, output)
				}
			}
		})
	}
}
