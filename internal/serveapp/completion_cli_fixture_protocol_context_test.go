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

const completion2270ContextProbeEnv = "SWARM_TEST_COMPLETION2270_CONTEXT_PROBE"
const completion2270ContextRefusalMarker = "context termination accepted as a protocol refusal"

func TestClaudeCLICompletion2270ProtocolContextTerminationCannotRefuse(t *testing.T) {
	if mode := os.Getenv(completion2270ContextProbeEnv); mode != "" {
		completion2270TerminatedProtocolChild(t, mode)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"canceled_parent", "expired_parent"} {
		t.Run(mode, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), executable, "-test.run=^TestClaudeCLICompletion2270ProtocolContextTerminationCannotRefuse$", "-test.v")
			command.Env = append(os.Environ(), completion2270ContextProbeEnv+"="+mode)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("context-terminated fixture did not explicitly fail: command=%v output=%s", err, output)
			}
			cause := "context canceled"
			if mode == "expired_parent" {
				cause = "context deadline exceeded"
			}
			for _, required := range []string{"protocol fixture context terminated", "command=", "context=" + cause, "parent=" + cause, "stdout=", "stderr="} {
				if !strings.Contains(string(output), required) {
					t.Fatalf("fixture lost %q diagnostics: %s", required, output)
				}
			}
			if strings.Contains(string(output), completion2270ContextRefusalMarker) {
				t.Fatalf("context termination reached the ordinary refusal oracle: %s", output)
			}
		})
	}
}

func completion2270TerminatedProtocolChild(t *testing.T, mode string) {
	t.Helper()
	root, args, _ := completion2270ProtocolFixture(t, "")
	var ctx context.Context
	var cancel context.CancelFunc
	switch mode {
	case "canceled_parent":
		ctx, cancel = context.WithCancel(t.Context())
		cancel()
	case "expired_parent":
		ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	default:
		t.Fatalf("unknown context probe %q", mode)
	}
	defer cancel()
	stdout, _, err := runCompletion2270ProtocolContext(t, ctx, root, args, "business request")
	// This is the existing ordinary-refusal oracle. Context termination must
	// fail inside the shared helper, before this branch can accept it.
	if err != nil && stdout == "" {
		t.Log(completion2270ContextRefusalMarker)
		return
	}
	t.Fatalf("context probe did not reach its required failure cut: stdout=%q command=%v", stdout, err)
}
