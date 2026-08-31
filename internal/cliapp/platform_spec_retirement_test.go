package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPlatformSpecFlagRetiredAcrossCommandSurface(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "serve", args: []string{"serve", ".", "--platform-spec", "platform.yaml"}},
		{name: "verify", args: []string{"verify", ".", "--platform-spec", "platform.yaml"}},
		{name: "run start", args: []string{"run", "start", ".", "--platform-spec", "platform.yaml"}},
		{name: "describe", args: []string{"describe", ".", "--platform-spec", "platform.yaml"}},
		{name: "describe routes", args: []string{"describe", "routes", ".", "--platform-spec", "platform.yaml"}},
		{name: "doctor", args: []string{"doctor", "--platform-spec", "platform.yaml"}},
		{name: "test", args: []string{"test", ".", "--platform-spec", "platform.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
			}
			for _, want := range []string{
				"--platform-spec is retired",
				"embeds its own platform spec",
				"paths.platform_spec_path",
			} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
				}
			}
		})
	}

	t.Run("connections status has no compatibility flag", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"connections", "status", "--platform-spec", "platform.yaml"}, &stdout, &stderr, defaultRootCommandOptions())
		if code != CLIExitValidation || !strings.Contains(stderr.String(), "unknown flag: --platform-spec") {
			t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	})
}
