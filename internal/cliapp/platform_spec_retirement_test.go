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
		{name: "import", args: []string{"import", "pack", "--platform-spec", "platform.yaml"}},
		{name: "packs list", args: []string{"packs", "list", "--platform-spec", "platform.yaml"}},
		{name: "packs show", args: []string{"packs", "show", "pack", "--platform-spec", "platform.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "unknown flag: --platform-spec") || strings.Contains(stderr.String(), "retired") {
				t.Fatalf("stderr must use ordinary flag admission:\n%s", stderr.String())
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
