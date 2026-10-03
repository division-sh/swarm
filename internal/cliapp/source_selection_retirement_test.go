package cliapp

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func Test2376RetiredHashSelectorsFailBeforeIO(t *testing.T) {
	for _, row := range []struct {
		name string
		args []string
		flag string
	}{
		{"serve", []string{"serve"}, "bundle-hash"},
		{"event_publish", []string{"event", "publish", "event.name"}, "bundle-hash"},
		{"data_import", []string{"data", "import", "data-name"}, "bundle-hash"},
		{"data_show", []string{"data", "show", "data-name"}, "bundle-hash"},
		{"data_prune", []string{"data", "prune", "data-name"}, "bundle-hash"},
		{"run_fork", []string{"run", "fork", "00000000-0000-4000-8000-000000000001"}, "bundle-hash"},
		{"run_start", []string{"run", "start"}, "bundle-hash"},
		{"agent_frame", []string{"agent", "frame", "worker"}, "bundle-hash"},
		{"channel_connect", []string{"channel", "connect", "telegram"}, "bundle"},
		{"channel_reconnect", []string{"channel", "reconnect", "telegram"}, "bundle"},
		{"channel_rebind", []string{"channel", "rebind", "telegram"}, "bundle"},
		{"channel_status", []string{"channel", "status", "interface"}, "bundle"},
	} {
		t.Run(row.name, func(t *testing.T) {
			root, err := NewInvocationRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var out, errOut bytes.Buffer
			called := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int { called = true; return 0 }
			args := append(append([]string(nil), row.args...), "--"+row.flag, "bundle-v2:sha256:"+strings.Repeat("a", 64))
			code := executeRootCommandAtInvocation(context.Background(), root, args, &out, &errOut, opts)
			if code != CLIExitValidation || !strings.Contains(errOut.String(), "unknown flag: --"+row.flag) || called {
				t.Fatalf("code=%d called=%v out=%q error=%q; want retired flag rejected before IO", code, called, out.String(), errOut.String())
			}
		})
	}
}

func Test2376NoAuthoredHashSelectorInCommandTree(t *testing.T) {
	root, err := NewInvocationRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		for _, flag := range []string{"bundle-hash", "bundle"} {
			if cmd.LocalNonPersistentFlags().Lookup(flag) != nil {
				t.Errorf("%s still admits --%s", cmd.CommandPath(), flag)
			}
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(newRootCommandAtInvocation(context.Background(), root, io.Discard, io.Discard, defaultRootCommandOptions()))
}

func Test2376OperatorJourneyFixturesDoNotRestoreRetiredSelectors(t *testing.T) {
	for _, file := range []string{
		"fan_out_resource_journey_test.go",
		"fan_out_resource_multi_pin_test.go",
		"data_text_file_journey_2456_test.go",
		"data_text_file_support_2456_test.go",
	} {
		t.Run(file, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(RepoRoot(), "internal", "runtime", "conformance", file))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(source), `"--bundle-hash"`) {
				t.Fatal("operator journey must consume selected-server or permanent replay identity, not an authored hash flag")
			}
		})
	}
}

func Test2376ChannelStatusRequiresPairedSourceAndTargetBeforeIO(t *testing.T) {
	for _, args := range [][]string{
		{"channel", "status", "operator", "--source", "missing-directory"},
		{"channel", "status", "operator", "--target", "flow"},
	} {
		t.Run(args[3], func(t *testing.T) {
			root, err := NewInvocationRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var out, errOut bytes.Buffer
			code := executeRootCommandAtInvocation(context.Background(), root, args, &out, &errOut, defaultRootCommandOptions())
			if code != CLIExitValidation || !strings.Contains(errOut.String(), "requires --source and --target together") {
				t.Fatalf("code=%d out=%q error=%q", code, out.String(), errOut.String())
			}
		})
	}
}
