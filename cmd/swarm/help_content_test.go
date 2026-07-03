package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// transportLeakPattern matches internal transport vocabulary that must never
// appear in user-facing help content. Help describes operator outcomes; how a
// command reaches the runtime is an implementation detail (#1649).
var transportLeakPattern = regexp.MustCompile(`v1 RPC|/v1/|API owners|v1 API`)

func walkCommands(cmd *cobra.Command, visit func(*cobra.Command)) {
	visit(cmd)
	for _, sub := range cmd.Commands() {
		walkCommands(sub, visit)
	}
}

func TestHelpContentHasNoTransportLeakage(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	walkCommands(root, func(cmd *cobra.Command) {
		for field, value := range map[string]string{
			"Short":   cmd.Short,
			"Long":    cmd.Long,
			"Example": cmd.Example,
		} {
			if match := transportLeakPattern.FindString(value); match != "" {
				t.Errorf("%s: %s leaks transport vocabulary %q: %q", cmd.CommandPath(), field, match, value)
			}
		}
		checkFlags := func(fs *pflag.FlagSet) {
			fs.VisitAll(func(f *pflag.Flag) {
				if match := transportLeakPattern.FindString(f.Usage); match != "" {
					t.Errorf("%s: flag --%s usage leaks transport vocabulary %q: %q", cmd.CommandPath(), f.Name, match, f.Usage)
				}
			})
		}
		checkFlags(cmd.LocalFlags())
		checkFlags(cmd.PersistentFlags())
	})
}

func TestRootHelpGroupsRenderInJourneyOrder(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), t.TempDir(), []string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("--help code = %d stderr=%s", code, stderr.String())
	}
	help := stdout.String()
	groups := []string{
		"Getting started:",
		"Author & validate:",
		"Run & operate:",
		"Observe & debug:",
		"Utilities:",
	}
	last := -1
	for _, title := range groups {
		idx := strings.Index(help, title)
		if idx < 0 {
			t.Fatalf("root help missing group %q:\n%s", title, help)
		}
		if idx < last {
			t.Fatalf("group %q renders out of journey order:\n%s", title, help)
		}
		last = idx
	}
}

func TestEveryVisibleCommandBelongsToAGroup(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	for _, sub := range root.Commands() {
		if sub.Hidden {
			continue
		}
		if sub.Name() == "help" {
			continue // grouped via SetHelpCommandGroupID
		}
		if sub.GroupID == "" {
			t.Errorf("visible command %q has no help group", sub.Name())
		}
	}
}

func TestIdempotencyKeyFlagIsHiddenEverywhere(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	found := 0
	walkCommands(root, func(cmd *cobra.Command) {
		flag := cmd.LocalFlags().Lookup("idempotency-key")
		if flag == nil {
			return
		}
		found++
		if !flag.Hidden {
			t.Errorf("%s: --idempotency-key must be hidden from default help", cmd.CommandPath())
		}
	})
	if found == 0 {
		t.Fatal("no --idempotency-key flags found; test wiring is broken")
	}
}

func TestIdempotencyKeyFlagStillParses(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	var target *cobra.Command
	walkCommands(root, func(cmd *cobra.Command) {
		if target == nil && cmd.LocalFlags().Lookup("idempotency-key") != nil {
			target = cmd
		}
	})
	if target == nil {
		t.Fatal("no command with --idempotency-key found")
	}
	if err := target.LocalFlags().Set("idempotency-key", "test-key"); err != nil {
		t.Fatalf("hidden --idempotency-key no longer parses on %s: %v", target.CommandPath(), err)
	}
	value, err := target.LocalFlags().GetString("idempotency-key")
	if err != nil || value != "test-key" {
		t.Fatalf("hidden --idempotency-key round-trip failed on %s: value=%q err=%v", target.CommandPath(), value, err)
	}
}

func TestRootHelpHidesIdempotencyAndRetiredCommands(t *testing.T) {
	for _, cmdArgs := range [][]string{{"--help"}, {"fork", "--help"}} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommand(context.Background(), t.TempDir(), cmdArgs, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("%v code = %d stderr=%s", cmdArgs, code, stderr.String())
		}
		// The rendered flag list must not advertise the hidden flag. The Long
		// command-shape line (e.g. "[--idempotency-key <key>]") mirrors the
		// promoted command_catalog row and intentionally remains.
		if strings.Contains(stdout.String(), "--idempotency-key string") {
			t.Fatalf("%v flag list still advertises --idempotency-key:\n%s", cmdArgs, stdout.String())
		}
	}
}
