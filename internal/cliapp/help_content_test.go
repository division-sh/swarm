package cliapp

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
	for _, cmdArgs := range [][]string{{"--help"}, {"run", "fork", "--help"}} {
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

// Example blocks are runnable invocations, not prose: every line must start
// with "swarm", resolve to a real command path, and reference only flags that
// command defines. Trailing "# comment" annotations are allowed.
func TestExamplesReferenceRealCommandsAndFlags(t *testing.T) {
	var out, errOut bytes.Buffer
	root := newRootCommand(context.Background(), t.TempDir(), &out, &errOut)
	walkCommands(root, func(cmd *cobra.Command) {
		if strings.TrimSpace(cmd.Example) == "" {
			return
		}
		for _, rawLine := range strings.Split(cmd.Example, "\n") {
			line := rawLine
			if idx := strings.Index(line, "  #"); idx >= 0 {
				line = line[:idx]
			}
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			if fields[0] != "swarm" {
				t.Errorf("%s: example line does not start with \"swarm\": %q", cmd.CommandPath(), rawLine)
				continue
			}
			target := root
			args := fields[1:]
			for len(args) > 0 && !strings.HasPrefix(args[0], "-") {
				next := findSubcommand(target, args[0])
				if next == nil {
					break // positional argument, not a subcommand
				}
				target = next
				args = args[1:]
			}
			if target == root && len(fields) > 1 {
				t.Errorf("%s: example references unknown command %q: %q", cmd.CommandPath(), fields[1], rawLine)
				continue
			}
			flags := target.LocalFlags()
			inherited := target.InheritedFlags()
			for _, tok := range args {
				switch {
				case strings.Contains(tok, "<"):
					// placeholder value
				case strings.HasPrefix(tok, "--"):
					name := strings.TrimPrefix(tok, "--")
					if idx := strings.Index(name, "="); idx >= 0 {
						name = name[:idx]
					}
					if flags.Lookup(name) == nil && inherited.Lookup(name) == nil {
						t.Errorf("%s: example references flag --%s that %s does not define: %q", cmd.CommandPath(), name, target.CommandPath(), rawLine)
					}
				case strings.HasPrefix(tok, "-") && len(tok) == 2:
					if flags.ShorthandLookup(tok[1:]) == nil && inherited.ShorthandLookup(tok[1:]) == nil {
						t.Errorf("%s: example references shorthand %s that %s does not define: %q", cmd.CommandPath(), tok, target.CommandPath(), rawLine)
					}
				}
			}
		}
	})
}

func findSubcommand(cmd *cobra.Command, name string) *cobra.Command {
	for _, sub := range cmd.Commands() {
		if sub.Name() == name || sub.HasAlias(name) {
			return sub
		}
	}
	return nil
}
