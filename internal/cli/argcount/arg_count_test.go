package argcount

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestCLIArgCountDiagnosticExactMissingWithHint(t *testing.T) {
	cmd := cliArgCountTestCommand("packs", "show <pack-id>", ExactArgs(1))
	SetDiscoveryHint(cmd, "List pack IDs with `swarm packs list`.")

	err := cmd.Args(cmd, nil)
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := strings.Join([]string{
		"ERROR: 'swarm packs show' requires <pack-id>.",
		"Usage: swarm packs show <pack-id>",
		"  List pack IDs with `swarm packs list`.",
	}, "\n")
	if got := err.Error(); got != want {
		t.Fatalf("diagnostic = %q, want %q", got, want)
	}
}

func TestCLIArgCountDiagnosticExactExtraQuotesReceivedTokens(t *testing.T) {
	cmd := cliArgCountTestCommand("packs", "show <pack-id>", ExactArgs(1))
	err := cmd.Args(cmd, []string{"ab", "cd", "ef"})
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := "ERROR: 'swarm packs show' accepts one argument (<pack-id>); got 3: \"ab\" \"cd\" \"ef\"."
	if got := strings.Split(err.Error(), "\n")[0]; got != want {
		t.Fatalf("problem line = %q, want %q", got, want)
	}
}

func TestCLIArgCountDiagnosticExactMissingSecondNamesPosition(t *testing.T) {
	cmd := cliArgCountTestCommand("conversation", "turn <session-id> <turn-id-or-prefix>", ExactArgs(2))
	err := cmd.Args(cmd, []string{"sess-1"})
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := "ERROR: 'swarm conversation turn' requires <turn-id-or-prefix> (got <session-id>)."
	if got := strings.Split(err.Error(), "\n")[0]; got != want {
		t.Fatalf("problem line = %q, want %q", got, want)
	}
}

func TestCLIArgCountDiagnosticMaximumArgs(t *testing.T) {
	cmd := cliArgCountTestCommand("run", "trace [run-id]", MaximumNArgs(1))
	err := cmd.Args(cmd, []string{"run-1", "extra"})
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := "ERROR: 'swarm run trace' accepts at most one argument ([run-id]); got 2: \"run-1\" \"extra\"."
	if got := strings.Split(err.Error(), "\n")[0]; got != want {
		t.Fatalf("problem line = %q, want %q", got, want)
	}
}

func TestCLIArgCountDiagnosticRangeArgs(t *testing.T) {
	cmd := cliArgCountTestCommand("packs", "show <pack-id> [directory]", RangeArgs(1, 2))
	if err := cmd.Args(cmd, []string{"pack-id"}); err != nil {
		t.Fatalf("one argument: %v", err)
	}
	if err := cmd.Args(cmd, []string{"pack-id", "root"}); err != nil {
		t.Fatalf("two arguments: %v", err)
	}
	if err := cmd.Args(cmd, nil); err == nil || !strings.Contains(err.Error(), "requires <pack-id>") {
		t.Fatalf("missing diagnostic = %v", err)
	}
	if err := cmd.Args(cmd, []string{"pack-id", "root", "extra"}); err == nil || !strings.Contains(err.Error(), "accepts at most two arguments") {
		t.Fatalf("extra diagnostic = %v", err)
	}
}

func TestCLIArgCountDiagnosticUsePipeInsidePlaceholder(t *testing.T) {
	cmd := cliArgCountTestCommand("", "completion <bash|zsh|fish|powershell>", ExactArgs(1))
	err := cmd.Args(cmd, nil)
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	if want := "Usage: swarm completion <bash|zsh|fish|powershell>"; !strings.Contains(err.Error(), want) {
		t.Fatalf("diagnostic = %q, want usage substring %q", err.Error(), want)
	}
}

func TestCLIArgCountDiagnosticAcceptsNoPositionals(t *testing.T) {
	err := NewDiagnosticFromUse(
		"swarm health",
		"health",
		"health",
		[]string{"extra"},
		Rule{Max: 0},
		"",
	)
	want := strings.Join([]string{
		"ERROR: 'swarm health' accepts no positional arguments; got 1: \"extra\".",
		"Usage: swarm health",
	}, "\n")
	if got := err.Error(); got != want {
		t.Fatalf("diagnostic = %q, want %q", got, want)
	}
}

func cliArgCountTestCommand(parent, use string, args cobra.PositionalArgs) *cobra.Command {
	root := &cobra.Command{Use: "swarm"}
	cmd := &cobra.Command{Use: use, Args: args}
	if parent == "" {
		root.AddCommand(cmd)
		return cmd
	}
	parentCmd := &cobra.Command{Use: parent}
	root.AddCommand(parentCmd)
	parentCmd.AddCommand(cmd)
	return cmd
}
