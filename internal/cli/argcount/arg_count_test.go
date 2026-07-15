package argcount

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestCLIArgCountDiagnosticExactMissingWithHint(t *testing.T) {
	cmd := cliArgCountTestCommand("bundle", "show <bundle-hash>", ExactArgs(1))
	SetDiscoveryHint(cmd, "List bundle hashes with `swarm bundle list`.")

	err := cmd.Args(cmd, nil)
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := strings.Join([]string{
		"ERROR: 'swarm bundle show' requires <bundle-hash>.",
		"Usage: swarm bundle show <bundle-hash>",
		"  List bundle hashes with `swarm bundle list`.",
	}, "\n")
	if got := err.Error(); got != want {
		t.Fatalf("diagnostic = %q, want %q", got, want)
	}
}

func TestCLIArgCountDiagnosticExactExtraQuotesReceivedTokens(t *testing.T) {
	cmd := cliArgCountTestCommand("bundle", "show <bundle-hash>", ExactArgs(1))
	err := cmd.Args(cmd, []string{"ab", "cd", "ef"})
	if err == nil {
		t.Fatal("Args returned nil, want diagnostic")
	}
	want := "ERROR: 'swarm bundle show' accepts one argument (<bundle-hash>); got 3: \"ab\" \"cd\" \"ef\"."
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

func TestCLIArgCountDiagnosticRegisterContractsAcceptsNoPositionals(t *testing.T) {
	err := NewDiagnosticFromUse(
		"swarm bundle register",
		"register",
		"register <registration-envelope-yaml> | --contracts <contracts-directory>",
		[]string{"envelope.yaml"},
		Rule{Max: 0},
		"",
	)
	want := strings.Join([]string{
		"ERROR: 'swarm bundle register' accepts no positional arguments; got 1: \"envelope.yaml\".",
		"Usage: swarm bundle register <registration-envelope-yaml> | --contracts <contracts-directory>",
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
