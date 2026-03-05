package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/testutil"
)

func TestMain_EntrypointHandlesSubcommandSuccess(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "empire.yaml")
	if err := os.WriteFile(cfgFile, []byte("llm:\n  runtime_mode: cli_test\n"), 0o644); err != nil {
		t.Fatalf("write temp config file: %v", err)
	}

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })

	// Route through main() with a successful subcommand so it exits before runtime boot.
	os.Args = []string{"empire", "config", "set", "--file", cfgFile, "llm.session.rotate_after_turns", "41"}
	main()

	// Second successful call ensures main can be re-entered in test process.
	os.Args = []string{"empire", "config", "get", "--file", cfgFile, "llm.session.rotate_after_turns"}
	main()
}

func TestMain_EntrypointOperatorMailboxStatus(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	port := portFromDSN(t, dsn)
	cfgFile := writeTempEmpireConfig(t, port)

	origArgs := os.Args
	origFlagSet := flag.CommandLine
	t.Cleanup(func() {
		os.Args = origArgs
		flag.CommandLine = origFlagSet
	})
	flag.CommandLine = flag.NewFlagSet("empire-test", flag.ContinueOnError)

	// Route through main() non-subcommand path, but exit early via operator action.
	os.Args = []string{"empire", "-config", cfgFile, "-store", "postgres", "-mailbox-status"}
	main()
}
