package main

import (
	"context"
	"fmt"
	"io"
	goruntime "runtime"
	"strings"

	"github.com/spf13/cobra"
)

var (
	binaryVersion = "dev"
	binaryCommit  = "unknown"
	binaryDate    = "unknown"
)

type commandExitError struct {
	code int
}

func (e commandExitError) Error() string {
	return fmt.Sprintf("exit %d", e.code)
}

func executeRootCommand(ctx context.Context, repo string, args []string, out, errOut io.Writer) int {
	return executeRootCommandWithOptions(ctx, repo, args, out, errOut, defaultRootCommandOptions())
}

func executeRootCommandWithOptions(ctx context.Context, repo string, args []string, out, errOut io.Writer, opts rootCommandOptions) int {
	cmd := newRootCommandWithOptions(ctx, repo, out, errOut, opts)
	cmd.SetArgs(args)
	if err := cmd.ExecuteContext(ctx); err != nil {
		if exit, ok := err.(commandExitError); ok {
			return exit.code
		}
		if errOut != nil {
			fmt.Fprintln(errOut, err)
		}
		return 2
	}
	return 0
}

func newRootCommand(ctx context.Context, repo string, out, errOut io.Writer) *cobra.Command {
	return newRootCommandWithOptions(ctx, repo, out, errOut, defaultRootCommandOptions())
}

func newRootCommandWithOptions(ctx context.Context, repo string, out, errOut io.Writer, opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "swarm",
		Short:         "Run and inspect Swarm workflows.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.AddCommand(
		newServeCommand(ctx, repo, opts.runServe),
		newRunCommand(repo, opts),
		newVerifyCommand(ctx, repo),
		newVersionCommand(opts),
		newCompletionCommand(),
		newRunsCommand(opts),
		newStatusCommand(opts),
		newTraceCommand(opts),
		newHealthCommand(opts),
		newLogsCommand(opts),
		newIncidentsCommand(opts),
		newInvestigateCommand(opts),
		newEventsCommand(opts),
		newEventCommand(opts),
		newAgentsCommand(opts),
		newAgentCommand(opts),
		newMailboxCommand(opts),
		newControlCommand(opts),
		newRetiredForkCommand(),
	)
	return cmd
}

func newServeCommand(ctx context.Context, repo string, runServe func(context.Context, string, serveOptions) int) *cobra.Command {
	opts := defaultServeOptions()
	if runServe == nil {
		runServe = runServeRuntime
	}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Swarm runtime, API, health, and MCP surfaces.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("require-bundle-match") && cmd.Flags().Changed("no-require-bundle-match") && opts.RequireBundleMatch && opts.NoRequireBundleMatch {
				return fmt.Errorf("--require-bundle-match and --no-require-bundle-match cannot both be set")
			}
			if opts.Dev && cmd.Flags().Changed("require-bundle-match") && opts.RequireBundleMatch {
				return fmt.Errorf("--dev cannot be combined with --require-bundle-match")
			}
			if opts.Dev {
				opts.AbandonActiveRuns = true
				opts.NoRequireBundleMatch = true
				opts.Verbose = true
			}
			if opts.NoRequireBundleMatch {
				opts.RequireBundleMatch = false
			}
			if opts.ShutdownGrace <= 0 {
				return fmt.Errorf("--shutdown-grace must be a positive duration")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Output = cmd.OutOrStdout()
			code := runServe(ctx, repo, opts)
			if code != 0 {
				return commandExitError{code: code}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.ConfigPath, "config", opts.ConfigPath, "Optional path to Swarm runtime config")
	cmd.Flags().StringVar(&opts.ContractsPath, "contracts", opts.ContractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.PlatformSpecPath, "platform-spec", opts.PlatformSpecPath, "Path to platform spec yaml")
	cmd.Flags().StringVar(&opts.StoreMode, "store", opts.StoreMode, "Store mode: postgres")
	cmd.Flags().StringVar(&opts.HealthAddr, "health-addr", opts.HealthAddr, "HTTP bind address for the unified serve listener: health, readiness, API, WebSocket, MCP, and tools routes")
	cmd.Flags().DurationVar(&opts.ShutdownGrace, "shutdown-grace", opts.ShutdownGrace, "Time to wait for in-flight work to drain after shutdown starts")
	cmd.Flags().BoolVar(&opts.SelfCheck, "self-check", opts.SelfCheck, "Run runtime self-check during boot")
	cmd.Flags().BoolVar(&opts.Dev, "dev", opts.Dev, "Enable local development mode: abandon active runs, skip bundle-match admission, emit verbose boot output, and clean up dev entity containers on shutdown")
	cmd.Flags().BoolVar(&opts.RequireBundleMatch, "require-bundle-match", opts.RequireBundleMatch, "Refuse startup when active runs belong to a different non-NULL bundle fingerprint")
	cmd.Flags().BoolVar(&opts.NoRequireBundleMatch, "no-require-bundle-match", opts.NoRequireBundleMatch, "Allow startup even when active runs belong to a different bundle fingerprint")
	cmd.Flags().BoolVar(&opts.AbandonActiveRuns, "abandon-active-runs", opts.AbandonActiveRuns, "Cancel active runs and quiesce recoverable work before startup recovery")
	cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", opts.Verbose, "Emit the serve boot sequence to stdout as each phase completes")
	return cmd
}

func newVerifyCommand(ctx context.Context, repo string) *cobra.Command {
	return &cobra.Command{
		Use:                "verify",
		Short:              "Validate local Swarm contract files.",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			code := runVerifyCommand(ctx, repo, args, cmd.OutOrStdout())
			if code != 0 {
				return commandExitError{code: code}
			}
			return nil
		},
	}
}

type versionCommandOptions struct {
	apiOptions rootCommandOptions
	server     bool
}

func newVersionCommand(opts rootCommandOptions) *cobra.Command {
	versionOpts := versionCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print local Swarm binary version information.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVersionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), versionOpts)
		},
	}
	cmd.Flags().BoolVar(&versionOpts.server, "server", false, "Also query /v1/rpc health.check and print server bundle/runtime identity")
	return cmd
}

func runVersionCommand(ctx context.Context, out, errOut io.Writer, opts versionCommandOptions) error {
	if !opts.server {
		writeLocalVersion(out)
		return nil
	}
	result, err := fetchDiagnosticHealthCheck(ctx, opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, cliAPIErrorClassifier{})
	}
	writeLocalVersion(out)
	writeVersionServerIdentity(out, result)
	return nil
}

func writeLocalVersion(out io.Writer) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Swarm %s\n", binaryVersion)
	fmt.Fprintf(out, "Commit: %s\n", binaryCommit)
	fmt.Fprintf(out, "Built: %s\n", binaryDate)
	fmt.Fprintf(out, "Go: %s %s/%s\n", goruntime.Version(), goruntime.GOOS, goruntime.GOARCH)
}

func writeVersionServerIdentity(out io.Writer, result diagnosticHealthCheckResult) {
	if out == nil {
		return
	}
	fmt.Fprintln(out, "Server:")
	writeDiagnosticHealth(out, result)
}

func newCompletionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "completion <bash|zsh|fish|powershell>",
		Short: "Generate shell completion scripts.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			root := cmd.Root()
			switch strings.ToLower(strings.TrimSpace(args[0])) {
			case "bash":
				return root.GenBashCompletion(cmd.OutOrStdout())
			case "zsh":
				return root.GenZshCompletion(cmd.OutOrStdout())
			case "fish":
				return root.GenFishCompletion(cmd.OutOrStdout(), true)
			case "powershell":
				return root.GenPowerShellCompletion(cmd.OutOrStdout())
			default:
				return fmt.Errorf("unsupported completion shell %q", args[0])
			}
		},
	}
}

func newRetiredForkCommand() *cobra.Command {
	return &cobra.Command{
		Use:                "fork",
		Short:              "Removed v1 command; use future swarm control run fork.",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			writeForkRetiredMessage(cmd.ErrOrStderr())
			return commandExitError{code: 2}
		},
	}
}

func writeForkRetiredMessage(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "ERROR: `swarm fork` was removed in v1.")
	fmt.Fprintln(w, "  Forking is a mutating control action; use")
	fmt.Fprintln(w, "  `swarm control run fork <run-id>` once that command ships.")
	fmt.Fprintln(w, "  For v1, manual run forking goes through the API owner; see `run.start` and the API spec.")
}
