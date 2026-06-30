package main

import (
	"context"
	"fmt"
	"io"
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
	if err := validateCLIAPIConnectionFlagPlacement(args); err != nil {
		if errOut != nil {
			fmt.Fprintln(errOut, err)
		}
		return cliExitValidation
	}
	if err := validateCLILoggingFlagPlacement(args); err != nil {
		if errOut != nil {
			fmt.Fprintln(errOut, err)
		}
		return cliExitValidation
	}
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
		newDoctorCommand(ctx, repo),
		newVerifyCommand(ctx, repo),
		newSecretsCommand(ctx, repo),
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
		newConversationsCommand(opts),
		newConversationCommand(opts),
		newAgentsCommand(opts),
		newAgentCommand(opts),
		newForkChatCommand(opts),
		newBundleCommand(repo, opts),
		newEntitiesCommand(opts),
		newEntityCommand(opts),
		newMailboxCommand(opts),
		newControlCommand(opts),
		newForkCommand(opts),
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
			if cmd.Flags().Changed("bundle-hash") {
				hashes, err := serveBundleHashes(opts)
				if err != nil {
					return err
				}
				opts.BundleHash = hashes[0]
				opts.BundleHashes = append([]string(nil), hashes[1:]...)
				if cmd.Flags().Changed("contracts") {
					return fmt.Errorf("--bundle-hash is mutually exclusive with --contracts")
				}
				if opts.Dev {
					return fmt.Errorf("--bundle-hash is mutually exclusive with --dev")
				}
				if cmd.Flags().Changed("store") && !strings.EqualFold(strings.TrimSpace(opts.StoreMode), "postgres") {
					return fmt.Errorf("--bundle-hash requires --store postgres")
				}
			}
			if cmd.Flags().Changed("data") {
				opts.DataSource = strings.TrimSpace(opts.DataSource)
				if opts.DataSource == "" {
					return fmt.Errorf("--data must be non-empty")
				}
			}
			if cmd.Flags().Changed("workspace-backend") {
				backend, err := normalizeWorkspaceBackend(opts.WorkspaceBackend, "--workspace-backend")
				if err != nil {
					return err
				}
				opts.WorkspaceBackend = backend
			}
			if opts.Dev {
				opts.AbandonActiveRuns = true
				opts.NoRequireBundleMatch = true
				opts.Verbose = true
			}
			if opts.NoRequireBundleMatch {
				opts.RequireBundleMatch = false
			}
			opts.StoreModeSet = cmd.Flags().Changed("store")
			opts.WorkspaceBackendSet = cmd.Flags().Changed("workspace-backend")
			if opts.ShutdownGrace <= 0 {
				return fmt.Errorf("--shutdown-grace must be a positive duration")
			}
			apiListenAddr, mcpListenAddr, err := resolveCLIServeListenerAddresses(cliServeListenerAddressOptions{
				APIListenAddr:        opts.APIListenAddr,
				MCPListenAddr:        opts.MCPListenAddr,
				APIListenAddrFlagSet: cmd.Flags().Changed("api-listen-addr"),
				MCPListenAddrFlagSet: cmd.Flags().Changed("mcp-listen-addr"),
			})
			if err != nil {
				return err
			}
			opts.APIListenAddr = apiListenAddr
			opts.MCPListenAddr = mcpListenAddr
			if err := validateServeListenAddr("--api-listen-addr", opts.APIListenAddr); err != nil {
				return err
			}
			if err := validateServeListenAddr("--mcp-listen-addr", opts.MCPListenAddr); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Output = cmd.OutOrStdout()
			code := runServe(ctx, assetCommandRepoRoot(repo), opts)
			if code != 0 {
				return commandExitError{code: code}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.ConfigPath, "config", opts.ConfigPath, "Optional path to Swarm runtime config")
	cmd.Flags().StringVar(&opts.Backend, "backend", opts.Backend, "LLM backend profile for local runtime startup: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.ContractsPath, "contracts", opts.ContractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.DataSource, "data", opts.DataSource, "Path to agent-visible read-only /data reference directory")
	cmd.Flags().StringVar(&opts.WorkspaceBackend, "workspace-backend", opts.WorkspaceBackend, "Workspace backend for local serve: docker (default isolation backend) or host (explicit trusted/unsafe local-dev opt-in)")
	cmd.Flags().StringArrayVar(&opts.BundleHashes, "bundle-hash", opts.BundleHashes, "Load a persisted bundle catalog row by canonical bundle_hash; repeat to boot multiple pinned contexts")
	cmd.Flags().StringVar(&opts.PlatformSpecPath, "platform-spec", opts.PlatformSpecPath, "Path to platform spec yaml")
	cmd.Flags().StringVar(&opts.StoreMode, "store", opts.StoreMode, runtimeStoreBackendHelp)
	cmd.Flags().StringVar(&opts.APIListenAddr, "api-listen-addr", opts.APIListenAddr, "HTTP bind address for API, WebSocket, health, and readiness routes")
	cmd.Flags().StringVar(&opts.MCPListenAddr, "mcp-listen-addr", opts.MCPListenAddr, "HTTP bind address for MCP and tools routes")
	cmd.Flags().DurationVar(&opts.ShutdownGrace, "shutdown-grace", opts.ShutdownGrace, "Time to wait for in-flight work to drain after shutdown starts")
	cmd.Flags().BoolVar(&opts.SelfCheck, "self-check", opts.SelfCheck, "Run runtime self-check during boot")
	cmd.Flags().BoolVar(&opts.Dev, "dev", opts.Dev, "Enable local development mode: abandon active runs, skip bundle-match admission, emit verbose boot output, and clean up dev entity containers on shutdown")
	cmd.Flags().BoolVar(&opts.RequireBundleMatch, "require-bundle-match", opts.RequireBundleMatch, "Refuse startup when active runs have unavailable bundle source state")
	cmd.Flags().BoolVar(&opts.NoRequireBundleMatch, "no-require-bundle-match", opts.NoRequireBundleMatch, "Allow startup even when active runs have unavailable bundle source state")
	cmd.Flags().BoolVar(&opts.AbandonActiveRuns, "abandon-active-runs", opts.AbandonActiveRuns, "Cancel active runs and quiesce recoverable work before startup recovery")
	cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", opts.Verbose, "Emit the serve boot sequence to stdout as each phase completes")
	return cmd
}

func newVerifyCommand(ctx context.Context, repo string) *cobra.Command {
	opts := defaultVerifyCommandOptions()
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Validate local Swarm contract files.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if len(args) > 0 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("unexpected argument %q", args[0]))
			}
			code := runVerifyCommandWithOutput(ctx, assetCommandRepoRoot(repo), opts, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if code != 0 {
				return commandExitError{code: code}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	bindCLIOutputFlags(cmd, &opts.output)
	bindCLILoggingFlags(cmd, &opts.logging)
	return cmd
}

type versionCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	logging    cliLoggingOptions
	server     bool
}

type versionOutputResult struct {
	BinaryVersion   string                       `json:"binary_version"`
	ModuleVersion   string                       `json:"module_version"`
	PlatformVersion string                       `json:"platform_version"`
	Commit          string                       `json:"commit"`
	Built           string                       `json:"built"`
	GoVersion       string                       `json:"go_version"`
	GOOS            string                       `json:"goos"`
	GOARCH          string                       `json:"goarch"`
	Server          *diagnosticHealthCheckResult `json:"server,omitempty"`
}

func newVersionCommand(opts rootCommandOptions) *cobra.Command {
	versionOpts := versionCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print local Swarm binary version information.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !versionOpts.server && cliAPIConnectionFlagsChanged(cmd) {
				return fmt.Errorf("--api-server and --api-token-file require --server")
			}
			if err := versionOpts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := versionOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runVersionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), versionOpts)
		},
	}
	cmd.Flags().BoolVar(&versionOpts.server, "server", false, "Also query /v1/rpc health.check and print server bundle/runtime identity")
	bindCLIOutputFlags(cmd, &versionOpts.output)
	bindCLILoggingFlags(cmd, &versionOpts.logging)
	bindCLIAPIConnectionFlags(cmd, &versionOpts.apiOptions)
	return cmd
}

func runVersionCommand(ctx context.Context, out, errOut io.Writer, opts versionCommandOptions) error {
	metadata, err := resolveLocalVersionMetadata()
	if err != nil {
		return err
	}
	result := versionOutputResult{
		BinaryVersion:   metadata.BinaryVersion,
		ModuleVersion:   metadata.ModuleVersion,
		PlatformVersion: metadata.PlatformVersion,
		Commit:          metadata.Commit,
		Built:           metadata.Built,
		GoVersion:       metadata.GoVersion,
		GOOS:            metadata.GOOS,
		GOARCH:          metadata.GOARCH,
	}
	if !opts.server {
		return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
			writeLocalVersion(w, metadata)
		}, func() ([]string, error) {
			return []string{metadata.BinaryVersion}, nil
		})
	}
	health, err := fetchDiagnosticHealthCheck(ctx, opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, cliAPIErrorClassifier{})
	}
	result.Server = &health
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeLocalVersion(w, metadata)
		writeVersionServerIdentity(w, health)
	}, func() ([]string, error) {
		return []string{metadata.BinaryVersion, health.Bundle.Fingerprint}, nil
	})
}

func writeLocalVersion(out io.Writer, metadata localVersionMetadata) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Swarm %s\n", metadata.BinaryVersion)
	fmt.Fprintf(out, "Commit: %s\n", metadata.Commit)
	fmt.Fprintf(out, "Built: %s\n", metadata.Built)
	fmt.Fprintf(out, "Go: %s %s/%s\n", metadata.GoVersion, metadata.GOOS, metadata.GOARCH)
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
