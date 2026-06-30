package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type doctorOptions struct {
	configPath          string
	backend             string
	contractsPath       string
	dataSource          string
	workspaceBackend    string
	workspaceBackendSet bool
	platformSpecPath    string
	apiListenAddr       string
	mcpListenAddr       string
	asJSON              bool
}

func newDoctorCommand(ctx context.Context, repo string) *cobra.Command {
	opts := doctorOptions{
		apiListenAddr: defaultAPIListenAddr,
		mcpListenAddr: defaultMCPListenAddr,
	}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose local Swarm runtime prerequisites.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("data") {
				opts.dataSource = strings.TrimSpace(opts.dataSource)
				if opts.dataSource == "" {
					return fmt.Errorf("--data must be non-empty")
				}
			}
			if cmd.Flags().Changed("workspace-backend") {
				backend, err := normalizeWorkspaceBackend(opts.workspaceBackend, "--workspace-backend")
				if err != nil {
					return err
				}
				opts.workspaceBackend = backend
			}
			opts.workspaceBackendSet = cmd.Flags().Changed("workspace-backend")
			apiListenAddr, mcpListenAddr, err := resolveCLIServeListenerAddresses(cliServeListenerAddressOptions{
				APIListenAddr:        opts.apiListenAddr,
				MCPListenAddr:        opts.mcpListenAddr,
				APIListenAddrFlagSet: cmd.Flags().Changed("api-listen-addr"),
				MCPListenAddrFlagSet: cmd.Flags().Changed("mcp-listen-addr"),
			})
			if err != nil {
				return err
			}
			opts.apiListenAddr = apiListenAddr
			opts.mcpListenAddr = mcpListenAddr
			if err := validateServeListenAddr("--api-listen-addr", opts.apiListenAddr); err != nil {
				return err
			}
			if err := validateServeListenAddr("--mcp-listen-addr", opts.mcpListenAddr); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDoctorCommand(ctx, assetCommandRepoRoot(repo), cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.configPath, "config", opts.configPath, "Optional path to Swarm runtime config")
	cmd.Flags().StringVar(&opts.backend, "backend", opts.backend, "LLM backend profile to diagnose: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.dataSource, "data", opts.dataSource, "Path to agent-visible read-only /data reference directory")
	cmd.Flags().StringVar(&opts.workspaceBackend, "workspace-backend", opts.workspaceBackend, "Workspace backend for local diagnostics: docker or host")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().StringVar(&opts.apiListenAddr, "api-listen-addr", opts.apiListenAddr, "HTTP bind address to preflight for API, WebSocket, health, and readiness routes")
	cmd.Flags().StringVar(&opts.mcpListenAddr, "mcp-listen-addr", opts.mcpListenAddr, "HTTP bind address to preflight for MCP and tools routes")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render the diagnostic report as JSON")
	return cmd
}

func runDoctorCommand(ctx context.Context, repo string, cmd *cobra.Command, opts doctorOptions) error {
	if err := loadRepoDotEnv(repo); err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("load .env: %w", err))
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
	})
	if err != nil {
		report := localPreflightReport{Owner: localPreflightOwner, Mode: "doctor"}
		report.add(localPreflightBackendPrerequisite, "path_resolution_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --contracts or --platform-spec")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.configPath,
		BackendOverride: opts.backend,
	})
	if err != nil {
		report := localPreflightReport{Owner: localPreflightOwner, Mode: "doctor"}
		report.add(localPreflightBackendPrerequisite, "config_load_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --config, --backend, retired env vars, or llm.backend")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	workspaceBackend, err := resolveWorkspaceBackend(opts.workspaceBackend, opts.workspaceBackendSet, cfgResult.Config)
	if err != nil {
		report := localPreflightReport{Owner: localPreflightOwner, Mode: "doctor"}
		report.add(localPreflightWorkspacePrerequisite, "workspace_backend_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --workspace-backend, workspace.backend, or SWARM_WORKSPACE_BACKEND")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	report := runLocalClaudeCLIPreflight(ctx, localPreflightRequest{
		Mode:                   "doctor",
		RepoRoot:               repo,
		Config:                 cfgResult.Config,
		ResolvedPaths:          resolvedPaths,
		DataSource:             opts.dataSource,
		WorkspaceBackend:       workspaceBackend,
		APIListenAddr:          opts.apiListenAddr,
		MCPListenAddr:          opts.mcpListenAddr,
		CheckListeners:         true,
		CheckGatewayEnv:        true,
		CheckContractSecrets:   cmd.Flags().Changed("contracts"),
		ContractSecretSeverity: localPreflightCommandSeverityForContractSecrets("doctor"),
	})
	return returnLocalPreflightResult(cmd, report, opts.asJSON)
}
