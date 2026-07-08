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
	dataSourceSet       bool
	workspaceBackend    string
	workspaceBackendSet bool
	platformSpecPath    string
	apiListenAddr       string
	mcpListenAddr       string
	target              bool
	asJSON              bool
	apiOptions          rootCommandOptions
}

func newDoctorCommand(ctx context.Context, repo string, rootOpts rootCommandOptions) *cobra.Command {
	opts := doctorOptions{
		apiListenAddr: defaultAPIListenAddr,
		mcpListenAddr: defaultMCPListenAddr,
	}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check local prerequisites and diagnose setup problems.",
		Example: `  swarm doctor
  swarm doctor --target    # show which runtime this CLI targets`,
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if cliAPIConnectionFlagsChanged(cmd) && !opts.target {
				return fmt.Errorf("--api-server and --api-token-file require --target")
			}
			if cmd.Flags().Changed("data") {
				opts.dataSource = strings.TrimSpace(opts.dataSource)
				if opts.dataSource == "" {
					return fmt.Errorf("--data must be non-empty")
				}
			}
			opts.dataSourceSet = cmd.Flags().Changed("data")
			if path, set, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config")); err != nil {
				return err
			} else if set {
				opts.configPath = path
			}
			if opts.target {
				return nil
			}
			if cmd.Flags().Changed("workspace-backend") {
				backend, err := normalizeWorkspaceBackend(opts.workspaceBackend, "--workspace-backend")
				if err != nil {
					return err
				}
				opts.workspaceBackend = backend
			}
			opts.workspaceBackendSet = cmd.Flags().Changed("workspace-backend")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDoctorCommand(ctx, assetCommandRepoRoot(repo), cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.configPath, "config", opts.configPath, "Optional path to unified Swarm config (swarm.yaml)")
	cmd.Flags().StringVar(&opts.backend, "backend", opts.backend, "LLM backend profile to diagnose: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.dataSource, "data", opts.dataSource, "Path to agent-visible read-only /data reference directory")
	cmd.Flags().StringVar(&opts.workspaceBackend, "workspace-backend", opts.workspaceBackend, "Workspace backend for local diagnostics: docker or host")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().StringVar(&opts.apiListenAddr, "api-listen-addr", opts.apiListenAddr, "HTTP bind address to preflight for API, WebSocket, health, and readiness routes")
	cmd.Flags().StringVar(&opts.mcpListenAddr, "mcp-listen-addr", opts.mcpListenAddr, "HTTP bind address to preflight for MCP and tools routes")
	cmd.Flags().BoolVar(&opts.target, "target", false, "Explain local target, state directory, project, and context resolution without runtime preflight")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render the diagnostic report as JSON")
	opts.apiOptions.rootFlags = rootOpts.rootFlags
	bindCLIAPIConnectionFlags(cmd, &opts.apiOptions)
	return cmd
}

func runDoctorCommand(ctx context.Context, repo string, cmd *cobra.Command, opts doctorOptions) error {
	if opts.target {
		return runDoctorTargetCommand(repo, cmd, opts)
	}
	envFindings := doctorSwarmEnvFindings(repo, opts.configPath)
	newReport := func() localPreflightReport {
		report := localPreflightReport{Owner: localPreflightOwner, Mode: "doctor"}
		addSwarmEnvFindingsToLocalPreflightReport(&report, envFindings)
		return report
	}
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.configPath,
		BackendOverride: opts.backend,
	})
	if err != nil {
		report := newReport()
		addUnifiedConfigDiagnosticsToReport(&report, unifiedConfigDiagnosticsFromError(err))
		report.add(localPreflightBackendPrerequisite, "config_load_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --config, --backend, retired env vars, or llm.backend")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	configReport := newReport()
	addUnifiedConfigDiagnosticsToReport(&configReport, cfgResult.Diagnostics)
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	})
	if err != nil {
		report := configReport
		report.add(localPreflightBackendPrerequisite, "path_resolution_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --contracts or --platform-spec")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	providerPackLoad, err := loadConfiguredProviderTriggerPacks(repo, cfgResult)
	if err != nil {
		report := configReport
		report.add(localPreflightProviderPackPrerequisite, "provider_trigger_pack_load_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider_triggers.packs.external_dirs or the referenced provider pack envelope")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	apiListenAddr, mcpListenAddr, err := resolveCLIServeListenerAddresses(cliServeListenerAddressOptions{
		APIListenAddr:        opts.apiListenAddr,
		MCPListenAddr:        opts.mcpListenAddr,
		APIListenAddrFlagSet: cmd.Flags().Changed("api-listen-addr"),
		MCPListenAddrFlagSet: cmd.Flags().Changed("mcp-listen-addr"),
		ConfigPath:           opts.configPath,
		RepoRoot:             repo,
	})
	if err != nil {
		report := configReport
		addUnifiedConfigDiagnosticsToReport(&report, unifiedConfigDiagnosticsFromError(err))
		report.add(localPreflightServeListenerPrerequisite, "listener_resolution_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --api-listen-addr, --mcp-listen-addr, config listener addresses, or SWARM_CONFIG")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	opts.apiListenAddr = apiListenAddr
	opts.mcpListenAddr = mcpListenAddr
	if err := validateServeListenAddr("--api-listen-addr", opts.apiListenAddr); err != nil {
		report := configReport
		report.add(localPreflightServeListenerPrerequisite, "api_listener_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --api-listen-addr or config serve.api_listen_addr")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	if err := validateServeListenAddr("--mcp-listen-addr", opts.mcpListenAddr); err != nil {
		report := configReport
		report.add(localPreflightServeListenerPrerequisite, "mcp_listener_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --mcp-listen-addr or config serve.mcp_listen_addr")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	workspaceBackend, err := resolveWorkspaceBackend(opts.workspaceBackend, opts.workspaceBackendSet, cfgResult.Config)
	if err != nil {
		report := configReport
		report.add(localPreflightWorkspacePrerequisite, "workspace_backend_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --workspace-backend or workspace.backend")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	cliCfg, err := loadCLIAPIConfigFileWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		report := configReport
		addUnifiedConfigDiagnosticsToReport(&report, unifiedConfigDiagnosticsFromError(err))
		report.add(localPreflightBackendPrerequisite, "cli_config_load_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix SWARM_CONFIG or the unified Swarm config (swarm.yaml)")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, cliCfg)
	if err != nil {
		report := configReport
		report.add(localPreflightBackendPrerequisite, "swarm_dir_resolution_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --swarm-dir or config paths.swarm_dir")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	localState, err := resolveLocalRuntimeState(localRuntimeStateOptions{
		RepoRoot:                repo,
		ResolvedPaths:           resolvedPaths,
		SwarmDir:                swarmDir,
		Config:                  cfgResult.Config,
		DataSource:              opts.dataSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		report := configReport
		report.add(localPreflightWorkspacePrerequisite, "workspace_data_source_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --data or workspace.data_source")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	report := runLocalClaudeCLIPreflight(ctx, localPreflightRequest{
		Mode:                   "doctor",
		RepoRoot:               repo,
		Config:                 cfgResult.Config,
		ResolvedPaths:          resolvedPaths,
		DataSource:             opts.dataSource,
		MountSources:           localState.MountSources,
		WorkspaceBackend:       workspaceBackend,
		APIListenAddr:          opts.apiListenAddr,
		MCPListenAddr:          opts.mcpListenAddr,
		CheckListeners:         true,
		CheckGatewayEnv:        true,
		CheckContractSecrets:   cmd.Flags().Changed("contracts"),
		ContractSecretSeverity: localPreflightCommandSeverityForContractSecrets("doctor"),
	})
	appendProviderTriggerPackSurfaceFindings(&report, providerPackLoad.Loaded)
	addUnifiedConfigDiagnosticsToReport(&report, cfgResult.Diagnostics)
	addSwarmEnvFindingsToLocalPreflightReport(&report, envFindings)
	return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
}
