package cliapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/spf13/cobra"
)

type doctorOptions struct {
	configPath          string
	backend             string
	dataSource          string
	dataSourceSet       bool
	workspaceBackend    string
	workspaceBackendSet bool
	platformSpecPath    string
	apiListenAddr       string
	mcpListenAddr       string
	target              bool
	asJSON              bool
	schemaInventory     bool
	apiOptions          rootCommandOptions
}

func newDoctorCommand(ctx context.Context, root InvocationRoot, rootOpts rootCommandOptions) *cobra.Command {
	opts := doctorOptions{
		apiListenAddr: defaultAPIListenAddr,
		mcpListenAddr: defaultMCPListenAddr,
		apiOptions:    rootOpts,
	}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check local prerequisites and diagnose setup problems.",
		Example: `  swarm doctor
  swarm doctor --target    # show which runtime this CLI targets`,
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := rejectRetiredPlatformSpecFlag(cmd); err != nil {
				return err
			}
			if cliAPIConnectionFlagsChanged(cmd) && !opts.target {
				return fmt.Errorf("--api-server and --api-token-file require --target")
			}
			if opts.target && opts.schemaInventory {
				return fmt.Errorf("--schema-inventory cannot be combined with --target")
			}
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
			return runDoctorCommand(ctx, root.Path(), cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.configPath, "config", opts.configPath, "Path to swarm.yaml config")
	cmd.Flags().StringVar(&opts.backend, "backend", opts.backend, "LLM backend profile to diagnose: anthropic, claude_cli, openai_compatible, or openai_responses")
	cmd.Flags().StringVar(&opts.workspaceBackend, "workspace-backend", opts.workspaceBackend, "Workspace backend for local diagnostics: docker or host")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().StringVar(&opts.apiListenAddr, "api-listen-addr", opts.apiListenAddr, "HTTP bind address to preflight for API, WebSocket, health, and readiness routes")
	cmd.Flags().StringVar(&opts.mcpListenAddr, "mcp-listen-addr", opts.mcpListenAddr, "HTTP bind address to preflight for MCP and tools routes")
	cmd.Flags().BoolVar(&opts.target, "target", false, "Explain local target, state directory, project, and context resolution without runtime preflight")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render the diagnostic report as JSON")
	cmd.Flags().BoolVar(&opts.schemaInventory, "schema-inventory", false, "Show the generated state-store table and column inventory without starting runtime")
	bindCLIAPIConnectionFlags(cmd, &opts.apiOptions)
	return cmd
}

func runDoctorCommand(ctx context.Context, repo string, cmd *cobra.Command, opts doctorOptions) error {
	if opts.target {
		return runDoctorTargetCommand(repo, cmd, opts)
	}
	envFindings := doctorSwarmEnvFindings(repo, opts.configPath)
	newReport := func() LocalPreflightReport {
		report := LocalPreflightReport{Owner: localPreflightOwner, Mode: "doctor"}
		addSwarmEnvFindingsToLocalPreflightReport(&report, envFindings)
		return report
	}
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.configPath,
		BackendOverride: opts.backend,
	})
	if err != nil {
		report := newReport()
		addUnifiedConfigDiagnosticsToReport(&report, unifiedConfigDiagnosticsFromError(err))
		report.add(localPreflightBackendPrerequisite, "config_load_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --config, --backend, retired env vars, or llm.backend")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	configReport := newReport()
	addUnifiedConfigDiagnosticsToReport(&configReport, cfgResult.Diagnostics)
	platformSpecPath := strings.TrimSpace(cfgResult.cli.Paths.PlatformSpecPath)
	if platformSpecPath == "" {
		platformSpecPath, err = EmbeddedPlatformSpecPath()
		if err != nil {
			report := configReport
			report.add(localPreflightBackendPrerequisite, "platform_spec_resolution_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "repair the embedded platform spec")
			return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
		}
	} else {
		platformSpecPath = ResolvePath(repo, platformSpecPath)
	}
	platformPackBase, err := LoadConfiguredPlatformPackBase(repo, cfgResult)
	if err != nil {
		report := configReport
		report.add(localPreflightProviderPackPrerequisite, "platform_pack_load_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix platform.packs.platform_dirs or the referenced platform pack inventory")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	if opts.schemaInventory {
		inventory, err := buildDoctorSchemaInventory(platformSpecPath)
		if err != nil {
			report := configReport
			report.add(localPreflightBackendPrerequisite, "schema_inventory_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "repair the platform store schema")
			return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
		}
		configReport.SchemaInventory = &inventory
	}
	effectiveInventory, err := packartifact.NewEffectivePackInventory(platformPackBase, nil)
	if err != nil {
		report := configReport
		report.add(localPreflightProviderPackPrerequisite, "platform_pack_load_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "repair the platform pack inventory")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	appendDoctorPackInventoryReadback(&configReport, platformSpecPath, effectiveInventory)
	_, err = BuildProviderCredentialStore()
	if err != nil {
		report := configReport
		report.add(localPreflightProviderPackPrerequisite, "channel_provider_credentials_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix provider credential configuration")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	_, err = BuildManagedCredentialStore()
	if err != nil {
		report := configReport
		report.add(localPreflightProviderPackPrerequisite, "channel_managed_credentials_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix managed credential configuration")
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
		report.add(localPreflightServeListenerPrerequisite, "listener_resolution_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --api-listen-addr, --mcp-listen-addr, config listener addresses, or SWARM_CONFIG")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	opts.apiListenAddr = apiListenAddr
	opts.mcpListenAddr = mcpListenAddr
	if err := ValidateServeListenAddr("--api-listen-addr", opts.apiListenAddr); err != nil {
		report := configReport
		report.add(localPreflightServeListenerPrerequisite, "api_listener_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --api-listen-addr or config serve.api_listen_addr")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	if err := ValidateServeListenAddr("--mcp-listen-addr", opts.mcpListenAddr); err != nil {
		report := configReport
		report.add(localPreflightServeListenerPrerequisite, "mcp_listener_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --mcp-listen-addr or config serve.mcp_listen_addr")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	workspaceBackend, err := ResolveWorkspaceBackend(opts.workspaceBackend, opts.workspaceBackendSet, cfgResult.Config)
	if err != nil {
		report := configReport
		report.add(localPreflightWorkspacePrerequisite, "workspace_backend_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --workspace-backend or workspace.backend")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	cliCfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		report := configReport
		addUnifiedConfigDiagnosticsToReport(&report, unifiedConfigDiagnosticsFromError(err))
		report.add(localPreflightBackendPrerequisite, "cli_config_load_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix SWARM_CONFIG or swarm.yaml")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(opts.apiOptions.invocationRoot, cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, cliCfg)
	if err != nil {
		report := configReport
		report.add(localPreflightBackendPrerequisite, "swarm_dir_resolution_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix --swarm-dir or config paths.swarm_dir")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	localState, err := ResolveLocalRuntimeState(LocalRuntimeStateOptions{
		RepoRoot:                repo,
		ResolvedPaths:           CLISourcePlatformSpecPaths{PlatformSpecPath: platformSpecPath},
		SwarmDir:                swarmDir,
		Config:                  cfgResult.Config,
		DataSource:              opts.dataSource,
		CreateDefaultDataSource: true,
	})
	if err != nil {
		report := configReport
		report.add(localPreflightWorkspacePrerequisite, "workspace_data_source_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "remove retired workspace data paths; declare flow_data_access or data_access, or import a dataset with swarm run start --data name=file.jsonl")
		return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
	}
	report := runLocalClaudeCLIPreflight(ctx, localPreflightRequest{
		Mode:             "doctor",
		SourceFree:       true,
		RepoRoot:         repo,
		Config:           cfgResult.Config,
		DataSource:       opts.dataSource,
		MountSources:     localState.MountSources,
		WorkspaceBackend: workspaceBackend,
		APIListenAddr:    opts.apiListenAddr,
		MCPListenAddr:    opts.mcpListenAddr,
		CheckListeners:   true,
		CheckGatewayEnv:  true,
		PlatformPackBase: platformPackBase,
	})
	report.SchemaInventory = configReport.SchemaInventory
	addUnifiedConfigDiagnosticsToReport(&report, cfgResult.Diagnostics)
	addSwarmEnvFindingsToLocalPreflightReport(&report, envFindings)
	return returnLocalPreflightResult(cmd, report.finalize(), opts.asJSON)
}

func appendDoctorPackInventoryReadback(report *LocalPreflightReport, platformSpecPath string, inventory *packartifact.EffectivePackInventory) {
	if report == nil || inventory == nil {
		return
	}
	platformSpec, err := loadChannelPlatformSpecDocument(platformSpecPath)
	if err != nil {
		return
	}
	projection, err := packadmission.Admit(inventory, platformSpec)
	if err != nil {
		return
	}
	appendProviderTriggerCapabilitySubjects(report, projection.LoadedProviderPacks)
	packReadback := packInventoryReadbackFromInventory(inventory)
	report.PackInventory = &packReadback
}
