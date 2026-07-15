package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/spf13/cobra"
)

const (
	localTargetOwner = "platform-spec.yaml#cli_specification.foundations.local_target_resolution_authority"
)

type cliSwarmDirOptions struct {
	SwarmDir        string
	SwarmDirFlagSet bool
}

type CLISwarmDirResolution struct {
	Path   string
	Source string
}

func resolveCLISwarmDir(opts cliSwarmDirOptions) (CLISwarmDirResolution, error) {
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{})
	if err != nil {
		return CLISwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(opts, cfg)
}

func resolveCLISwarmDirFromConfig(opts cliSwarmDirOptions, cfg cliCommandConfig) (CLISwarmDirResolution, error) {
	if opts.SwarmDirFlagSet {
		path, err := normalizeCLISwarmDir(opts.SwarmDir, "--swarm-dir")
		return CLISwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	if cfg.Paths.SwarmDirSet {
		path, err := normalizeCLISwarmDir(cfg.Paths.SwarmDir, "config paths.swarm_dir")
		return CLISwarmDirResolution{Path: path, Source: "config paths.swarm_dir"}, err
	}
	path, err := defaultCLISwarmDir()
	return CLISwarmDirResolution{Path: path, Source: "default ~/.swarm"}, err
}

func normalizeCLISwarmDir(raw, source string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must be non-empty", source)}
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", &cliAPIValidationError{message: fmt.Sprintf("resolve %s: %v", source, err)}
		}
		path = abs
	}
	return filepath.Clean(path), nil
}

func defaultCLISwarmDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", &cliAPIValidationError{message: fmt.Sprintf("resolve default ~/.swarm: %v", err)}
	}
	return filepath.Join(home, ".swarm"), nil
}

type doctorTargetReport struct {
	Owner           string                     `json:"owner"`
	Mode            string                     `json:"mode"`
	OK              bool                       `json:"ok"`
	SwarmDir        doctorTargetPath           `json:"swarm_dir"`
	Project         doctorTargetProject        `json:"project"`
	API             doctorTargetAPI            `json:"api"`
	Context         doctorTargetContext        `json:"context"`
	RuntimeIdentity doctorTargetPendingFact    `json:"runtime_identity"`
	Store           doctorTargetPath           `json:"store"`
	Data            doctorTargetPath           `json:"data"`
	Env             []localPreflightFinding    `json:"env,omitempty"`
	Config          []localPreflightFinding    `json:"config,omitempty"`
	CommandClasses  []doctorTargetCommandClass `json:"command_classes"`
	SplitSiblings   []string                   `json:"split_siblings"`
}

type doctorTargetPath struct {
	Path   string `json:"path,omitempty"`
	Source string `json:"source,omitempty"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type doctorTargetProject struct {
	ContractsPath          string `json:"contracts_path,omitempty"`
	ContractsSource        string `json:"contracts_source,omitempty"`
	ProjectRoot            string `json:"project_root,omitempty"`
	CanonicalProjectRoot   string `json:"canonical_project_root,omitempty"`
	CanonicalizationStatus string `json:"canonicalization_status"`
	Status                 string `json:"status"`
	Detail                 string `json:"detail,omitempty"`
}

type doctorTargetAPI struct {
	Server      string           `json:"server"`
	RPCEndpoint string           `json:"rpc_endpoint"`
	Source      string           `json:"source"`
	Auth        doctorTargetAuth `json:"auth"`
	Reason      string           `json:"reason"`
}

type doctorTargetAuth struct {
	Source string `json:"source"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type doctorTargetContext struct {
	ProjectScoped  doctorTargetPendingFact    `json:"project_scoped"`
	SelectedGlobal doctorTargetPendingFact    `json:"selected_global"`
	Registry       localContextRegistryReport `json:"registry"`
}

type doctorTargetPendingFact struct {
	Status string `json:"status"`
	Owner  string `json:"owner,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type doctorTargetCommandClass struct {
	Name        string   `json:"name"`
	Status      string   `json:"status"`
	Fallthrough string   `json:"fallthrough"`
	Commands    []string `json:"commands"`
}

func runDoctorTargetCommand(repo string, cmd *cobra.Command, opts doctorOptions) error {
	envFindings := doctorSwarmEnvFindings(repo, opts.configPath)
	envReport := doctorTargetEnvReport(envFindings)
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		if !envReport.HasBlockers() {
			return returnCLIValidationError(cmd.ErrOrStderr(), err)
		}
		cfg = cliCommandConfig{}
	}
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, cfg)
	if err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), err)
	}
	runtimeCfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.configPath,
		BackendOverride: opts.backend,
	})
	if err != nil {
		if !envReport.HasBlockers() {
			return returnCLIValidationError(cmd.ErrOrStderr(), err)
		}
		runtimeCfgResult.Config = &config.Config{}
	}
	configReport := LocalPreflightReport{Owner: unifiedConfigOwner, Mode: "doctor_target"}
	addUnifiedConfigDiagnosticsToReport(&configReport, runtimeCfgResult.Diagnostics)
	if diagnostics := unifiedConfigDiagnosticsFromError(err); len(diagnostics) > 0 {
		addUnifiedConfigDiagnosticsToReport(&configReport, diagnostics)
	}
	configReport = configReport.finalize()
	report, err := buildDoctorTargetReport(cmd.Context(), repo, opts, cfg, swarmDir, runtimeCfgResult.Config)
	if err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), err)
	}
	report.Env = envReport.Findings
	report.Config = configReport.Findings
	report.OK = report.OK && !envReport.HasBlockers() && !configReport.HasBlockers()
	if opts.asJSON {
		if err := writeDoctorTargetJSON(cmd.OutOrStdout(), report); err != nil {
			return err
		}
	} else {
		writeDoctorTargetText(cmd.OutOrStdout(), report)
	}
	if !report.OK {
		return commandExitError{code: CLIExitRuntime}
	}
	return nil
}

func doctorTargetEnvReport(findings []swarmEnvFinding) LocalPreflightReport {
	report := LocalPreflightReport{Owner: localPreflightOwner, Mode: "doctor_target"}
	addSwarmEnvFindingsToLocalPreflightReport(&report, findings)
	return report.finalize()
}

func buildDoctorTargetReport(ctx context.Context, repo string, opts doctorOptions, cfg cliCommandConfig, swarmDir CLISwarmDirResolution, runtimeCfg *config.Config) (doctorTargetReport, error) {
	api, err := resolveDoctorTargetAPI(repo, opts, cfg, swarmDir)
	if err != nil {
		return doctorTargetReport{}, err
	}
	registryReport, err := newLocalContextRegistry(swarmDir.Path).Inspect(ctx, cliRuntimeIdentityCaller{})
	if err != nil {
		return doctorTargetReport{}, err
	}
	project := resolveDoctorTargetProject(repo, opts, cfg)
	localStateProject := doctorTargetLocalRuntimeProject(repo, project)
	store, err := resolveDoctorTargetStore(repo, swarmDir, localStateProject, runtimeCfg)
	if err != nil {
		return doctorTargetReport{}, err
	}
	data := resolveDoctorTargetData(repo, opts, localStateProject, runtimeCfg)
	return doctorTargetReport{
		Owner:    localTargetOwner,
		Mode:     "target",
		OK:       true,
		SwarmDir: doctorTargetPath{Path: swarmDir.Path, Source: swarmDir.Source, Status: "resolved"},
		Project:  project,
		API:      api,
		Context: doctorTargetContext{
			ProjectScoped: doctorTargetProjectContextFact(ctx, newLocalContextRegistry(swarmDir.Path), project),
			SelectedGlobal: doctorTargetPendingFact{
				Status: registryReport.Status,
				Owner:  localContextRegistryOwner,
				Detail: registryReport.Detail,
			},
			Registry: registryReport,
		},
		RuntimeIdentity: doctorTargetRuntimeIdentityFact(registryReport),
		Store:           store,
		Data:            data,
		CommandClasses:  doctorTargetCommandClasses(),
		SplitSiblings:   doctorTargetSplitSiblings(),
	}, nil
}

func doctorTargetProjectContextFact(ctx context.Context, registry localContextRegistry, project doctorTargetProject) doctorTargetPendingFact {
	if project.Status != "resolved" || strings.TrimSpace(project.CanonicalProjectRoot) == "" {
		return doctorTargetPendingFact{
			Status: "no_project",
			Owner:  localContextRegistryOwner,
			Detail: "no resolved project root, so no project-scoped context lookup was attempted",
		}
	}
	entries, err := registry.ProjectEntries(ctx, project.CanonicalProjectRoot, cliRuntimeIdentityCaller{})
	if err != nil {
		return doctorTargetPendingFact{Status: localContextStatusPermissionDenied, Owner: localContextRegistryOwner, Detail: err.Error()}
	}
	if len(entries) == 0 {
		return doctorTargetPendingFact{
			Status: "missing",
			Owner:  localContextRegistryOwner,
			Detail: "no context descriptor is registered for this project; read-only commands may fall through, mutating/control commands require explicit target",
		}
	}
	okCount := 0
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Status == localContextStatusOK {
			okCount++
		}
		name := strings.TrimSpace(entry.Descriptor.Name)
		if name == "" {
			name = "<unknown>"
		}
		parts = append(parts, fmt.Sprintf("%s=%s", name, entry.Status))
	}
	if okCount == 1 && len(entries) == 1 {
		return doctorTargetPendingFact{Status: localContextStatusOK, Owner: localContextRegistryOwner, Detail: strings.Join(parts, ", ")}
	}
	if okCount > 1 {
		return doctorTargetPendingFact{Status: "multiple_live", Owner: localContextRegistryOwner, Detail: strings.Join(parts, ", ")}
	}
	return doctorTargetPendingFact{Status: "blocked", Owner: localContextRegistryOwner, Detail: strings.Join(parts, ", ")}
}

func doctorTargetRuntimeIdentityFact(report localContextRegistryReport) doctorTargetPendingFact {
	if report.Current == nil {
		return doctorTargetPendingFact{
			Status: "unavailable",
			Owner:  "platform-spec.yaml#api_specification.method_catalog.runtime.identity",
			Detail: "no current context descriptor is selected; explicit API flags still use existing API auth precedence",
		}
	}
	return doctorTargetPendingFact{
		Status: report.Current.Status,
		Owner:  "platform-spec.yaml#api_specification.method_catalog.runtime.identity",
		Detail: report.Current.Detail,
	}
}

func rootSwarmDirFlag(cmd *cobra.Command) (string, bool) {
	if cmd == nil || cmd.Root() == nil {
		return "", false
	}
	flag := cmd.Root().PersistentFlags().Lookup("swarm-dir")
	if flag == nil {
		return "", false
	}
	return flag.Value.String(), flag.Changed
}

func resolveDoctorTargetAPI(repo string, opts doctorOptions, cfg cliCommandConfig, swarmDir CLISwarmDirResolution) (doctorTargetAPI, error) {
	targetOpts := rootCommandOptions{
		apiServer:       opts.apiOptions.apiServer,
		apiTokenFile:    opts.apiOptions.apiTokenFile,
		contextName:     opts.apiOptions.contextName,
		swarmDir:        swarmDir.Path,
		rootFlags:       &rootCommandFlagState{swarmDir: swarmDir.Path, swarmDirSet: true},
		RepoRoot:        repo,
		apiCommandClass: cliAPICommandClassTargetDiagnostic,
	}
	target, err := resolveCLIAPITarget(targetOpts, cfg)
	if err != nil {
		return doctorTargetAPI{}, err
	}
	server, err := cliAPIServerBaseFromRPCEndpoint(target.rpcEndpoint, target.source)
	if err != nil {
		return doctorTargetAPI{}, err
	}
	token, err := resolveDoctorTargetAPITokenForTarget(rootCommandOptions{
		apiTokenFile: opts.apiOptions.apiTokenFile,
	}, cfg, target)
	auth := doctorTargetAuth{}
	if err != nil {
		if !errors.Is(err, errCLIAPITokenRequired) {
			return doctorTargetAPI{}, err
		}
		auth = doctorTargetMissingExplicitTokenAuth()
	} else {
		auth = doctorTargetAuthFromTokenResolution(token)
	}
	return doctorTargetAPI{
		Server:      server,
		RPCEndpoint: target.rpcEndpoint,
		Source:      target.source,
		Auth:        auth,
		Reason:      doctorTargetAPIReason(target.source),
	}, nil
}

func resolveDoctorTargetAPITokenForTarget(opts rootCommandOptions, cfg cliCommandConfig, target cliAPITargetResolution) (cliAPITokenResolution, error) {
	if tokenFile := strings.TrimSpace(opts.apiTokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "--api-token-file")
	}
	if target.descriptor == nil {
		return resolveDoctorTargetCLIToken(cfg, target.rpcEndpoint)
	}
	token, err := localContextDescriptorToken(*target.descriptor, target.rpcEndpoint)
	if err != nil {
		return cliAPITokenResolution{}, err
	}
	return cliAPITokenResolution{token: token, source: "context descriptor " + target.descriptor.Auth.Mode}, nil
}

func resolveDoctorTargetCLIToken(cfg cliCommandConfig, rpcEndpoint string) (cliAPITokenResolution, error) {
	if tokenFile := strings.TrimSpace(cfg.Connection.APITokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, cliAPIConfigTokenFileSource)
	}
	if cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return cliAPITokenResolution{
			token:  apiv1.DefaultLoopbackAPIToken,
			source: string(apiv1.AuthTokenSourceBuiltInLoopbackToken),
		}, nil
	}
	return cliAPITokenResolution{}, errCLIAPITokenRequired
}

func cliAPIServerBaseFromRPCEndpoint(rpcEndpoint, source string) (string, error) {
	parsed, err := normalizeCLIAPIRPCEndpoint(rpcEndpoint, source)
	if err != nil {
		return "", err
	}
	idx := strings.LastIndex(parsed, cliAPIRPCPath)
	if idx < 0 {
		return "", &cliAPIValidationError{message: fmt.Sprintf("%s must end with %s", source, cliAPIRPCPath)}
	}
	return parsed[:idx], nil
}

func doctorTargetAuthFromTokenResolution(token cliAPITokenResolution) doctorTargetAuth {
	switch token.source {
	case "--api-token-file", "config connection.api_token_file":
		return doctorTargetAuth{Source: token.source, Status: "configured", Detail: "token file"}
	case string(apiv1.AuthTokenSourceBuiltInLoopbackToken):
		return doctorTargetAuth{Source: token.source, Status: "available", Detail: "numeric loopback target"}
	default:
		return doctorTargetAuth{Source: token.source, Status: "configured", Detail: "context/runtime auth"}
	}
}

func doctorTargetMissingExplicitTokenAuth() doctorTargetAuth {
	return doctorTargetAuth{
		Source: "none",
		Status: "missing_explicit_token",
		Detail: "non-loopback target requires --api-token-file, context descriptor auth, or config connection.api_token_file",
	}
}

func doctorTargetAPIReason(source string) string {
	switch source {
	case "--api-server":
		return "explicit API server flag wins target precedence for this diagnostic"
	case "--context":
		return "explicit context flag wins target precedence after explicit API server"
	case "project context":
		return "live project-scoped context wins before selected context and config"
	case "selected context":
		return "selected context wins before typed config when no live project context is selected"
	case cliAPIConfigServerSource:
		return "typed config API source wins after explicit target and context resolution"
	default:
		return "when no explicit API source is configured, API-backed commands resolve project context, selected context, or built-in loopback according to command class"
	}
}

func resolveDoctorTargetProject(repo string, opts doctorOptions, cfg cliCommandConfig) doctorTargetProject {
	contractsPath, source := firstDoctorTargetContractsPath(repo, opts, cfg)
	if strings.TrimSpace(contractsPath) == "" {
		return doctorTargetProject{
			CanonicalizationStatus: "not_applicable",
			Status:                 "no_contract_source",
			Detail:                 "no --contracts, SWARM_CONTRACTS_PATH, config paths.contracts_path, or repo-local contracts/package.yaml source was resolved",
		}
	}
	projectRoot := inferProjectRootFromContractsPath(contractsPath)
	canonical, canonicalStatus := canonicalizeDoctorTargetPath(projectRoot)
	return doctorTargetProject{
		ContractsPath:          filepath.Clean(contractsPath),
		ContractsSource:        source,
		ProjectRoot:            filepath.Clean(projectRoot),
		CanonicalProjectRoot:   canonical,
		CanonicalizationStatus: canonicalStatus,
		Status:                 "resolved",
	}
}

func firstDoctorTargetContractsPath(repo string, opts doctorOptions, cfg cliCommandConfig) (string, string) {
	if path := strings.TrimSpace(opts.contractsPath); path != "" {
		return ResolvePath(repo, path), "--contracts"
	}
	if path := strings.TrimSpace(os.Getenv(cliContractsPathEnv)); path != "" {
		return ResolvePath(repo, path), cliContractsPathEnv
	}
	if path := strings.TrimSpace(cfg.Paths.ContractsPath); path != "" {
		return ResolvePath(repo, path), "config paths.contracts_path"
	}
	if path := discoverRepoContractsPath(repo); path != "" {
		return path, "repo contracts/package.yaml"
	}
	return "", ""
}

func inferProjectRootFromContractsPath(contractsPath string) string {
	contractsPath = filepath.Clean(contractsPath)
	if filepath.Base(contractsPath) == "package.yaml" {
		contractsPath = filepath.Dir(contractsPath)
	}
	if filepath.Base(contractsPath) == "contracts" {
		return filepath.Dir(contractsPath)
	}
	return contractsPath
}

func canonicalizeDoctorTargetPath(path string) (string, string) {
	if strings.TrimSpace(path) == "" {
		return "", "not_applicable"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path), "unavailable: " + err.Error()
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return filepath.Clean(abs), "unavailable: " + err.Error()
	}
	return filepath.Clean(real), "resolved"
}

func doctorTargetLocalRuntimeProject(repo string, project doctorTargetProject) localRuntimeStateProject {
	if project.Status != "resolved" {
		return localRuntimeStateProject{Status: "no_project", Detail: project.Detail}
	}
	canonicalProjectRoot := strings.TrimSpace(project.CanonicalProjectRoot)
	canonicalRepoRoot, _ := canonicalizeDoctorTargetPath(repo)
	projectLocal := localRuntimePathWithin(canonicalProjectRoot, canonicalRepoRoot)
	status := "borrowed_project"
	if projectLocal {
		status = "project_local"
	}
	return localRuntimeStateProject{
		ContractsPath:        project.ContractsPath,
		ProjectRoot:          project.ProjectRoot,
		CanonicalProjectRoot: canonicalProjectRoot,
		ProjectLocal:         projectLocal,
		Status:               status,
		Detail:               "doctor target dry-run",
	}
}

func resolveDoctorTargetStore(repo string, swarmDir CLISwarmDirResolution, project localRuntimeStateProject, cfg *config.Config) (doctorTargetPath, error) {
	defaultPath, defaultSource := localRuntimeSQLiteDefault(swarmDir, project)
	selection, err := resolveRuntimeStoreSelectionWithDefault(repo, storebackend.ActiveDefaultBackend().String(), false, cfg, defaultPath, defaultSource)
	if err != nil {
		return doctorTargetPath{}, err
	}
	if selection.Backend != storebackend.BackendSQLite {
		return doctorTargetPath{Source: string(selection.BackendSource), Status: "not_applicable", Detail: "postgres runtime store selected"}, nil
	}
	status := "resolved"
	detail := "target dry-run; no store directories were created"
	if err := legacyProjectSQLiteStoreError(project, selection); err != nil {
		status = "legacy_conflict"
		detail = err.Error()
	}
	return doctorTargetPath{
		Path:   selection.SQLitePath,
		Source: string(selection.SQLitePathSource),
		Status: status,
		Detail: detail,
	}, nil
}

func normalizeDoctorTargetSQLitePath(repo, raw, source string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("sqlite path from %s must be non-empty", source)
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	root := strings.TrimSpace(repo)
	if root == "" {
		return filepath.Clean(path), nil
	}
	return filepath.Clean(filepath.Join(root, path)), nil
}

func resolveDoctorTargetData(repo string, opts doctorOptions, project localRuntimeStateProject, cfg *config.Config) doctorTargetPath {
	mountSources, err := resolveWorkspaceMountSourcesForLocalState(repo, doctorTargetDataSourceFlag(opts), cfg, project, false)
	if err != nil {
		return doctorTargetPath{Status: "missing", Detail: err.Error()}
	}
	if strings.TrimSpace(mountSources.DataSource) == "" {
		return doctorTargetPath{Source: "workspace.volumes_from", Status: "no_host_data_dir", Detail: "workspace volumes_from supplies container mounts"}
	}
	return doctorTargetPath{
		Path:   mountSources.DataSource,
		Source: mountSources.DataSourceSource,
		Status: "resolved",
		Detail: "target dry-run; no data directory was created",
	}
}

func doctorTargetDataSourceFlag(opts doctorOptions) string {
	if opts.dataSourceSet {
		return opts.dataSource
	}
	return ""
}

func doctorTargetCommandClasses() []doctorTargetCommandClass {
	return []doctorTargetCommandClass{
		{
			Name:        "target_diagnostic",
			Status:      "implemented",
			Fallthrough: "not_applicable",
			Commands:    []string{"swarm doctor --target"},
		},
		{
			Name:        "read_only_inspection",
			Status:      "implemented",
			Fallthrough: "may use selected/global/default target only outside a project or when a project has no known context; stale/mismatched/corrupt/multiple project contexts fail closed",
			Commands: []string{
				"swarm run list",
				"swarm run status",
				"swarm run trace",
				"swarm health",
				"swarm logs",
				"swarm incidents",
				"swarm version --server",
				"swarm event list",
				"swarm event follow",
				"swarm event view",
				"swarm bundle list",
				"swarm bundle show",
				"swarm bundle agents",
				"swarm agent list",
				"swarm agent deliveries",
				"swarm agent diagnose",
				"swarm agent view",
				"swarm conversation list",
				"swarm conversation view",
				"swarm conversation turn",
				"swarm entity list",
				"swarm entity view",
				"swarm entity aggregate",
				"swarm mailbox list",
				"swarm mailbox view",
				"swarm forkchat list",
				"swarm forkchat view",
			},
		},
		{
			Name:        "mutating_runtime_state",
			Status:      "implemented",
			Fallthrough: "must not fall through to selected/global/default from inside a project with no live project context; requires explicit target or live project context",
			Commands: []string{
				"swarm event publish",
				"swarm event replay",
				"swarm agent restart",
				"swarm agent replay",
				"swarm agent replay-backlog",
				"swarm agent directive",
				"swarm bundle register",
				"swarm bundle delete",
				"swarm mailbox defer",
				"swarm run fork",
				"swarm forkchat new",
				"swarm forkchat resume",
				"swarm forkchat delete",
			},
		},
		{
			Name:        "control_destructive",
			Status:      "implemented",
			Fallthrough: "requires explicit or unambiguous selected target plus existing command confirmation rules",
			Commands:    []string{"swarm control pause", "swarm control stop", "swarm control nuke"},
		},
		{
			Name:        "startup_and_run",
			Status:      "implemented",
			Fallthrough: "serve --dev and default foreground swarm run start consume local runtime state authority; swarm run start blocks when a live project runtime exists unless connected targeting is explicit",
			Commands:    []string{"swarm serve --dev", "swarm run start"},
		},
	}
}

func doctorTargetSplitSiblings() []string {
	return []string{
		"project-scoped serve/API command targeting remains a broader targeting follow-up",
		"store/data migration and swarm run start semantics are implemented",
		"transport-aware descriptors and IPC/ephemeral-port direction remain broader targeting follow-up",
	}
}

func writeDoctorTargetJSON(out io.Writer, report doctorTargetReport) error {
	if out == nil {
		return nil
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func writeDoctorTargetText(out io.Writer, report doctorTargetReport) {
	if out == nil {
		return
	}
	status := "ok"
	if !report.OK {
		status = "failed"
	}
	fmt.Fprintf(out, "swarm target diagnostics: %s\n", status)
	fmt.Fprintf(out, "owner: %s\n", report.Owner)
	if len(report.Env) > 0 {
		fmt.Fprintln(out, "env:")
		for _, finding := range report.Env {
			fmt.Fprintln(out, formatLocalPreflightFinding(report.Mode, finding))
		}
	}
	if len(report.Config) > 0 {
		fmt.Fprintln(out, "config:")
		for _, finding := range report.Config {
			fmt.Fprintln(out, formatLocalPreflightFinding(report.Mode, finding))
		}
	}
	fmt.Fprintf(out, "swarm_dir: %s (source: %s)\n", report.SwarmDir.Path, report.SwarmDir.Source)
	if report.Project.Status == "resolved" {
		fmt.Fprintf(out, "project_root: %s (source: %s; canonical: %s; canonicalization: %s)\n", report.Project.ProjectRoot, report.Project.ContractsSource, report.Project.CanonicalProjectRoot, report.Project.CanonicalizationStatus)
	} else {
		fmt.Fprintf(out, "project_root: %s (%s)\n", report.Project.Status, report.Project.Detail)
	}
	fmt.Fprintf(out, "api_server: %s (source: %s)\n", report.API.Server, report.API.Source)
	fmt.Fprintf(out, "rpc_endpoint: %s\n", report.API.RPCEndpoint)
	fmt.Fprintf(out, "api_auth: %s (%s", report.API.Auth.Status, report.API.Auth.Source)
	if report.API.Auth.Detail != "" {
		fmt.Fprintf(out, "; %s", report.API.Auth.Detail)
	}
	fmt.Fprintln(out, ")")
	fmt.Fprintf(out, "target_reason: %s\n", report.API.Reason)
	fmt.Fprintf(out, "project_context: %s (%s)\n", report.Context.ProjectScoped.Status, report.Context.ProjectScoped.Owner)
	fmt.Fprintf(out, "selected_global_context: %s (%s)\n", report.Context.SelectedGlobal.Status, report.Context.SelectedGlobal.Owner)
	fmt.Fprintf(out, "descriptor_registry: %s (%s", report.Context.Registry.Status, report.Context.Registry.Owner)
	if len(report.Context.Registry.Entries) > 0 {
		fmt.Fprintf(out, "; entries=%d", len(report.Context.Registry.Entries))
	}
	if report.Context.Registry.Detail != "" {
		fmt.Fprintf(out, "; %s", report.Context.Registry.Detail)
	}
	fmt.Fprintln(out, ")")
	fmt.Fprintf(out, "runtime_identity: %s (%s)\n", report.RuntimeIdentity.Status, report.RuntimeIdentity.Owner)
	fmt.Fprintf(out, "store_path: %s (source: %s; status: %s)\n", report.Store.Path, report.Store.Source, report.Store.Status)
	if report.Data.Path != "" {
		fmt.Fprintf(out, "data_dir: %s (source: %s; status: %s)\n", report.Data.Path, report.Data.Source, report.Data.Status)
	} else {
		fmt.Fprintf(out, "data_dir: %s (source: %s; %s)\n", report.Data.Status, report.Data.Source, report.Data.Detail)
	}
	fmt.Fprintln(out, "command_classes:")
	for _, class := range report.CommandClasses {
		fmt.Fprintf(out, "  - %s: %s; fallthrough: %s\n", class.Name, class.Status, class.Fallthrough)
	}
	fmt.Fprintln(out, "split_siblings:")
	for _, sibling := range report.SplitSiblings {
		fmt.Fprintf(out, "  - %s\n", sibling)
	}
}
