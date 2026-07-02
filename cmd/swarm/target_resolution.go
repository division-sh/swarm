package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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

type cliSwarmDirResolution struct {
	Path   string
	Source string
}

func resolveCLISwarmDir(opts cliSwarmDirOptions) (cliSwarmDirResolution, error) {
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return cliSwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(opts, cfg)
}

func resolveCLISwarmDirFromConfig(opts cliSwarmDirOptions, cfg cliAPIConfigFile) (cliSwarmDirResolution, error) {
	if opts.SwarmDirFlagSet {
		path, err := normalizeCLISwarmDir(opts.SwarmDir, "--swarm-dir")
		return cliSwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	if cfg.SwarmDirSet {
		path, err := normalizeCLISwarmDir(cfg.SwarmDir, "config swarm_dir")
		return cliSwarmDirResolution{Path: path, Source: "config swarm_dir"}, err
	}
	path, err := defaultCLISwarmDir()
	return cliSwarmDirResolution{Path: path, Source: "default ~/.swarm"}, err
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
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), err)
	}
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, cfg)
	if err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), err)
	}
	report, err := buildDoctorTargetReport(cmd.Context(), repo, opts, cfg, swarmDir)
	if err != nil {
		return returnCLIValidationError(cmd.ErrOrStderr(), err)
	}
	if opts.asJSON {
		return writeDoctorTargetJSON(cmd.OutOrStdout(), report)
	}
	writeDoctorTargetText(cmd.OutOrStdout(), report)
	return nil
}

func buildDoctorTargetReport(ctx context.Context, repo string, opts doctorOptions, cfg cliAPIConfigFile, swarmDir cliSwarmDirResolution) (doctorTargetReport, error) {
	api, err := resolveDoctorTargetAPI(opts, cfg)
	if err != nil {
		return doctorTargetReport{}, err
	}
	store, err := resolveDoctorTargetStore(repo)
	if err != nil {
		return doctorTargetReport{}, err
	}
	data, err := resolveDoctorTargetData(repo, opts)
	if err != nil {
		return doctorTargetReport{}, err
	}
	registryReport, err := newLocalContextRegistry(swarmDir.Path).Inspect(ctx, cliRuntimeIdentityCaller{})
	if err != nil {
		return doctorTargetReport{}, err
	}
	project := resolveDoctorTargetProject(repo, opts, cfg)
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

func resolveDoctorTargetAPI(opts doctorOptions, cfg cliAPIConfigFile) (doctorTargetAPI, error) {
	server, source := firstDoctorTargetAPIServer(opts, cfg)
	base, err := normalizeCLIAPIServerBase(server, source)
	if err != nil {
		return doctorTargetAPI{}, err
	}
	rpcEndpoint, err := cliAPIRPCEndpointFromServer(base.String(), source)
	if err != nil {
		return doctorTargetAPI{}, err
	}
	return doctorTargetAPI{
		Server:      base.String(),
		RPCEndpoint: rpcEndpoint,
		Source:      source,
		Auth:        resolveDoctorTargetAuth(opts, cfg, rpcEndpoint),
		Reason:      doctorTargetAPIReason(source),
	}, nil
}

func firstDoctorTargetAPIServer(opts doctorOptions, cfg cliAPIConfigFile) (string, string) {
	if server := strings.TrimSpace(opts.apiOptions.apiServer); server != "" {
		return server, "--api-server"
	}
	if server := strings.TrimSpace(os.Getenv("SWARM_API_SERVER")); server != "" {
		return server, "SWARM_API_SERVER"
	}
	if server := strings.TrimSpace(cfg.APIServer); server != "" {
		return server, "config api_server"
	}
	return defaultCLIAPIServer, "built-in loopback default"
}

func resolveDoctorTargetAuth(opts doctorOptions, cfg cliAPIConfigFile, rpcEndpoint string) doctorTargetAuth {
	if tokenFile := strings.TrimSpace(opts.apiOptions.apiTokenFile); tokenFile != "" {
		return doctorTargetAuth{Source: "--api-token-file", Status: "configured", Detail: tokenFile}
	}
	if strings.TrimSpace(os.Getenv("SWARM_API_TOKEN")) != "" {
		return doctorTargetAuth{Source: "SWARM_API_TOKEN", Status: "configured", Detail: "value redacted"}
	}
	if tokenFile := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN_FILE")); tokenFile != "" {
		return doctorTargetAuth{Source: "SWARM_API_TOKEN_FILE", Status: "configured", Detail: tokenFile}
	}
	if tokenFile := strings.TrimSpace(cfg.APITokenFile); tokenFile != "" {
		return doctorTargetAuth{Source: "config api_token_file", Status: "configured", Detail: tokenFile}
	}
	if cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return doctorTargetAuth{Source: "built-in loopback default", Status: "available", Detail: "numeric loopback target"}
	}
	return doctorTargetAuth{Source: "none", Status: "missing_explicit_token", Detail: "non-loopback targets require --api-token-file, SWARM_API_TOKEN, SWARM_API_TOKEN_FILE, or config api_token_file"}
}

func doctorTargetAPIReason(source string) string {
	switch source {
	case "--api-server":
		return "explicit API server flag wins target precedence for this diagnostic"
	case "SWARM_API_SERVER":
		return "existing explicit API environment source wins after flags"
	case "config api_server":
		return "existing bootstrap config API source wins after flags and environment"
	default:
		return "when no explicit API source is configured, API-backed commands resolve project context, selected context, or built-in loopback according to command class"
	}
}

func resolveDoctorTargetProject(repo string, opts doctorOptions, cfg cliAPIConfigFile) doctorTargetProject {
	contractsPath, source := firstDoctorTargetContractsPath(repo, opts, cfg)
	if strings.TrimSpace(contractsPath) == "" {
		return doctorTargetProject{
			CanonicalizationStatus: "not_applicable",
			Status:                 "no_contract_source",
			Detail:                 "no --contracts, SWARM_CONTRACTS_PATH, config contracts_path, or repo-local contracts/package.yaml source was resolved",
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

func firstDoctorTargetContractsPath(repo string, opts doctorOptions, cfg cliAPIConfigFile) (string, string) {
	if path := strings.TrimSpace(opts.contractsPath); path != "" {
		return resolvePath(repo, path), "--contracts"
	}
	if path := strings.TrimSpace(os.Getenv(cliContractsPathEnv)); path != "" {
		return resolvePath(repo, path), cliContractsPathEnv
	}
	if path := strings.TrimSpace(cfg.ContractsPath); path != "" {
		return resolvePath(repo, path), "config contracts_path"
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

func resolveDoctorTargetStore(repo string) (doctorTargetPath, error) {
	if raw, ok := os.LookupEnv(storebackend.EnvSQLitePath); ok {
		path, err := normalizeDoctorTargetSQLitePath(repo, raw, storebackend.EnvSQLitePath)
		if err != nil {
			return doctorTargetPath{}, err
		}
		return doctorTargetPath{
			Path:   path,
			Source: storebackend.EnvSQLitePath,
			Status: "current_behavior",
			Detail: "runtime store migration and project-local defaults remain split to #1615",
		}, nil
	}
	return doctorTargetPath{
		Path:   filepath.Clean(resolvePath(repo, storebackend.DefaultSQLiteRelativePath)),
		Source: "current rollout default",
		Status: "current_behavior",
		Detail: "runtime config store.sqlite.path is not loaded in target-only mode; store/data migration remains split to #1615",
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

func resolveDoctorTargetData(repo string, opts doctorOptions) (doctorTargetPath, error) {
	if opts.dataSourceSet {
		path, err := normalizeWorkspaceDataSourcePath(repo, opts.dataSource, "--data")
		if err != nil {
			return doctorTargetPath{}, err
		}
		return doctorTargetPath{Path: path, Source: "--data", Status: "resolved"}, nil
	}
	if raw, ok := os.LookupEnv(envWorkspaceDataSource); ok {
		path, err := normalizeWorkspaceDataSourcePath(repo, raw, envWorkspaceDataSource)
		if err != nil {
			return doctorTargetPath{}, err
		}
		return doctorTargetPath{Path: path, Source: envWorkspaceDataSource, Status: "current_behavior"}, nil
	}
	if raw, ok := os.LookupEnv(envWorkspaceVolumesFrom); ok && strings.TrimSpace(raw) != "" {
		return doctorTargetPath{
			Source: envWorkspaceVolumesFrom,
			Status: "no_host_data_dir",
			Detail: "workspace volumes_from supplies container mounts; store/data migration remains split to #1615",
		}, nil
	}
	return doctorTargetPath{
		Path:   filepath.Clean(resolvePath(repo, defaultWorkspaceDataSourceRelativePath)),
		Source: defaultWorkspaceDataSourceSource,
		Status: "current_behavior",
		Detail: "directory is reported only; target mode does not create it and migration remains split to #1615",
	}, nil
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
			Status:      "implemented_#1614",
			Fallthrough: "may use selected/global/default target only outside a project or when a project has no known context; stale/mismatched/corrupt/multiple project contexts fail closed",
			Commands: []string{
				"swarm runs",
				"swarm status",
				"swarm trace",
				"swarm health",
				"swarm logs",
				"swarm incidents",
				"swarm version --server",
				"swarm events list",
				"swarm events follow",
				"swarm event view",
				"swarm bundle list",
				"swarm bundle show",
				"swarm bundle agents",
				"swarm agents list",
				"swarm agent deliveries",
				"swarm agent diagnose",
				"swarm agent view",
				"swarm conversations list",
				"swarm conversation view",
				"swarm conversation turn",
				"swarm entities list",
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
			Status:      "implemented_#1614",
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
				"swarm mailbox approve",
				"swarm mailbox reject",
				"swarm mailbox defer",
				"swarm fork",
				"swarm forkchat new",
				"swarm forkchat resume",
				"swarm forkchat delete",
			},
		},
		{
			Name:        "control_destructive",
			Status:      "implemented_#1614",
			Fallthrough: "requires explicit or unambiguous selected target plus existing command confirmation rules",
			Commands:    []string{"swarm control pause", "swarm control stop", "swarm control nuke"},
		},
		{
			Name:        "startup_and_run",
			Status:      "implemented_serve_split_run",
			Fallthrough: "serve --dev registers a project context in #1614; swarm run default/foreground semantics remain split to #1615",
			Commands:    []string{"swarm serve --dev", "swarm run"},
		},
	}
}

func doctorTargetSplitSiblings() []string {
	return []string{
		"#1614 project-scoped serve/API command targeting",
		"#1615 store/data migration and swarm run semantics",
		"#1576 transport-aware descriptors and IPC/ephemeral-port direction",
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
	fmt.Fprintf(out, "swarm target diagnostics: ok\n")
	fmt.Fprintf(out, "owner: %s\n", report.Owner)
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
