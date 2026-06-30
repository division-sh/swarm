package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/spf13/cobra"
)

const localPreflightOwner = "cmd/swarm local claude_cli preflight/admission owner"

type localPreflightCategory string

const (
	localPreflightBackendPrerequisite        localPreflightCategory = "backend_prerequisite"
	localPreflightWorkspacePrerequisite      localPreflightCategory = "workspace_prerequisite"
	localPreflightServeListenerPrerequisite  localPreflightCategory = "serve_listener_prerequisite"
	localPreflightGatewayPrerequisite        localPreflightCategory = "gateway_prerequisite"
	localPreflightContractSecretPrerequisite localPreflightCategory = "contract_secret_prerequisite"
	localPreflightDevConveniencePrerequisite localPreflightCategory = "dev_convenience_prerequisite"
)

type localPreflightSeverity string

const (
	localPreflightSeverityInfo    localPreflightSeverity = "info"
	localPreflightSeverityWarning localPreflightSeverity = "warning"
	localPreflightSeverityBlocker localPreflightSeverity = "blocker"
)

type localPreflightFindingStatus string

const (
	localPreflightStatusOK      localPreflightFindingStatus = "ok"
	localPreflightStatusFailed  localPreflightFindingStatus = "failed"
	localPreflightStatusSkipped localPreflightFindingStatus = "skipped"
)

type localPreflightFinding struct {
	Category    localPreflightCategory      `json:"category"`
	Code        string                      `json:"code"`
	Status      localPreflightFindingStatus `json:"status"`
	Severity    localPreflightSeverity      `json:"severity"`
	Message     string                      `json:"message"`
	Remediation string                      `json:"remediation,omitempty"`
	Owner       string                      `json:"owner"`
}

type localPreflightReport struct {
	OK       bool                    `json:"ok"`
	Owner    string                  `json:"owner"`
	Mode     string                  `json:"mode"`
	Backend  string                  `json:"backend"`
	Findings []localPreflightFinding `json:"findings"`
}

type localPreflightRequest struct {
	Mode                   string
	RepoRoot               string
	Config                 *config.Config
	ResolvedPaths          cliContractPlatformSpecPaths
	DataSource             string
	WorkspaceBackend       workspaceBackendSelection
	APIListenAddr          string
	MCPListenAddr          string
	CheckListeners         bool
	CheckGatewayEnv        bool
	CheckContractSecrets   bool
	ContractSecretSeverity localPreflightSeverity
}

func runLocalClaudeCLIPreflight(ctx context.Context, req localPreflightRequest) localPreflightReport {
	report := localPreflightReport{
		OK:    true,
		Owner: localPreflightOwner,
		Mode:  strings.TrimSpace(req.Mode),
	}
	if report.Mode == "" {
		report.Mode = "unknown"
	}
	if req.Config == nil {
		report.add(localPreflightBackendPrerequisite, "config_missing", localPreflightSeverityBlocker, localPreflightStatusFailed, "runtime config is required", "load runtime config through the serve/run config owner")
		return report.finalize()
	}
	profile, err := req.Config.LLMBackendProfile()
	if err != nil {
		report.add(localPreflightBackendPrerequisite, "backend_profile_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "set a supported backend with --backend or llm.backend")
		return report.finalize()
	}
	report.Backend = strings.TrimSpace(profile.ID)
	if profile.ID != llmselection.BackendClaudeCLI {
		report.add(localPreflightBackendPrerequisite, "backend_not_claude_cli", localPreflightSeverityInfo, localPreflightStatusSkipped, fmt.Sprintf("backend %q does not require claude_cli local proof prerequisites", profile.ID), "")
		return report.finalize()
	}

	if err := llmselection.RequireCredential(profile, os.LookupEnv); err != nil {
		report.add(localPreflightBackendPrerequisite, "missing_backend_credential", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), fmt.Sprintf("set %s for claude_cli backend authentication", strings.TrimSpace(profile.Credential.EnvVar)))
	} else {
		report.add(localPreflightBackendPrerequisite, "backend_credential_present", localPreflightSeverityInfo, localPreflightStatusOK, fmt.Sprintf("%s is present", strings.TrimSpace(profile.Credential.EnvVar)), "")
	}

	if req.CheckListeners {
		report.checkListener("api_listener", "api", req.APIListenAddr)
		report.checkListener("mcp_listener", "mcp", req.MCPListenAddr)
	}
	if req.CheckGatewayEnv {
		report.checkGatewayEnv(req.MCPListenAddr)
	}

	source, contractsRoot, err := loadLocalPreflightSource(req.RepoRoot, req.ResolvedPaths)
	if err != nil {
		report.add(localPreflightWorkspacePrerequisite, "contract_source_load_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the selected --contracts and --platform-spec paths")
		return report.finalize()
	}
	if req.CheckContractSecrets {
		report.checkContractSecrets(ctx, source, req.ContractSecretSeverity)
	}
	if !sourceDeclaresAgents(source) {
		report.add(localPreflightWorkspacePrerequisite, "agent_free_source", localPreflightSeverityInfo, localPreflightStatusSkipped, "selected contract source declares no agents; claude_cli workspace proof is not required", "")
		return report.finalize()
	}
	report.checkWorkspace(ctx, req.RepoRoot, req.Config, source, contractsRoot, req.DataSource, req.WorkspaceBackend)
	return report.finalize()
}

func (r *localPreflightReport) add(category localPreflightCategory, code string, severity localPreflightSeverity, status localPreflightFindingStatus, message, remediation string) {
	if r == nil {
		return
	}
	code = strings.TrimSpace(code)
	message = strings.TrimSpace(message)
	if severity == "" {
		severity = localPreflightSeverityInfo
	}
	if status == "" {
		status = localPreflightStatusOK
	}
	r.Findings = append(r.Findings, localPreflightFinding{
		Category:    category,
		Code:        code,
		Status:      status,
		Severity:    severity,
		Message:     message,
		Remediation: strings.TrimSpace(remediation),
		Owner:       localPreflightOwner,
	})
}

func (r localPreflightReport) finalize() localPreflightReport {
	r.OK = !r.HasBlockers()
	sort.SliceStable(r.Findings, func(i, j int) bool {
		if r.Findings[i].Severity != r.Findings[j].Severity {
			return localPreflightSeverityRank(r.Findings[i].Severity) > localPreflightSeverityRank(r.Findings[j].Severity)
		}
		if r.Findings[i].Category != r.Findings[j].Category {
			return r.Findings[i].Category < r.Findings[j].Category
		}
		return r.Findings[i].Code < r.Findings[j].Code
	})
	return r
}

func localPreflightSeverityRank(severity localPreflightSeverity) int {
	switch severity {
	case localPreflightSeverityBlocker:
		return 3
	case localPreflightSeverityWarning:
		return 2
	default:
		return 1
	}
}

func (r localPreflightReport) HasBlockers() bool {
	for _, finding := range r.Findings {
		if finding.Severity == localPreflightSeverityBlocker && finding.Status == localPreflightStatusFailed {
			return true
		}
	}
	return false
}

func (r localPreflightReport) BlockerSummary() string {
	parts := []string{}
	for _, finding := range r.Findings {
		if finding.Severity == localPreflightSeverityBlocker && finding.Status == localPreflightStatusFailed {
			parts = append(parts, strings.TrimSpace(finding.Code)+": "+strings.TrimSpace(finding.Message))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}

func (r *localPreflightReport) checkListener(code, name, addr string) {
	listener, err := listenServeHTTPListener(name, addr)
	if err != nil {
		r.add(localPreflightServeListenerPrerequisite, code, localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), fmt.Sprintf("free the %s listener address or choose a different --%s-listen-addr", name, name))
		return
	}
	_ = listener.Close()
	r.add(localPreflightServeListenerPrerequisite, code, localPreflightSeverityInfo, localPreflightStatusOK, fmt.Sprintf("%s listener address %s is available", name, strings.TrimSpace(addr)), "")
}

func (r *localPreflightReport) checkGatewayEnv(mcpListenAddr string) {
	addr, ok, err := resolvedGatewayCheckAddr(mcpListenAddr)
	if err != nil {
		r.add(localPreflightGatewayPrerequisite, "mcp_listener_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "set a valid --mcp-listen-addr")
		return
	}
	if !ok {
		r.add(localPreflightGatewayPrerequisite, "gateway_env_port_deferred", localPreflightSeverityInfo, localPreflightStatusSkipped, "MCP listener uses port 0; gateway env validation is deferred until serve binds the actual listener", "")
		return
	}
	for _, name := range []string{"SWARM_TOOL_GATEWAY_URL", "SWARM_TOOL_GATEWAY_CONTAINER_URL"} {
		if err := validateExistingServeGatewayURL(name, lookupEnvValue(name), addr); err != nil {
			r.add(localPreflightGatewayPrerequisite, strings.ToLower(name)+"_stale", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), fmt.Sprintf("unset %s or point it at the selected MCP listener port", name))
		} else {
			r.add(localPreflightGatewayPrerequisite, strings.ToLower(name)+"_valid", localPreflightSeverityInfo, localPreflightStatusOK, fmt.Sprintf("%s is empty or targets the selected MCP listener", name), "")
		}
	}
}

func (r *localPreflightReport) checkContractSecrets(ctx context.Context, source semanticview.Source, severity localPreflightSeverity) {
	if severity == "" {
		severity = localPreflightSeverityWarning
	}
	store, err := buildCredentialStore()
	if err != nil {
		r.add(localPreflightContractSecretPrerequisite, "credential_store_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the local credential store or SWARM_CREDENTIALS_FILE")
		return
	}
	missing, err := runtimecredentials.MissingRequired(ctx, store, source)
	if err != nil {
		r.add(localPreflightContractSecretPrerequisite, "contract_secret_check_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix contract credential references or credential store access")
		return
	}
	if len(missing) == 0 {
		r.add(localPreflightContractSecretPrerequisite, "contract_secrets_present", localPreflightSeverityInfo, localPreflightStatusOK, "all contract-required secrets are present", "")
		return
	}
	for _, desc := range missing {
		record := secretRecordFromDescriptor(desc)
		message := fmt.Sprintf("required secret %q is missing", record.Key)
		if requiredBy := formatSecretRequirements(record.RequiredBy); requiredBy != "" {
			message += " for " + requiredBy
		}
		r.add(localPreflightContractSecretPrerequisite, "missing_contract_secret", severity, localPreflightStatusFailed, message, fmt.Sprintf("run `swarm secrets set %s` or provide the matching environment variable", record.Key))
	}
}

func (r *localPreflightReport) checkWorkspace(ctx context.Context, repo string, cfg *config.Config, source semanticview.Source, contractsRoot, dataSource string, backend workspaceBackendSelection) {
	mountSources, err := resolveWorkspaceMountSources(repo, dataSource, cfg)
	if err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_data_source_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix --data, workspace.data_source, or SWARM_WORKSPACE_DATA_SOURCE")
		return
	}
	workspaces, err := configuredWorkspaceLifecycleForServe(nil, contractsRoot, source, mountSources, backend)
	if err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_config_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix workspace backend or mount configuration")
		return
	}
	if workspaces == nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_lifecycle_missing", localPreflightSeverityBlocker, localPreflightStatusFailed, "workspace lifecycle is not configured", "select a supported workspace backend")
		return
	}
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_source_invalid", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix workspace_class declarations in the selected contracts")
		return
	}
	dockerManager, ok := workspaces.(*workspace.DockerManager)
	if !ok {
		r.add(localPreflightWorkspacePrerequisite, "workspace_backend_unsupported", localPreflightSeverityBlocker, localPreflightStatusFailed, fmt.Sprintf("workspace backend %q does not support claude_cli local proof", strings.TrimSpace(backend.Backend)), "use --workspace-backend docker for claude_cli local proof; host claude_cli support is split separately")
		return
	}
	if err := dockerManager.CheckDockerAvailable(ctx); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "docker_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "start Docker or set SWARM_DOCKER_BIN to a working Docker-compatible CLI")
	} else {
		r.add(localPreflightWorkspacePrerequisite, "docker_available", localPreflightSeverityInfo, localPreflightStatusOK, "Docker is reachable", "")
	}
	if err := dockerManager.CheckWorkspaceImageAvailable(ctx); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_image_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "build or pull the configured workspace image, or set SWARM_WORKSPACE_IMAGE")
	} else {
		r.add(localPreflightWorkspacePrerequisite, "workspace_image_available", localPreflightSeverityInfo, localPreflightStatusOK, "workspace image is available", "")
	}
	if err := dockerManager.CheckWorkspaceCLICommandAvailable(ctx, cfg.LLM.ClaudeCLI.Command); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "build or pull a workspace image that contains the configured Claude CLI command")
	} else {
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_available", localPreflightSeverityInfo, localPreflightStatusOK, "workspace image can execute the configured Claude CLI command", "")
	}
}

func loadLocalPreflightSource(repo string, paths cliContractPlatformSpecPaths) (semanticview.Source, string, error) {
	contractsRoot, err := normalizeContractsRoot(paths.ContractsPath)
	if err != nil {
		return nil, "", fmt.Errorf("resolve contracts: %w", err)
	}
	_, bundle, err := newSwarmWorkflowModule(assetCommandRepoRoot(repo), contractsRoot, paths.PlatformSpecPath)
	if err != nil {
		return nil, "", fmt.Errorf("load Swarm contracts: %w", err)
	}
	return semanticview.Wrap(bundle), contractsRoot, nil
}

func sourceDeclaresAgents(source semanticview.Source) bool {
	return source != nil && len(source.AgentEntries()) > 0
}

func resolvedGatewayCheckAddr(mcpListenAddr string) (net.Addr, bool, error) {
	addr := strings.TrimSpace(mcpListenAddr)
	if err := validateServeListenAddr("--mcp-listen-addr", addr); err != nil {
		return nil, false, err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(port) == "" || strings.TrimSpace(port) == "0" {
		return nil, false, nil
	}
	resolved, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, false, err
	}
	return resolved, true, nil
}

func lookupEnvValue(name string) string {
	return os.Getenv(name)
}

func writeLocalPreflightText(out io.Writer, report localPreflightReport) {
	if out == nil {
		return
	}
	status := "ok"
	if !report.OK {
		status = "failed"
	}
	fmt.Fprintf(out, "claude_cli preflight: %s\n", status)
	for _, finding := range report.Findings {
		fmt.Fprintf(out, "[%s] %s/%s: %s\n", strings.ToUpper(string(finding.Severity)), finding.Category, finding.Code, finding.Message)
		if finding.Remediation != "" {
			fmt.Fprintf(out, "  remediation: %s\n", finding.Remediation)
		}
	}
}

func writeLocalPreflightJSON(out io.Writer, report localPreflightReport) error {
	if out == nil {
		return nil
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func returnLocalPreflightResult(cmd *cobra.Command, report localPreflightReport, asJSON bool) error {
	if asJSON {
		if err := writeLocalPreflightJSON(cmd.OutOrStdout(), report); err != nil {
			return err
		}
	} else {
		writeLocalPreflightText(cmd.OutOrStdout(), report)
	}
	if !report.OK {
		return commandExitError{code: cliExitRuntime}
	}
	return nil
}

func localPreflightCommandSeverityForContractSecrets(mode string) localPreflightSeverity {
	if strings.EqualFold(strings.TrimSpace(mode), "doctor") {
		return localPreflightSeverityBlocker
	}
	return localPreflightSeverityWarning
}

func shouldRunServeLocalClaudeCLIPreflight(opts serveOptions) bool {
	if strings.TrimSpace(opts.BundleHash) != "" || len(opts.BundleHashes) > 0 {
		return false
	}
	return true
}

func serveLocalPreflightMode(opts serveOptions) string {
	if opts.LocalRun {
		return "run_local"
	}
	if opts.Dev {
		return "serve_dev"
	}
	return "serve"
}

func runServeLocalClaudeCLIPreflight(ctx context.Context, repo string, opts serveOptions, cfg *config.Config, resolvedPaths cliContractPlatformSpecPaths, workspaceBackend workspaceBackendSelection) localPreflightReport {
	mode := serveLocalPreflightMode(opts)
	return runLocalClaudeCLIPreflight(ctx, localPreflightRequest{
		Mode:                   mode,
		RepoRoot:               repo,
		Config:                 cfg,
		ResolvedPaths:          resolvedPaths,
		DataSource:             opts.DataSource,
		WorkspaceBackend:       workspaceBackend,
		APIListenAddr:          opts.APIListenAddr,
		MCPListenAddr:          opts.MCPListenAddr,
		CheckListeners:         true,
		CheckGatewayEnv:        true,
		CheckContractSecrets:   true,
		ContractSecretSeverity: localPreflightCommandSeverityForContractSecrets(mode),
	})
}
