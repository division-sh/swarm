package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/spf13/cobra"
)

const localPreflightOwner = "internal/cliapp local claude_cli preflight/admission owner"

type localPreflightCategory string

const (
	localPreflightBackendPrerequisite        localPreflightCategory = "backend_prerequisite"
	localPreflightWorkspacePrerequisite      localPreflightCategory = "workspace_prerequisite"
	localPreflightServeListenerPrerequisite  localPreflightCategory = "serve_listener_prerequisite"
	localPreflightGatewayPrerequisite        localPreflightCategory = "gateway_prerequisite"
	localPreflightContractSecretPrerequisite localPreflightCategory = "contract_secret_prerequisite"
	localPreflightProviderPackPrerequisite   localPreflightCategory = "provider_pack_prerequisite"
	localPreflightDevConveniencePrerequisite localPreflightCategory = "dev_convenience_prerequisite"
	localPreflightEnvPrerequisite            localPreflightCategory = "env"
	localPreflightConfigPrerequisite         localPreflightCategory = "config"
)

type LocalPreflightSeverity string

const (
	LocalPreflightSeverityInfo    LocalPreflightSeverity = "info"
	LocalPreflightSeverityWarning LocalPreflightSeverity = "warning"
	LocalPreflightSeverityBlocker LocalPreflightSeverity = "blocker"
)

type LocalPreflightFindingStatus string

const (
	LocalPreflightStatusOK      LocalPreflightFindingStatus = "ok"
	LocalPreflightStatusFailed  LocalPreflightFindingStatus = "failed"
	LocalPreflightStatusSkipped LocalPreflightFindingStatus = "skipped"
)

type localPreflightFinding struct {
	Category    localPreflightCategory      `json:"category"`
	Code        string                      `json:"code"`
	Status      LocalPreflightFindingStatus `json:"status"`
	Severity    LocalPreflightSeverity      `json:"severity"`
	Message     string                      `json:"message"`
	Remediation string                      `json:"remediation,omitempty"`
	Owner       string                      `json:"owner"`
}

type LocalPreflightReport struct {
	OK                 bool                    `json:"ok"`
	Owner              string                  `json:"owner"`
	Mode               string                  `json:"mode"`
	Backend            string                  `json:"backend"`
	CapabilitySubjects []packs.Subject         `json:"capability_subjects,omitempty"`
	Findings           []localPreflightFinding `json:"findings"`
	SchemaInventory    *doctorSchemaInventory  `json:"schema_inventory,omitempty"`
}

type localPreflightRequest struct {
	Mode                   string
	RepoRoot               string
	Config                 *config.Config
	ResolvedPaths          CLIContractPlatformSpecPaths
	DataSource             string
	MountSources           WorkspaceMountSources
	WorkspaceBackend       WorkspaceBackendSelection
	APIListenAddr          string
	MCPListenAddr          string
	CheckListeners         bool
	CheckGatewayEnv        bool
	CheckContractSecrets   bool
	ContractSecretSeverity LocalPreflightSeverity
	ProviderTriggerPacks   []providertriggers.LoadedPack
	ProviderTriggerCatalog *providertriggers.CatalogSnapshot
}

func runLocalClaudeCLIPreflight(ctx context.Context, req localPreflightRequest) LocalPreflightReport {
	report := LocalPreflightReport{
		OK:    true,
		Owner: localPreflightOwner,
		Mode:  strings.TrimSpace(req.Mode),
	}
	if report.Mode == "" {
		report.Mode = "unknown"
	}
	if req.Config == nil {
		report.add(localPreflightBackendPrerequisite, "config_missing", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, "swarm.yaml config is required", "load swarm.yaml through the serve/run config owner")
		return report.finalize()
	}
	profile, err := req.Config.LLMBackendProfile()
	if err != nil {
		report.add(localPreflightBackendPrerequisite, "backend_profile_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "set a supported backend with --backend or llm.backend")
		return report.finalize()
	}
	report.Backend = strings.TrimSpace(profile.ID)
	if profile.ID != llmselection.BackendClaudeCLI && report.Mode != "doctor" {
		if _, _, ok := loadLocalPreflightCapabilitySource(ctx, req, &report); !ok {
			return report.finalize()
		}
		report.add(localPreflightBackendPrerequisite, "backend_not_claude_cli", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, fmt.Sprintf("backend %q does not require claude_cli local proof prerequisites", profile.ID), "")
		return report.finalize()
	}
	if req.CheckListeners {
		report.checkListener("api_listener", "api", req.APIListenAddr)
		report.checkListener("mcp_listener", "mcp", req.MCPListenAddr)
	}
	if req.CheckGatewayEnv {
		report.checkGatewayEnv()
	}

	source, contractsRoot, ok := loadLocalPreflightCapabilitySource(ctx, req, &report)
	if !ok {
		return report.finalize()
	}
	workspaceBackend, err := DecideWorkspaceBackend(req.WorkspaceBackend, req.Config, source)
	if err != nil {
		message, remediation := workspaceBackendDecisionDiagnostic(err)
		report.add(localPreflightWorkspacePrerequisite, "workspace_backend_decision_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, message, remediation)
		return report.finalize()
	}
	status := LocalPreflightStatusOK
	severity := LocalPreflightSeverityInfo
	code := "workspace_backend_decision"
	remediation := ""
	if workspaceBackend.UnsafeHost {
		severity = LocalPreflightSeverityWarning
		code = "workspace_backend_unsafe_host"
		remediation = "use Docker for container isolation unless this is a trusted local-only run"
	}
	report.add(localPreflightWorkspacePrerequisite, code, severity, status, workspaceBackendDecisionDetail(workspaceBackend), remediation)
	if req.CheckContractSecrets {
		report.checkContractSecrets(ctx, source, req.ContractSecretSeverity)
	}
	if profile.ID != llmselection.BackendClaudeCLI {
		report.add(localPreflightBackendPrerequisite, "backend_not_claude_cli", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, fmt.Sprintf("backend %q does not require claude_cli local proof prerequisites", profile.ID), "")
		return report.finalize()
	}
	if !sourceDeclaresAgents(source) {
		report.add(localPreflightBackendPrerequisite, "backend_credential_skipped_agent_free", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "selected contract source declares no agents; claude_cli backend credential is not required", "")
		report.add(localPreflightWorkspacePrerequisite, "agent_free_source", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "selected contract source declares no agents; claude_cli workspace proof is not required", "")
		return report.finalize()
	}
	providerCredentialStore, err := BuildProviderCredentialStore()
	if err != nil {
		report.add(localPreflightBackendPrerequisite, "provider_credential_store_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the local credential store used by swarm secrets")
		return report.finalize()
	}
	resolver := runtimellm.NewProviderCredentialResolver(providerCredentialStore)
	credential, err := resolver.Inspect(ctx, profile)
	if err != nil {
		report.add(localPreflightBackendPrerequisite, "backend_credential_check_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the local credential store used by swarm secrets")
	} else if strings.TrimSpace(credential.Value) == "" {
		_, err := resolver.Resolve(ctx, profile)
		if err == nil {
			err = fmt.Errorf("%s is required for %s", runtimellm.ProviderCredentialKey(profile), profile.Credential.Purpose)
		}
		report.add(localPreflightBackendPrerequisite, "missing_backend_credential", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), fmt.Sprintf("store %s with `swarm secrets set %s`", runtimellm.ProviderCredentialKey(profile), runtimellm.ProviderCredentialKey(profile)))
	} else {
		message := fmt.Sprintf("%s is present in swarm secrets", runtimellm.ProviderCredentialKey(profile))
		if credential.EnvShadowed {
			message += "; process env value is ignored"
		}
		report.add(localPreflightBackendPrerequisite, "backend_credential_present", LocalPreflightSeverityInfo, LocalPreflightStatusOK, message, "")
	}
	report.checkWorkspace(ctx, req.Config, source, contractsRoot, req.MountSources, workspaceBackend, req.Config.LLM.ClaudeCLI.Command)
	return report.finalize()
}

func workspaceBackendDecisionDiagnostic(err error) (string, string) {
	message := err.Error()
	remediation := "fix workspace.backend, workspace.allow_exec_on_host, or the selected contract capabilities"
	var decisionErr *workspaceBackendDecisionError
	if errors.As(err, &decisionErr) {
		message = decisionErr.Problem
		remediation = decisionErr.Remediation
	}
	return message, remediation
}

func WriteWorkspaceBackendDecisionFailure(out io.Writer, location string, err error) {
	if out == nil || err == nil {
		return
	}
	message, remediation := workspaceBackendDecisionDiagnostic(err)
	fmt.Fprintln(out, formatLocalPreflightFinding(location, localPreflightFinding{
		Category:    localPreflightWorkspacePrerequisite,
		Code:        "workspace_backend_decision_failed",
		Status:      LocalPreflightStatusFailed,
		Severity:    LocalPreflightSeverityBlocker,
		Message:     message,
		Remediation: remediation,
		Owner:       localPreflightOwner,
	}))
}

func WriteWorkspacePrerequisiteFailure(out io.Writer, location string, err error) bool {
	if out == nil || err == nil {
		return false
	}
	var prerequisiteErr *workspace.PrerequisiteError
	if !errors.As(err, &prerequisiteErr) {
		return false
	}
	fmt.Fprintln(out, formatLocalPreflightFinding(location, localPreflightFinding{
		Category:    localPreflightWorkspacePrerequisite,
		Code:        "workspace_prerequisite_failed",
		Status:      LocalPreflightStatusFailed,
		Severity:    LocalPreflightSeverityBlocker,
		Message:     prerequisiteErr.Problem,
		Remediation: prerequisiteErr.Remediation,
		Owner:       localPreflightOwner,
	}))
	return true
}

func loadLocalPreflightCapabilitySource(ctx context.Context, req localPreflightRequest, report *LocalPreflightReport) (semanticview.Source, string, bool) {
	appendProviderTriggerCapabilitySubjects(report, req.ProviderTriggerPacks)
	source, contractsRoot, err := loadLocalPreflightSource(req.RepoRoot, req.ResolvedPaths)
	if err != nil {
		message := err.Error()
		remediation := "fix the selected --contracts and --platform-spec paths"
		if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
			message = diagnostic.Problem
			if strings.TrimSpace(diagnostic.Remediation) != "" {
				remediation = diagnostic.Remediation
			}
		}
		report.add(localPreflightWorkspacePrerequisite, "contract_source_load_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, message, remediation)
		return nil, "", false
	}
	appendProviderConnectorCapabilitySubjects(ctx, report, source)
	appendEffectiveProviderTriggerCapabilitySubjects(report, source, req.ProviderTriggerCatalog)
	return source, contractsRoot, true
}

func (r *LocalPreflightReport) add(category localPreflightCategory, code string, severity LocalPreflightSeverity, status LocalPreflightFindingStatus, message, remediation string) {
	r.addWithOwner(category, code, severity, status, message, remediation, localPreflightOwner)
}

func (r *LocalPreflightReport) addWithOwner(category localPreflightCategory, code string, severity LocalPreflightSeverity, status LocalPreflightFindingStatus, message, remediation, owner string) {
	if r == nil {
		return
	}
	code = strings.TrimSpace(code)
	message = strings.TrimSpace(message)
	if severity == "" {
		severity = LocalPreflightSeverityInfo
	}
	if status == "" {
		status = LocalPreflightStatusOK
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		owner = localPreflightOwner
	}
	r.Findings = append(r.Findings, localPreflightFinding{
		Category:    category,
		Code:        code,
		Status:      status,
		Severity:    severity,
		Message:     message,
		Remediation: strings.TrimSpace(remediation),
		Owner:       owner,
	})
}

func (r *LocalPreflightReport) addCapabilitySubjects(subjects []packs.Subject) {
	if r == nil || len(subjects) == 0 {
		return
	}
	combined := append(append([]packs.Subject(nil), r.CapabilitySubjects...), subjects...)
	normalized, err := packs.NormalizeSubjects(combined)
	if err != nil {
		r.add(localPreflightProviderPackPrerequisite, "provider_capability_model_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix provider pack or connector capability declarations")
		return
	}
	r.CapabilitySubjects = normalized
	for _, subject := range subjects {
		prefix := "provider_connector_"
		identity := subject.ID
		if subject.Kind == packs.SubjectProviderTrigger {
			prefix = "provider_trigger_pack_"
			identity = subject.Provider
		}
		remediation := ""
		for _, requirement := range subject.Requirements {
			if requirement.Satisfied != nil && !*requirement.Satisfied && requirement.Remediation != "" {
				remediation = requirement.Remediation
				break
			}
		}
		r.add(localPreflightProviderPackPrerequisite, prefix+findingCode(identity), LocalPreflightSeverityInfo, LocalPreflightStatusOK, packs.RenderSubject(subject, false), remediation)
	}
}

func findingCode(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.NewReplacer(".", "_", "-", "_", " ", "_").Replace(value)
	return strings.Trim(value, "_")
}

func (r LocalPreflightReport) finalize() LocalPreflightReport {
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

func localPreflightSeverityRank(severity LocalPreflightSeverity) int {
	switch severity {
	case LocalPreflightSeverityBlocker:
		return 3
	case LocalPreflightSeverityWarning:
		return 2
	default:
		return 1
	}
}

func (r LocalPreflightReport) HasBlockers() bool {
	for _, finding := range r.Findings {
		if finding.Severity == LocalPreflightSeverityBlocker && finding.Status == LocalPreflightStatusFailed {
			return true
		}
	}
	return false
}

func (r LocalPreflightReport) BlockerSummary() string {
	parts := []string{}
	for _, finding := range r.Findings {
		if finding.Severity == LocalPreflightSeverityBlocker && finding.Status == LocalPreflightStatusFailed {
			parts = append(parts, strings.TrimSpace(finding.Code)+": "+strings.TrimSpace(finding.Message))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}

func (r *LocalPreflightReport) checkListener(code, name, addr string) {
	listener, err := ListenServeHTTPListener(name, addr)
	if err != nil {
		r.add(localPreflightServeListenerPrerequisite, code, LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), fmt.Sprintf("free the %s listener address or choose a different --%s-listen-addr", name, name))
		return
	}
	_ = listener.Close()
	r.add(localPreflightServeListenerPrerequisite, code, LocalPreflightSeverityInfo, LocalPreflightStatusOK, fmt.Sprintf("%s listener address %s is available", name, strings.TrimSpace(addr)), "")
}

func (r *LocalPreflightReport) checkGatewayEnv() {
	for _, name := range RetiredToolGatewayURLEnvNames {
		raw := strings.TrimSpace(lookupEnvValue(name))
		if raw == "" {
			r.add(localPreflightGatewayPrerequisite, strings.ToLower(name)+"_empty", LocalPreflightSeverityInfo, LocalPreflightStatusOK, fmt.Sprintf("%s is empty; gateway endpoints are derived from ToolGatewayBinding", name), "")
			continue
		}
		severity := LocalPreflightSeverityWarning
		remediation := fmt.Sprintf("unset %s; local serve/run derives the gateway binding from the bound MCP listener and ignores this retired URL", name)
		if r.Mode == "serve" {
			severity = LocalPreflightSeverityBlocker
			remediation = fmt.Sprintf("unset %s; non-dev serve rejects retired gateway URL env because ToolGatewayBinding owns endpoint configuration", name)
		}
		r.add(localPreflightGatewayPrerequisite, strings.ToLower(name)+"_retired", severity, LocalPreflightStatusFailed, ValidateRetiredToolGatewayURLEnv(name, raw).Error(), remediation)
	}
}

func (r *LocalPreflightReport) checkContractSecrets(ctx context.Context, source semanticview.Source, severity LocalPreflightSeverity) {
	if severity == "" {
		severity = LocalPreflightSeverityWarning
	}
	store, err := BuildCredentialStore()
	if err != nil {
		r.add(localPreflightContractSecretPrerequisite, "credential_store_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the local credential store or SWARM_CREDENTIALS_FILE")
		return
	}
	missing, err := runtimecredentials.MissingRequired(ctx, store, source)
	if err != nil {
		r.add(localPreflightContractSecretPrerequisite, "contract_secret_check_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix contract credential references or credential store access")
		return
	}
	if len(missing) == 0 {
		r.add(localPreflightContractSecretPrerequisite, "contract_secrets_present", LocalPreflightSeverityInfo, LocalPreflightStatusOK, "all contract-required secrets are present", "")
		return
	}
	for _, desc := range missing {
		record := secretRecordFromDescriptor(desc)
		message := fmt.Sprintf("required secret %q is missing", record.Key)
		if requiredBy := formatSecretRequirements(record.RequiredBy); requiredBy != "" {
			message += " for " + requiredBy
		}
		r.add(localPreflightContractSecretPrerequisite, "missing_contract_secret", severity, LocalPreflightStatusFailed, message, fmt.Sprintf("run `swarm secrets set %s` or provide the matching environment variable", record.Key))
	}
}

func (r *LocalPreflightReport) checkWorkspace(ctx context.Context, cfg *config.Config, source semanticview.Source, contractsRoot string, mountSources WorkspaceMountSources, backend WorkspaceBackendSelection, claudeCLICommand string) {
	workspaces, err := ConfiguredWorkspaceLifecycleForServe(nil, cfg, contractsRoot, source, mountSources, backend)
	if err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_config_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix workspace backend or mount configuration")
		return
	}
	if workspaces == nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_lifecycle_missing", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, "workspace lifecycle is not configured", "select a supported workspace backend")
		return
	}
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_source_invalid", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix workspace_class declarations in the selected contracts")
		return
	}
	dockerManager, ok := workspaces.(*workspace.DockerManager)
	if !ok {
		r.add(localPreflightWorkspacePrerequisite, "workspace_backend_unsupported", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, fmt.Sprintf("workspace backend %q does not support claude_cli local proof", strings.TrimSpace(backend.Backend)), "use --workspace-backend docker for claude_cli local proof; host claude_cli support is split separately")
		return
	}
	if err := dockerManager.CheckDockerAvailable(ctx); err != nil {
		message, remediation := workspacePrerequisiteDiagnostic(err, fmt.Sprintf("Start the Docker daemon, then verify with `%s`", workspace.DockerInfoCommand(dockerManager.DockerBin())))
		r.add(localPreflightWorkspacePrerequisite, "docker_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, message, remediation)
		r.add(localPreflightWorkspacePrerequisite, "workspace_image_unavailable", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "workspace image was not checked because Docker is unreachable", remediation)
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_unavailable", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "configured Claude CLI command was not checked because the workspace image was not measured", remediation)
		return
	} else {
		r.add(localPreflightWorkspacePrerequisite, "docker_available", LocalPreflightSeverityInfo, LocalPreflightStatusOK, "Docker is reachable", "")
	}
	if err := dockerManager.CheckWorkspaceImageAvailable(ctx); err != nil {
		message, remediation := workspacePrerequisiteDiagnostic(err, "Run `swarm workspace build --backend claude_cli` before startup")
		r.add(localPreflightWorkspacePrerequisite, "workspace_image_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, message, remediation)
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_unavailable", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "configured Claude CLI command was not checked because the workspace image is unavailable", remediation)
		return
	} else {
		r.add(localPreflightWorkspacePrerequisite, "workspace_image_available", LocalPreflightSeverityInfo, LocalPreflightStatusOK, "workspace image is available", "")
	}
	if err := dockerManager.CheckWorkspaceCLICommandAvailable(ctx, claudeCLICommand); err != nil {
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "run `swarm workspace build --backend claude_cli` or pull a workspace image that contains the configured Claude CLI command")
	} else {
		r.add(localPreflightWorkspacePrerequisite, "workspace_claude_cli_available", LocalPreflightSeverityInfo, LocalPreflightStatusOK, "workspace image can execute the configured Claude CLI command", "")
	}
}

func workspacePrerequisiteDiagnostic(err error, fallbackRemediation string) (string, string) {
	message := err.Error()
	remediation := strings.TrimSpace(fallbackRemediation)
	var prerequisiteErr *workspace.PrerequisiteError
	if errors.As(err, &prerequisiteErr) {
		message = prerequisiteErr.Problem
		if strings.TrimSpace(prerequisiteErr.Remediation) != "" {
			remediation = prerequisiteErr.Remediation
		}
	}
	return message, remediation
}

func loadLocalPreflightSource(repo string, paths CLIContractPlatformSpecPaths) (semanticview.Source, string, error) {
	contractsRoot, err := NormalizeContractsRoot(paths.ContractsPath)
	if err != nil {
		return nil, "", err
	}
	_, bundle, err := NewSwarmWorkflowModule(assetCommandRepoRoot(repo), contractsRoot, paths.PlatformSpecPath)
	if err != nil {
		return nil, "", fmt.Errorf("load Swarm contracts: %w", err)
	}
	return semanticview.Wrap(bundle), contractsRoot, nil
}

func sourceDeclaresAgents(source semanticview.Source) bool {
	return source != nil && len(source.AgentEntries()) > 0
}

func lookupEnvValue(name string) string {
	return os.Getenv(name)
}

func WriteLocalPreflightText(out io.Writer, report LocalPreflightReport) {
	if out == nil {
		return
	}
	status := "ok"
	if !report.OK {
		status = "failed"
	}
	fmt.Fprintf(out, "claude_cli preflight: %s\n", status)
	for _, finding := range report.Findings {
		fmt.Fprintln(out, formatLocalPreflightFinding(report.Mode, finding))
	}
	writeDoctorSchemaInventoryText(out, report.SchemaInventory)
}

func formatLocalPreflightFinding(mode string, finding localPreflightFinding) string {
	checkID := strings.TrimSpace(string(finding.Category))
	if code := strings.TrimSpace(finding.Code); code != "" {
		if checkID != "" {
			checkID += "/"
		}
		checkID += code
	}
	location := strings.TrimSpace(mode)
	if location == "" {
		location = "local_preflight"
	}
	severity, blocking := localPreflightTypedSeverity(finding.Severity)
	return runtimebootverify.FormatTypedDiagnosticFinding(runtimebootverify.TypedDiagnosticFinding{
		CheckID:     checkID,
		Severity:    severity,
		Location:    location,
		Message:     strings.TrimSpace(finding.Message),
		Remediation: strings.TrimSpace(finding.Remediation),
	}, blocking)
}

func localPreflightTypedSeverity(severity LocalPreflightSeverity) (string, bool) {
	switch severity {
	case LocalPreflightSeverityBlocker:
		return runtimebootverify.SeverityHardInvalidity, false
	case LocalPreflightSeverityWarning:
		return runtimebootverify.SeveritySemanticDriftWarn, false
	default:
		return runtimebootverify.SeverityLintEvidence, false
	}
}

func writeLocalPreflightJSON(out io.Writer, report LocalPreflightReport) error {
	if out == nil {
		return nil
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func returnLocalPreflightResult(cmd *cobra.Command, report LocalPreflightReport, asJSON bool) error {
	if asJSON {
		if err := writeLocalPreflightJSON(cmd.OutOrStdout(), report); err != nil {
			return err
		}
	} else {
		WriteLocalPreflightText(cmd.OutOrStdout(), report)
	}
	if !report.OK {
		return commandExitError{code: CLIExitRuntime}
	}
	return nil
}

func localPreflightCommandSeverityForContractSecrets(mode string) LocalPreflightSeverity {
	if strings.EqualFold(strings.TrimSpace(mode), "doctor") {
		return LocalPreflightSeverityBlocker
	}
	return LocalPreflightSeverityWarning
}

func ShouldRunServeLocalClaudeCLIPreflight(opts ServeOptions) bool {
	if strings.TrimSpace(opts.BundleHash) != "" || len(opts.BundleHashes) > 0 {
		return false
	}
	return true
}

func serveLocalPreflightMode(opts ServeOptions) string {
	if opts.LocalRun {
		return "run_local"
	}
	if opts.Dev {
		return "serve_dev"
	}
	return "serve"
}

func RunServeLocalClaudeCLIPreflight(ctx context.Context, repo string, opts ServeOptions, cfg *config.Config, resolvedPaths CLIContractPlatformSpecPaths, workspaceBackend WorkspaceBackendSelection, mountSources WorkspaceMountSources, providerTriggerPacks []providertriggers.LoadedPack, providerTriggerCatalog *providertriggers.CatalogSnapshot) LocalPreflightReport {
	mode := serveLocalPreflightMode(opts)
	return runLocalClaudeCLIPreflight(ctx, localPreflightRequest{
		Mode:                   mode,
		RepoRoot:               repo,
		Config:                 cfg,
		ResolvedPaths:          resolvedPaths,
		DataSource:             opts.DataSource,
		MountSources:           mountSources,
		WorkspaceBackend:       workspaceBackend,
		APIListenAddr:          opts.APIListenAddr,
		MCPListenAddr:          opts.MCPListenAddr,
		CheckListeners:         true,
		CheckGatewayEnv:        true,
		CheckContractSecrets:   true,
		ContractSecretSeverity: localPreflightCommandSeverityForContractSecrets(mode),
		ProviderTriggerPacks:   providerTriggerPacks,
		ProviderTriggerCatalog: providerTriggerCatalog,
	})
}
