package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const workspaceBackendNone = "none"

type workspaceCapabilityClass string

const (
	workspaceCapabilityNone           workspaceCapabilityClass = "none"
	workspaceCapabilityWorkspaceWrite workspaceCapabilityClass = "workspace_write"
	workspaceCapabilityExec           workspaceCapabilityClass = "exec"
)

type workspaceCapabilityReasonKind string

const (
	workspaceReasonNoAgents     workspaceCapabilityReasonKind = "no_agents"
	workspaceReasonLifecycle    workspaceCapabilityReasonKind = "workspace_lifecycle"
	workspaceReasonClaudeCLI    workspaceCapabilityReasonKind = "claude_cli"
	workspaceReasonNativeBash   workspaceCapabilityReasonKind = "native_bash"
	workspaceReasonExecTool     workspaceCapabilityReasonKind = "exec_tool"
	workspaceReasonNativeFileIO workspaceCapabilityReasonKind = "native_file_io"
)

type workspaceCapabilityReason struct {
	Kind    workspaceCapabilityReasonKind
	AgentID string
	Tool    string
}

func (r workspaceCapabilityReason) String() string {
	switch r.Kind {
	case workspaceReasonNoAgents:
		return "selected contract source declares no agents"
	case workspaceReasonLifecycle:
		return "declared agents use runtime-mediated workspace lifecycle"
	case workspaceReasonClaudeCLI:
		return fmt.Sprintf("agent %s uses claude_cli backend", workspaceBackendAgentLabel(r.AgentID))
	case workspaceReasonNativeBash:
		return fmt.Sprintf("agent %s has native_tools.bash", workspaceBackendAgentLabel(r.AgentID))
	case workspaceReasonExecTool:
		return fmt.Sprintf("agent %s has exec-class tool %s", workspaceBackendAgentLabel(r.AgentID), strings.TrimSpace(r.Tool))
	case workspaceReasonNativeFileIO:
		return fmt.Sprintf("agent %s has native_tools.file_io", workspaceBackendAgentLabel(r.AgentID))
	default:
		return "the selected contract"
	}
}

func (r workspaceCapabilityReason) isExec() bool {
	switch r.Kind {
	case workspaceReasonClaudeCLI, workspaceReasonNativeBash, workspaceReasonExecTool:
		return true
	default:
		return false
	}
}

type workspaceBackendDecisionError struct {
	Problem     string
	Remediation string
}

func (e *workspaceBackendDecisionError) Error() string {
	if e == nil {
		return ""
	}
	problem := strings.TrimSpace(e.Problem)
	remediation := strings.TrimSpace(e.Remediation)
	if remediation == "" {
		return problem
	}
	return problem + ". " + remediation
}

type workspaceBackendSelection struct {
	Backend string
	Source  string

	PreferenceExplicit bool
	AllowExecOnHost    bool
	NoWorkspace        bool
	UnsafeHost         bool

	CapabilityClass workspaceCapabilityClass
	Reasons         []workspaceCapabilityReason
}

type workspaceBackendInput struct {
	FlagBackend string
	FlagSet     bool

	ConfigBackend string
	ConfigSet     bool

	AllowExecOnHost bool
}

func resolveWorkspaceBackend(flagBackend string, flagSet bool, cfg *config.Config) (workspaceBackendSelection, error) {
	configBackend, configSet := runtimeConfigWorkspaceBackend(cfg)
	return resolveWorkspaceBackendFromInput(workspaceBackendInput{
		FlagBackend:     flagBackend,
		FlagSet:         flagSet,
		ConfigBackend:   configBackend,
		ConfigSet:       configSet,
		AllowExecOnHost: runtimeConfigAllowExecOnHost(cfg),
	})
}

func resolveWorkspaceBackendDecision(flagBackend string, flagSet bool, cfg *config.Config, source semanticview.Source) (workspaceBackendSelection, error) {
	preference, err := resolveWorkspaceBackend(flagBackend, flagSet, cfg)
	if err != nil {
		return preference, err
	}
	return decideWorkspaceBackend(preference, cfg, source)
}

func resolveWorkspaceBackendDiagnostic(repo, configPath string, source semanticview.Source) (workspaceBackendSelection, error) {
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return workspaceBackendSelection{}, err
	}
	return resolveWorkspaceBackendDecision("", false, cfgResult.Config, source)
}

func resolveWorkspaceBackendFromInput(in workspaceBackendInput) (workspaceBackendSelection, error) {
	switch {
	case in.FlagSet:
		backend, err := normalizeWorkspaceBackend(in.FlagBackend, "--workspace-backend")
		return workspaceBackendSelection{
			Backend:            backend,
			Source:             "--workspace-backend",
			PreferenceExplicit: true,
			AllowExecOnHost:    strings.EqualFold(backend, workspace.BackendHost),
		}, err
	case in.ConfigSet:
		backend, err := normalizeWorkspaceBackend(in.ConfigBackend, "workspace.backend")
		return workspaceBackendSelection{
			Backend:            backend,
			Source:             "workspace.backend",
			PreferenceExplicit: true,
			AllowExecOnHost:    in.AllowExecOnHost,
		}, err
	default:
		return workspaceBackendSelection{Source: "capability-derived"}, nil
	}
}

func runtimeConfigWorkspaceBackend(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	return cfg.Workspace.Backend, cfg.Workspace.BackendConfigured()
}

func runtimeConfigAllowExecOnHost(cfg *config.Config) bool {
	return cfg != nil && cfg.Workspace.AllowExecOnHost
}

func normalizeWorkspaceBackend(raw string, source string) (string, error) {
	backend := strings.ToLower(strings.TrimSpace(raw))
	if backend == "" {
		return "", fmt.Errorf("workspace backend from %s must be non-empty", source)
	}
	switch backend {
	case workspace.BackendDocker, workspace.BackendHost:
		return backend, nil
	default:
		return "", fmt.Errorf("workspace backend from %s must be docker or host, got %q", source, strings.TrimSpace(raw))
	}
}

func decideWorkspaceBackend(preference workspaceBackendSelection, cfg *config.Config, source semanticview.Source) (workspaceBackendSelection, error) {
	class, reasons, err := classifyWorkspaceBackendRequirement(cfg, source)
	if err != nil {
		return preference, err
	}
	decision := preference
	decision.CapabilityClass = class
	decision.Reasons = reasons

	switch class {
	case workspaceCapabilityNone:
		if decision.Backend == "" {
			decision.Backend = workspaceBackendNone
			decision.NoWorkspace = true
			decision.Source = "capability-derived"
		}
	case workspaceCapabilityWorkspaceWrite:
		if decision.Backend == "" {
			decision.Backend = workspace.BackendHost
			decision.Source = "capability-derived"
		}
	case workspaceCapabilityExec:
		if decision.Backend == "" {
			decision.Backend = workspace.BackendDocker
			decision.Source = "capability-derived"
			break
		}
		if decision.Backend == workspace.BackendHost {
			if workspaceBackendHasReason(reasons, workspaceReasonClaudeCLI) {
				problem := fmt.Sprintf("workspace backend host is unsupported for this exec-capable contract: %s", workspaceBackendExecReasonSummary(reasons))
				remediation := "Use Docker"
				if workspaceBackendExecReasonsAreClaudeOnly(reasons) {
					remediation += ", or switch to an API backend (`llm.backend: anthropic`) for a Docker-free local run; host execution is refused only for exec-capable agents"
				} else {
					remediation += ". For a Docker-free local run, switch to an API backend (`llm.backend: anthropic`) and explicitly authorize the remaining host execution with `workspace.allow_exec_on_host: true`"
				}
				return decision, &workspaceBackendDecisionError{Problem: problem, Remediation: remediation}
			}
			if !decision.AllowExecOnHost {
				switch decision.Source {
				case "--workspace-backend":
					decision.AllowExecOnHost = true
				case "workspace.backend":
					return decision, &workspaceBackendDecisionError{
						Problem:     fmt.Sprintf("workspace.backend: host grants agent execution on this machine for %s", workspaceBackendReasonSummary(reasons)),
						Remediation: "Set `workspace.allow_exec_on_host: true` or use Docker",
					}
				default:
					return decision, &workspaceBackendDecisionError{
						Problem:     fmt.Sprintf("workspace backend host cannot authorize unsafe host execution for %s", workspaceBackendReasonSummary(reasons)),
						Remediation: "Use `--workspace-backend host` for one command, or set `workspace.backend: host` with `workspace.allow_exec_on_host: true`",
					}
				}
			}
			decision.UnsafeHost = true
		}
	}
	return decision, nil
}

func classifyWorkspaceBackendRequirement(cfg *config.Config, source semanticview.Source) (workspaceCapabilityClass, []workspaceCapabilityReason, error) {
	if cfg == nil {
		return "", nil, fmt.Errorf("runtime config is required")
	}
	if source == nil {
		return "", nil, fmt.Errorf("semantic source is required")
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return "", nil, err
	}
	entries := source.AgentEntries()
	if len(entries) == 0 {
		return workspaceCapabilityNone, []workspaceCapabilityReason{{Kind: workspaceReasonNoAgents}}, nil
	}

	agentIDs := make([]string, 0, len(entries))
	for id := range entries {
		agentIDs = append(agentIDs, strings.TrimSpace(id))
	}
	sort.Strings(agentIDs)

	class := workspaceCapabilityWorkspaceWrite
	reasons := []workspaceCapabilityReason{{Kind: workspaceReasonLifecycle}}
	for _, agentID := range agentIDs {
		entry := entries[agentID]
		label := workspaceBackendAgentLabel(agentID, entry.ID, entry.Role)
		if profile.ID == llmselection.BackendClaudeCLI {
			class = workspaceCapabilityExec
			reasons = append(reasons, workspaceCapabilityReason{Kind: workspaceReasonClaudeCLI, AgentID: label})
		}
		if nativeToolEnabled(entry.NativeTools, "bash") {
			class = workspaceCapabilityExec
			reasons = append(reasons, workspaceCapabilityReason{Kind: workspaceReasonNativeBash, AgentID: label})
		}
		for _, tool := range entry.ConfiguredTools() {
			if workspaceExecClassTool(tool) {
				class = workspaceCapabilityExec
				reasons = append(reasons, workspaceCapabilityReason{Kind: workspaceReasonExecTool, AgentID: label, Tool: strings.TrimSpace(tool)})
			}
		}
		if nativeToolEnabled(entry.NativeTools, "file_io") {
			reasons = append(reasons, workspaceCapabilityReason{Kind: workspaceReasonNativeFileIO, AgentID: label})
		}
	}
	return class, uniqueWorkspaceCapabilityReasons(reasons), nil
}

func nativeToolEnabled(raw map[string]any, name string) bool {
	value, ok := raw[strings.TrimSpace(name)]
	if !ok {
		return false
	}
	enabled, ok := value.(bool)
	return ok && enabled
}

func workspaceExecClassTool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "bash", "shell", "code_execution", "code-execution", "native_command":
		return true
	default:
		return false
	}
}

func workspaceBackendAgentLabel(parts ...string) string {
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			return part
		}
	}
	return "<unknown>"
}

func workspaceBackendReasonSummary(reasons []workspaceCapabilityReason) string {
	if len(reasons) == 0 {
		return "the selected contract"
	}
	formatted := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		formatted = append(formatted, reason.String())
	}
	return strings.Join(formatted, "; ")
}

func workspaceBackendExecReasonSummary(reasons []workspaceCapabilityReason) string {
	execReasons := make([]workspaceCapabilityReason, 0, len(reasons))
	for _, reason := range reasons {
		if reason.isExec() {
			execReasons = append(execReasons, reason)
		}
	}
	return workspaceBackendReasonSummary(execReasons)
}

func workspaceBackendHasReason(reasons []workspaceCapabilityReason, kind workspaceCapabilityReasonKind) bool {
	for _, reason := range reasons {
		if reason.Kind == kind {
			return true
		}
	}
	return false
}

func workspaceBackendExecReasonsAreClaudeOnly(reasons []workspaceCapabilityReason) bool {
	found := false
	for _, reason := range reasons {
		if !reason.isExec() {
			continue
		}
		found = true
		if reason.Kind != workspaceReasonClaudeCLI {
			return false
		}
	}
	return found
}

func workspaceBackendDecisionDetail(decision workspaceBackendSelection) string {
	backend := strings.TrimSpace(decision.Backend)
	if backend == workspaceBackendNone || decision.NoWorkspace {
		backend = "none"
	}
	if backend == "" {
		backend = "unknown"
	}
	reason := workspaceBackendReasonSummary(decision.Reasons)
	if decision.UnsafeHost {
		return fmt.Sprintf("workspace backend: %s (%s); UNSAFE: grants the agent execution on this machine", backend, reason)
	}
	return fmt.Sprintf("workspace backend: %s (%s)", backend, reason)
}

func workspaceBackendUnsafeWarning(decision workspaceBackendSelection) string {
	if !decision.UnsafeHost {
		return ""
	}
	return fmt.Sprintf("UNSAFE: grants the agent execution on this machine via workspace backend host (%s)", workspaceBackendReasonSummary(decision.Reasons))
}

func uniqueWorkspaceCapabilityReasons(values []workspaceCapabilityReason) []workspaceCapabilityReason {
	seen := map[workspaceCapabilityReason]struct{}{}
	out := make([]workspaceCapabilityReason, 0, len(values))
	for _, value := range values {
		value.AgentID = strings.TrimSpace(value.AgentID)
		value.Tool = strings.TrimSpace(value.Tool)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
