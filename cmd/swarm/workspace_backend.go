package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const (
	envWorkspaceBackend = "SWARM_WORKSPACE_BACKEND"

	workspaceBackendNone = "none"
)

type workspaceCapabilityClass string

const (
	workspaceCapabilityNone           workspaceCapabilityClass = "none"
	workspaceCapabilityWorkspaceWrite workspaceCapabilityClass = "workspace_write"
	workspaceCapabilityExec           workspaceCapabilityClass = "exec"
)

type workspaceBackendSelection struct {
	Backend string
	Source  string

	PreferenceExplicit bool
	AllowExecOnHost    bool
	NoWorkspace        bool
	UnsafeHost         bool

	CapabilityClass workspaceCapabilityClass
	Reasons         []string
	HostUnsupported []string
}

type workspaceBackendInput struct {
	FlagBackend string
	FlagSet     bool

	ConfigBackend string
	ConfigSet     bool

	EnvBackend string
	EnvSet     bool

	AllowExecOnHost bool
}

func resolveWorkspaceBackend(flagBackend string, flagSet bool, cfg *config.Config) (workspaceBackendSelection, error) {
	envBackend, envSet := os.LookupEnv(envWorkspaceBackend)
	configBackend, configSet := runtimeConfigWorkspaceBackend(cfg)
	return resolveWorkspaceBackendFromInput(workspaceBackendInput{
		FlagBackend:     flagBackend,
		FlagSet:         flagSet,
		ConfigBackend:   configBackend,
		ConfigSet:       configSet,
		EnvBackend:      envBackend,
		EnvSet:          envSet,
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

func resolveWorkspaceBackendDiagnostic(repo string, source semanticview.Source) (workspaceBackendSelection, error) {
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo})
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
	case in.EnvSet:
		backend, err := normalizeWorkspaceBackend(in.EnvBackend, envWorkspaceBackend)
		return workspaceBackendSelection{
			Backend:            backend,
			Source:             envWorkspaceBackend,
			PreferenceExplicit: true,
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
	class, reasons, hostUnsupported, err := classifyWorkspaceBackendRequirement(cfg, source)
	if err != nil {
		return preference, err
	}
	decision := preference
	decision.CapabilityClass = class
	decision.Reasons = reasons
	decision.HostUnsupported = hostUnsupported

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
			if len(hostUnsupported) > 0 {
				return decision, fmt.Errorf("workspace backend host is unsupported for this exec-capable contract: %s; use Docker", strings.Join(hostUnsupported, "; "))
			}
			if !decision.AllowExecOnHost {
				switch decision.Source {
				case "--workspace-backend":
					decision.AllowExecOnHost = true
				case "workspace.backend":
					return decision, fmt.Errorf("workspace.backend: host grants agent execution on this machine for %s; set workspace.allow_exec_on_host: true or use Docker", workspaceBackendReasonSummary(reasons))
				default:
					return decision, fmt.Errorf("%s=host cannot authorize unsafe host execution for %s; use --workspace-backend host for one command or workspace.backend: host with workspace.allow_exec_on_host: true", envWorkspaceBackend, workspaceBackendReasonSummary(reasons))
				}
			}
			decision.UnsafeHost = true
		}
	}
	return decision, nil
}

func classifyWorkspaceBackendRequirement(cfg *config.Config, source semanticview.Source) (workspaceCapabilityClass, []string, []string, error) {
	if cfg == nil {
		return "", nil, nil, fmt.Errorf("runtime config is required")
	}
	if source == nil {
		return "", nil, nil, fmt.Errorf("semantic source is required")
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return "", nil, nil, err
	}
	entries := source.AgentEntries()
	if len(entries) == 0 {
		return workspaceCapabilityNone, []string{"selected contract source declares no agents"}, nil, nil
	}

	agentIDs := make([]string, 0, len(entries))
	for id := range entries {
		agentIDs = append(agentIDs, strings.TrimSpace(id))
	}
	sort.Strings(agentIDs)

	class := workspaceCapabilityWorkspaceWrite
	reasons := []string{"declared agents use runtime-mediated workspace lifecycle"}
	hostUnsupported := []string{}
	for _, agentID := range agentIDs {
		entry := entries[agentID]
		label := workspaceBackendAgentLabel(agentID, entry.ID, entry.Role)
		if profile.ID == llmselection.BackendClaudeCLI {
			class = workspaceCapabilityExec
			reasons = append(reasons, fmt.Sprintf("agent %s uses claude_cli backend", label))
			hostUnsupported = append(hostUnsupported, fmt.Sprintf("agent %s uses claude_cli backend", label))
		}
		if nativeToolEnabled(entry.NativeTools, "bash") {
			class = workspaceCapabilityExec
			reasons = append(reasons, fmt.Sprintf("agent %s has native_tools.bash", label))
		}
		for _, tool := range entry.ConfiguredTools() {
			if workspaceExecClassTool(tool) {
				class = workspaceCapabilityExec
				reasons = append(reasons, fmt.Sprintf("agent %s has exec-class tool %s", label, strings.TrimSpace(tool)))
			}
		}
		if class != workspaceCapabilityExec && nativeToolEnabled(entry.NativeTools, "file_io") {
			reasons = append(reasons, fmt.Sprintf("agent %s has native_tools.file_io", label))
		}
	}
	return class, uniqueNonEmptyStrings(reasons), uniqueNonEmptyStrings(hostUnsupported), nil
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

func workspaceBackendReasonSummary(reasons []string) string {
	if len(reasons) == 0 {
		return "the selected contract"
	}
	return strings.Join(reasons, "; ")
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

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
