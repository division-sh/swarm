package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"

	"empireai/internal/models"
	"empireai/internal/promptcontracts"
	"gopkg.in/yaml.v3"
)

var (
	promptBundleOnce sync.Once
	promptBundle     *WorkflowContractBundle
	promptBundleErr  error

	promptTemplateFieldPattern = regexp.MustCompile(`\{[^}]+\}`)
	promptTokenPattern         = regexp.MustCompile(`\{\{([a-zA-Z0-9_]+)\}\}`)

	promptVariablesOnce sync.Once
	promptVariables     map[string]any
	promptVariablesErr  error
)

func LoadPromptForAgent(cfg models.AgentConfig, mode string) (string, bool, error) {
	candidates, dirs := promptLookupPlan(cfg)
	repoRoot := promptContractsRepoRoot()
	for _, agentID := range candidates {
		for _, dir := range dirs[agentID] {
			prompt, found, err := loadPromptForAgentFromDir(repoRoot, dir, agentID, mode)
			if err != nil {
				return "", false, err
			}
			if found {
				return prompt, true, nil
			}
		}
	}
	return "", false, nil
}

func PromptAgentIDForConfig(cfg models.AgentConfig) string {
	candidates := promptIDCandidates(cfg)
	bundle, err := promptWorkflowBundle()
	if err == nil && bundle != nil {
		if matched, ok := resolveBundlePromptAgentID(bundle, cfg, candidates); ok {
			return matched
		}
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}

func promptLookupPlan(cfg models.AgentConfig) ([]string, map[string][]string) {
	candidates := promptIDCandidates(cfg)
	bundle, err := promptWorkflowBundle()
	if err != nil || bundle == nil {
		fallback := promptLegacyDirs()
		dirs := make(map[string][]string, len(candidates))
		for _, candidate := range candidates {
			dirs[candidate] = fallback
		}
		return candidates, dirs
	}

	resolved := make([]string, 0, len(candidates)+1)
	if matched, ok := resolveBundlePromptAgentID(bundle, cfg, candidates); ok {
		resolved = append(resolved, matched)
	}
	resolved = append(resolved, candidates...)
	resolved = uniqueStrings(resolved...)

	globalDirs := promptBundlePromptDirs(bundle)
	legacyDirs := promptLegacyDirs()
	dirs := make(map[string][]string, len(resolved))
	for _, agentID := range resolved {
		if agentID == "" {
			continue
		}
		bundleDirs := uniqueStrings(append(promptDirsForBundleAgent(bundle, agentID), globalDirs...)...)
		if promptBundleHasAgent(bundle, agentID) {
			dirs[agentID] = bundleDirs
			continue
		}
		dirs[agentID] = uniqueStrings(append(bundleDirs, legacyDirs...)...)
	}
	return resolved, dirs
}

func promptIDCandidates(cfg models.AgentConfig) []string {
	agentID := strings.TrimSpace(cfg.ID)
	parent := strings.TrimSpace(cfg.ParentAgent)

	candidates := make([]string, 0, 3)
	if parent != "" && strings.Contains(agentID, "-shard-") {
		candidates = append(candidates, parent)
	}
	if agentID != "" {
		candidates = append(candidates, agentID)
	}
	return uniqueStrings(candidates...)
}

func canonicalRuntimeRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func resolveBundlePromptAgentID(bundle *WorkflowContractBundle, cfg models.AgentConfig, candidates []string) (string, bool) {
	for _, candidate := range candidates {
		if _, ok := bundle.MergedAgents[candidate]; ok {
			return candidate, true
		}
	}

	for _, candidate := range candidates {
		for logicalID, entry := range bundle.MergedAgents {
			if promptRegistryIDMatches(entry.ID, candidate) {
				return strings.TrimSpace(logicalID), true
			}
		}
	}

	role := canonicalPromptLookupValue(cfg.Role)
	if role == "" {
		return "", false
	}
	mode := canonicalPromptLookupValue(cfg.Mode)
	for logicalID, entry := range bundle.MergedAgents {
		if canonicalPromptLookupValue(entry.Role) != role {
			continue
		}
		if mode != "" {
			if source, ok := bundle.AgentSources[strings.TrimSpace(logicalID)]; ok {
				if flowMode := promptFlowMode(bundle, source.FlowID); flowMode != "" && flowMode != mode {
					continue
				}
			}
		}
		return strings.TrimSpace(logicalID), true
	}
	return "", false
}

func promptRegistryIDMatches(template, candidate string) bool {
	template = strings.TrimSpace(template)
	candidate = strings.TrimSpace(candidate)
	if template == "" || candidate == "" {
		return false
	}
	if template == candidate {
		return true
	}
	pattern := promptTemplateMatchPattern(template)
	matched, err := regexp.MatchString(pattern, candidate)
	return err == nil && matched
}

func promptTemplateMatchPattern(template string) string {
	matches := promptTemplateFieldPattern.FindAllStringIndex(template, -1)
	if len(matches) == 0 {
		return "^" + regexp.QuoteMeta(template) + "$"
	}
	var builder strings.Builder
	builder.WriteString("^")
	last := 0
	for _, match := range matches {
		builder.WriteString(regexp.QuoteMeta(template[last:match[0]]))
		builder.WriteString(".+")
		last = match[1]
	}
	builder.WriteString(regexp.QuoteMeta(template[last:]))
	builder.WriteString("$")
	return builder.String()
}

func promptDirsForBundleAgent(bundle *WorkflowContractBundle, agentID string) []string {
	if bundle == nil {
		return nil
	}
	source, ok := bundle.AgentSources[strings.TrimSpace(agentID)]
	if !ok {
		return nil
	}
	dirs := make([]string, 0, 4)
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		if flow, ok := bundle.FlowContracts[flowID]; ok && strings.TrimSpace(flow.Paths.PromptsDir) != "" {
			dirs = append(dirs, flow.Paths.PromptsDir)
		}
	}
	if pkgKey := strings.TrimSpace(source.PackageKey); pkgKey != "" {
		if pkg, ok := bundle.ProjectContracts[pkgKey]; ok && strings.TrimSpace(pkg.Paths.ProjectPromptsDir) != "" {
			dirs = append(dirs, pkg.Paths.ProjectPromptsDir)
		}
	}
	if strings.TrimSpace(bundle.Paths.ProjectPromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.ProjectPromptsDir)
	}
	if strings.TrimSpace(bundle.Paths.PromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.PromptsDir)
	}
	return uniqueStrings(dirs...)
}

func promptBundleHasAgent(bundle *WorkflowContractBundle, agentID string) bool {
	if bundle == nil {
		return false
	}
	_, ok := bundle.MergedAgents[strings.TrimSpace(agentID)]
	return ok
}

func promptBundlePromptDirs(bundle *WorkflowContractBundle) []string {
	if bundle == nil {
		return nil
	}
	dirs := make([]string, 0, 2+len(bundle.ProjectContracts)+len(bundle.FlowContracts))
	if strings.TrimSpace(bundle.Paths.ProjectPromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.ProjectPromptsDir)
	}
	if strings.TrimSpace(bundle.Paths.PromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.PromptsDir)
	}
	for _, pkg := range bundle.ProjectContracts {
		if strings.TrimSpace(pkg.Paths.ProjectPromptsDir) != "" {
			dirs = append(dirs, pkg.Paths.ProjectPromptsDir)
		}
	}
	for _, flow := range bundle.FlowContracts {
		if strings.TrimSpace(flow.Paths.PromptsDir) != "" {
			dirs = append(dirs, flow.Paths.PromptsDir)
		}
	}
	return uniqueStrings(dirs...)
}

func promptLegacyDirs() []string {
	dir, ok := promptcontracts.ResolveDir()
	if !ok {
		return nil
	}
	return []string{dir}
}

func promptFlowMode(bundle *WorkflowContractBundle, flowID string) string {
	if bundle == nil {
		return ""
	}
	flow, ok := bundle.FlowContracts[strings.TrimSpace(flowID)]
	if !ok {
		return ""
	}
	return canonicalPromptLookupValue(flow.Paths.Mode)
}

func canonicalPromptLookupValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	return value
}

func uniqueStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || slices.Contains(out, value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func promptWorkflowBundle() (*WorkflowContractBundle, error) {
	promptBundleOnce.Do(func() {
		repoRoot := promptContractsRepoRoot()
		promptBundle, promptBundleErr = LoadWorkflowContractBundleWithOverrides(
			repoRoot,
			filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
			filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
		)
	})
	return promptBundle, promptBundleErr
}

func promptContractsRepoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}

func loadPromptForAgentFromDir(repoRoot, promptsDir, agentID, mode string) (string, bool, error) {
	agentID = strings.TrimSpace(agentID)
	mode = strings.TrimSpace(strings.ToLower(mode))
	if agentID == "" || strings.TrimSpace(promptsDir) == "" {
		return "", false, nil
	}

	candidates := make([]string, 0, 2)
	if mode != "" {
		candidates = append(candidates, filepath.Join(promptsDir, agentID+"."+mode+".md"))
	}
	candidates = append(candidates, filepath.Join(promptsDir, agentID+".md"))

	for _, candidate := range candidates {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", false, err
		}
		rendered, err := renderPromptWithRuntimeVariables(repoRoot, string(raw))
		if err != nil {
			return "", false, fmt.Errorf("render prompt %s: %w", filepath.Base(candidate), err)
		}
		return strings.TrimSpace(rendered), true, nil
	}
	return "", false, nil
}

func renderPromptWithRuntimeVariables(repoRoot, promptText string) (string, error) {
	if !promptTokenPattern.MatchString(promptText) {
		return promptText, nil
	}
	vars, err := promptRuntimeVariables(repoRoot)
	if err != nil {
		return promptText, nil
	}
	return renderPromptTemplatePreservingUnknown(promptText, vars), nil
}

func promptRuntimeVariables(repoRoot string) (map[string]any, error) {
	promptVariablesOnce.Do(func() {
		path := filepath.Join(repoRoot, "contracts", "prompt-variables.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			promptVariablesErr = err
			return
		}
		var vars map[string]any
		if err := yaml.Unmarshal(raw, &vars); err != nil {
			promptVariablesErr = err
			return
		}
		promptVariables = vars
	})
	return promptVariables, promptVariablesErr
}

func renderPromptTemplatePreservingUnknown(promptText string, vars map[string]any) string {
	matches := promptTokenPattern.FindAllStringSubmatchIndex(promptText, -1)
	if len(matches) == 0 {
		return promptText
	}
	var out strings.Builder
	last := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		keyStart, keyEnd := match[2], match[3]
		key := promptText[keyStart:keyEnd]

		out.WriteString(promptText[last:start])
		replacement := promptText[start:end]
		if value, ok := vars[key]; ok {
			replacement = renderPromptTemplateValue(value)
			prefix := promptLinePrefix(promptText, start)
			if prefix != "" {
				replacement = indentPromptTemplateValue(replacement, prefix)
			}
		}
		out.WriteString(replacement)
		last = end
	}
	out.WriteString(promptText[last:])
	return out.String()
}

func renderPromptTemplateValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", typed)
	default:
		raw, err := yaml.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return strings.TrimSpace(string(raw))
	}
}

func promptLinePrefix(promptText string, tokenStart int) string {
	lineStart := strings.LastIndex(promptText[:tokenStart], "\n") + 1
	prefix := promptText[lineStart:tokenStart]
	if strings.TrimSpace(prefix) != "" {
		return ""
	}
	return prefix
}

func indentPromptTemplateValue(rendered, prefix string) string {
	if prefix == "" || !strings.Contains(rendered, "\n") {
		return rendered
	}
	lines := strings.Split(rendered, "\n")
	for i := 1; i < len(lines); i++ {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}
