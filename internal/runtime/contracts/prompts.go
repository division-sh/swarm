package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	models "swarm/internal/runtime/core/actors"
)

var (
	promptBundleOnce   sync.Once
	promptBundle       *WorkflowContractBundle
	promptBundleErr    error
	activePromptMu     sync.RWMutex
	activePromptBundle *WorkflowContractBundle

	promptTemplateFieldPattern = regexp.MustCompile(`\{[^}]+\}`)
	promptTokenPattern         = regexp.MustCompile(`\{\{([a-zA-Z0-9_]+)\}\}`)

	promptVariablesMu sync.RWMutex
	promptVariables   = map[string]map[string]any{}
)

func SetActivePromptBundle(bundle *WorkflowContractBundle) {
	activePromptMu.Lock()
	defer activePromptMu.Unlock()
	activePromptBundle = bundle
	promptVariablesMu.Lock()
	promptVariables = map[string]map[string]any{}
	promptVariablesMu.Unlock()
}

func LoadPromptForAgent(cfg models.AgentConfig, mode string) (string, bool, error) {
	candidates, dirs := promptLookupPlan(cfg)
	repoRoot := promptContractsRepoRoot()
	bundle, _ := promptWorkflowBundle()
	for _, agentID := range candidates {
		record := bundleAgentRecord{}
		if bundle != nil {
			record, _ = bundleAgentRecordByLogicalID(bundle, agentID)
		}
		for _, dir := range dirs[agentID] {
			prompt, found, err := loadPromptTemplateFromDir(repoRoot, dir, agentID, record.Entry, record.Source, cfg, mode)
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

func promptLookupPlan(cfg models.AgentConfig) ([]string, map[string][]string) {
	candidates := promptIDCandidates(cfg)
	bundle, err := promptWorkflowBundle()
	if err != nil || bundle == nil {
		dirs := make(map[string][]string, len(candidates))
		for _, candidate := range candidates {
			dirs[candidate] = nil
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
	dirs := make(map[string][]string, len(resolved))
	for _, agentID := range resolved {
		if agentID == "" {
			continue
		}
		bundleDirs := uniqueStrings(append(promptDirsForBundleAgent(bundle, agentID), globalDirs...)...)
		dirs[agentID] = bundleDirs
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
		for _, record := range bundleAgentRecords(bundle) {
			if strings.TrimSpace(record.LogicalID) == candidate {
				return candidate, true
			}
		}
	}

	for _, candidate := range candidates {
		for _, record := range bundleAgentRecords(bundle) {
			if promptRegistryIDMatches(record.Entry.ID, candidate) {
				return strings.TrimSpace(record.LogicalID), true
			}
		}
	}

	role := canonicalPromptLookupValue(cfg.Role)
	if role == "" {
		return "", false
	}
	mode := canonicalPromptLookupValue(cfg.Mode)
	for _, record := range bundleAgentRecords(bundle) {
		if canonicalPromptLookupValue(record.Entry.Role) != role {
			continue
		}
		if mode != "" {
			if flowMode := promptFlowMode(bundle, record.Source.FlowID); flowMode != "" && flowMode != mode {
				continue
			}
		}
		return strings.TrimSpace(record.LogicalID), true
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
	source, ok := bundle.AgentContractSource(agentID)
	if !ok {
		return nil
	}
	dirs := make([]string, 0, 4)
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		if flow, ok := bundle.FlowViewByID(flowID); ok && strings.TrimSpace(flow.Paths.PromptsDir) != "" {
			dirs = append(dirs, flow.Paths.PromptsDir)
		}
	}
	if pkgKey := strings.TrimSpace(source.PackageKey); pkgKey != "" {
		for _, pkg := range bundle.ProjectViews() {
			if strings.TrimSpace(pkg.Paths.Key) == pkgKey && strings.TrimSpace(pkg.Paths.ProjectPromptsDir) != "" {
				dirs = append(dirs, pkg.Paths.ProjectPromptsDir)
				break
			}
		}
	}
	if strings.TrimSpace(bundle.Paths.ProjectPromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.ProjectPromptsDir)
	}
	return uniqueStrings(dirs...)
}

func promptBundleHasAgent(bundle *WorkflowContractBundle, agentID string) bool {
	if bundle == nil {
		return false
	}
	agentID = strings.TrimSpace(agentID)
	for _, record := range bundleAgentRecords(bundle) {
		if strings.TrimSpace(record.LogicalID) == agentID {
			return true
		}
	}
	return false
}

func promptBundlePromptDirs(bundle *WorkflowContractBundle) []string {
	if bundle == nil {
		return nil
	}
	dirs := make([]string, 0, 2+len(bundle.ProjectViews())+len(bundle.FlowTree.ByID))
	if strings.TrimSpace(bundle.Paths.ProjectPromptsDir) != "" {
		dirs = append(dirs, bundle.Paths.ProjectPromptsDir)
	}
	for _, pkg := range bundle.ProjectViews() {
		if strings.TrimSpace(pkg.Paths.ProjectPromptsDir) != "" {
			dirs = append(dirs, pkg.Paths.ProjectPromptsDir)
		}
	}
	for _, flow := range bundle.FlowViews() {
		if strings.TrimSpace(flow.Paths.PromptsDir) != "" {
			dirs = append(dirs, flow.Paths.PromptsDir)
		}
	}
	return uniqueStrings(dirs...)
}

func promptFlowMode(bundle *WorkflowContractBundle, flowID string) string {
	if bundle == nil {
		return ""
	}
	flow, ok := bundle.FlowViewByID(flowID)
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
	activePromptMu.RLock()
	active := activePromptBundle
	activePromptMu.RUnlock()
	if active != nil {
		return active, nil
	}
	promptBundleOnce.Do(func() {
		repoRoot := promptContractsRepoRoot()
		contractsDir := DefaultWorkflowContractsDir(repoRoot)
		if strings.TrimSpace(contractsDir) == "" {
			return
		}
		promptBundle, promptBundleErr = LoadWorkflowContractBundleWithOverrides(
			repoRoot,
			contractsDir,
			DefaultPlatformSpecFile(repoRoot),
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
	return loadPromptTemplateFromDir(repoRoot, promptsDir, agentID, AgentRegistryEntry{}, ContractItemSource{}, models.AgentConfig{}, mode)
}

func loadPromptTemplateFromDir(repoRoot, promptsDir, agentID string, entry AgentRegistryEntry, source ContractItemSource, cfg models.AgentConfig, mode string) (string, bool, error) {
	agentID = strings.TrimSpace(agentID)
	mode = strings.TrimSpace(strings.ToLower(mode))
	if agentID == "" || strings.TrimSpace(promptsDir) == "" {
		return "", false, nil
	}

	candidates := promptPathCandidates(promptsDir, agentID, entry, mode)

	for _, candidate := range candidates {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", false, err
		}
		rendered, err := renderPromptWithRuntimeVariables(repoRoot, string(raw), source, cfg)
		if err != nil {
			return "", false, fmt.Errorf("render prompt %s: %w", filepath.Base(candidate), err)
		}
		return strings.TrimSpace(rendered), true, nil
	}
	return "", false, nil
}

func promptPathCandidates(promptsDir, logicalID string, entry AgentRegistryEntry, mode string) []string {
	refs := []string{
		strings.TrimSpace(entry.PromptRef),
		strings.TrimSpace(logicalID),
		strings.TrimSpace(entry.ID),
		strings.TrimSpace(promptWorkspaceRoleRef(entry)),
	}
	refs = uniqueStrings(refs...)
	paths := make([]string, 0, len(refs)*2)
	for _, ref := range refs {
		if ref == "" {
			continue
		}
		if filepath.Ext(ref) != "" {
			paths = append(paths, filepath.Join(promptsDir, ref))
			continue
		}
		if mode != "" {
			paths = append(paths, filepath.Join(promptsDir, ref+"."+mode+".md"))
		}
		paths = append(paths, filepath.Join(promptsDir, ref+".md"))
	}
	return uniqueStrings(paths...)
}

func promptWorkspaceRoleRef(entry AgentRegistryEntry) string {
	workspaceClass := strings.TrimSpace(entry.WorkspaceClass)
	role := strings.TrimSpace(entry.Role)
	if workspaceClass == "" || role == "" {
		return ""
	}
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.TrimSpace(role)
	if role == "" {
		return ""
	}
	return workspaceClass + "-" + role
}

func renderPromptWithRuntimeVariables(repoRoot, promptText string, source ContractItemSource, cfg models.AgentConfig) (string, error) {
	if !promptTokenPattern.MatchString(promptText) {
		return promptText, nil
	}
	vars, err := promptRuntimeVariables(repoRoot, source, cfg)
	if err != nil {
		return promptText, nil
	}
	return renderPromptTemplatePreservingUnknown(promptText, vars), nil
}

func promptRuntimeVariables(repoRoot string, source ContractItemSource, cfg models.AgentConfig) (map[string]any, error) {
	bundle, err := promptWorkflowBundle()
	if err != nil {
		return nil, fmt.Errorf("load workflow contract bundle: %w", err)
	}
	scopeKey := promptVariableScopeKey(source, cfg)
	promptVariablesMu.RLock()
	if vars, ok := promptVariables[scopeKey]; ok {
		defer promptVariablesMu.RUnlock()
		return clonePromptVariables(vars), nil
	}
	promptVariablesMu.RUnlock()

	vars := promptVariableValues(bundle, source, cfg)

	promptVariablesMu.Lock()
	promptVariables[scopeKey] = clonePromptVariables(vars)
	promptVariablesMu.Unlock()
	return vars, nil
}

func promptVariableScopeKey(source ContractItemSource, cfg models.AgentConfig) string {
	cacheSeed := strings.TrimSpace(source.PackageKey) + "|" + strings.TrimSpace(source.FlowID)
	configSeed := strings.TrimSpace(string(cfg.Config))
	dateSeed := time.Now().In(time.Local).Format("2006-01-02")
	if configSeed == "" {
		return cacheSeed + "|" + dateSeed
	}
	return cacheSeed + "|" + configSeed + "|" + dateSeed
}

func clonePromptVariables(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func promptResolvedPolicy(bundle *WorkflowContractBundle, source ContractItemSource) PolicyDocument {
	if bundle == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		return bundle.ResolvedPolicyForFlow(flowID)
	}
	out := clonePolicyDocument(bundle.Policy)
	if out.Values == nil {
		out.Values = map[string]PolicyValue{}
	}
	for _, pkg := range promptPackagePolicyLineage(bundle, strings.TrimSpace(source.PackageKey)) {
		for key, value := range pkg.Policy.Values {
			out.Values[key] = value
		}
	}
	return out
}

func promptVariableValues(bundle *WorkflowContractBundle, source ContractItemSource, cfg models.AgentConfig) map[string]any {
	vars := promptRuntimeTokens(cfg)
	for key, value := range promptEntityStateFields(cfg.Config) {
		vars[key] = value
	}
	policy := promptResolvedPolicy(bundle, source)
	for key, value := range policy.Values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		vars[key] = value.Value
	}
	for key, value := range promptInstanceVariables(cfg.Config) {
		vars[key] = value
	}
	return vars
}

func promptRuntimeTokens(cfg models.AgentConfig) map[string]any {
	out := map[string]any{
		"current_date": time.Now().In(time.Local).Format("2006-01-02"),
		"agent_id":     strings.TrimSpace(cfg.ID),
	}
	if config := parsePromptConfig(cfg.Config); len(config) > 0 {
		if flowPath := strings.TrimSpace(asPromptString(config["flow_path"])); flowPath != "" {
			out["flow_instance_path"] = flowPath
		}
	}
	return out
}

func promptInstanceVariables(raw json.RawMessage) map[string]any {
	config := parsePromptConfig(raw)
	if len(config) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range config {
		key = strings.TrimSpace(key)
		if key == "" || promptReservedConfigKeys()[key] {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func promptEntityStateFields(raw json.RawMessage) map[string]any {
	config := parsePromptConfig(raw)
	if len(config) == 0 {
		return nil
	}
	if entityState, ok := config["entity_state"].(map[string]any); ok {
		if fields, ok := entityState["fields"].(map[string]any); ok && len(fields) > 0 {
			return clonePromptVariables(fields)
		}
	}
	if fields, ok := config["fields"].(map[string]any); ok && len(fields) > 0 {
		return clonePromptVariables(fields)
	}
	return nil
}

func parsePromptConfig(raw json.RawMessage) map[string]any {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || len(obj) == 0 {
		return nil
	}
	return obj
}

func promptReservedConfigKeys() map[string]bool {
	return map[string]bool{
		"system_prompt":      true,
		"subscriptions":      true,
		"workspace_class":    true,
		"flow_path":          true,
		"manager_fallback":   true,
		"model_tier":         true,
		"conversation_mode":  true,
		"max_turns_per_task": true,
		"constraints":        true,
		"tools":              true,
		"allowed_tools":      true,
		"native_tools":       true,
		"emit_events":        true,
		"fields":             true,
		"entity_state":       true,
	}
}

func asPromptString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func promptPackagePolicyLineage(bundle *WorkflowContractBundle, packageKey string) []ProjectContractView {
	packageKey = strings.TrimSpace(packageKey)
	if bundle == nil || packageKey == "" {
		return nil
	}
	parentByKey := make(map[string]string, len(bundle.PackageTree))
	for _, pkg := range bundle.PackageTree {
		parentByKey[strings.TrimSpace(pkg.Key)] = strings.TrimSpace(pkg.ParentKey)
	}
	keys := make([]string, 0, 4)
	current := packageKey
	for current != "" {
		keys = append(keys, current)
		next, ok := parentByKey[current]
		if !ok {
			break
		}
		current = next
	}
	slices.Reverse(keys)
	lineage := make([]ProjectContractView, 0, len(keys))
	for _, key := range keys {
		view, ok := bundle.ProjectViewByKey(key)
		if !ok {
			continue
		}
		lineage = append(lineage, view)
	}
	return lineage
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
