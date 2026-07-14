package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"gopkg.in/yaml.v3"
)

type PromptResolver interface {
	LoadPromptForAgent(cfg models.AgentConfig, mode string) (string, bool, error)
}

type PromptFileResolution struct {
	AgentID string
	Entry   AgentRegistryEntry
	Source  ContractItemSource
	Path    string
}

type BundlePromptResolver struct {
	bundle            *WorkflowContractBundle
	repoRoot          string
	policyResolver    PromptPolicyResolver
	promptVariablesMu sync.RWMutex
	promptVariables   map[string]map[string]any
}

type PromptPolicyResolver func(source ContractItemSource) PolicyDocument

type BundlePromptResolverOptions struct {
	PolicyResolver PromptPolicyResolver
}

var (
	promptTemplateFieldPattern = regexp.MustCompile(`\{[^}]+\}`)
	promptTokenPattern         = regexp.MustCompile(`\{\{([a-zA-Z0-9_]+)\}\}`)
)

func NewBundlePromptResolver(bundle *WorkflowContractBundle) *BundlePromptResolver {
	return NewBundlePromptResolverWithOptions(bundle, BundlePromptResolverOptions{})
}

func NewBundlePromptResolverWithOptions(bundle *WorkflowContractBundle, opts BundlePromptResolverOptions) *BundlePromptResolver {
	return &BundlePromptResolver{
		bundle:          bundle,
		repoRoot:        promptContractsRepoRoot(),
		policyResolver:  opts.PolicyResolver,
		promptVariables: map[string]map[string]any{},
	}
}

func (r *BundlePromptResolver) LoadPromptForAgent(cfg models.AgentConfig, mode string) (string, bool, error) {
	resolution, found, err := r.ResolvePromptFileForAgent(cfg, mode)
	if err != nil {
		return "", false, err
	}
	if !found {
		if refs := r.undeliverableCriteriaRefsForAgent(cfg); len(refs) > 0 {
			agentID := strings.TrimSpace(cfg.ID)
			if agentID == "" {
				agentID = strings.TrimSpace(cfg.Role)
			}
			return "", false, fmt.Errorf("criteria delivery requires a resolved prompt for criteria-bearing agent %s; criteria refs: %s", agentID, strings.Join(refs, ", "))
		}
		return "", false, nil
	}
	raw, err := os.ReadFile(resolution.Path)
	if err != nil {
		return "", false, err
	}
	repoRoot := promptContractsRepoRoot()
	if r != nil && strings.TrimSpace(r.repoRoot) != "" {
		repoRoot = r.repoRoot
	}
	prompt, err := r.renderPromptWithRuntimeVariables(repoRoot, string(raw), resolution.Source, cfg)
	if err != nil {
		return "", false, fmt.Errorf("render prompt %s: %w", filepath.Base(resolution.Path), err)
	}
	prompt, err = generatedCriteriaPromptSection(r.workflowBundle(), resolution.Source, resolution.Entry, cfg, prompt)
	if err != nil {
		return "", false, fmt.Errorf("render criteria for prompt %s: %w", filepath.Base(resolution.Path), err)
	}
	return strings.TrimSpace(prompt), true, nil
}

func (r *BundlePromptResolver) undeliverableCriteriaRefsForAgent(cfg models.AgentConfig) []string {
	if refs := normalizeStrings(cfg.Criteria); len(refs) > 0 {
		return refs
	}
	bundle := r.workflowBundle()
	if bundle == nil {
		return nil
	}
	candidates, _ := r.promptLookupPlan(cfg)
	for _, agentID := range candidates {
		record, ok := bundlePromptAgentRecordByLogicalID(bundle, agentID)
		if !ok {
			continue
		}
		if refs := normalizeStrings(record.Entry.Criteria); len(refs) > 0 {
			return refs
		}
	}
	return nil
}

func generatedCriteriaPromptSection(bundle *WorkflowContractBundle, source ContractItemSource, entry AgentRegistryEntry, cfg models.AgentConfig, prompt string) (string, error) {
	refs := normalizeStrings(entry.Criteria)
	runtimeRefs := normalizeStrings(cfg.Criteria)
	if len(runtimeRefs) > 0 {
		switch {
		case len(refs) == 0:
			return "", fmt.Errorf("criteria delivery requires contract agent criteria; runtime criteria refs are not authoritative: %s", strings.Join(runtimeRefs, ", "))
		case !sameStringSet(refs, runtimeRefs):
			return "", fmt.Errorf("criteria delivery runtime refs must match contract agent criteria; contract refs: %s; runtime refs: %s", strings.Join(refs, ", "), strings.Join(runtimeRefs, ", "))
		}
	}
	if len(refs) == 0 {
		return prompt, nil
	}
	flowID := strings.TrimSpace(source.FlowID)
	if flowID == "" {
		return "", fmt.Errorf("criteria delivery requires a flow-scoped agent")
	}
	if bundle == nil {
		return "", fmt.Errorf("criteria delivery requires a workflow bundle")
	}
	policy := bundle.ResolvedPolicyForFlow(flowID)
	var section strings.Builder
	section.WriteString("\n\n## Contract Criteria\n\n")
	for i, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		set, ok := policy.Criteria[ref]
		if !ok {
			return "", fmt.Errorf("criteria set %q does not resolve in flow %s", ref, flowID)
		}
		if i > 0 {
			section.WriteString("\n")
		}
		writeCriteriaSetPromptSection(&section, ref, set)
	}
	return strings.TrimSpace(prompt) + section.String(), nil
}

func writeCriteriaSetPromptSection(out *strings.Builder, name string, set PolicyCriteriaSet) {
	out.WriteString("### ")
	out.WriteString(strings.TrimSpace(name))
	out.WriteString("\n\nClasses:\n")
	for _, className := range sortedCriteriaClassNames(set.Classes) {
		out.WriteString("- ")
		out.WriteString(className)
		disposition := strings.TrimSpace(set.Classes[className].Disposition)
		if disposition != "" {
			out.WriteString(": ")
			out.WriteString(disposition)
		}
		out.WriteString("\n")
	}
	out.WriteString("\nRules:\n")
	for _, rule := range sortedCriteriaRules(set.Rules) {
		out.WriteString("- ")
		out.WriteString(strings.TrimSpace(rule.ID))
		if className := strings.TrimSpace(rule.Class); className != "" {
			out.WriteString(" [")
			out.WriteString(className)
			out.WriteString("]")
		}
		out.WriteString(": ")
		out.WriteString(strings.TrimSpace(rule.Text))
		out.WriteString("\n")
		if len(rule.Params) > 0 {
			paramNames := make([]string, 0, len(rule.Params))
			for name := range rule.Params {
				name = strings.TrimSpace(name)
				if name != "" {
					paramNames = append(paramNames, name)
				}
			}
			sort.Strings(paramNames)
			for _, paramName := range paramNames {
				out.WriteString("  - ")
				out.WriteString(paramName)
				out.WriteString(": ")
				out.WriteString(renderCriteriaParamValue(rule.Params[paramName].Value))
				out.WriteString("\n")
			}
		}
	}
}

func sortedCriteriaRules(in []PolicyCriteriaRule) []PolicyCriteriaRule {
	out := append([]PolicyCriteriaRule{}, in...)
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.TrimSpace(out[i].ID)
		right := strings.TrimSpace(out[j].ID)
		if left == right {
			return strings.TrimSpace(out[i].Class) < strings.TrimSpace(out[j].Class)
		}
		return left < right
	})
	return out
}

func renderCriteriaParamValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func (r *BundlePromptResolver) ResolvePromptFileForAgent(cfg models.AgentConfig, mode string) (PromptFileResolution, bool, error) {
	candidates, dirs := r.promptLookupPlan(cfg)
	bundle := r.workflowBundle()
	for _, agentID := range candidates {
		record := bundleAgentRecord{}
		if bundle != nil {
			record, _ = bundlePromptAgentRecordByLogicalID(bundle, agentID)
		}
		resolution, found, err := resolvePromptFileInDirs(dirs[agentID], agentID, record.Entry, record.Source, mode)
		if err != nil {
			return PromptFileResolution{}, false, err
		}
		if found {
			return resolution, true, nil
		}
	}
	return PromptFileResolution{}, false, nil
}

func bundlePromptAgentRecordByLogicalID(bundle *WorkflowContractBundle, logicalID string) (bundleAgentRecord, bool) {
	record, found := bundleAgentRecordByLogicalID(bundle, logicalID)
	if bundle == nil {
		return record, found
	}
	source, sourceOK := bundle.AgentContractSource(logicalID)
	entry, entryOK := bundle.AgentEntry(logicalID)
	if !sourceOK || !entryOK {
		return record, found
	}
	return bundleAgentRecord{
		LogicalID: strings.TrimSpace(logicalID),
		Entry:     entry,
		Source:    source,
	}, true
}

func ResolvePromptFileForContractAgent(bundle *WorkflowContractBundle, logicalID string, entry AgentRegistryEntry, source ContractItemSource, mode string) (PromptFileResolution, bool, error) {
	if bundle == nil {
		return PromptFileResolution{}, false, nil
	}
	return resolvePromptFileInDirs(promptDirsForAgentSource(bundle, source), logicalID, entry, source, mode)
}

func (r *BundlePromptResolver) promptLookupPlan(cfg models.AgentConfig) ([]string, map[string][]string) {
	candidates := promptIDCandidates(cfg)
	bundle := r.workflowBundle()
	if bundle == nil {
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

func (r *BundlePromptResolver) workflowBundle() *WorkflowContractBundle {
	if r == nil {
		return nil
	}
	return r.bundle
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
	mode := canonicalPromptLookupValue(cfg.FlowID)
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
	if mode := strings.TrimSpace(flow.Schema.Mode); mode != "" {
		return canonicalPromptLookupValue(mode)
	}
	return canonicalPromptLookupValue(flow.Paths.Mode)
}

func promptFlowPromptMode(bundle *WorkflowContractBundle, flowID string) string {
	if bundle == nil {
		return ""
	}
	flow, ok := bundle.FlowViewByID(flowID)
	if !ok {
		return ""
	}
	if mode := strings.TrimSpace(flow.Schema.Mode); mode != "" {
		return strings.ToLower(mode)
	}
	return strings.ToLower(strings.TrimSpace(flow.Paths.Mode))
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

func promptContractsRepoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}

func resolvePromptFileInDirs(dirs []string, logicalID string, entry AgentRegistryEntry, source ContractItemSource, mode string) (PromptFileResolution, bool, error) {
	logicalID = strings.TrimSpace(logicalID)
	if logicalID == "" {
		return PromptFileResolution{}, false, nil
	}
	for _, dir := range uniqueStrings(dirs...) {
		path, found, err := resolvePromptFileInDir(dir, logicalID, entry, mode)
		if err != nil {
			return PromptFileResolution{}, false, err
		}
		if found {
			return PromptFileResolution{
				AgentID: logicalID,
				Entry:   entry,
				Source:  source,
				Path:    path,
			}, true, nil
		}
	}
	return PromptFileResolution{}, false, nil
}

func resolvePromptFileInDir(promptsDir, logicalID string, entry AgentRegistryEntry, mode string) (string, bool, error) {
	promptsDir = strings.TrimSpace(promptsDir)
	if promptsDir == "" {
		return "", false, nil
	}
	for _, candidate := range promptPathCandidates(promptsDir, logicalID, entry, strings.ToLower(strings.TrimSpace(mode))) {
		info, err := os.Stat(candidate)
		if err == nil {
			if info.IsDir() {
				continue
			}
			return candidate, true, nil
		}
		if !os.IsNotExist(err) {
			return "", false, err
		}
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

func promptDirsForAgentSource(bundle *WorkflowContractBundle, source ContractItemSource) []string {
	if bundle == nil {
		return nil
	}
	dirs := promptDirsForSource(bundle, source)
	dirs = append(dirs, promptBundlePromptDirs(bundle)...)
	return uniqueStrings(dirs...)
}

func promptDirsForSource(bundle *WorkflowContractBundle, source ContractItemSource) []string {
	if bundle == nil {
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

func (r *BundlePromptResolver) renderPromptWithRuntimeVariables(repoRoot, promptText string, source ContractItemSource, cfg models.AgentConfig) (string, error) {
	if !promptTokenPattern.MatchString(promptText) {
		return promptText, nil
	}
	vars, err := r.promptRuntimeVariables(repoRoot, source, cfg)
	if err != nil {
		return promptText, nil
	}
	return renderPromptTemplatePreservingUnknown(promptText, vars), nil
}

func (r *BundlePromptResolver) promptRuntimeVariables(repoRoot string, source ContractItemSource, cfg models.AgentConfig) (map[string]any, error) {
	bundle := r.workflowBundle()
	scopeKey := promptVariableScopeKey(source, cfg)
	if r == nil {
		return promptVariableValues(bundle, source, cfg), nil
	}
	r.promptVariablesMu.RLock()
	if vars, ok := r.promptVariables[scopeKey]; ok {
		defer r.promptVariablesMu.RUnlock()
		return clonePromptVariables(vars), nil
	}
	r.promptVariablesMu.RUnlock()

	vars := promptVariableValuesWithPolicy(bundle, source, cfg, r.promptPolicy(source))

	r.promptVariablesMu.Lock()
	r.promptVariables[scopeKey] = clonePromptVariables(vars)
	r.promptVariablesMu.Unlock()
	return vars, nil
}

func (r *BundlePromptResolver) promptPolicy(source ContractItemSource) PolicyDocument {
	if r != nil && r.policyResolver != nil {
		return r.policyResolver(source)
	}
	return promptResolvedPolicy(r.workflowBundle(), source)
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
	return promptVariableValuesWithPolicy(bundle, source, cfg, promptResolvedPolicy(bundle, source))
}

func promptVariableValuesWithPolicy(bundle *WorkflowContractBundle, source ContractItemSource, cfg models.AgentConfig, policy PolicyDocument) map[string]any {
	vars := promptRuntimeTokens(cfg)
	for key, value := range promptEntityStateFields(cfg.Config) {
		vars[key] = value
	}
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
	if flowPath := cfg.CanonicalFlowPath(); flowPath != "" {
		out["flow_instance_path"] = flowPath
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
		"model":              true,
		"memory":             true,
		"max_turns_per_task": true,
		"constraints":        true,
		"tools":              true,
		"native_tools":       true,
		"criteria":           true,
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
