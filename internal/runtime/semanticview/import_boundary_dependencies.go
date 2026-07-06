package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

const (
	ImportBoundaryDependencyPolicy     = "policy"
	ImportBoundaryDependencyCredential = "credential"
)

type ImportBoundaryDependencyIssue struct {
	Kind             string
	Dependency       string
	ParentPackageKey string
	ChildPackageKey  string
	ImportLabel      string
	Reference        string
	Message          string
}

func ResolvePolicyForFlow(source Source, flowID string) runtimecontracts.PolicyDocument {
	if source == nil {
		return emptyPolicyDocument()
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		return resolvePolicyForFlowWithRaw(source, flowID, bundle.ResolvedPolicyForFlow)
	}
	return resolvePolicyForFlowWithRaw(source, flowID, source.ResolvedPolicyForFlow)
}

func ResolvePolicyForNode(source Source, nodeID string) runtimecontracts.PolicyDocument {
	if source == nil {
		return emptyPolicyDocument()
	}
	if nodeSource, ok := source.NodeContractSource(nodeID); ok {
		return ResolvePolicyForFlow(source, nodeSource.FlowID)
	}
	return ResolvePolicyForFlow(source, "")
}

func CredentialStoreKeyForFlow(source Source, flowID, key string) (string, bool) {
	key = strings.TrimSpace(key)
	if source == nil || key == "" {
		return key, false
	}
	deps, ok := importBoundaryDependencyContext(source, flowID)
	if !ok {
		return key, false
	}
	required := normalizeDependencySet(deps.child.Manifest.Requires.Credentials)
	if _, ok := required[key]; !ok {
		return "", true
	}
	value := strings.TrimSpace(deps.site.Bind.Credentials[key])
	return value, true
}

func CredentialStoreKeyForActor(source Source, actorID, key string) (string, bool) {
	return CredentialStoreKeyForActorFlow(source, actorID, "", key)
}

func CredentialStoreKeyForActorFlow(source Source, actorID, flowID, key string) (string, bool) {
	actorID = strings.TrimSpace(actorID)
	flowID = strings.TrimSpace(flowID)
	if source == nil {
		return strings.TrimSpace(key), false
	}
	if flowID != "" {
		return CredentialStoreKeyForFlow(source, flowID, key)
	}
	if agentSource, ok := source.AgentContractSource(actorID); ok {
		return CredentialStoreKeyForFlow(source, agentSource.FlowID, key)
	}
	return strings.TrimSpace(key), false
}

func ImportBoundaryDependencyIssues(source Source) []ImportBoundaryDependencyIssue {
	if source == nil {
		return nil
	}
	var issues []ImportBoundaryDependencyIssue
	for _, deps := range importBoundaryDependencyContexts(source) {
		issues = append(issues, importBoundaryPolicyDependencyIssues(source, deps)...)
		issues = append(issues, importBoundaryCredentialDependencyIssues(deps)...)
	}
	sort.Slice(issues, func(i, j int) bool {
		return strings.Compare(importBoundaryDependencyIssueSortKey(issues[i]), importBoundaryDependencyIssueSortKey(issues[j])) < 0
	})
	return issues
}

func resolvePolicyForFlowWithRaw(source Source, flowID string, raw func(string) runtimecontracts.PolicyDocument) runtimecontracts.PolicyDocument {
	flowID = strings.TrimSpace(flowID)
	if source == nil || raw == nil {
		return emptyPolicyDocument()
	}
	deps, ok := importBoundaryDependencyContext(source, flowID)
	if !ok {
		return clonePolicyDocument(raw(flowID))
	}
	doc := localPolicyForImportedFlow(source, deps.child, flowID)
	parentFlowID := strings.TrimSpace(deps.parent.OwningFlowID)
	parentPolicy := ResolvePolicyForFlow(source, parentFlowID)
	for _, key := range deps.child.Manifest.Requires.Policy {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		deletePolicyValueAtPath(&doc, key)
		if ref := strings.TrimSpace(deps.site.Bind.Policy[key]); ref != "" {
			if path, ok := importBoundaryParentPolicyPath(ref); ok {
				if value, found := policyValueAtPath(parentPolicy, path); found {
					setPolicyValueAtPath(&doc, key, value)
				}
			}
			continue
		}
		if value, ok := deps.child.Manifest.Requires.PolicyDefaults[key]; ok {
			setPolicyValueAtPath(&doc, key, value)
		}
	}
	return doc
}

type importBoundaryDependencyCtx struct {
	parent ProjectScope
	child  ProjectScope
	site   importBoundarySite
}

func importBoundaryDependencyContext(source Source, flowID string) (importBoundaryDependencyCtx, bool) {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return importBoundaryDependencyCtx{}, false
	}
	flow, ok := source.FlowScopeByID(flowID)
	if !ok {
		return importBoundaryDependencyCtx{}, false
	}
	flowPackageKey := normalizeImportPackageKey(flow.PackageKey)
	var bestPackage importBoundaryDependencyCtx
	bestPackageLen := -1
	var bestPackageWithRequires importBoundaryDependencyCtx
	bestPackageWithRequiresLen := -1
	for _, deps := range importBoundaryDependencyContexts(source) {
		siteFlowID := strings.TrimSpace(deps.site.FlowID)
		if siteFlowID != "" {
			if siteFlowID == flowID {
				return deps, true
			}
			continue
		}
		childKey := normalizeImportPackageKey(deps.child.Key)
		if importBoundaryPackageKeyContains(childKey, flowPackageKey) {
			if keyLen := len(childKey); keyLen > bestPackageLen {
				bestPackage = deps
				bestPackageLen = keyLen
			}
			if !importRequiresEmpty(deps.child.Manifest.Requires) {
				if keyLen := len(childKey); keyLen > bestPackageWithRequiresLen {
					bestPackageWithRequires = deps
					bestPackageWithRequiresLen = keyLen
				}
			}
			continue
		}
		if strings.TrimSpace(deps.child.OwningFlowID) == flowID {
			return deps, true
		}
	}
	if bestPackageWithRequiresLen >= 0 {
		return bestPackageWithRequires, true
	}
	if bestPackageLen >= 0 {
		return bestPackage, true
	}
	return importBoundaryDependencyCtx{}, false
}

func importBoundaryPackageKeyContains(parentKey, flowPackageKey string) bool {
	parentKey = normalizeImportPackageKey(parentKey)
	flowPackageKey = normalizeImportPackageKey(flowPackageKey)
	if parentKey == "" || parentKey == "." || flowPackageKey == "" || flowPackageKey == "." {
		return false
	}
	return flowPackageKey == parentKey || strings.HasPrefix(flowPackageKey, parentKey+"/")
}

func importBoundaryDependencyContexts(source Source) []importBoundaryDependencyCtx {
	projectByKey, _ := importBoundaryScopeIndexes(source)
	var out []importBoundaryDependencyCtx
	for _, parent := range source.ProjectScopes() {
		parent.Key = normalizeImportPackageKey(parent.Key)
		for _, site := range importBoundarySites(parent) {
			child, ok := projectByKey[site.PackageKey]
			if !ok {
				continue
			}
			out = append(out, importBoundaryDependencyCtx{parent: parent, child: child, site: site})
		}
	}
	return out
}

func importBoundaryPolicyDependencyIssues(source Source, deps importBoundaryDependencyCtx) []ImportBoundaryDependencyIssue {
	required := normalizeDependencySet(deps.child.Manifest.Requires.Policy)
	var issues []ImportBoundaryDependencyIssue
	for key := range required {
		ref := strings.TrimSpace(deps.site.Bind.Policy[key])
		if ref == "" {
			if _, ok := deps.child.Manifest.Requires.PolicyDefaults[key]; !ok {
				issues = append(issues, importBoundaryDependencyIssue(deps, "missing_policy_binding", key, "", "declared package policy dependency has no import binding or package default"))
			}
			continue
		}
		path, ok := importBoundaryParentPolicyPath(ref)
		if !ok {
			issues = append(issues, importBoundaryDependencyIssue(deps, "unsupported_policy_reference", key, ref, "policy binding must reference parent.policy.<path> or policy.<path>"))
			continue
		}
		parentPolicy := ResolvePolicyForFlow(source, deps.parent.OwningFlowID)
		if _, ok := policyValueAtPath(parentPolicy, path); !ok {
			issues = append(issues, importBoundaryDependencyIssue(deps, "missing_parent_policy", key, ref, "policy binding references a parent policy value that does not exist"))
		}
	}
	for key := range deps.site.Bind.Policy {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := required[key]; !ok {
			issues = append(issues, importBoundaryDependencyIssue(deps, "unknown_policy_binding", key, deps.site.Bind.Policy[key], "policy bind key is not declared by the imported package requires.policy"))
		}
	}
	return issues
}

func importBoundaryCredentialDependencyIssues(deps importBoundaryDependencyCtx) []ImportBoundaryDependencyIssue {
	required := normalizeDependencySet(deps.child.Manifest.Requires.Credentials)
	var issues []ImportBoundaryDependencyIssue
	for key := range required {
		if strings.TrimSpace(deps.site.Bind.Credentials[key]) == "" {
			issues = append(issues, importBoundaryDependencyIssue(deps, "missing_credential_binding", key, "", "declared package credential dependency has no import binding"))
		}
	}
	for key := range deps.site.Bind.Credentials {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := required[key]; !ok {
			issues = append(issues, importBoundaryDependencyIssue(deps, "unknown_credential_binding", key, deps.site.Bind.Credentials[key], "credential bind key is not declared by the imported package requires.credentials"))
		}
	}
	return issues
}

func importBoundaryDependencyIssue(deps importBoundaryDependencyCtx, kind, dependency, ref, message string) ImportBoundaryDependencyIssue {
	return ImportBoundaryDependencyIssue{
		Kind:             kind,
		Dependency:       strings.TrimSpace(dependency),
		ParentPackageKey: normalizeImportPackageKey(deps.parent.Key),
		ChildPackageKey:  normalizeImportPackageKey(deps.child.Key),
		ImportLabel:      strings.TrimSpace(deps.site.Label),
		Reference:        strings.TrimSpace(ref),
		Message:          strings.TrimSpace(message),
	}
}

func localPolicyForImportedFlow(source Source, child ProjectScope, flowID string) runtimecontracts.PolicyDocument {
	out := clonePolicyDocument(child.Policy)
	if flow, ok := source.FlowScopeByID(flowID); ok {
		mergePolicyValues(&out, flow.Policy)
	}
	return out
}

func importBoundaryParentPolicyPath(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	switch {
	case strings.HasPrefix(ref, "parent.policy."):
		return strings.TrimSpace(strings.TrimPrefix(ref, "parent.policy.")), true
	case strings.HasPrefix(ref, "policy."):
		return strings.TrimSpace(strings.TrimPrefix(ref, "policy.")), true
	default:
		return "", false
	}
}

func policyValueAtPath(doc runtimecontracts.PolicyDocument, path string) (runtimecontracts.PolicyValue, bool) {
	path = strings.TrimSpace(path)
	if path == "" || len(doc.Values) == 0 {
		return runtimecontracts.PolicyValue{}, false
	}
	root, rest := splitPolicyKey(path)
	value, ok := doc.Values[root]
	if !ok {
		return runtimecontracts.PolicyValue{}, false
	}
	if strings.TrimSpace(rest) == "" {
		return clonePolicyValue(value), true
	}
	descended, ok := descendPolicyValue(value.Value, rest)
	if !ok {
		return runtimecontracts.PolicyValue{}, false
	}
	return runtimecontracts.PolicyValue{Value: cloneAny(descended)}, true
}

func setPolicyValueAtPath(doc *runtimecontracts.PolicyDocument, path string, value runtimecontracts.PolicyValue) {
	if doc == nil {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	if doc.Values == nil {
		doc.Values = map[string]runtimecontracts.PolicyValue{}
	}
	root, rest := splitPolicyKey(path)
	if rest == "" {
		doc.Values[root] = clonePolicyValue(value)
		return
	}
	current := map[string]any{}
	if existing, ok := doc.Values[root]; ok {
		if typed, ok := cloneAny(existing.Value).(map[string]any); ok {
			current = typed
		}
	}
	setNestedValue(current, rest, cloneAny(value.Value))
	doc.Values[root] = runtimecontracts.PolicyValue{Value: current}
}

func setNestedValue(root map[string]any, path string, value any) {
	parts := strings.Split(strings.TrimSpace(path), ".")
	current := root
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return
		}
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		next, _ := current[part].(map[string]any)
		if next == nil {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
}

func mergePolicyValues(dst *runtimecontracts.PolicyDocument, src runtimecontracts.PolicyDocument) {
	if dst == nil {
		return
	}
	if dst.Values == nil {
		dst.Values = map[string]runtimecontracts.PolicyValue{}
	}
	for key, value := range src.Values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		dst.Values[key] = clonePolicyValue(value)
	}
	if dst.Criteria == nil {
		dst.Criteria = map[string]runtimecontracts.PolicyCriteriaSet{}
	}
	for key, value := range src.Criteria {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		dst.Criteria[key] = clonePolicyCriteriaSet(value)
	}
	if dst.Validation == nil {
		dst.Validation = map[string]runtimecontracts.PolicyValidationSet{}
	}
	for key, value := range src.Validation {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		dst.Validation[key] = clonePolicyValidationSet(value)
	}
}

func deletePolicyValueAtPath(doc *runtimecontracts.PolicyDocument, path string) {
	if doc == nil || len(doc.Values) == 0 {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	root, rest := splitPolicyKey(path)
	if rest == "" {
		delete(doc.Values, root)
		return
	}
	existing, ok := doc.Values[root]
	if !ok {
		return
	}
	current, ok := cloneAny(existing.Value).(map[string]any)
	if !ok {
		return
	}
	deleteNestedValue(current, rest)
	doc.Values[root] = runtimecontracts.PolicyValue{Value: current}
}

func deleteNestedValue(root map[string]any, path string) {
	parts := strings.Split(strings.TrimSpace(path), ".")
	current := root
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return
		}
		if i == len(parts)-1 {
			delete(current, part)
			return
		}
		next, _ := current[part].(map[string]any)
		if next == nil {
			return
		}
		current = next
	}
}

func clonePolicyDocument(in runtimecontracts.PolicyDocument) runtimecontracts.PolicyDocument {
	out := runtimecontracts.PolicyDocument{
		Values:     map[string]runtimecontracts.PolicyValue{},
		Criteria:   map[string]runtimecontracts.PolicyCriteriaSet{},
		Validation: map[string]runtimecontracts.PolicyValidationSet{},
	}
	for key, value := range in.Values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Values[key] = clonePolicyValue(value)
	}
	for key, value := range in.Criteria {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Criteria[key] = clonePolicyCriteriaSet(value)
	}
	for key, value := range in.Validation {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Validation[key] = clonePolicyValidationSet(value)
	}
	return out
}

func clonePolicyCriteriaSet(in runtimecontracts.PolicyCriteriaSet) runtimecontracts.PolicyCriteriaSet {
	out := runtimecontracts.PolicyCriteriaSet{
		Classes: map[string]runtimecontracts.PolicyCriteriaClass{},
		Rules:   make([]runtimecontracts.PolicyCriteriaRule, 0, len(in.Rules)),
	}
	for key, value := range in.Classes {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Classes[key] = runtimecontracts.PolicyCriteriaClass{
			Disposition: strings.TrimSpace(value.Disposition),
		}
	}
	for _, rule := range in.Rules {
		cloned := runtimecontracts.PolicyCriteriaRule{
			ID:     strings.TrimSpace(rule.ID),
			Class:  strings.TrimSpace(rule.Class),
			Text:   strings.TrimSpace(rule.Text),
			Params: map[string]runtimecontracts.PolicyCriteriaParam{},
		}
		for key, value := range rule.Params {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			cloned.Params[key] = runtimecontracts.PolicyCriteriaParam{Value: cloneAny(value.Value)}
		}
		out.Rules = append(out.Rules, cloned)
	}
	return out
}

func clonePolicyValue(in runtimecontracts.PolicyValue) runtimecontracts.PolicyValue {
	return runtimecontracts.PolicyValue{
		Value:       cloneAny(in.Value),
		Description: strings.TrimSpace(in.Description),
		Override:    in.Override,
	}
}

func clonePolicyValidationSet(in runtimecontracts.PolicyValidationSet) runtimecontracts.PolicyValidationSet {
	out := runtimecontracts.PolicyValidationSet{
		Classes: map[string]runtimecontracts.PolicyValidationClass{},
		Inputs:  map[string]string{},
		Rules:   make([]runtimecontracts.PolicyValidationRule, 0, len(in.Rules)),
	}
	for key, value := range in.Classes {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Classes[key] = runtimecontracts.PolicyValidationClass{
			Disposition: strings.TrimSpace(value.Disposition),
		}
	}
	for key, value := range in.Inputs {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out.Inputs[key] = strings.TrimSpace(value)
	}
	for _, rule := range in.Rules {
		cloned := runtimecontracts.PolicyValidationRule{
			ID:     strings.TrimSpace(rule.ID),
			Class:  strings.TrimSpace(rule.Class),
			Text:   strings.TrimSpace(rule.Text),
			Params: map[string]runtimecontracts.PolicyCriteriaParam{},
			Check: runtimecontracts.PolicyValidationCheck{
				Equal: clonePolicyValidationEqualCheck(rule.Check.Equal),
			},
		}
		if rule.PinCandidate != nil {
			value := *rule.PinCandidate
			cloned.PinCandidate = &value
		}
		for key, value := range rule.Params {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			cloned.Params[key] = runtimecontracts.PolicyCriteriaParam{Value: cloneAny(value.Value)}
		}
		if len(cloned.Params) == 0 {
			cloned.Params = nil
		}
		out.Rules = append(out.Rules, cloned)
	}
	return out
}

func clonePolicyValidationEqualCheck(in *runtimecontracts.PolicyValidationEqualCheck) *runtimecontracts.PolicyValidationEqualCheck {
	if in == nil {
		return nil
	}
	return &runtimecontracts.PolicyValidationEqualCheck{
		Left:  strings.TrimSpace(in.Left),
		Right: strings.TrimSpace(in.Right),
	}
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneAny(item)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[fmt.Sprint(key)] = cloneAny(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneAny(item)
		}
		return out
	default:
		return value
	}
}

func emptyPolicyDocument() runtimecontracts.PolicyDocument {
	return runtimecontracts.PolicyDocument{
		Values:     map[string]runtimecontracts.PolicyValue{},
		Criteria:   map[string]runtimecontracts.PolicyCriteriaSet{},
		Validation: map[string]runtimecontracts.PolicyValidationSet{},
	}
}

func normalizeDependencySet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func importBoundaryDependencyIssueSortKey(issue ImportBoundaryDependencyIssue) string {
	return strings.Join([]string{
		issue.Kind,
		issue.ParentPackageKey,
		issue.ChildPackageKey,
		issue.Dependency,
		issue.Reference,
	}, "|")
}
