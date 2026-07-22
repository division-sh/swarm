package semanticview

import (
	"strconv"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

const handlerRetryBasePolicyKey = "handler_retry_base_seconds"

// HandlerRetryBase is the canonical projection of the retry cadence shared by
// system handlers and agent sessions.
func HandlerRetryBase(source Source) time.Duration {
	value, ok := PolicyValueForFlow(source, "", handlerRetryBasePolicyKey)
	if !ok {
		return time.Second
	}
	var seconds float64
	switch typed := value.Value.(type) {
	case int:
		seconds = float64(typed)
	case int64:
		seconds = float64(typed)
	case float64:
		seconds = typed
	case string:
		seconds, _ = strconv.ParseFloat(strings.TrimSpace(typed), 64)
	}
	if !(seconds > 0) {
		return time.Second
	}
	duration := time.Duration(seconds * float64(time.Second))
	if duration <= 0 {
		return time.Second
	}
	return duration
}

type PolicyValueResolution struct {
	Value    runtimecontracts.PolicyValue
	OwnerKey string
}

func PolicyValueForFlow(source Source, flowID, key string) (runtimecontracts.PolicyValue, bool) {
	if source == nil {
		return runtimecontracts.PolicyValue{}, false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return runtimecontracts.PolicyValue{}, false
	}
	doc := source.ResolvedPolicyForFlow(strings.TrimSpace(flowID))
	root, rest := splitPolicyKey(key)
	value, ok := doc.Values[root]
	if !ok {
		return runtimecontracts.PolicyValue{}, false
	}
	if strings.TrimSpace(rest) == "" {
		return value, true
	}
	descended, ok := descendPolicyValue(value.Value, rest)
	if !ok {
		return runtimecontracts.PolicyValue{}, false
	}
	return runtimecontracts.PolicyValue{Value: descended}, true
}

func PolicyValueForFlowWithOwner(source Source, flowID, key string) (PolicyValueResolution, bool) {
	return policyValueForFlowWithOwner(source, flowID, key, map[string]struct{}{})
}

func policyValueForFlowWithOwner(source Source, flowID, key string, seen map[string]struct{}) (PolicyValueResolution, bool) {
	if source == nil {
		return PolicyValueResolution{}, false
	}
	flowID = strings.TrimSpace(flowID)
	key = strings.TrimSpace(key)
	if key == "" {
		return PolicyValueResolution{}, false
	}
	seenKey := flowID + "\x00" + key
	if _, ok := seen[seenKey]; ok {
		return PolicyValueResolution{}, false
	}
	seen[seenKey] = struct{}{}
	if deps, ok := importBoundaryDependencyContext(source, flowID); ok {
		return importBoundaryPolicyValueWithOwner(source, deps, flowID, key, seen)
	}
	return rawPolicyValueForFlowWithOwner(source, flowID, key)
}

func rawPolicyValueForFlowWithOwner(source Source, flowID, key string) (PolicyValueResolution, bool) {
	flowID = strings.TrimSpace(flowID)
	for _, scope := range policyScopesForFlow(source, flowID) {
		if value, ok := policyValueAtPath(scope.Policy, key); ok {
			return PolicyValueResolution{Value: value, OwnerKey: policyOwnerFlowKey(scope.ID)}, true
		}
	}
	if value, ok := policyValueAtPath(source.ResolvedPolicyForFlow(flowID), key); ok {
		return PolicyValueResolution{Value: value, OwnerKey: policyOwnerRootKey()}, true
	}
	return PolicyValueResolution{}, false
}

func importBoundaryPolicyValueWithOwner(source Source, deps importBoundaryDependencyCtx, flowID, key string, seen map[string]struct{}) (PolicyValueResolution, bool) {
	required := normalizeDependencySet(deps.child.Manifest.Requires.Policy)
	if _, ok := required[key]; ok {
		if ref := strings.TrimSpace(deps.site.Bind.Policy[key]); ref != "" {
			path, ok := importBoundaryParentPolicyPath(ref)
			if !ok {
				return PolicyValueResolution{}, false
			}
			return policyValueForFlowWithOwner(source, strings.TrimSpace(deps.parent.OwningFlowID), path, seen)
		}
		if value, ok := deps.child.Manifest.Requires.PolicyDefaults[key]; ok {
			return PolicyValueResolution{Value: clonePolicyValue(value), OwnerKey: policyOwnerPackageKey(deps.child.Key)}, true
		}
		return PolicyValueResolution{}, false
	}
	if flow, ok := source.FlowScopeByID(strings.TrimSpace(flowID)); ok {
		if value, ok := policyValueAtPath(flow.Policy, key); ok {
			return PolicyValueResolution{Value: value, OwnerKey: policyOwnerFlowKey(flow.ID)}, true
		}
	}
	if value, ok := policyValueAtPath(deps.child.Policy, key); ok {
		return PolicyValueResolution{Value: value, OwnerKey: policyOwnerPackageKey(deps.child.Key)}, true
	}
	return PolicyValueResolution{}, false
}

func policyScopesForFlow(source Source, flowID string) []FlowScope {
	if source == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return nil
	}
	target, ok := source.FlowScopeByID(flowID)
	if !ok {
		return nil
	}
	targetPath := strings.Trim(strings.TrimSpace(target.Path), "/")
	if targetPath == "" {
		return []FlowScope{target}
	}
	scopes := source.FlowScopes()
	out := make([]FlowScope, 0, len(scopes))
	for _, scope := range scopes {
		path := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if path == "" {
			continue
		}
		if targetPath == path || strings.HasPrefix(targetPath, path+"/") {
			out = append(out, scope)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if len(strings.Trim(strings.TrimSpace(out[i].Path), "/")) > len(strings.Trim(strings.TrimSpace(out[j].Path), "/")) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func policyOwnerRootKey() string {
	return "root"
}

func policyOwnerFlowKey(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return policyOwnerRootKey()
	}
	return "flow:" + flowID
}

func policyOwnerPackageKey(packageKey string) string {
	packageKey = normalizeImportPackageKey(packageKey)
	if packageKey == "" {
		return "package:."
	}
	return "package:" + packageKey
}

func FindAgentEntry(source Source, agentID, role string) (runtimecontracts.AgentRegistryEntry, bool) {
	if source == nil {
		return runtimecontracts.AgentRegistryEntry{}, false
	}
	agentID = strings.TrimSpace(agentID)
	role = strings.TrimSpace(role)
	if agentID != "" {
		if entry, ok := source.AgentEntries()[agentID]; ok {
			return entry, true
		}
	}
	if role != "" {
		for _, entry := range source.AgentEntries() {
			if strings.EqualFold(strings.TrimSpace(entry.Role), role) || strings.EqualFold(strings.TrimSpace(entry.ID), role) {
				return entry, true
			}
		}
	}
	return runtimecontracts.AgentRegistryEntry{}, false
}

func splitPolicyKey(key string) (string, string) {
	key = strings.TrimSpace(key)
	if idx := strings.IndexByte(key, '.'); idx >= 0 {
		return strings.TrimSpace(key[:idx]), strings.TrimSpace(key[idx+1:])
	}
	return key, ""
}

func descendPolicyValue(value any, remainder string) (any, bool) {
	if strings.TrimSpace(remainder) == "" {
		return value, true
	}
	for _, part := range strings.Split(strings.TrimSpace(remainder), ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		switch current := value.(type) {
		case map[string]any:
			next, ok := current[part]
			if !ok {
				return nil, false
			}
			value = next
		case map[string]runtimecontracts.PolicyValue:
			next, ok := current[part]
			if !ok {
				return nil, false
			}
			value = next.Value
		default:
			return nil, false
		}
	}
	return value, true
}
