package semanticview

import (
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
)

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
