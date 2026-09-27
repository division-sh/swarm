package contracts

import (
	"fmt"
	"reflect"
	"strings"
)

func mergeNodeContracts(bundle *WorkflowContractBundle, entries map[string]SystemNodeContract, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedNodeSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedNodes[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped node id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedNodes[scopedKey] = entry
		bundle.scopedNodeSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousNodeAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.nodeSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Nodes[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged node id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Nodes, key)
			delete(bundle.nodeSources, key)
			bundle.ambiguousNodeAliases[key] = struct{}{}
			continue
		}
		bundle.Nodes[key] = entry
		bundle.nodeSources[key] = source
	}
	return nil
}
func mergeEventContracts(bundle *WorkflowContractBundle, entries map[string]EventCatalogEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedEventSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedEvents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped event id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedEvents[scopedKey] = entry
		bundle.scopedEventSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousEventAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.eventSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Events[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged event id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Events, key)
			delete(bundle.eventSources, key)
			bundle.ambiguousEventAliases[key] = struct{}{}
			continue
		}
		bundle.Events[key] = entry
		bundle.eventSources[key] = source
	}
	return nil
}
func mergeAgentContracts(bundle *WorkflowContractBundle, entries map[string]AgentRegistryEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if err := validateAgentRegistryMapKey(key, source.File); err != nil {
			return err
		}
		entry = EffectiveAgentRegistryEntry(key, entry)
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedAgentSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedAgents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped agent id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedAgents[scopedKey] = entry
		bundle.scopedAgentSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousAgentAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.agentSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Agents[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged agent id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Agents, key)
			delete(bundle.agentSources, key)
			bundle.ambiguousAgentAliases[key] = struct{}{}
			continue
		}
		bundle.Agents[key] = entry
		bundle.agentSources[key] = source
	}
	return nil
}
func mergeToolContracts(bundle *WorkflowContractBundle, entries map[string]ToolSchemaEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedToolSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedTools[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped tool id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedTools[scopedKey] = entry
		bundle.scopedToolSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousToolAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.toolSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Tools[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged tool id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Tools, key)
			delete(bundle.toolSources, key)
			bundle.ambiguousToolAliases[key] = struct{}{}
			continue
		}
		bundle.Tools[key] = entry
		bundle.toolSources[key] = source
	}
	return nil
}
