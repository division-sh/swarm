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
				merged, ok := mergeEventCatalogEntry(bundle.Events[key], entry)
				if !ok {
					return fmt.Errorf("duplicate merged event id %q from %s and %s", key, existing.File, source.File)
				}
				bundle.Events[key] = merged
				bundle.eventSources[key] = source
				continue
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
func mergeEventCatalogEntry(existing EventCatalogEntry, incoming EventCatalogEntry) (EventCatalogEntry, bool) {
	merged := existing
	var ok bool
	if merged.Emitter, ok = mergeEventEmitterRef(existing.Emitter, incoming.Emitter); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.EmitterType, ok = mergeStringValue(existing.EmitterType, incoming.EmitterType); !ok {
		return EventCatalogEntry{}, false
	}
	merged.AlternateEmitters = mergeStringLists(existing.AlternateEmitters, incoming.AlternateEmitters)
	if merged.Consumer, ok = mergeStringSliceValue(existing.Consumer, incoming.Consumer); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.ConsumerType, ok = mergeStringSliceValue(existing.ConsumerType, incoming.ConsumerType); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Intercepted, ok = mergeBoolValue(existing.Intercepted, incoming.Intercepted); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Passthrough, ok = mergeBoolValue(existing.Passthrough, incoming.Passthrough); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.RuntimeHandling, ok = mergeStringValue(existing.RuntimeHandling, incoming.RuntimeHandling); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.OwningNode, ok = mergeStringValue(existing.OwningNode, incoming.OwningNode); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.DeliveryChannel, ok = mergeStringValue(existing.DeliveryChannel, incoming.DeliveryChannel); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Payload, ok = mergeEventPayloadSpec(existing.Payload, incoming.Payload); !ok {
		return EventCatalogEntry{}, false
	}
	merged.Required = mergeStringLists(existing.Required, incoming.Required)
	return merged, true
}
func mergeStringValue(existing, incoming string) (string, bool) {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	switch {
	case existing == "":
		return incoming, true
	case incoming == "":
		return existing, true
	case existing == incoming:
		return existing, true
	default:
		return "", false
	}
}
func mergeStringLists(existing, incoming []string) []string {
	return normalizeStrings(append(append([]string{}, existing...), incoming...))
}
func mergeStringSliceValue(existing, incoming []string) ([]string, bool) {
	switch {
	case len(existing) == 0:
		return append([]string{}, incoming...), true
	case len(incoming) == 0:
		return append([]string{}, existing...), true
	default:
		return mergeStringLists(existing, incoming), true
	}
}
func mergeBoolValue(existing, incoming bool) (bool, bool) {
	if !existing {
		return incoming, true
	}
	if !incoming {
		return existing, true
	}
	return existing == incoming, existing == incoming
}
func mergeEventEmitterRef(existing, incoming EventEmitterRef) (EventEmitterRef, bool) {
	switch {
	case isEmptyEventEmitterRef(existing):
		return incoming, true
	case isEmptyEventEmitterRef(incoming):
		return existing, true
	case reflect.DeepEqual(existing, incoming):
		return existing, true
	default:
		return EventEmitterRef{}, false
	}
}
func mergeEventPayloadSpec(existing, incoming EventPayloadSpec) (EventPayloadSpec, bool) {
	switch {
	case isEmptyEventPayloadSpec(existing):
		return incoming, true
	case isEmptyEventPayloadSpec(incoming):
		return existing, true
	case reflect.DeepEqual(existing, incoming):
		return existing, true
	default:
		return EventPayloadSpec{}, false
	}
}
func isEmptyEventEmitterRef(ref EventEmitterRef) bool {
	return strings.TrimSpace(ref.AgentID) == "" && strings.TrimSpace(ref.NodeID) == ""
}
func isEmptyEventPayloadSpec(spec EventPayloadSpec) bool {
	return strings.TrimSpace(spec.Type) == "" && len(spec.Properties) == 0 && len(spec.Required) == 0
}
func mergeAgentContracts(bundle *WorkflowContractBundle, entries map[string]AgentRegistryEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
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
func contractSourceWithFile(source ContractItemSource, file string) ContractItemSource {
	source.File = file
	return source
}
