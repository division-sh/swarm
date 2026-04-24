package runstart

import (
	"fmt"
	"sort"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	"swarm/internal/runtime/semanticview"
)

type RootInputSet struct {
	Declared []string
	Routable []string
}

func DeriveRootInputSet(source semanticview.Source) (RootInputSet, error) {
	if source == nil {
		return RootInputSet{}, fmt.Errorf("semantic source is required")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.RootSchema == nil {
		return RootInputSet{}, fmt.Errorf("root schema is required")
	}
	declared := normalizeUnique(bundle.RootSchema.Pins.Inputs.Events)
	routeTable, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		return RootInputSet{}, err
	}
	routable := make([]string, 0, len(declared))
	for _, eventType := range declared {
		if len(routeTable.Resolve(eventType)) > 0 || rootInputConsumedByFlow(source, eventType) {
			routable = append(routable, eventType)
		}
	}
	return RootInputSet{Declared: declared, Routable: routable}, nil
}

func ValidateInputEvents(source semanticview.Source, inputEvents []string) (RootInputSet, error) {
	inputEvents = normalizeUnique(inputEvents)
	if len(inputEvents) == 0 {
		return RootInputSet{}, nil
	}
	set, err := DeriveRootInputSet(source)
	if err != nil {
		return set, err
	}
	declared := stringSet(set.Declared)
	routable := stringSet(set.Routable)
	for _, eventType := range inputEvents {
		if _, ok := declared[eventType]; !ok {
			return set, fmt.Errorf("run.start input %q is not declared in root input pins; declared root inputs: %s", eventType, strings.Join(set.Declared, ", "))
		}
		if _, ok := routable[eventType]; !ok {
			return set, fmt.Errorf("run.start input %q is declared but has no runtime route; routable root inputs: %s", eventType, strings.Join(set.Routable, ", "))
		}
	}
	return set, nil
}

func normalizeUnique(values []string) []string {
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
	sort.Strings(out)
	return out
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func rootInputConsumedByFlow(source semanticview.Source, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if source == nil || eventType == "" {
		return false
	}
	for _, scope := range source.FlowScopes() {
		if !containsString(scope.InputEvents, eventType) {
			continue
		}
		for _, node := range scope.Nodes {
			nodeID := strings.TrimSpace(node.ID)
			if nodeID == "" {
				continue
			}
			if containsString(source.NodeRuntimeSubscriptions(nodeID), eventType) {
				return true
			}
		}
		for _, agent := range scope.Agents {
			if containsString(agent.Subscriptions, eventType) {
				return true
			}
		}
	}
	return false
}

func containsString(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}
