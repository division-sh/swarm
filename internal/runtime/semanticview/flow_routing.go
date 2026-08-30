package semanticview

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ResolveFlowInputAutoWire reports non-connect producer evidence. Cross-flow
// delivery is owned exclusively by compiled schema.yaml connect route plans.
func ResolveFlowInputAutoWire(source Source, targetFlowPath, eventType string) runtimecontracts.FlowInputAutoWireResolution {
	return ResolveNonConnectFlowInputProducer(source, targetFlowPath, eventType).AutoWireResolution()
}

func FlowInputProducerPatterns(source Source, targetFlowPath, eventType string) []string {
	return append([]string(nil), ResolveFlowInputAutoWire(source, targetFlowPath, eventType).Patterns...)
}

// RuntimeEventOwners derives receivers only from admitted flow-scoped handler
// subscriptions. Bare names that resolve to multiple canonical events are
// ambiguous and deliberately produce no owner.
func RuntimeEventOwners(source Source, eventType string) []runtimeidentity.ExecutableNode {
	if source == nil {
		return nil
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return nil
	}
	owners := make([]runtimeidentity.ExecutableNode, 0)
	canonicalEvents := map[string]struct{}{}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		resolution := ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
		if !resolution.Matched {
			continue
		}
		canonical := eventType
		if !strings.Contains(eventType, "/") {
			canonical = eventidentity.Normalize(source.ResolveExecutableNodeEventReference(node, eventType))
		}
		if canonical == "" {
			continue
		}
		owners = append(owners, node)
		canonicalEvents[canonical] = struct{}{}
	}
	if len(canonicalEvents) > 1 {
		return nil
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].Key() < owners[j].Key() })
	return owners
}
