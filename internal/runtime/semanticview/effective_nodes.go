package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func NodeEffectiveProduces(source Source, nodeID string) []string {
	nodeID = strings.TrimSpace(nodeID)
	if source == nil || nodeID == "" {
		return nil
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		return bundle.NodeEffectiveProduces(nodeID)
	}
	entry, ok := source.NodeEntries()[nodeID]
	if !ok {
		return nil
	}
	if handlers := source.NodeEventHandlers(nodeID); len(handlers) > 0 {
		entry.EventHandlers = handlers
	}
	return runtimecontracts.EffectiveSystemNodeProduces(entry)
}

func NodeEffectiveSubscriptions(source Source, nodeID string) []string {
	nodeID = strings.TrimSpace(nodeID)
	if source == nil || nodeID == "" {
		return nil
	}
	if subs := source.NodeRuntimeSubscriptions(nodeID); len(subs) > 0 {
		return append([]string{}, subs...)
	}
	entry, ok := source.NodeEntries()[nodeID]
	if !ok {
		return nil
	}
	if handlers := source.NodeEventHandlers(nodeID); len(handlers) > 0 {
		entry.EventHandlers = handlers
	}
	return runtimecontracts.EffectiveSystemNodeSubscriptions(entry)
}
