package semanticview

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type toolOverlaySource struct {
	Source
	tools map[string]runtimecontracts.ToolSchemaEntry
}

func (s toolOverlaySource) BaseSemanticSource() Source { return s.Source }

func (s toolOverlaySource) ConnectorPackImportsApplied() bool {
	type marker interface{ ConnectorPackImportsApplied() bool }
	marked, ok := s.Source.(marker)
	return ok && marked.ConnectorPackImportsApplied()
}

func (s toolOverlaySource) ProviderTriggerEventsApplied() bool {
	type marker interface{ ProviderTriggerEventsApplied() bool }
	marked, ok := s.Source.(marker)
	return ok && marked.ProviderTriggerEventsApplied()
}

func (s toolOverlaySource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := s.Source.ToolEntries()
	for id, tool := range s.tools {
		out[id] = tool
	}
	return out
}

func (s toolOverlaySource) ToolEntryForAgent(agentID, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	if tool, ok := s.Source.ToolEntryForAgent(agentID, toolID); ok {
		return tool, true
	}
	tool, ok := s.tools[strings.TrimSpace(toolID)]
	return tool, ok
}

// WithRuntimeTools adds platform-compiled tools without mutating the authored
// bundle or granting authors a second declaration path.
func WithRuntimeTools(source Source, tools map[string]runtimecontracts.ToolSchemaEntry) (Source, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required")
	}
	if len(tools) == 0 {
		return source, nil
	}
	existing := source.ToolEntries()
	cloned := make(map[string]runtimecontracts.ToolSchemaEntry, len(tools))
	for rawID, tool := range tools {
		id := strings.TrimSpace(rawID)
		if id == "" {
			return nil, fmt.Errorf("runtime tool id is required")
		}
		if _, exists := existing[id]; exists {
			return nil, fmt.Errorf("runtime tool %q collides with an authored or imported tool", id)
		}
		if _, exists := cloned[id]; exists {
			return nil, fmt.Errorf("duplicate runtime tool %q", id)
		}
		cloned[id] = tool
	}
	return toolOverlaySource{Source: source, tools: cloned}, nil
}
