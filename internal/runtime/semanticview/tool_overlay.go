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

func (s toolOverlaySource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := s.Source.ToolEntries()
	for id, tool := range s.tools {
		out[id] = tool
	}
	return out
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
		if err := tool.Validate(); err != nil {
			return nil, fmt.Errorf("runtime tool %q: %w", id, err)
		}
		cloned[id] = tool
	}
	return toolOverlaySource{Source: source, tools: cloned}, nil
}

// WithChannelRuntimeToolProjection replaces the complete executable channel
// tool projection while retaining every non-channel tool from the admitted
// source. The caller supplies one already-compiled current activation snapshot.
func WithChannelRuntimeToolProjection(source Source, tools map[string]runtimecontracts.ToolSchemaEntry) (Source, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required")
	}
	existing := source.ToolEntries()
	projected := make(map[string]runtimecontracts.ToolSchemaEntry, len(tools))
	for rawID, tool := range tools {
		id := strings.TrimSpace(rawID)
		if id == "" {
			return nil, fmt.Errorf("channel runtime tool id is required")
		}
		if tool.Category() != runtimecontracts.ToolCategoryChannelOperation {
			return nil, fmt.Errorf("channel runtime tool %q has category %q", id, tool.Category().String())
		}
		if err := tool.Validate(); err != nil {
			return nil, fmt.Errorf("channel runtime tool %q: %w", id, err)
		}
		if prior, collision := existing[id]; collision && prior.Category() != runtimecontracts.ToolCategoryChannelOperation {
			return nil, fmt.Errorf("channel runtime tool %q collides with a non-channel tool", id)
		}
		if _, duplicate := projected[id]; duplicate {
			return nil, fmt.Errorf("duplicate channel runtime tool %q", id)
		}
		projected[id] = tool
	}
	return channelToolProjectionSource{Source: source, tools: projected}, nil
}

type channelToolProjectionSource struct {
	Source
	tools map[string]runtimecontracts.ToolSchemaEntry
}

func (s channelToolProjectionSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := s.Source.ToolEntries()
	for id, tool := range out {
		if tool.Category() == runtimecontracts.ToolCategoryChannelOperation {
			delete(out, id)
		}
	}
	for id, tool := range s.tools {
		out[id] = tool
	}
	return out
}

func toolEntryMapSnapshot(in map[string]runtimecontracts.ToolSchemaEntry) map[string]runtimecontracts.ToolSchemaEntry {
	out := make(map[string]runtimecontracts.ToolSchemaEntry, len(in))
	for id, tool := range in {
		out[id] = tool
	}
	return out
}
