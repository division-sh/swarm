package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeAccumulateValue(value yamlsource.Value) (*AccumulateSpec, error) {
	fields, err := nodeValueFields(value, "accumulate", accumulateFieldOptions, nil)
	if err != nil {
		return nil, err
	}
	var out AccumulateSpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"into", &out.Into}, {"from", &out.From},
		{"description", &out.Description}, {"window", &out.Window},
		{"dedup_by", &out.DedupBy},
	} {
		field, present := fields[entry.key]
		if !present {
			continue
		}
		text, err := nodeValueText(field, "accumulate."+entry.key)
		if err != nil {
			return nil, err
		}
		*entry.target = strings.TrimSpace(text)
	}
	out.WindowPath = paths.Parse(out.Window)
	out.DedupPath = paths.Parse(out.DedupBy)
	return &out, nil
}

func projectNodeFanOutValue(value yamlsource.Value) (*FanOutSpec, error) {
	fields, err := nodeValueFields(value, "fan_out", fanOutFieldOptions, map[string]string{
		"element_id":    "fan-out identity derives from the canonical declaration site",
		"target":        "route fan_out.emit through typed consumers, output pins and connect",
		"emit_per_item": "use emit: <event> or emit: {event, fields}",
		"emit_mapping":  "move per-item payload ownership into fan_out.emit",
	})
	if err != nil {
		return nil, err
	}
	var out FanOutSpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"items_from", &out.ItemsFrom}, {"as", &out.As}, {"identity", &out.Identity},
	} {
		field, present := fields[entry.key]
		if !present {
			continue
		}
		text, err := nodeValueText(field, "fan_out."+entry.key)
		if err != nil {
			return nil, err
		}
		*entry.target = strings.TrimSpace(text)
	}
	if err := ValidateFanOutAlias(out.As); err != nil {
		return nil, fmt.Errorf("fan_out.%w", err)
	}
	if max, present := fields["max_items"]; present {
		if max.Presence() != yamlsource.PresenceScalar {
			return nil, fmt.Errorf("fan_out.max_items at %s must be a positive integer", max.Location())
		}
		out.MaxItemsSet = true
		if err := max.Project(&out.MaxItems); err != nil {
			return nil, fmt.Errorf("fan_out.max_items at %s: %w", max.Location(), err)
		}
	}
	if emit, present := fields["emit"]; present {
		out.Emit, err = projectNodeEmitValue(emit)
		if err != nil {
			return nil, err
		}
	}
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	if _, err := ValidateFanOutItemsSource(out); err != nil {
		return nil, err
	}
	if err := ValidateFanOutMaxItems(out); err != nil {
		return nil, err
	}
	return &out, nil
}
