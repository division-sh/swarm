package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeDataAccumulationValue(value yamlsource.Value) (WorkflowDataAccumulation, error) {
	fields, err := nodeValueFields(value, "data_accumulation", map[string]struct{}{
		"writes": {}, "source_event": {},
	}, nil)
	if err != nil {
		return WorkflowDataAccumulation{}, err
	}
	var out WorkflowDataAccumulation
	if source, present := fields["source_event"]; present {
		out.SourceEvent, err = nodeValueText(source, "data_accumulation.source_event")
		if err != nil {
			return WorkflowDataAccumulation{}, err
		}
		out.SourceEvent = strings.TrimSpace(out.SourceEvent)
	}
	if writes, present := fields["writes"]; present {
		items, err := writes.Sequence()
		if err != nil {
			return WorkflowDataAccumulation{}, err
		}
		for _, item := range items {
			write, err := projectNodeDataWriteValue(item)
			if err != nil {
				return WorkflowDataAccumulation{}, err
			}
			out.Writes = append(out.Writes, write)
		}
	}
	return out, nil
}

func projectNodeDataWriteValue(value yamlsource.Value) (WorkflowDataWrite, error) {
	if value.Presence() == yamlsource.PresenceScalar || value.Presence() == yamlsource.PresenceEmptyScalar {
		field, err := nodeValueText(value, "data_accumulation.writes field")
		if err != nil {
			return WorkflowDataWrite{}, err
		}
		out := WorkflowDataWrite{Field: strings.TrimSpace(field)}
		return out, hydrateWorkflowDataWrite(&out)
	}
	fields, err := nodeValueFields(value, "data_accumulation.writes", map[string]struct{}{
		"field": {}, "source_field": {}, "target_field": {}, "target_path": {}, "target": {},
		"op": {}, "key": {}, "index": {}, "value": {},
	}, map[string]string{"expression": "use value: ${...}"})
	if err != nil {
		return WorkflowDataWrite{}, err
	}
	var out WorkflowDataWrite
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"field", &out.Field}, {"source_field", &out.SourceField},
		{"target_field", &out.TargetField}, {"target_path", &out.TargetPathRef},
		{"target", &out.TargetRef},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "data_accumulation.writes."+entry.key)
			if err != nil {
				return WorkflowDataWrite{}, err
			}
		}
	}
	if operation, present := fields["op"]; present {
		name, err := nodeValueText(operation, "data_accumulation.writes.op")
		if err != nil {
			return WorkflowDataWrite{}, err
		}
		out.Operation = WorkflowDataOperation(name)
	}
	for _, entry := range []struct {
		key    string
		target *ExpressionValue
	}{
		{"key", &out.Key}, {"index", &out.Index}, {"value", &out.Value},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = projectNodeExpressionValue(field)
			if err != nil {
				return WorkflowDataWrite{}, fmt.Errorf("data_accumulation.writes.%s at %s: %w", entry.key, field.Location(), err)
			}
		}
	}
	if err := hydrateWorkflowDataWrite(&out); err != nil {
		return WorkflowDataWrite{}, fmt.Errorf("data_accumulation.writes at %s: %w", value.Location(), err)
	}
	return out, nil
}
