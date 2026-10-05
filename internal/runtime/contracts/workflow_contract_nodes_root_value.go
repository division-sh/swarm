package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeDeclarationsValue(root yamlsource.Value) (map[string]SystemNodeContract, error) {
	if err := root.ValidateAcyclic(); err != nil {
		return nil, err
	}
	fields, err := uniqueYAMLMappingFields(root, "nodes.yaml declarations")
	if err != nil {
		return nil, err
	}
	if err := validateExactDeclarationNames(fields, "nodes.yaml declaration"); err != nil {
		return nil, err
	}
	var annotations []yamlsource.Value
	for _, field := range fields {
		notes, err := nodeHandlerAnnotations(field.Value)
		if err != nil {
			return nil, err
		}
		annotations = append(annotations, notes...)
	}
	if err := root.ValidateExpansion(annotations...); err != nil {
		return nil, err
	}
	out := make(map[string]SystemNodeContract, len(fields))
	for _, field := range fields {
		projected, err := projectSystemNodeValue(field.Value)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", field.Name, err)
		}
		projected.admissionProvenance = map[string]EffectiveValueProvenance{
			"declaration": authoredSourceProvenance(field.Value),
		}
		if err := collectNodeValueProvenance(field.Value, "", projected.admissionProvenance, annotations); err != nil {
			return nil, fmt.Errorf("node %q provenance: %w", field.Name, err)
		}
		// Rules have one effective sequence identity even when authored as a keyed
		// or singleton mapping. Transfer their admitted source facts, not their IDs.
		for event, handler := range projected.EventHandlers {
			for collection, rows := range map[string][]HandlerRuleEntry{"rules": handler.Rules, "on_complete": handler.OnComplete} {
				for index := range rows {
					prefix := NodeProvenanceMapPath("event_handlers", event) + fmt.Sprintf(".%s[%d]", collection, index)
					for path, source := range rows[index].admissionProvenance {
						projected.admissionProvenance[prefix+"."+path] = source
					}
					rows[index].admissionProvenance = nil
				}
			}
		}
		out[field.Name] = projected
	}
	return out, nil
}

func projectSystemNodeValue(value yamlsource.Value) (SystemNodeContract, error) {
	fields, err := nodeValueFields(value, "node", systemNodeContractFields)
	if err != nil {
		return SystemNodeContract{}, err
	}
	var out SystemNodeContract
	if err := projectNodeScalarFields(fields, &out); err != nil {
		return SystemNodeContract{}, err
	}
	if timers, present := fields["timers"]; present {
		items, err := timers.Sequence()
		if err != nil {
			return SystemNodeContract{}, err
		}
		for _, item := range items {
			timer, err := projectNodeTimerValue(item)
			if err != nil {
				return SystemNodeContract{}, err
			}
			out.Timers = append(out.Timers, timer)
		}
	}
	if state, present := fields["state_schema"]; present {
		out.StateSchema, err = projectNodeStateSchemaValue(state)
		if err != nil {
			return SystemNodeContract{}, err
		}
	}
	if gates, present := fields["gate_state"]; present {
		out.GateState, err = projectNodeGateStateValue(gates)
		if err != nil {
			return SystemNodeContract{}, err
		}
	}
	if handlers, present := fields["event_handlers"]; present {
		out.EventHandlers, err = projectNodeEventHandlersValue(handlers)
		if err != nil {
			return SystemNodeContract{}, err
		}
	}
	return out, nil
}

func projectNodeEventHandlersValue(value yamlsource.Value) (map[string]SystemNodeEventHandler, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("event_handlers at %s must be a mapping", value.Location())
	}
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]SystemNodeEventHandler, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" || name != field.Name {
			return nil, fmt.Errorf("event handler name %q at %s is invalid", field.Name, field.KeyLocation)
		}
		if _, duplicate := out[name]; duplicate {
			return nil, fmt.Errorf("duplicate event handler %q at %s", name, field.KeyLocation)
		}
		projected, err := projectNodeHandlerValue(field.Value)
		if err != nil {
			return nil, fmt.Errorf("event handler %q: %w", name, err)
		}
		out[name] = projected
	}
	return out, nil
}

func projectNodeHandlerValue(value yamlsource.Value) (SystemNodeEventHandler, error) {
	fields, err := nodeValueFields(value, "handler", handlerFieldOptions)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	var out SystemNodeEventHandler
	for _, name := range sortedContractKeys(fields) {
		field := fields[name]
		switch name {
		case "description":
			out.Description, err = nodeValueText(field, "handler.description")
			out.Description = strings.TrimSpace(out.Description)
		case "_note":
			continue
		case "advances_to":
			if field.Presence() == yamlsource.PresenceSequence || field.Presence() == yamlsource.PresenceEmptySequence {
				return SystemNodeEventHandler{}, fmt.Errorf("DIALECT-ADV-LIST: advances_to at %s is list, must be string", field.Location())
			}
			out.AdvancesTo, err = nodeValueText(field, "handler.advances_to")
			out.AdvancesTo = strings.TrimSpace(out.AdvancesTo)
		case "emit":
			out.Emit, err = projectNodeEmitValue(field)
		case "activity":
			out.Activity, err = projectNodeActivityValue(field)
		case "guard":
			out.Guard, err = projectNodeGuardValue(field)
		case "data_accumulation":
			out.DataAccumulation, err = projectNodeDataAccumulationValue(field)
		case "loop":
			out.Loop, err = projectNodeLoopValue(field)
		case "compute":
			out.Compute, err = projectNodeComputeValue(field)
		case "rules":
			out.Rules, err = projectNodeRuleRowsValue(field, handlerRuleDecodeContextRules)
		case "on_complete":
			out.OnComplete, err = projectNodeRuleRowsValue(field, handlerRuleDecodeContextOnComplete)
		case "join":
			out.Join, err = projectNodeJoinValue(field)
		case "on_success":
			out.OnSuccess, err = projectNodeOnSuccessValue(field)
		case "sets_gate":
			out.SetsGate, err = projectNodeGateEffectValue(field)
		case "query":
			out.Query, err = projectNodeQueryValue(field)
		case "accumulate":
			out.Accumulate, err = projectNodeAccumulateValue(field)
		case "fan_out":
			out.FanOut, err = projectNodeFanOutValue(field)
		case "group_by":
			out.GroupBy, err = projectNodeGroupByValue(field)
		case "filter":
			out.Filter, err = projectNodeFilterValue(field)
		case "reduce":
			out.Reduce, err = projectNodeReduceValue(field)
		case "count":
			out.Count, err = projectNodeCountValue(field)
		case "clear":
			out.Clear, err = projectNodeClearValue(field)
		case "clear_gates":
			out.ClearGates, err = projectNodeClearGatesValue(field)
		default:
			return SystemNodeEventHandler{}, fmt.Errorf("unsupported handler field %q at %s", name, field.Location())
		}
		if err != nil {
			return SystemNodeEventHandler{}, err
		}
	}
	if err := HandlerEmitSiteOwnershipError(out); err != nil {
		return SystemNodeEventHandler{}, err
	}
	return out, nil
}

func projectNodeGateEffectValue(value yamlsource.Value) (*GateSpec, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return nil, nil
	}
	if value.Presence() == yamlsource.PresenceScalar || value.Presence() == yamlsource.PresenceEmptyScalar {
		name, err := nodeValueText(value, "sets_gate")
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(name) == "" {
			return nil, nil
		}
		return &GateSpec{Name: strings.TrimSpace(name), Value: true}, nil
	}
	fields, err := nodeValueFields(value, "sets_gate", map[string]struct{}{"name": {}})
	if err != nil {
		return nil, err
	}
	name, present := fields["name"]
	if !present {
		return nil, nil
	}
	text, err := nodeValueText(name, "sets_gate.name")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	return &GateSpec{Name: strings.TrimSpace(text), Value: true}, nil
}
