package contracts

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectAgentDeclarationsValue(root yamlsource.Value) (map[string]AgentRegistryEntry, error) {
	if err := root.ValidateExpansion(); err != nil {
		return nil, err
	}
	declarations, err := uniqueYAMLMappingFields(root, "agents.yaml declarations")
	if err != nil {
		return nil, err
	}
	if err := validateExactDeclarationNames(declarations, "agents.yaml declaration"); err != nil {
		return nil, err
	}
	out := make(map[string]AgentRegistryEntry, len(declarations))
	for _, declaration := range declarations {
		if err := validateAgentRegistryMapKey(declaration.Name, declaration.Value.Location().File); err != nil {
			return nil, nodeValueError(declaration.Value, err)
		}
		entry, err := projectAgentValue(declaration.Name, declaration.Value)
		if err != nil {
			return nil, fmt.Errorf("agent %q: %w", declaration.Name, err)
		}
		entry.admissionProvenance = map[string]EffectiveValueProvenance{"declaration": authoredSourceProvenance(declaration.Value)}
		if err := collectNodeValueProvenance(declaration.Value, "", entry.admissionProvenance, nil); err != nil {
			return nil, err
		}
		// Memory is an enablement fact, not a separately carried source fact.
		delete(entry.admissionProvenance, "memory")
		out[declaration.Name] = entry
	}
	return out, nil
}

func projectAgentValue(key string, value yamlsource.Value) (AgentRegistryEntry, error) {
	fields, err := nodeValueFields(value, "agent", agentRegistryEntryFieldOptions)
	if err != nil {
		return AgentRegistryEntry{}, err
	}
	if _, present := fields["intent"]; !present {
		return AgentRegistryEntry{}, nodeValueError(value, (agentintent.Source{}).ValidateSyntax())
	}
	out := AgentRegistryEntry{AuthoredFields: map[string]bool{}}
	texts := map[string]*string{
		"id": &out.ID, "type": &out.Type, "role": &out.Role, "permissions_bundle": &out.PermissionsBundle,
		"workspace_class": &out.WorkspaceClass, "manager_fallback": &out.ManagerFallback,
		"node_type": &out.NodeType, "model": &out.Model, "implementation": &out.Implementation,
	}
	lists := map[string]*[]string{
		"subscriptions": &out.Subscriptions, "tools": &out.Tools, "permissions": &out.Permissions,
		"flow_data_access": &out.FlowDataAccess, "criteria": &out.Criteria, "emit_events": &out.EmitEvents,
	}
	for _, name := range sortedContractKeys(fields) {
		field := fields[name]
		out.AuthoredFields[name] = true
		switch {
		case texts[name] != nil:
			*texts[name], err = agentValueText(field)
		case lists[name] != nil:
			*lists[name], err = agentValueTextList(field)
		default:
			switch name {
			case "intent":
				out.Intent, err = agentintent.AdmitSource(field)
			case "memory":
				out.Memory, err = agentValueBool(field)
			case "max_turns_per_task":
				out.MaxTurnsPerTask, err = agentValueInteger(field)
			case "turn_timeout":
				out.TurnTimeout, err = projectAgentTurnTimeoutValue(field)
			case "entity_writes":
				out.EntityWrites, err = projectAgentWritesValue(field)
			case "native_tools":
				out.NativeTools, err = projectAgentNativeValue(field)
			case "mock":
				err = projectAgentMockValue(field, &out)
			case "data_access":
				out.DataAccess, err = projectAgentDataAccessValue(field)
			}
		}
		if err != nil {
			return AgentRegistryEntry{}, nodeValueError(field, err)
		}
		text := ""
		if texts[name] != nil {
			text = *texts[name]
		}
		if err := validateAgentAuthoredSpelling(key, name, text, out.Memory, out.MaxTurnsPerTask); err != nil {
			return AgentRegistryEntry{}, nodeValueError(field, err)
		}
	}
	if _, err := DeclaredAgentID(key, out); err != nil {
		return AgentRegistryEntry{}, nodeValueError(value, err)
	}
	return out, nil
}

func projectAgentTurnTimeoutValue(value yamlsource.Value) (*timeridentity.TurnTimeout, error) {
	fields, err := nodeValueFields(value, "turn_timeout", map[string]struct{}{"after": {}, "emit": {}})
	if err != nil {
		return nil, err
	}
	after, hasAfter := fields["after"]
	emit, hasEmit := fields["emit"]
	if !hasAfter || !hasEmit {
		return nil, fmt.Errorf("turn_timeout requires after and emit")
	}
	text, err := agentValueText(after)
	if err != nil {
		return nil, fmt.Errorf("turn_timeout.after: %w", err)
	}
	duration, err := time.ParseDuration(text)
	if err != nil {
		return nil, fmt.Errorf("turn_timeout.after: %w", err)
	}
	event, err := agentValueText(emit)
	if err != nil {
		return nil, fmt.Errorf("turn_timeout.emit: %w", err)
	}
	out := &timeridentity.TurnTimeout{After: duration, Emit: event}
	return out, out.Validate()
}

func validateAgentAuthoredSpelling(key, name, text string, memory bool, turns int) error {
	duplicate := false
	switch name {
	case "id":
		if text != strings.TrimSpace(text) {
			return fmt.Errorf("id must be canonical non-empty text")
		}
		duplicate = text == key
	case "type":
		if strings.TrimSpace(text) == "" {
			return fmt.Errorf("type must be non-empty text")
		}
		duplicate = strings.TrimSpace(text) == DefaultAgentType
	case "memory":
		duplicate = !memory
		name = "memory: false"
	case "max_turns_per_task":
		if turns <= 0 {
			return fmt.Errorf("max_turns_per_task must be positive")
		}
		duplicate = turns == DefaultAgentMaxTurnsPerTask
	case "workspace_class":
		duplicate = strings.TrimSpace(text) == ""
	}
	if duplicate {
		return fmt.Errorf("explicit %s repeats the implicit default and is not supported", name)
	}
	return nil
}

func projectAgentNativeValue(value yamlsource.Value) (map[string]any, error) {
	fields, err := nodeValueFields(value, "native_tools", map[string]struct{}{"bash": {}, "web_search": {}, "file_io": {}})
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, capability := range sortedContractKeys(fields) {
		enabled, err := agentValueBool(fields[capability])
		if err != nil {
			return nil, nodeValueError(fields[capability], err)
		}
		out[capability] = enabled
	}
	return out, nil
}

func agentValueText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	if scalar.Tag != "!!str" {
		return "", fmt.Errorf("%s at %s must be text", value.SemanticPath(), value.Location())
	}
	return scalar.Value, nil
}

func agentValueBool(value yamlsource.Value) (bool, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return false, err
	}
	if scalar.Tag != "!!bool" {
		return false, fmt.Errorf("must be a boolean")
	}
	var out bool
	err = value.Project(&out)
	return out, err
}

func agentValueInteger(value yamlsource.Value) (int, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return 0, err
	}
	if scalar.Tag != "!!int" {
		return 0, fmt.Errorf("must be an integer")
	}
	var out int
	err = value.Project(&out)
	return out, err
}

func agentValueTextList(value yamlsource.Value) ([]string, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := agentValueText(item)
		if err != nil {
			return nil, err
		}
		out = append(out, text)
	}
	return out, nil
}

func projectAgentWritesValue(value yamlsource.Value) (map[string]AgentEntityWriteDecl, error) {
	entities, err := uniqueYAMLMappingFields(value, "entity_writes")
	if err != nil {
		return nil, err
	}
	out := map[string]AgentEntityWriteDecl{}
	for _, entity := range entities {
		fields, err := nodeValueFields(entity.Value, "entity_writes", map[string]struct{}{"create": {}, "save": {}})
		if err != nil {
			return nil, err
		}
		var declaration AgentEntityWriteDecl
		for _, branch := range []struct {
			name   string
			target *AgentEntityWriteRule
		}{{"create", &declaration.Create}, {"save", &declaration.Save}} {
			if field, present := fields[branch.name]; present {
				*branch.target, err = projectAgentWriteRuleValue(field)
				if err != nil {
					return nil, nodeValueError(field, err)
				}
			}
		}
		out[entity.Name] = declaration
	}
	return out, nil
}

func projectAgentWriteRuleValue(value yamlsource.Value) (AgentEntityWriteRule, error) {
	if value.Presence() == yamlsource.PresenceScalar {
		text, err := agentValueText(value)
		if err != nil {
			return AgentEntityWriteRule{}, err
		}
		if strings.TrimSpace(text) == "all" {
			return AgentEntityWriteRule{All: true}, nil
		}
		return AgentEntityWriteRule{}, fmt.Errorf("entity write rule must be all or an explicit field list")
	}
	fields, err := agentValueTextList(value)
	if err != nil {
		return AgentEntityWriteRule{}, err
	}
	out := AgentEntityWriteRule{}
	seen := map[string]bool{}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "all" {
			return out, fmt.Errorf("entity write rule cannot mix reserved keyword all with explicit fields")
		}
		if field != "" && !seen[field] {
			out.Fields = append(out.Fields, field)
			seen[field] = true
		}
	}
	if len(out.Fields) == 0 {
		return out, fmt.Errorf("entity write rule explicit field list must not be empty")
	}
	return out, nil
}

func projectAgentMockValue(value yamlsource.Value, out *AgentRegistryEntry) error {
	fields, err := nodeValueFields(value, "mock", map[string]struct{}{"kind": {}, "module": {}, "post_tool_tail_latency_ms": {}})
	if err != nil {
		return err
	}
	for _, field := range []struct {
		name   string
		target *string
	}{{"kind", &out.Mock.Kind}, {"module", &out.Mock.Module}} {
		if value, present := fields[field.name]; present {
			*field.target, err = agentValueText(value)
			if err != nil {
				return nodeValueError(value, err)
			}
		}
	}
	if value, present := fields["post_tool_tail_latency_ms"]; present {
		out.Mock.PostToolTailLatencyMS, err = agentValueInteger(value)
		if err != nil {
			return nodeValueError(value, err)
		}
		if out.Mock.PostToolTailLatencyMS < 0 {
			return nodeValueError(value, fmt.Errorf("mock latency must be nonnegative"))
		}
	}
	return nil
}

func projectAgentDataAccessValue(value yamlsource.Value) ([]DurableDataAccessRef, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]DurableDataAccessRef, 0, len(items))
	for _, item := range items {
		fields, err := nodeValueFields(item, "data_access", map[string]struct{}{"data": {}, "flow_path": {}})
		if err != nil {
			return nil, err
		}
		var access DurableDataAccessRef
		for _, field := range []struct {
			name   string
			target *string
		}{{"data", &access.Data}, {"flow_path", &access.FlowPath}} {
			value, present := fields[field.name]
			if !present {
				if field.name == "flow_path" {
					continue
				}
				return nil, nodeValueError(item, fmt.Errorf("data_access data is required"))
			}
			*field.target, err = agentValueText(value)
			if err != nil {
				return nil, nodeValueError(value, err)
			}
			if *field.target == "" || strings.TrimSpace(*field.target) != *field.target {
				return nil, nodeValueError(value, fmt.Errorf("data_access %s must be canonical non-empty text", field.name))
			}
		}
		out = append(out, access)
	}
	return out, nil
}
