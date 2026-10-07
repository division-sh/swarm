package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// AdmitFlowSchemaValue is the sole source-schema vocabulary admission owner.
func AdmitFlowSchemaValue(root yamlsource.Value) (FlowSchemaDocument, error) {
	if err := root.ValidateExpansion(); err != nil {
		return FlowSchemaDocument{}, err
	}
	fields, err := schemaValueFields(root, "schema", flowSchemaDocumentFields, true)
	if err != nil {
		return FlowSchemaDocument{}, err
	}
	var out FlowSchemaDocument
	if err := schemaValueTexts(fields, map[string]*string{"name": &out.Name}, false); err != nil {
		return out, err
	}
	for _, name := range sortedContractKeys(fields) {
		value := fields[name]
		switch name {
		case "name":
			continue
		case "instance":
			var text string
			text, err = schemaValueText(value, true)
			if err == nil {
				out.Instance, err = ParseTemplateInstanceField(text)
			}
			if err != nil {
				err = fmt.Errorf("instance must use instance: <field>: %w", err)
			}
		case "ingress":
			out.Ingress, err = projectSchemaIngressValue(value)
		case "imports":
			out.Imports, err = projectSchemaImportsValue(value)
		case "connect":
			out.Connect, err = projectSchemaConnectValue(value)
		case "pins":
			out.Pins, err = projectSchemaPinsValue(value)
		case "required_agents":
			out.RequiredAgentsDeclared = true
			out.RequiredAgents, err = projectSchemaRequiredAgentsValue(value)
		case "auto_emit_on_create":
			var members map[string]yamlsource.Value
			members, err = schemaValueFields(value, "auto_emit_on_create", map[string]struct{}{"event": {}, "description": {}}, true)
			if err == nil {
				err = schemaValueRequiredTexts(value, members, map[string]*string{"event": &out.AutoEmitOnCreate.Event})
			}
			if err == nil {
				err = schemaValueTexts(members, map[string]*string{"description": &out.AutoEmitOnCreate.Description}, false)
			}
		case "stages":
			out.StageDeclarations, err = projectSchemaStagesValue(value)
		case "loops":
			out.LoopDeclarations, err = projectSchemaLoopsValue(value)
		case "schedules":
			out.Schedules, err = projectSchemaSchedulesValue(value)
		}
		if err != nil {
			return FlowSchemaDocument{}, nodeValueError(value, err)
		}
	}
	if len(out.Schedules) > 0 && !out.Instance.Empty() {
		return FlowSchemaDocument{}, nodeValueError(fields["schedules"], fmt.Errorf("schedules fire per run; per-instance cadence is not supported"))
	}
	out.admissionProvenance = map[string]EffectiveValueProvenance{"declaration": authoredSourceProvenance(root)}
	if err := collectNodeValueProvenance(root, "", out.admissionProvenance, nil); err != nil {
		return FlowSchemaDocument{}, err
	}
	deriveSchemaProvenance(&out)
	return out, nil
}

func deriveSchemaProvenance(out *FlowSchemaDocument) {
	out.admissionProvenance["mode"] = EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "flow.shape_from_instance", InputPaths: []string{"instance"}}
	if out.Instance.Empty() {
		fact := out.admissionProvenance["mode"]
		fact.SourcePresence = "missing"
		out.admissionProvenance["mode"] = fact
	}
	for _, direction := range []string{"inputs", "outputs"} {
		count := len(out.Pins.Inputs.EventPins)
		if direction == "outputs" {
			count = len(out.Pins.Outputs.EventPins)
		}
		for i := 0; i < count; i++ {
			path := fmt.Sprintf("pins.%s[%d]", direction, i)
			if fact := out.admissionProvenance[path]; fact.SourcePresence == yamlsource.PresenceScalar.String() {
				out.admissionProvenance[path+".event"] = EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "flow.pin_local_event_identity", InputPaths: []string{path}}
			}
		}
	}
	for _, stage := range out.StageDeclarations.Entries {
		for i := range stage.Timers {
			path := NodeProvenanceMapPath("stages", stage.ID) + fmt.Sprintf(".timers[%d]", i)
			if _, authored := out.admissionProvenance[path+".id"]; !authored {
				inputs := []string{NodeProvenanceMapPath("stages", stage.ID)}
				for _, key := range []string{"emit", "advances_to"} {
					if _, present := out.admissionProvenance[path+"."+key]; present {
						inputs = append(inputs, path+"."+key)
					}
				}
				out.admissionProvenance[path+".id"] = EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "flow.stage_timer_default_id", InputPaths: inputs}
			}
		}
	}
}

func schemaValueFields(value yamlsource.Value, owner string, allowed map[string]struct{}, nonempty bool) (map[string]yamlsource.Value, error) {
	fields, err := nodeValueFields(value, owner, allowed)
	if err != nil {
		return nil, err
	}
	if nonempty && len(fields) == 0 {
		return nil, nodeValueError(value, fmt.Errorf("%s must be a non-empty mapping", owner))
	}
	return fields, nil
}

func schemaValueText(value yamlsource.Value, exact bool) (string, error) {
	text, err := nodeValueText(value, "text")
	if err != nil {
		return "", err
	}
	scalar, err := value.Scalar()
	if err != nil {
		return "", err
	}
	if scalar.Tag != "!!str" {
		return "", nodeValueError(value, fmt.Errorf("must have YAML text kind"))
	}
	if exact && (text == "" || text != strings.TrimSpace(text)) {
		return "", nodeValueError(value, fmt.Errorf("must be exact non-empty text"))
	}
	return text, nil
}

func schemaValueBool(value yamlsource.Value, owner string) (bool, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return false, err
	}
	if scalar.Tag != "!!bool" {
		return false, nodeValueError(value, fmt.Errorf("%s must have YAML boolean kind", owner))
	}
	return nodeValueBool(value, owner)
}

func schemaValueTexts(fields map[string]yamlsource.Value, targets map[string]*string, exact bool) error {
	for _, key := range sortedContractKeys(targets) {
		if value, present := fields[key]; present {
			text, err := schemaValueText(value, exact)
			if err != nil {
				return err
			}
			*targets[key] = text
		}
	}
	return nil
}

func schemaValueRequiredTexts(parent yamlsource.Value, fields map[string]yamlsource.Value, targets map[string]*string) error {
	for _, key := range sortedContractKeys(targets) {
		if _, present := fields[key]; !present {
			return nodeValueError(parent, fmt.Errorf("%s is required", key))
		}
	}
	return schemaValueTexts(fields, targets, true)
}

func schemaValueSequence(value yamlsource.Value, nonempty bool) ([]yamlsource.Value, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	if nonempty && len(items) == 0 {
		return nil, nodeValueError(value, fmt.Errorf("must be a non-empty sequence"))
	}
	return items, nil
}

func schemaValueDeclarations(value yamlsource.Value, nonempty bool) ([]yamlsource.MappingField, error) {
	fields, err := uniqueYAMLMappingFields(value, value.SemanticPath())
	if err != nil {
		return nil, nodeValueError(value, err)
	}
	if nonempty && len(fields) == 0 {
		return nil, nodeValueError(value, fmt.Errorf("must be a non-empty mapping"))
	}
	if err := validateExactDeclarationNames(fields, value.SemanticPath()); err != nil {
		return nil, err
	}
	return fields, nil
}

func projectSchemaRequiredAgentsValue(value yamlsource.Value) ([]FlowRequiredAgent, error) {
	items, err := schemaValueSequence(value, false)
	if err != nil {
		return nil, err
	}
	out := make([]FlowRequiredAgent, 0, len(items))
	for _, item := range items {
		fields, err := schemaValueFields(item, "required agent", map[string]struct{}{"role": {}, "subscribes_to": {}, "emits": {}, "description": {}}, true)
		if err != nil {
			return nil, err
		}
		var row FlowRequiredAgent
		if err := schemaValueRequiredTexts(item, fields, map[string]*string{"role": &row.Role}); err != nil {
			return nil, err
		}
		if err := schemaValueTexts(fields, map[string]*string{"description": &row.Description}, false); err != nil {
			return nil, err
		}
		for _, entry := range []struct {
			key    string
			target *[]string
		}{{"subscribes_to", &row.SubscribesTo}, {"emits", &row.Emits}} {
			if field, present := fields[entry.key]; present {
				items, err := schemaValueSequence(field, false)
				if err != nil {
					return nil, err
				}
				for _, item := range items {
					text, err := schemaValueText(item, true)
					if err != nil {
						return nil, err
					}
					*entry.target = append(*entry.target, text)
				}
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func populateEffectiveSchemaProvenance(bundle *WorkflowContractBundle, builder *effectiveProvenanceBuilder) {
	for _, source := range sortedFlowSources(bundle.FlowSources) {
		schema, ok := bundle.FlowSchemaByID(source.FlowPath)
		if !ok {
			continue
		}
		prefix := "schemas[" + strconv.Quote(source.FlowPath) + "]"
		for relative, fact := range schema.admissionProvenance {
			fact.InputPaths = qualifyEffectiveEventInputPaths(prefix, fact.InputPaths)
			builder.set(prefix+"."+relative, fact)
		}
		if alias, present := bundle.Semantics.flowIngressAliases[source.FlowPath]; present {
			builder.set(prefix+".ingress.alias", alias.provenance)
		}
		for _, fact := range bundle.FlowRequiredAgentFacts(source.FlowPath) {
			if fact.Source != RequiredAgentSourceInferred {
				continue
			}
			builder.set(prefix+".effective_required_agents["+strconv.Quote(fact.Role)+"]", EffectiveValueProvenance{
				Origin: EffectiveValueOriginDerived, RuleID: "flow.required_agents_from_local_agent_keys", InputPaths: []string{fact.SourceFile + "[" + strconv.Quote(fact.Role) + "]"}, SourceFile: fact.SourceFile,
			})
		}
	}
}
