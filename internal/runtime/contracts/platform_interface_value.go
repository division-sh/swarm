package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func admitPackInterfaceValue(value yamlsource.Value) (PackInterfaceDefinition, error) {
	f, err := platformValueFields(value, "kind", "schemas", "operations", "events")
	var out PackInterfaceDefinition
	if err != nil {
		return out, err
	}
	if err = schemaValueRequiredTexts(value, f, map[string]*string{"kind": &out.Kind}); err != nil {
		return out, err
	}
	for _, name := range []string{"schemas", "operations", "events"} {
		v, e := platformRequired(value, f, name)
		if e != nil {
			return out, e
		}
		members, e := platformValueFields(v)
		if e != nil {
			return out, e
		}
		if len(members) == 0 {
			return out, nodeValueError(v, fmt.Errorf("%s requires a nonempty mapping", name))
		}
		switch name {
		case "schemas":
			out.Schemas, err = platformValueMap(v, AdmitToolInputSchemaValue)
		case "operations":
			out.Operations, err = platformValueMap(v, admitPackInterfaceOperation)
		case "events":
			out.Events, err = platformValueMap(v, func(v yamlsource.Value) (PackInterfaceEvent, error) {
				var event PackInterfaceEvent
				f, e := platformValueFields(v, "required_fields")
				if e != nil {
					return event, e
				}
				fields, e := platformRequired(v, f, "required_fields")
				if e != nil {
					return event, e
				}
				event.RequiredFields, e = platformValueMap(fields, admitPackInterfaceField)
				if e == nil && len(event.RequiredFields) == 0 {
					e = nodeValueError(fields, fmt.Errorf("required_fields must be nonempty"))
				}
				return event, e
			})
		}
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func admitPackInterfaceOperation(value yamlsource.Value) (PackInterfaceOperation, error) {
	f, err := platformValueFields(value, "effect_class", "input", "context", "output")
	var out PackInterfaceOperation
	if err != nil {
		return out, err
	}
	if err = schemaValueRequiredTexts(value, f, map[string]*string{"effect_class": &out.EffectClass}); err != nil {
		return out, err
	}
	for _, name := range []string{"input", "context", "output"} {
		if v, ok := f[name]; ok {
			fields, e := platformValueMap(v, admitPackInterfaceField)
			if e != nil {
				return out, e
			}
			switch name {
			case "input":
				out.Input = fields
			case "context":
				out.Context = fields
			case "output":
				out.Output = fields
			}
		}
	}
	return out, nil
}

func admitPackInterfaceField(value yamlsource.Value) (PackInterfaceField, error) {
	f, err := platformValueFields(value, "schema", "opaque")
	var out PackInterfaceField
	if err != nil {
		return out, err
	}
	if len(f) != 1 {
		return out, nodeValueError(value, fmt.Errorf("interface field must declare exactly one of schema or opaque"))
	}
	err = schemaValueTexts(f, map[string]*string{"schema": &out.Schema, "opaque": &out.Opaque}, true)
	return out, err
}
