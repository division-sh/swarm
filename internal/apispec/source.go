package apispec

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func (api APISpecification) SourceValue() yamlsource.Value { return api.source }

func AdmitPlatformAPIValue(root yamlsource.Value) (*APISpecification, error) {
	if err := root.ValidateExpansion(); err != nil {
		return nil, err
	}
	fields, err := apiFields(root)
	if err != nil {
		return nil, err
	}
	value, err := apiRequired(root, fields, "api_specification")
	if err != nil {
		return nil, err
	}
	f, err := apiFields(value)
	if err != nil {
		return nil, err
	}
	out := &APISpecification{source: value}
	if err = apiTexts(f, map[string]*string{"description": &out.Description}, false); err != nil {
		return nil, err
	}
	if v, ok := f["components"]; ok {
		m, e := apiFields(v)
		if e != nil {
			return nil, e
		}
		for _, entry := range []struct {
			name   string
			target *map[string]any
		}{{"schemas", &out.Components.Schemas}, {"errors", &out.Components.Errors}, {"error_catalog_metadata", &out.Components.ErrorCatalogMetadata}} {
			if child, ok := m[entry.name]; ok {
				if *entry.target, err = apiLiteralMap(child); err != nil {
					return nil, err
				}
			}
		}
	}
	if v, ok := f["method_catalog_metadata"]; ok {
		if out.MethodCatalogMetadata, err = apiLiteralMap(v); err != nil {
			return nil, err
		}
	}
	if v, ok := f["examples_policy"]; ok {
		m, e := apiFields(v)
		if e != nil {
			return nil, e
		}
		p := &out.ExamplesPolicy
		if e = apiTexts(m, map[string]*string{"status": &p.Status, "owner": &p.Owner, "applies_to": &p.AppliesTo, "openrpc_method_examples": &p.OpenRPCMethodExamples, "runtime_probe_fixtures": &p.RuntimeProbeFixtures}, true); e != nil {
			return nil, e
		}
		if e = apiTexts(m, map[string]*string{"reason": &p.Reason}, false); e != nil {
			return nil, e
		}
		if child, ok := m["requirements"]; ok {
			if p.Requirements, err = apiTextList(child); err != nil {
				return nil, err
			}
		}
		if child, ok := m["future_source_model_required"]; ok {
			if p.FutureSourceModelRequired, err = apiBool(child); err != nil {
				return nil, err
			}
		}
	}
	if v, ok := f["service_discovery_policy"]; ok {
		m, e := apiFields(v)
		if e != nil {
			return nil, e
		}
		p := &out.ServiceDiscoveryPolicy
		if e = apiTexts(m, map[string]*string{"status": &p.Status, "owner": &p.Owner, "applies_to": &p.AppliesTo, "rpc_discover": &p.RPCDiscover, "publication_artifact": &p.PublicationArtifact, "runtime_behavior": &p.RuntimeBehavior}, true); e != nil {
			return nil, e
		}
		if e = apiTexts(m, map[string]*string{"reason": &p.Reason}, false); e != nil {
			return nil, e
		}
		if child, ok := m["requirements"]; ok {
			if p.Requirements, err = apiTextList(child); err != nil {
				return nil, err
			}
		}
	}
	methods, err := apiRequired(value, f, "method_catalog")
	if err != nil {
		return nil, err
	}
	m, err := apiFields(methods)
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, apiError(methods, "method_catalog is empty")
	}
	out.MethodCatalog = make(map[string]Method, len(m))
	for _, name := range apiNames(m) {
		if out.MethodCatalog[name], err = admitAPIMethod(m[name]); err != nil {
			return nil, err
		}
	}
	if v, ok := f["conventions"]; ok {
		if out.Conventions, err = admitAPIConventions(v); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func admitAPIMethod(value yamlsource.Value) (Method, error) {
	f, err := apiFields(value)
	var out Method
	if err != nil {
		return out, err
	}
	if err = apiTexts(f, map[string]*string{"tier": &out.Tier}, true); err != nil {
		return out, err
	}
	if err = apiTexts(f, map[string]*string{"description": &out.Description}, false); err != nil {
		return out, err
	}
	if v, ok := f["deprecated"]; ok {
		if out.Deprecated, err = apiBool(v); err != nil {
			return out, err
		}
	}
	if v, ok := f["scope"]; ok {
		m, e := apiFields(v)
		if e != nil {
			return out, e
		}
		if child, ok := m["required"]; ok {
			if out.Scope.Required, err = apiTextList(child); err != nil {
				return out, err
			}
		}
	}
	if v, ok := f["params"]; ok {
		rows, e := v.Sequence()
		if e != nil {
			return out, e
		}
		out.Params = make([]ContentDescriptor, len(rows))
		for i, row := range rows {
			if out.Params[i], err = admitAPIDescriptor(row); err != nil {
				return out, err
			}
		}
	}
	if v, ok := f["result"]; ok {
		result, e := admitAPIDescriptor(v)
		if e != nil {
			return out, e
		}
		out.Result = &result
	}
	for _, entry := range []struct {
		name   string
		target *any
	}{{"idempotency", &out.Idempotency}, {"notification_schema", &out.NotificationSchema}} {
		if v, ok := f[entry.name]; ok {
			if err = v.ValidateUniqueMappings(); err != nil {
				return out, err
			}
			if err = v.Project(entry.target); err != nil {
				return out, err
			}
		}
	}
	if v, ok := f["errors"]; ok {
		if out.Errors, err = apiTextList(v); err != nil {
			return out, err
		}
	}
	return out, nil
}

func admitAPIDescriptor(value yamlsource.Value) (ContentDescriptor, error) {
	f, err := apiFields(value)
	var out ContentDescriptor
	if err != nil {
		return out, err
	}
	name, err := apiRequired(value, f, "name")
	if err != nil {
		return out, err
	}
	if out.Name, err = apiText(name, true); err != nil {
		return out, err
	}
	if err = apiTexts(f, map[string]*string{"description": &out.Description}, false); err != nil {
		return out, err
	}
	required, err := apiRequired(value, f, "required")
	if err != nil {
		return out, err
	}
	if out.Required, err = apiBool(required); err != nil {
		return out, err
	}
	schema, err := apiRequired(value, f, "schema")
	if err != nil {
		return out, err
	}
	if _, err = apiFields(schema); err != nil {
		return out, err
	}
	if err = schema.ValidateUniqueMappings(); err != nil {
		return out, err
	}
	if err = schema.Project(&out.Schema); err != nil {
		return out, err
	}
	return out, nil
}

func admitAPIConventions(value yamlsource.Value) (Conventions, error) {
	f, err := apiFields(value)
	var out Conventions
	if err != nil {
		return out, err
	}
	for _, entry := range []struct {
		group, name string
		target      *[]string
	}{{"idempotency", "mutating_methods", &out.Idempotency.MutatingMethods}, {"scopes", "catalog", &out.Scopes.Catalog}} {
		if v, ok := f[entry.group]; ok {
			m, e := apiFields(v)
			if e != nil {
				return out, e
			}
			if child, ok := m[entry.name]; ok {
				if *entry.target, err = apiTextList(child); err != nil {
					return out, err
				}
			}
		}
	}
	if v, ok := f["mailbox"]; ok {
		m, e := apiFields(v)
		if e != nil {
			return out, e
		}
		if e = apiTexts(m, map[string]*string{"status_storage_model": &out.Mailbox.StatusStorageModel}, true); e != nil {
			return out, e
		}
		if child, ok := m["decision_event_routes"]; ok {
			rows, e := child.Sequence()
			if e != nil {
				return out, e
			}
			for _, row := range rows {
				fields, e := apiFields(row)
				if e != nil {
					return out, e
				}
				var route MailboxDecisionEventRoute
				for _, entry := range []struct {
					name   string
					target *string
				}{{"item_type", &route.ItemType}, {"terminal_event_name", &route.TerminalEventName}, {"deferred_event_name", &route.DeferredEventName}} {
					v, e := apiRequired(row, fields, entry.name)
					if e != nil {
						return out, e
					}
					if *entry.target, e = apiText(v, true); e != nil {
						return out, e
					}
				}
				out.Mailbox.DecisionEventRoutes = append(out.Mailbox.DecisionEventRoutes, route)
			}
		}
	}
	return out, nil
}

func apiFields(value yamlsource.Value) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" || strings.TrimSpace(entry.Name) == "" {
			return nil, apiError(entry.Value, "field name must be nonempty text")
		}
		if _, duplicate := out[entry.Name]; duplicate {
			return nil, apiError(entry.Value, "duplicate field "+entry.Name)
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func apiRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (yamlsource.Value, error) {
	if v, ok := fields[name]; ok {
		return v, nil
	}
	missing, err := parent.Lookup(name)
	if err != nil {
		return yamlsource.Value{}, err
	}
	return missing.Value, fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
}

func apiText(value yamlsource.Value, nonempty bool) (string, error) {
	s, err := value.Scalar()
	if err != nil || s.Tag != "!!str" || (nonempty && strings.TrimSpace(s.Value) == "") {
		return "", apiError(value, "want text")
	}
	return s.Value, nil
}

func apiTexts(fields map[string]yamlsource.Value, targets map[string]*string, nonempty bool) error {
	for _, name := range apiNames(fields) {
		if target, ok := targets[name]; ok {
			text, err := apiText(fields[name], nonempty)
			if err != nil {
				return err
			}
			*target = text
		}
	}
	return nil
}

func apiBool(value yamlsource.Value) (bool, error) {
	s, err := value.Scalar()
	if err != nil || s.Tag != "!!bool" {
		return false, apiError(value, "want boolean")
	}
	var out bool
	err = value.Project(&out)
	return out, err
}

func apiTextList(value yamlsource.Value) ([]string, error) {
	rows, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, row := range rows {
		if out[i], err = apiText(row, true); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func apiLiteralMap(value yamlsource.Value) (map[string]any, error) {
	if _, err := apiFields(value); err != nil {
		return nil, err
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, err
	}
	var out map[string]any
	err := value.Project(&out)
	return out, err
}

func apiNames(fields map[string]yamlsource.Value) []string {
	out := make([]string, 0, len(fields))
	for name := range fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func apiError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}
