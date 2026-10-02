package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func (s PlatformSpecDocument) SourceValue() yamlsource.Value { return s.source }

func LoadPlatformSpecDocument(path string) (PlatformSpecDocument, error) {
	var out PlatformSpecDocument
	err := loadYAMLFile(path, &out)
	return out, err
}

func ParsePlatformSpecDocument(body []byte, file string) (PlatformSpecDocument, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return PlatformSpecDocument{}, err
	}
	return AdmitPlatformSpecValue(snapshot.Document(file).Root())
}

// The specification is an open document. Only consumed law is projected; its
// inert prose remains in the immutable source, not in reflected raw carriers.
func AdmitPlatformSpecValue(root yamlsource.Value) (PlatformSpecDocument, error) {
	if err := root.ValidateExpansion(); err != nil {
		return PlatformSpecDocument{}, err
	}
	f, err := platformValueFields(root)
	if err != nil {
		return PlatformSpecDocument{}, err
	}
	out := PlatformSpecDocument{source: root, eventCatalog: map[string]EventCatalogEntry{}}
	if v, ok := f["platform"]; ok {
		if err := admitPlatformHeader(root, v, &out); err != nil {
			return out, err
		}
	}
	if v, ok := f["interfaces"]; ok {
		out.Interfaces, err = platformValueMap(v, func(v yamlsource.Value) (map[string]PackInterfaceDefinition, error) {
			return platformValueMap(v, admitPackInterfaceValue)
		})
		if err != nil {
			return out, err
		}
	}
	if v, ok := f["platform_events"]; ok {
		members, e := platformValueFields(v)
		if e != nil {
			return out, e
		}
		if catalog, ok := members["catalog"]; ok {
			if out.eventCatalog, err = admitPlatformEventCatalogValue(catalog); err != nil {
				return out, err
			}
		}
	}
	if v, ok := f["permissions_model"]; ok {
		if err := admitPlatformPermissions(v, &out); err != nil {
			return out, err
		}
	}
	if v, ok := f["vocabulary"]; ok {
		if err := admitPlatformVocabulary(v, &out); err != nil {
			return out, err
		}
	}
	if v, ok := f["workflow_state"]; ok {
		if err := admitPlatformWorkflowState(v, &out); err != nil {
			return out, err
		}
	}
	if v, ok := f["platform_tables"]; ok {
		if err := admitPlatformTables(v, &out); err != nil {
			return out, err
		}
	}
	if v, ok := f["builtin_hooks"]; ok {
		if err := admitPlatformGuards(v, &out); err != nil {
			return out, err
		}
	}
	return out, nil
}

func platformValueFields(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := uniqueYAMLMappingFields(value, "platform law")
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" || strings.TrimSpace(entry.Name) == "" {
			return nil, nodeValueError(entry.Value, fmt.Errorf("platform field name must be nonempty text"))
		}
		if len(allowed) > 0 {
			known := false
			for _, name := range allowed {
				known = known || entry.Name == name
			}
			if !known {
				return nil, nodeValueError(entry.Value, fmt.Errorf("field %s not found in platform declaration", entry.Name))
			}
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func platformRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (yamlsource.Value, error) {
	if v, ok := fields[name]; ok {
		return v, nil
	}
	missing, err := parent.Lookup(name)
	if err != nil {
		return yamlsource.Value{}, err
	}
	return missing.Value, fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
}

func platformValueMap[T any](value yamlsource.Value, project func(yamlsource.Value) (T, error)) (map[string]T, error) {
	f, err := platformValueFields(value)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(f))
	for _, name := range sortedContractKeys(f) {
		if out[name], err = project(f[name]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func platformText(value yamlsource.Value, nonempty bool) (string, error) {
	text, err := schemaValueText(value, false)
	if err != nil {
		return "", err
	}
	if nonempty && strings.TrimSpace(text) == "" {
		return "", nodeValueError(value, fmt.Errorf("want nonempty text"))
	}
	return text, nil
}

func platformTextList(value yamlsource.Value) ([]string, error) {
	rows, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, row := range rows {
		if out[i], err = platformText(row, true); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func admitPlatformHeader(root yamlsource.Value, v yamlsource.Value, out *PlatformSpecDocument) error {
	var err error
	if out.Platform.Version, err = platform.PlatformVersionFromValue(root); err != nil {
		return err
	}
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	if err = schemaValueTexts(members, map[string]*string{"name": &out.Platform.Name}, false); err != nil {
		return err
	}

	return nil
}

func admitPlatformPermissions(v yamlsource.Value, out *PlatformSpecDocument) error {
	var err error
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	list, e := platformRequired(v, members, "permissions")
	if e != nil {
		return e
	}
	if out.PermissionsModel.Permissions, err = platformTextList(list); err != nil {
		return err
	}

	return nil
}

func admitPlatformVocabulary(v yamlsource.Value, out *PlatformSpecDocument) error {
	var err error
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	if p, ok := members["participant"]; ok {
		members, e = platformValueFields(p)
		if e != nil {
			return e
		}
		if types, ok := members["types"]; ok {
			out.Vocabulary.Participant.Types, err = platformValueMap(types, func(v yamlsource.Value) (struct {
				Execution string `yaml:"execution"`
			}, error) {
				var kind struct {
					Execution string `yaml:"execution"`
				}
				m, e := platformValueFields(v)
				if e != nil {
					return kind, e
				}
				e = schemaValueRequiredTexts(v, m, map[string]*string{"execution": &kind.Execution})
				if e == nil && kind.Execution != "deterministic" && kind.Execution != "llm" && kind.Execution != "implicit" {
					e = nodeValueError(v, fmt.Errorf("execution must be deterministic, llm, or implicit"))
				}
				return kind, e
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func admitPlatformWorkflowState(v yamlsource.Value, out *PlatformSpecDocument) error {
	var err error
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	if ddl, ok := members["ddl"]; ok {
		if out.WorkflowState.DDL, err = platformText(ddl, true); err != nil {
			return err
		}
	}
	if fields, ok := members["fields"]; ok {
		out.WorkflowState.Fields, err = platformValueMap(fields, func(v yamlsource.Value) (struct {
			Type string `yaml:"type"`
		}, error) {
			var field struct {
				Type string `yaml:"type"`
			}
			m, e := platformValueFields(v)
			if e == nil {
				e = schemaValueRequiredTexts(v, m, map[string]*string{"type": &field.Type})
			}
			return field, e
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func admitPlatformTables(v yamlsource.Value, out *PlatformSpecDocument) error {
	var err error
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	if tables, ok := members["tables"]; ok {
		out.PlatformTables.Tables, err = platformValueMap(tables, func(v yamlsource.Value) (struct {
			Description string `yaml:"description"`
			DDL         string `yaml:"ddl"`
		}, error) {
			var table struct {
				Description string `yaml:"description"`
				DDL         string `yaml:"ddl"`
			}
			m, e := platformValueFields(v)
			if e == nil {
				var ddl yamlsource.Value
				ddl, e = platformRequired(v, m, "ddl")
				if e == nil {
					table.DDL, e = platformText(ddl, true)
				}
			}
			if e == nil {
				e = schemaValueTexts(m, map[string]*string{"description": &table.Description}, false)
			}
			return table, e
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func admitPlatformGuards(v yamlsource.Value, out *PlatformSpecDocument) error {
	members, e := platformValueFields(v)
	if e != nil {
		return e
	}
	if guards, ok := members["guards"]; ok {
		rows, e := guards.Sequence()
		if e != nil {
			return e
		}
		for _, row := range rows {
			m, e := platformValueFields(row)
			if e != nil {
				return e
			}
			var guard struct {
				ID string `yaml:"id"`
			}
			if e = schemaValueRequiredTexts(row, m, map[string]*string{"id": &guard.ID}); e != nil {
				return e
			}
			out.BuiltinHooks.Guards = append(out.BuiltinHooks.Guards, guard)
		}
	}

	return nil
}
