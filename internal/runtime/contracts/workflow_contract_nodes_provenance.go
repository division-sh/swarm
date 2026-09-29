package contracts

import (
	"fmt"
	"strconv"
	"unicode"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func nodeHandlerAnnotations(value yamlsource.Value) ([]yamlsource.Value, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, nil
	}
	handlers, err := value.Lookup("event_handlers")
	if err != nil || handlers.Presence == yamlsource.PresenceMissing {
		return nil, err
	}
	fields, err := handlers.Value.Mapping()
	if err != nil {
		return nil, err
	}
	var annotations []yamlsource.Value
	for _, field := range fields {
		if field.Value.Presence() != yamlsource.PresenceMapping && field.Value.Presence() != yamlsource.PresenceEmptyMapping {
			continue
		}
		note, err := field.Value.Lookup("_note")
		if err != nil {
			return nil, err
		}
		if note.Presence != yamlsource.PresenceMissing {
			annotations = append(annotations, note.Value)
		}
	}
	return annotations, nil
}

func collectNodeValueProvenance(value yamlsource.Value, prefix string, out map[string]EffectiveValueProvenance, annotations []yamlsource.Value) error {
	for _, note := range annotations {
		if note.SemanticPath() == value.SemanticPath() {
			return nil
		}
	}
	switch value.Presence() {
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := value.Mapping()
		if err != nil {
			return err
		}
		for _, field := range fields {
			path := nodeProvenanceMapPath(prefix, field.Name)
			out[path] = authoredSourceProvenance(field.Value)
			if err := collectNodeValueProvenance(field.Value, path, out, annotations); err != nil {
				return err
			}
		}
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		items, err := value.Sequence()
		if err != nil {
			return err
		}
		for index, item := range items {
			path := prefix + fmt.Sprintf("[%d]", index)
			out[path] = authoredSourceProvenance(item)
			if err := collectNodeValueProvenance(item, path, out, annotations); err != nil {
				return err
			}
		}
	}
	return nil
}

func nodeProvenanceMapPath(prefix, key string) string {
	simple := key != ""
	for index, r := range key {
		if index == 0 {
			simple = unicode.IsLetter(r) || r == '_'
		} else if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			simple = false
		}
	}
	if simple {
		if prefix == "" {
			return key
		}
		return prefix + "." + key
	}
	return prefix + "[" + strconv.Quote(key) + "]"
}
