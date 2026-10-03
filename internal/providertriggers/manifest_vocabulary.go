package providertriggers

import (
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// Admission reads the manifest's current typed wire vocabulary. Open field,
// schema and metadata maps retain their existing semantic owners.
func admitManifestVocabulary(value yamlsource.Value, shape reflect.Type) error {
	for shape.Kind() == reflect.Pointer {
		shape = shape.Elem()
	}
	switch shape.Kind() {
	case reflect.Struct:
		if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
			return nil
		}
		declared := map[string]reflect.Type{}
		allowed := map[string]struct{}{}
		for i := 0; i < shape.NumField(); i++ {
			field := shape.Field(i)
			name := strings.Split(field.Tag.Get("yaml"), ",")[0]
			if field.IsExported() && name != "" && name != "-" {
				declared[name], allowed[name] = field.Type, struct{}{}
			}
		}
		fields, err := value.Mapping()
		if err != nil {
			return err
		}
		for _, field := range fields {
			child, present := declared[field.Name]
			if !present {
				return runtimecontracts.NewUndefinedFieldDiagnostic("provider trigger manifest", field.Name, allowed, field)
			}
			if err := admitManifestVocabulary(field.Value, child); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.Presence() == yamlsource.PresenceMapping || value.Presence() == yamlsource.PresenceEmptyMapping {
			fields, err := value.Mapping()
			if err != nil {
				return err
			}
			for _, field := range fields {
				if err := admitManifestVocabulary(field.Value, shape.Elem()); err != nil {
					return err
				}
			}
		}
	case reflect.Slice:
		if value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence {
			items, err := value.Sequence()
			if err != nil {
				return err
			}
			for _, item := range items {
				if err := admitManifestVocabulary(item, shape.Elem()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
