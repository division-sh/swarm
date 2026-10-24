package packmodel

import (
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func manifestMapping(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" {
			return nil, manifestError(entry.Value, "field name must be text")
		}
		if _, duplicate := out[entry.Name]; duplicate {
			return nil, manifestError(entry.Value, "duplicate field "+entry.Name)
		}
		if entry.Name == "receive_https_route" || entry.Name == "receive_session_events" {
			return nil, retiredManifestField(entry.Name, entry.Value)
		}
		known := false
		for _, name := range allowed {
			known = known || entry.Name == name
		}
		if !known {
			return nil, manifestError(entry.Value, "field "+entry.Name+" is not a manifest capability")
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func manifestRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, names ...string) error {
	for _, name := range names {
		if _, present := fields[name]; !present {
			missing, err := parent.Lookup(name)
			if err != nil {
				return err
			}
			return manifestError(missing.Value, "required field is missing")
		}
	}
	return nil
}

func manifestText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return "", manifestError(value, "want nonempty text")
	}
	return scalar.Value, nil
}

func manifestTexts(value yamlsource.Value) ([]string, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := manifestText(item)
		if err != nil {
			return nil, err
		}
		out = append(out, text)
	}
	return out, nil
}

func admitCanValues(can map[string]yamlsource.Value, capabilities *Capabilities) error {
	for name, target := range map[string]*string{"verify_secret": &capabilities.Can.VerifySecret} {
		if value, present := can[name]; present {
			text, err := manifestText(value)
			if err != nil {
				return err
			}
			*target = text
		}
	}
	for name, target := range map[string]*[]string{"emit_events": &capabilities.Can.EmitEvents, "call_provider_actions": &capabilities.Can.CallProviderActions} {
		if value, present := can[name]; present {
			texts, err := manifestTexts(value)
			if err != nil {
				return err
			}
			*target = texts
		}
	}
	for name, target := range map[string]*bool{"persist_dedupe_markers": &capabilities.Can.PersistDedupeMarkers, "lower_through_activity": &capabilities.Can.LowerThroughActivity, "journal_activity_attempts": &capabilities.Can.JournalActivityAttempts} {
		if value, present := can[name]; present {
			scalar, err := value.Scalar()
			if err != nil || scalar.Tag != "!!bool" {
				return manifestError(value, "want boolean")
			}
			if err := value.Project(target); err != nil {
				return err
			}
		}
	}
	return nil
}
