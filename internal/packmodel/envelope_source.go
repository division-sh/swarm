package packmodel

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// SourceValue is immutable lexical evidence, not a second envelope interpreter.
func (e Envelope) SourceValue() yamlsource.Value { return e.source }

func ParseEnvelopeAt(body []byte, file string) (Envelope, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return Envelope{}, err
	}
	root := snapshot.Document(file).Root()
	if err := root.ValidateExpansion(); err != nil {
		return Envelope{}, err
	}
	fields, err := envelopeFields(root, "id", "version", "platform_version", "type", "implements", "manifest_hash", "provenance", "capabilities", "requires", "tests")
	if err != nil {
		return Envelope{}, err
	}
	if err := envelopeRequired(root, fields, "id", "version", "platform_version", "type", "manifest_hash", "provenance", "capabilities", "requires", "tests"); err != nil {
		return Envelope{}, err
	}
	e := Envelope{source: root}
	if err := admitEnvelopeHeader(root, fields, &e); err != nil {
		return Envelope{}, err
	}
	if err := admitEnvelopeCapabilities(fields["capabilities"], fields["type"], &e); err != nil {
		return Envelope{}, err
	}
	if err := admitEnvelopeRequirements(fields["requires"], &e); err != nil {
		return Envelope{}, err
	}
	if e.Tests, err = envelopeTexts(fields["tests"]); err != nil {
		return Envelope{}, err
	}
	if len(e.Tests) == 0 {
		return Envelope{}, envelopeError(fields["tests"], "tests are required and must be a nonempty text list")
	}
	return e, nil
}

func envelopeError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}

func envelopeRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, names ...string) error {
	for _, name := range names {
		if _, present := fields[name]; !present {
			missing, err := parent.Lookup(name)
			if err != nil {
				return err
			}
			return fmt.Errorf("%s required field is missing from mapping at %s", missing.Value.SemanticPath(), parent.Location())
		}
	}
	return nil
}

func envelopeFields(value yamlsource.Value, allowed ...string) (map[string]yamlsource.Value, error) {
	entries, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]yamlsource.Value, len(entries))
	for _, entry := range entries {
		if entry.KeyTag != "!!str" {
			return nil, envelopeError(entry.Value, "field name must be text")
		}
		if _, duplicate := out[entry.Name]; duplicate {
			return nil, envelopeError(entry.Value, "duplicate field "+entry.Name)
		}
		known := false
		for _, name := range allowed {
			known = known || entry.Name == name
		}
		if !known {
			return nil, envelopeError(entry.Value, "field "+entry.Name+" not found in pack envelope")
		}
		out[entry.Name] = entry.Value
	}
	return out, nil
}

func envelopeText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return "", envelopeError(value, "want nonempty text")
	}
	return scalar.Value, nil
}

func envelopeTexts(value yamlsource.Value) ([]string, error) {
	entries, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(entries))
	for i, entry := range entries {
		if out[i], err = envelopeText(entry); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func admitEnvelopeHeader(root yamlsource.Value, fields map[string]yamlsource.Value, e *Envelope) error {
	var err error
	for _, field := range []struct {
		name   string
		target *string
	}{{"id", &e.ID}, {"version", &e.Version}, {"platform_version", &e.PlatformVersion}, {"type", &e.Type}, {"manifest_hash", &e.ManifestHash}} {
		if *field.target, err = envelopeText(fields[field.name]); err != nil {
			return fmt.Errorf("pack envelope %s: %w", field.name, err)
		}
	}
	if field, present := fields["implements"]; present {
		if e.Type != TypeChannel {
			return envelopeError(field, "implements is forbidden for non-channel packs")
		}
		if e.Implements, err = envelopeTexts(field); err != nil {
			return err
		}
		if len(e.Implements) != 1 {
			return envelopeError(field, "channel implements requires exactly one reference")
		}
	} else if e.Type == TypeChannel {
		return envelopeRequired(root, fields, "implements")
	}
	provenance, err := envelopeFields(fields["provenance"], "source")
	if err != nil {
		return err
	}
	if err := envelopeRequired(fields["provenance"], provenance, "source"); err != nil {
		return err
	}
	if e.Provenance.Source, err = envelopeText(provenance["source"]); err != nil {
		return err
	}
	return nil
}

func admitEnvelopeCapabilities(value, typeValue yamlsource.Value, e *Envelope) error {
	capabilities, err := envelopeFields(value, "can", "cannot")
	if err != nil {
		return err
	}
	if err := envelopeRequired(value, capabilities, "can", "cannot"); err != nil {
		return err
	}
	allowed := []string{}
	switch e.Type {
	case TypeTrigger:
		allowed = []string{"receive_https_route", "verify_secret", "emit_events", "persist_dedupe_markers"}
	case TypeConnector:
		allowed = []string{"call_provider_actions", "lower_through_activity", "journal_activity_attempts"}
	case TypeChannel:
	default:
		return envelopeError(typeValue, "unsupported pack type")
	}
	can, err := envelopeFields(capabilities["can"], allowed...)
	if err != nil {
		return err
	}
	switch e.Type {
	case TypeTrigger:
		if err := envelopeRequired(capabilities["can"], can, "receive_https_route", "emit_events", "persist_dedupe_markers"); err != nil {
			return err
		}
	case TypeConnector:
		if err := envelopeRequired(capabilities["can"], can, "call_provider_actions", "lower_through_activity", "journal_activity_attempts"); err != nil {
			return err
		}
	}
	if err := admitEnvelopeCanValues(can, e); err != nil {
		return err
	}
	if e.Capabilities.Cannot, err = envelopeTexts(capabilities["cannot"]); err != nil {
		return err
	}
	if len(e.Capabilities.Cannot) == 0 {
		return envelopeError(capabilities["cannot"], "capabilities.cannot is required and must be a nonempty text list")
	}
	for _, name := range []string{"emit_events", "call_provider_actions"} {
		if list, present := can[name]; present {
			entries, _ := list.Sequence()
			if len(entries) == 0 {
				return envelopeError(list, "capability requires nonempty text list")
			}
		}
	}
	return nil
}

func admitEnvelopeRequirements(value yamlsource.Value, e *Envelope) error {
	requires, err := envelopeFields(value, "secrets", "managed_credentials", "packs")
	if err != nil {
		return err
	}
	for name, target := range map[string]*[]string{"secrets": &e.Requires.Secrets, "managed_credentials": &e.Requires.ManagedCredentials} {
		if field, present := requires[name]; present {
			if *target, err = envelopeTexts(field); err != nil {
				return err
			}
		}
	}
	if field, present := requires["packs"]; present {
		entries, err := field.Mapping()
		if err != nil {
			return err
		}
		e.Requires.Packs = make(map[string]string, len(entries))
		for _, entry := range entries {
			if _, duplicate := e.Requires.Packs[entry.Name]; duplicate || entry.KeyTag != "!!str" {
				return envelopeError(entry.Value, "duplicate or non-text dependency role")
			}
			if e.Requires.Packs[entry.Name], err = envelopeText(entry.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func admitEnvelopeCanValues(can map[string]yamlsource.Value, e *Envelope) error {
	var err error
	for name, target := range map[string]*string{"receive_https_route": &e.Capabilities.Can.ReceiveHTTPSRoute, "verify_secret": &e.Capabilities.Can.VerifySecret} {
		if field, present := can[name]; present {
			if *target, err = envelopeText(field); err != nil {
				return err
			}
		}
	}
	for name, target := range map[string]*[]string{"emit_events": &e.Capabilities.Can.EmitEvents, "call_provider_actions": &e.Capabilities.Can.CallProviderActions} {
		if field, present := can[name]; present {
			if *target, err = envelopeTexts(field); err != nil {
				return err
			}
		}
	}
	for name, target := range map[string]*bool{"persist_dedupe_markers": &e.Capabilities.Can.PersistDedupeMarkers, "lower_through_activity": &e.Capabilities.Can.LowerThroughActivity, "journal_activity_attempts": &e.Capabilities.Can.JournalActivityAttempts} {
		if field, present := can[name]; present {
			scalar, scalarErr := field.Scalar()
			if scalarErr != nil || scalar.Tag != "!!bool" {
				return envelopeError(field, "want boolean")
			}
			if err := field.Project(target); err != nil {
				return err
			}
		}
	}
	return nil
}
