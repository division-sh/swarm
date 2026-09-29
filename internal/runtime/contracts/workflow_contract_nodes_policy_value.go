package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodePolicyValueRow(value yamlsource.Value, kind, rowID string) (PolicySheetRowMetadata, *ComputeSpec, error) {
	if kind == "lookup" {
		return projectNodePolicyLookupValue(value, rowID)
	}
	label := "validate"
	nameKey := "set"
	intoValidator := validatePolicySheetValidateInto
	operation := ComputeOpValidate
	rowKind := PolicySheetRowKindValidate
	if kind == "compute_module" {
		label, nameKey = "compute_module", "module"
		intoValidator = validatePolicySheetComputeModuleInto
		operation, rowKind = ComputeOpModule, PolicySheetRowKindModule
	}
	fields, err := nodeValueFields(value, label, map[string]struct{}{
		nameKey: {}, "input": {}, "into": {},
	}, nil)
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	name, err := projectNodePolicyRequiredField(fields, nameKey, label+"."+nameKey, value)
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	if !isPolicySheetPathSegment(name) {
		return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: %s.%s %q at %s must be a short name", label, nameKey, name, value.Location())
	}
	into, err := projectNodePolicyRequiredField(fields, "into", label+".into", value)
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	if err := intoValidator(into); err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	input, present := fields["input"]
	if !present {
		return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: %s.input is required at %s", label, value.Location())
	}
	inputs, parsed, err := projectNodePolicyInputsValue(input, label+".input")
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	compute := &ComputeSpec{Operation: operation, StoreAs: into}
	metadata := PolicySheetRowMetadata{Kind: rowKind}
	if kind == "validate" {
		spec := &ComputeValidationSpec{RowID: rowID, Set: name, Into: into, Input: inputs, InputPaths: parsed}
		compute.Validation, metadata.Validation = spec, spec
	} else {
		spec := &ComputeModuleSpec{RowID: rowID, Module: name, Into: into, Input: inputs, InputPaths: parsed}
		compute.Module, metadata.Module = spec, spec
	}
	return metadata, compute, nil
}

func projectNodePolicyRequiredField(fields map[string]yamlsource.Value, key, label string, parent yamlsource.Value) (string, error) {
	field, present := fields[key]
	if !present {
		return "", fmt.Errorf("POLICY-SHEET-ROW: %s is required at %s", label, parent.Location())
	}
	text, err := nodeValueText(field, label)
	return strings.TrimSpace(text), err
}

func projectNodePolicyInputsValue(value yamlsource.Value, label string) (map[string]string, map[string]paths.Path, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, nil, err
	}
	if len(fields) == 0 {
		return nil, nil, fmt.Errorf("POLICY-SHEET-ROW: %s at %s requires at least one input", label, value.Location())
	}
	out := make(map[string]string, len(fields))
	parsed := make(map[string]paths.Path, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if !isPolicySheetPathSegment(name) {
			return nil, nil, fmt.Errorf("POLICY-SHEET-ROW: %s key %q at %s must be a short input name", label, name, field.KeyLocation)
		}
		if _, duplicate := out[name]; duplicate {
			return nil, nil, fmt.Errorf("POLICY-SHEET-ROW: %s duplicate key %q at %s", label, name, field.KeyLocation)
		}
		ref, err := nodeValueText(field.Value, label+"."+name)
		if err != nil {
			return nil, nil, err
		}
		ref = strings.TrimSpace(ref)
		if err := validatePolicySheetPath(ref, label+"."+name, []string{"payload", "entity", "event", "computed"}); err != nil {
			return nil, nil, err
		}
		out[name], parsed[name] = ref, paths.Parse(ref)
	}
	return out, parsed, nil
}

func projectNodePolicyLookupValue(value yamlsource.Value, rowID string) (PolicySheetRowMetadata, *ComputeSpec, error) {
	fields, err := nodeValueFields(value, "lookup", map[string]struct{}{
		"on": {}, "entries": {}, "into": {}, "default": {},
	}, nil)
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	onField, present := fields["on"]
	if !present {
		return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.on is required at %s", value.Location())
	}
	on, err := projectNodePolicyStringListValue(onField, "lookup.on")
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	if len(on) == 0 {
		return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.on at %s requires an explicit path", onField.Location())
	}
	for _, selector := range on {
		if err := validatePolicySheetPath(selector, "lookup.on", []string{"payload"}); err != nil {
			return PolicySheetRowMetadata{}, nil, err
		}
	}
	into, err := projectNodePolicyRequiredField(fields, "into", "lookup.into", value)
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	if err := validatePolicySheetLookupInto(into); err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	entriesField, present := fields["entries"]
	if !present {
		return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.entries is required at %s", value.Location())
	}
	entries, err := projectNodePolicyLookupEntriesValue(entriesField, len(on))
	if err != nil {
		return PolicySheetRowMetadata{}, nil, err
	}
	spec := &ComputeLookupSpec{RowID: rowID, On: on, Entries: entries}
	for _, selector := range on {
		spec.OnPaths = append(spec.OnPaths, paths.Parse(selector))
	}
	if fallback, present := fields["default"]; present {
		text, err := nodeValueText(fallback, "lookup.default")
		if err != nil || strings.TrimSpace(text) != "fail" {
			return PolicySheetRowMetadata{}, nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.default at %s supports only fail", fallback.Location())
		}
		spec.DefaultFail, spec.DefaultDeclared = true, true
	}
	return PolicySheetRowMetadata{Kind: PolicySheetRowKindLookup, Lookup: spec},
		&ComputeSpec{Operation: ComputeOpLookup, StoreAs: into, Lookup: spec}, nil
}

func projectNodePolicyLookupEntriesValue(value yamlsource.Value, width int) ([]ComputeLookupEntry, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.entries at %s requires at least one entry", value.Location())
	}
	out := make([]ComputeLookupEntry, 0, len(items))
	seen := map[string]int{}
	valueKind := ""
	for index, item := range items {
		fields, err := nodeValueFields(item, "lookup.entry", map[string]struct{}{"key": {}, "value": {}}, nil)
		if err != nil {
			return nil, err
		}
		keyField, keyPresent := fields["key"]
		valueField, valuePresent := fields["value"]
		if !keyPresent || !valuePresent {
			return nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.entries[%d] at %s requires key and value", index, item.Location())
		}
		key, err := projectNodePolicyLookupKeyValue(keyField, width)
		if err != nil {
			return nil, err
		}
		literal, kind, summary, err := projectNodePolicyLookupScalarValue(valueField, "lookup.value")
		if err != nil {
			return nil, err
		}
		if previous, exists := seen[canonicalPolicySheetLookupKey(key)]; exists {
			return nil, fmt.Errorf("POLICY-SHEET-ROW: duplicate lookup key at entries[%d] and entries[%d]", previous, index)
		}
		seen[canonicalPolicySheetLookupKey(key)] = index
		if valueKind != "" && kind != valueKind {
			return nil, fmt.Errorf("POLICY-SHEET-ROW: lookup.entries[%d] value type %s differs from %s", index, kind, valueKind)
		}
		valueKind = kind
		out = append(out, ComputeLookupEntry{Key: key, Value: literal, ValueKind: kind, ValueSummary: summary})
	}
	return out, nil
}

func projectNodePolicyLookupKeyValue(value yamlsource.Value, width int) ([]ComputeLookupLiteral, error) {
	if width == 1 && value.Presence() != yamlsource.PresenceSequence && value.Presence() != yamlsource.PresenceEmptySequence {
		literal, err := projectNodePolicyLookupLiteralValue(value, "lookup.key")
		return []ComputeLookupLiteral{literal}, err
	}
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	if len(items) != width {
		return nil, fmt.Errorf("POLICY-SHEET-ROW: lookup key width %d at %s differs from selector width %d", len(items), value.Location(), width)
	}
	out := make([]ComputeLookupLiteral, 0, len(items))
	for index, item := range items {
		literal, err := projectNodePolicyLookupLiteralValue(item, fmt.Sprintf("lookup.key[%d]", index))
		if err != nil {
			return nil, err
		}
		out = append(out, literal)
	}
	return out, nil
}

func projectNodePolicyLookupLiteralValue(value yamlsource.Value, label string) (ComputeLookupLiteral, error) {
	literal, _, _, err := projectNodePolicyLookupScalarValue(value, label)
	if err != nil {
		return ComputeLookupLiteral{}, err
	}
	kind, summary, canonical, ok := CanonicalizeComputeLookupValue(literal)
	if !ok {
		return ComputeLookupLiteral{}, fmt.Errorf("POLICY-SHEET-ROW: %s at %s has unsupported key type", label, value.Location())
	}
	return ComputeLookupLiteral{Value: literal, Kind: kind, Summary: summary, Canonical: canonical}, nil
}

func projectNodePolicyLookupScalarValue(value yamlsource.Value, label string) (any, string, string, error) {
	scalar, err := value.Scalar()
	if err != nil {
		return nil, "", "", fmt.Errorf("POLICY-SHEET-ROW: %s at %s must be scalar: %w", label, value.Location(), err)
	}
	raw := strings.TrimSpace(scalar.Value)
	switch scalar.Tag {
	case "!!str", "":
		return raw, "string", strconv.Quote(raw), nil
	case "!!bool":
		if raw != "true" && raw != "false" {
			return nil, "", "", fmt.Errorf("POLICY-SHEET-ROW: %s invalid bool at %s", label, value.Location())
		}
		return raw == "true", "bool", raw, nil
	case "!!int":
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, "", "", err
		}
		return parsed, "int", strconv.FormatInt(parsed, 10), nil
	case "!!float":
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, "", "", err
		}
		return parsed, "number", strconv.FormatFloat(parsed, 'g', -1, 64), nil
	default:
		return nil, "", "", fmt.Errorf("POLICY-SHEET-ROW: %s at %s has unsupported literal tag %s", label, value.Location(), scalar.Tag)
	}
}
