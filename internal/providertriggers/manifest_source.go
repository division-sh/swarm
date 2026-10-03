package providertriggers

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func parseManifestAt(body []byte, file string) (Manifest, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", file, err)
	}
	root := snapshot.Document(file).Root()
	if err := root.ValidateExpansion(); err != nil {
		return Manifest{}, err
	}
	if err := root.ValidateUniqueMappings(); err != nil {
		return Manifest{}, err
	}
	definition, err := admitTriggerDefinition(root)
	if err != nil {
		return Manifest{}, err
	}
	if err := definition.validate(); err != nil {
		return Manifest{}, triggerSourceError(root, err.Error())
	}
	definition.Provider = NormalizeProviderName(definition.Provider)
	definition.outputs = definition.OutputManifest()
	return Manifest{value: &manifestValue{definition: definition, source: root, body: snapshot.Bytes()}}, nil
}

func admitTriggerDefinition(root yamlsource.Value) (manifestDefinition, error) {
	f, err := triggerFields(root, "provider", "payload_object_required", "payload_object_error", "payload_source", "secret", "signature", "challenge", "delivery_condition", "delivery_id", "event_type", "event_name", "normalized_events", "ack", "redact_keys", "metadata")
	if err != nil {
		return manifestDefinition{}, err
	}
	var out manifestDefinition
	if err := triggerScalars(f, map[string]*string{"provider": &out.Provider, "payload_object_error": &out.PayloadObjectError}, map[string]*bool{"payload_object_required": &out.PayloadObjectRequired}); err != nil {
		return out, err
	}
	if err := triggerEnum(f, "payload_source", &out.PayloadSource, "form"); err != nil {
		return out, err
	}
	blocks := []struct {
		name string
		read func(yamlsource.Value) error
	}{
		{"secret", func(v yamlsource.Value) error { return admitTriggerSecret(v, &out.Secret) }},
		{"signature", func(v yamlsource.Value) error { return admitTriggerSignature(v, &out.Signature) }},
		{"delivery_id", func(v yamlsource.Value) error { return admitTriggerValueSource(v, &out.DeliveryID) }},
		{"event_type", func(v yamlsource.Value) error { return admitTriggerValueSource(v, &out.EventType) }},
		{"event_name", func(v yamlsource.Value) error { return admitTriggerEventName(v, &out.EventName) }},
		{"ack", func(v yamlsource.Value) error { return admitTriggerAck(v, &out.Ack) }},
		{"challenge", func(v yamlsource.Value) error { return admitTriggerChallenge(v, &out.Challenge) }},
		{"delivery_condition", func(v yamlsource.Value) error {
			out.DeliveryCondition = &ConditionManifest{}
			return admitTriggerCondition(v, out.DeliveryCondition)
		}},
	}
	for _, block := range blocks {
		if value, present := f[block.name]; present {
			if err := block.read(value); err != nil {
				return out, err
			}
		}
	}
	return out, admitTriggerCollections(f, &out)
}

func admitTriggerCollections(f map[string]yamlsource.Value, out *manifestDefinition) error {
	var err error
	if value, present := f["normalized_events"]; present {
		out.NormalizedEvents, err = admitTriggerNormalizedEvents(value)
		if err != nil {
			return err
		}
	}
	if value, present := f["redact_keys"]; present {
		out.RedactKeys, err = triggerTextList(value)
		if err != nil {
			return err
		}
	}
	if value, present := f["metadata"]; present {
		out.Metadata, err = triggerTextMap(value)
	}
	return err
}

func admitTriggerSecret(value yamlsource.Value, out *SecretManifest) error {
	f, err := triggerFields(value, "required")
	if err != nil {
		return err
	}
	return triggerScalars(f, nil, map[string]*bool{"required": &out.Required})
}

func admitTriggerSignature(value yamlsource.Value, out *SignatureManifest) error {
	typeField, err := value.Lookup("type")
	if err != nil {
		return err
	}
	if out.Type, err = triggerText(typeField.Value); err != nil {
		return err
	}
	allowed := []string{"type", "header", "missing_error", "invalid_error"}
	if out.Type != signatureTypeTokenEquality {
		allowed = append(allowed, "encoding", "prefix", "signed_payload", "signature_param", "timestamp")
	}
	f, err := triggerFields(value, allowed...)
	if err != nil {
		return err
	}
	if err := triggerEnum(f, "type", &out.Type, signatureTypeHMACSHA256, signatureTypeHMACSHA1, signatureTypeTokenEquality); err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"header": &out.Header, "prefix": &out.Prefix, "signature_param": &out.SignatureParam, "missing_error": &out.MissingError, "invalid_error": &out.InvalidError}, nil); err != nil {
		return err
	}
	if err := triggerEnum(f, "encoding", &out.Encoding, "base64"); err != nil {
		return err
	}
	if err := triggerEnum(f, "signed_payload", &out.SignedPayload, "raw_body", "slack_v0", "timestamp_dot_raw_body", "url_plus_sorted_form"); err != nil {
		return err
	}
	if timestamp, present := f["timestamp"]; present {
		out.Timestamp = &TimestampManifest{}
		return admitTriggerTimestamp(timestamp, out.Timestamp)
	}
	return nil
}

func admitTriggerTimestamp(value yamlsource.Value, out *TimestampManifest) error {
	f, err := triggerFields(value, "header", "param", "tolerance", "missing_error", "invalid_error", "stale_error")
	if err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"header": &out.Header, "param": &out.Param, "tolerance": &out.Tolerance, "missing_error": &out.MissingError, "invalid_error": &out.InvalidError, "stale_error": &out.StaleError}, nil); err != nil {
		return err
	}
	if tolerance, present := f["tolerance"]; present {
		if strings.TrimSpace(out.Tolerance) == "" {
			return nil
		}
		if _, err := time.ParseDuration(strings.TrimSpace(out.Tolerance)); err != nil {
			return triggerSourceError(tolerance, "invalid timestamp tolerance: "+err.Error())
		}
	}
	return nil
}

func admitTriggerCondition(value yamlsource.Value, out *ConditionManifest) error {
	f, err := triggerFields(value, "json_path", "equals", "normalize", "missing_error", "mismatch_error")
	if err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"json_path": &out.JSONPath, "equals": &out.Equals, "missing_error": &out.MissingError, "mismatch_error": &out.MismatchErr}, map[string]*bool{"normalize": &out.Normalize}); err != nil {
		return err
	}
	return triggerRequiredText(value, f, "json_path", out.JSONPath)
}

func admitTriggerChallenge(value yamlsource.Value, out **ChallengeManifest) error {
	f, err := triggerFields(value, "when", "response")
	if err != nil {
		return err
	}
	result := &ChallengeManifest{}
	when, err := triggerRequired(value, f, "when")
	if err != nil {
		return err
	}
	if err := admitTriggerCondition(when, &result.When); err != nil {
		return err
	}
	response, err := triggerRequired(value, f, "response")
	if err != nil {
		return err
	}
	if err := admitTriggerResponse(response, &result.Response); err != nil {
		return err
	}
	*out = result
	return nil
}

func admitTriggerResponse(value yamlsource.Value, out *ResponseManifest) error {
	f, err := triggerFields(value, "json_path", "missing_error", "content_type", "status")
	if err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"json_path": &out.JSONPath, "missing_error": &out.MissingErr, "content_type": &out.ContentType}, nil); err != nil {
		return err
	}
	if status, present := f["status"]; present {
		scalar, err := status.Scalar()
		if err != nil || scalar.Tag != "!!int" {
			return triggerSourceError(status, "want integer")
		}
		if err := status.Project(&out.Status); err != nil {
			return err
		}
	}
	return triggerRequiredText(value, f, "json_path", out.JSONPath)
}

func admitTriggerValueSource(value yamlsource.Value, out *ValueSource) error {
	f, err := triggerFields(value, "header", "json_path", "form_param", "query_param", "literal", "required", "missing_error")
	if err != nil {
		return err
	}
	return triggerScalars(f, map[string]*string{"header": &out.Header, "json_path": &out.JSONPath, "form_param": &out.FormParam, "query_param": &out.QueryParam, "literal": &out.Literal, "missing_error": &out.MissingError}, map[string]*bool{"required": &out.Required})
}

func admitTriggerEventName(value yamlsource.Value, out *EventNameManifest) error {
	f, err := triggerFields(value, "literal", "template")
	if err != nil {
		return err
	}
	return triggerScalars(f, map[string]*string{"literal": &out.Literal, "template": &out.Template}, nil)
}

func admitTriggerAck(value yamlsource.Value, out *AckManifest) error {
	f, err := triggerFields(value, "mode")
	if err != nil {
		return err
	}
	return triggerEnum(f, "mode", &out.Mode, "durable_before_dispatch")
}

func admitTriggerNormalizedEvents(value yamlsource.Value) ([]NormalizedEventManifest, error) {
	rows, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]NormalizedEventManifest, len(rows))
	for i, row := range rows {
		if err := admitTriggerNormalizedEvent(row, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func admitTriggerNormalizedEvent(value yamlsource.Value, out *NormalizedEventManifest) error {
	f, err := triggerFields(value, "event", "fields", "when", "author_subject")
	if err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"event": &out.Event}, nil); err != nil {
		return err
	}
	fields, err := triggerRequired(value, f, "fields")
	if err != nil {
		return err
	}
	if out.Fields, err = admitTriggerNormalizedFields(fields); err != nil {
		return err
	}
	if when, present := f["when"]; present {
		if err := admitTriggerWhen(when, &out.When); err != nil {
			return err
		}
	}
	if author, present := f["author_subject"]; present {
		return admitTriggerAuthorSubject(author, &out.AuthorSubject)
	}
	return nil
}

func admitTriggerAuthorSubject(value yamlsource.Value, out *AuthorSubjectManifest) error {
	f, err := triggerFields(value, "type", "field")
	if err != nil {
		return err
	}
	if err := triggerScalars(f, map[string]*string{"type": &out.Type, "field": &out.Field}, nil); err != nil {
		return err
	}
	if err := triggerRequiredText(value, f, "type", out.Type); err != nil {
		return err
	}
	return triggerRequiredText(value, f, "field", out.Field)
}

func admitTriggerNormalizedFields(value yamlsource.Value) (map[string]NormalizedEventFieldProjection, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]NormalizedEventFieldProjection, len(fields))
	for _, field := range fields {
		if field.KeyTag != "!!str" || !normalizedFieldNamePattern.MatchString(field.Name) {
			return nil, triggerSourceError(field.Value, "invalid normalized field name "+field.Name)
		}
		projection, err := admitTriggerProjection(field.Value)
		if err != nil {
			return nil, err
		}
		out[field.Name] = projection
	}
	return out, nil
}

func admitTriggerProjection(value yamlsource.Value) (NormalizedEventFieldProjection, error) {
	f, err := triggerFields(value, "from", "schema", "optional", "convert", "values")
	var out NormalizedEventFieldProjection
	if err != nil {
		return out, err
	}
	if err := triggerScalars(f, map[string]*string{"from": &out.From}, map[string]*bool{"optional": &out.Optional}); err != nil {
		return out, err
	}
	from, err := triggerRequired(value, f, "from")
	if err != nil {
		return out, err
	}
	if err := validateTriggerRelativePath(out.From); err != nil {
		return out, triggerSourceError(from, err.Error())
	}
	schema, err := triggerRequired(value, f, "schema")
	if err != nil {
		return out, err
	}
	if out.Schema, err = runtimecontracts.AdmitToolInputSchemaValue(schema); err != nil {
		return out, triggerSourceError(schema, err.Error())
	}
	if conversion, present := f["convert"]; present {
		if err := admitTriggerConversion(conversion, &out); err != nil {
			return out, err
		}
	}
	if values, present := f["values"]; present {
		if out.Convert != normalizedFieldConvertTextEnumMap {
			return out, triggerSourceError(values, "values require convert: text_enum_map")
		}
		out.Values, err = triggerTextMap(values)
	}
	return out, err
}

func admitTriggerConversion(value yamlsource.Value, out *NormalizedEventFieldProjection) error {
	if value.Presence() == yamlsource.PresenceMapping || value.Presence() == yamlsource.PresenceEmptyMapping {
		f, err := triggerFields(value, "pattern")
		if err != nil {
			return err
		}
		pattern, err := triggerRequired(value, f, "pattern")
		if err != nil {
			return err
		}
		out.Convert = normalizedFieldConvertPattern
		out.Pattern, err = triggerText(pattern)
		if err != nil {
			return err
		}
		return admitTriggerPattern(pattern, out)
	}
	return triggerEnum(map[string]yamlsource.Value{"convert": value}, "convert", &out.Convert, runtimecontracts.FieldProjectionConvertNumberToText, normalizedFieldConvertTextEnumMap)
}

func admitTriggerPattern(value yamlsource.Value, out *NormalizedEventFieldProjection) error {
	if out.Schema.Kind() != runtimecontracts.ToolSchemaObject {
		return triggerSourceError(value, "pattern requires an object output schema")
	}
	compiled, err := regexp.Compile(out.Pattern)
	if err != nil {
		return triggerSourceError(value, "invalid pattern: "+err.Error())
	}
	seen := map[string]bool{}
	for _, name := range compiled.SubexpNames() {
		if name == "" {
			continue
		}
		if _, present := out.Schema.Property(name); !present {
			return triggerSourceError(value, "capture "+name+" is not a declared output property")
		}
		if seen[name] {
			return triggerSourceError(value, "duplicate named capture "+name)
		}
		seen[name] = true
	}
	out.pattern = compiled
	return nil
}

func admitTriggerWhen(value yamlsource.Value, out *NormalizedEventWhen) error {
	f, err := triggerFields(value, "exists", "absent", "equals", "one_of")
	if err != nil {
		return err
	}
	for _, list := range []struct {
		name   string
		target *[]string
	}{{"exists", &out.Exists}, {"absent", &out.Absent}} {
		if field, present := f[list.name]; present {
			if *list.target, err = triggerTextList(field); err != nil {
				return err
			}
			for _, path := range *list.target {
				if err := validateTriggerRelativePath(path); err != nil {
					return triggerSourceError(field, err.Error())
				}
			}
		}
	}
	if equals, present := f["equals"]; present {
		if out.Equals, err = triggerPredicateTextMap(equals); err != nil {
			return err
		}
	}
	if oneOf, present := f["one_of"]; present {
		members, err := oneOf.Mapping()
		if err != nil {
			return err
		}
		out.OneOf = make(map[string][]string, len(members))
		for _, field := range members {
			if field.KeyTag != "!!str" {
				return triggerSourceError(field.Value, "predicate path must be text")
			}
			if err := validateTriggerRelativePath(field.Name); err != nil {
				return triggerSourceError(field.Value, err.Error())
			}
			if out.OneOf[field.Name], err = triggerTextList(field.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func triggerPredicateTextMap(value yamlsource.Value) (map[string]string, error) {
	members, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	for _, member := range members {
		if member.KeyTag != "!!str" {
			return nil, triggerSourceError(member.Value, "predicate path must be text")
		}
		if err := validateTriggerRelativePath(member.Name); err != nil {
			return nil, triggerSourceError(member.Value, err.Error())
		}
	}
	out := make(map[string]string, len(members))
	for _, member := range members {
		text, err := triggerText(member.Value)
		if err != nil {
			return nil, err
		}
		if text == "" {
			return nil, triggerSourceError(member.Value, "want nonempty text")
		}
		out[member.Name] = text
	}
	return out, nil
}

func validateTriggerRelativePath(text string) error {
	parsed, err := runtimepaths.ParseStrictRelative(text)
	if err != nil {
		return err
	}
	if strings.Join(parsed.Segments, ".") != text {
		return fmt.Errorf("relative dotted path %q is not canonical", text)
	}
	return nil
}

func triggerFields(value yamlsource.Value, names ...string) (map[string]yamlsource.Value, error) {
	members, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	out := make(map[string]yamlsource.Value, len(members))
	for _, member := range members {
		if member.KeyTag != "!!str" {
			return nil, triggerSourceError(member.Value, "field name must be text")
		}
		if _, present := allowed[member.Name]; !present {
			return nil, runtimecontracts.NewUndefinedFieldDiagnostic("provider trigger manifest", member.Name, allowed, member)
		}
		out[member.Name] = member.Value
	}
	return out, nil
}

func triggerScalars(fields map[string]yamlsource.Value, texts map[string]*string, flags map[string]*bool) error {
	names := make([]string, 0, len(texts)+len(flags))
	for name := range texts {
		names = append(names, name)
	}
	for name := range flags {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, present := fields[name]
		if !present {
			continue
		}
		if target := texts[name]; target != nil {
			text, err := triggerText(value)
			if err != nil {
				return err
			}
			*target = text
			continue
		}
		scalar, err := value.Scalar()
		if err != nil || scalar.Tag != "!!bool" {
			return triggerSourceError(value, "want boolean")
		}
		if *flags[name], err = strconv.ParseBool(scalar.Value); err != nil {
			return triggerSourceError(value, "invalid boolean")
		}
	}
	return nil
}

func triggerText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" {
		return "", triggerSourceError(value, "want text")
	}
	return scalar.Value, nil
}

func triggerTextList(value yamlsource.Value) ([]string, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(items))
	for i, item := range items {
		if out[i], err = triggerText(item); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func triggerTextMap(value yamlsource.Value) (map[string]string, error) {
	members, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(members))
	for _, member := range members {
		if member.KeyTag != "!!str" {
			return nil, triggerSourceError(member.Value, "map key must be text")
		}
		if out[member.Name], err = triggerText(member.Value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func triggerEnum(fields map[string]yamlsource.Value, name string, target *string, allowed ...string) error {
	value, present := fields[name]
	if !present {
		return nil
	}
	text, err := triggerText(value)
	if err != nil {
		return err
	}
	for _, candidate := range allowed {
		if text == candidate {
			*target = text
			return nil
		}
	}
	return triggerSourceError(value, "want one of "+strings.Join(allowed, ", "))
}

func triggerRequired(parent yamlsource.Value, fields map[string]yamlsource.Value, name string) (yamlsource.Value, error) {
	if value, present := fields[name]; present {
		return value, nil
	}
	missing, err := parent.Lookup(name)
	if err != nil {
		return yamlsource.Value{}, err
	}
	return missing.Value, triggerSourceError(missing.Value, "required field is missing")
}

func triggerRequiredText(parent yamlsource.Value, fields map[string]yamlsource.Value, name, text string) error {
	value, err := triggerRequired(parent, fields, name)
	if err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return triggerSourceError(value, "want nonempty text")
	}
	return nil
}

func triggerSourceError(value yamlsource.Value, message string) error {
	return fmt.Errorf("%s at %s: %s", value.SemanticPath(), value.Location(), message)
}
