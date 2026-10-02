package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var retiredToolFields = map[string]string{
	"parameters": "use input_schema", "returns": "use output_schema", "endpoint": "use http.url",
	"type": "use handler_type", "required_permission": "use permission", "kind": "use handler_type: wasm or python",
}

var toolEntryFields = map[string]struct{}{
	"category": {}, "description": {}, "handler_type": {}, "effect_class": {}, "permission": {},
	"rate_limit": {}, "rate_limit_max_wait": {}, "input_schema": {}, "output_schema": {},
	"http": {}, "response_mapping": {}, "response_success": {}, "credentials": {}, "managed_credential": {},
	"path": {}, "abi": {}, "entry": {}, "digest": {}, "source_path": {}, "source_hash": {}, "runtime": {}, "limits": {},
}

func projectToolDeclarationsValue(root yamlsource.Value) (map[string]ToolSchemaEntry, error) {
	if err := root.ValidateExpansion(); err != nil {
		return nil, err
	}
	declarations, err := uniqueYAMLMappingFields(root, "tools.yaml declarations")
	if err != nil {
		return nil, err
	}
	if err := validateExactDeclarationNames(declarations, "tools.yaml declaration"); err != nil {
		return nil, err
	}
	out := make(map[string]ToolSchemaEntry, len(declarations))
	for _, declaration := range declarations {
		entry, err := projectToolValue(declaration.Value)
		if err != nil {
			return nil, fmt.Errorf("tool %q: %w", declaration.Name, err)
		}
		entry.admissionProvenance = map[string]EffectiveValueProvenance{"declaration": authoredSourceProvenance(declaration.Value)}
		if err := collectNodeValueProvenance(declaration.Value, "", entry.admissionProvenance, nil); err != nil {
			return nil, err
		}
		for _, name := range []string{"input_schema", "output_schema"} {
			if _, present := entry.admissionProvenance[name]; !present {
				entry.admissionProvenance[name] = EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "tool.default_object_schema"}
			}
		}
		out[declaration.Name] = entry
	}
	return out, nil
}

// AdmitToolDeclarationsValue is also the child handoff for connector packs;
// pack envelopes do not interpret the tool grammar again.
func AdmitToolDeclarationsValue(root yamlsource.Value) (map[string]ToolSchemaEntry, error) {
	return projectToolDeclarationsValue(root)
}

func projectToolValue(value yamlsource.Value) (ToolSchemaEntry, error) {
	fields, err := nodeValueFields(value, "tool", toolEntryFields, retiredToolFields)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	var category, description, handlerText, effect, permission, rate, wait string
	if err := schemaValueTexts(fields, map[string]*string{
		"category": &category, "description": &description, "handler_type": &handlerText,
		"effect_class": &effect, "permission": &permission, "rate_limit": &rate, "rate_limit_max_wait": &wait,
	}, false); err != nil {
		return ToolSchemaEntry{}, err
	}
	handler, err := ParseToolHandlerKind(handlerText)
	if err != nil {
		return ToolSchemaEntry{}, nodeValueError(value, err)
	}
	moduleKind := handler == ToolHandlerWasm || handler == ToolHandlerPython
	defaultSchema := MustToolInputSchema(ToolSchemaObject)
	schemas := map[string]ToolInputSchema{"input_schema": defaultSchema, "output_schema": defaultSchema}
	for _, name := range []string{"input_schema", "output_schema"} {
		if field, present := fields[name]; present {
			schema, err := AdmitToolInputSchemaValue(field)
			if err != nil {
				return ToolSchemaEntry{}, err
			}
			if !moduleKind && schema.Equal(defaultSchema) {
				return ToolSchemaEntry{}, nodeValueError(field, fmt.Errorf("RETIRED: %s object schema is the default; remove it", name))
			}
			schemas[name] = schema
		} else if moduleKind {
			return ToolSchemaEntry{}, nodeValueError(value, fmt.Errorf("module %s is required", name))
		}
	}
	options := []ToolSchemaEntryOption{
		WithToolCategory(category), WithToolDescription(description), WithToolHandler(handler),
		WithToolEffect(ActivityEffectClass(effect)), WithToolPermission(permission), WithToolRateLimit(rate, wait),
		WithToolSchemas(schemas["input_schema"], schemas["output_schema"]),
	}
	if moduleKind {
		module, err := projectToolModuleValue(value, fields, handler, schemas)
		if err != nil {
			return ToolSchemaEntry{}, err
		}
		options = append(options, WithToolModule(module))
	} else {
		transport, err := projectToolTransportOptions(fields)
		if err != nil {
			return ToolSchemaEntry{}, err
		}
		options = append(options, transport...)
	}
	entry, err := NewToolSchemaEntry(options...)
	if err != nil {
		return ToolSchemaEntry{}, nodeValueError(value, err)
	}
	return entry, nil
}

func projectToolTransportOptions(fields map[string]yamlsource.Value) ([]ToolSchemaEntryOption, error) {
	options := []ToolSchemaEntryOption{}
	for _, name := range []string{"path", "abi", "entry", "digest", "source_path", "source_hash", "runtime", "limits"} {
		if field, present := fields[name]; present {
			return nil, nodeValueError(field, fmt.Errorf("%s requires handler_type wasm or python", name))
		}
	}
	if field, present := fields["http"]; present {
		spec, err := projectToolHTTPValue(field)
		if err != nil {
			return nil, err
		}
		options = append(options, WithToolHTTP(spec))
	}
	if field, present := fields["response_mapping"]; present {
		if _, err := uniqueYAMLMappingFields(field, "response_mapping"); err != nil {
			return nil, err
		}
		var mapping map[string]any
		if err := field.Project(&mapping); err != nil {
			return nil, nodeValueError(field, err)
		}
		options = append(options, WithToolResponseMapping(mapping))
	}
	if field, present := fields["response_success"]; present {
		success, err := ProjectToolResponseSuccessValue(field)
		if err != nil {
			return nil, err
		}
		options = append(options, WithToolResponseSuccess(success))
	}
	if field, present := fields["credentials"]; present {
		credentials, err := agentValueTextList(field)
		if err != nil {
			return nil, err
		}
		options = append(options, WithToolCredentials(credentials...))
	}
	if field, present := fields["managed_credential"]; present {
		ref, err := ProjectToolManagedCredentialValue(field)
		if err != nil {
			return nil, err
		}
		options = append(options, WithToolManagedCredential(ref))
	}
	return options, nil
}

func ProjectToolResponseSuccessValue(field yamlsource.Value) (HTTPResponseSuccess, error) {
	members, err := schemaValueFields(field, "response_success", map[string]struct{}{"kind": {}, "path": {}, "equals": {}}, nil, true)
	if err != nil {
		return HTTPResponseSuccess{}, err
	}
	var success HTTPResponseSuccess
	if err := schemaValueRequiredTexts(field, members, map[string]*string{"kind": &success.Kind}); err != nil {
		return success, err
	}
	if success.Kind == "http_status_2xx" {
		for _, name := range []string{"path", "equals"} {
			if forbidden, present := members[name]; present {
				return success, nodeValueError(forbidden, fmt.Errorf("response_success.%s is forbidden for kind http_status_2xx", name))
			}
		}
	}
	if err := schemaValueTexts(members, map[string]*string{"path": &success.Path}, false); err != nil {
		return success, err
	}
	if equals, present := members["equals"]; present {
		success.EqualsPresent = true
		if err := equals.Project(&success.Equals); err != nil {
			return success, nodeValueError(equals, err)
		}
	}
	return success, nil
}

func projectToolHTTPValue(value yamlsource.Value) (HTTPToolSpec, error) {
	fields, err := schemaValueFields(value, "http", map[string]struct{}{"method": {}, "url": {}, "headers": {}, "body": {}, "timeout_seconds": {}}, nil, true)
	if err != nil {
		return HTTPToolSpec{}, err
	}
	var out HTTPToolSpec
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"method": &out.Method, "url": &out.URL}); err != nil {
		return out, err
	}
	if headers, present := fields["headers"]; present {
		out.Headers, err = toolValueTextMap(headers)
		if err != nil {
			return out, err
		}
	}
	if body, present := fields["body"]; present {
		out.BodyPresent = true
		if err := body.Project(&out.Body); err != nil {
			return out, nodeValueError(body, err)
		}
	}
	if timeout, present := fields["timeout_seconds"]; present {
		out.TimeoutSeconds, err = agentValueInteger(timeout)
		if err != nil {
			return out, nodeValueError(timeout, err)
		}
		if out.TimeoutSeconds <= 0 {
			return out, nodeValueError(timeout, fmt.Errorf("http.timeout_seconds must be positive"))
		}
	}
	return out, nil
}

func toolValueTextMap(value yamlsource.Value) (map[string]string, error) {
	fields, err := uniqueYAMLMappingFields(value, "text mapping")
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(fields))
	for _, field := range fields {
		text, err := agentValueText(field.Value)
		if err != nil {
			return nil, nodeValueError(field.Value, err)
		}
		out[field.Name] = text
	}
	return out, nil
}

func ProjectToolManagedCredentialValue(value yamlsource.Value) (ManagedCredentialRef, error) {
	fields, err := schemaValueFields(value, "managed_credential", map[string]struct{}{
		"key": {}, "header": {}, "prefix": {}, "grant_type": {}, "scopes": {}, "grant_model": {}, "token_request": {}, "installation_id_input": {},
	}, nil, true)
	if err != nil {
		return ManagedCredentialRef{}, err
	}
	var out ManagedCredentialRef
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"key": &out.Key}); err != nil {
		return out, err
	}
	if err := schemaValueTexts(fields, map[string]*string{"header": &out.Header, "prefix": &out.Prefix, "grant_type": &out.GrantType, "grant_model": &out.GrantModel, "installation_id_input": &out.InstallationIDInput}, false); err != nil {
		return out, err
	}
	if scopes, present := fields["scopes"]; present {
		out.Scopes, err = agentValueTextList(scopes)
		if err != nil {
			return out, err
		}
	}
	if request, present := fields["token_request"]; present {
		members, err := schemaValueFields(request, "token_request", map[string]struct{}{"client_auth": {}, "body": {}, "static_headers": {}}, nil, false)
		if err != nil {
			return out, err
		}
		if err := schemaValueTexts(members, map[string]*string{"client_auth": &out.TokenRequest.ClientAuth, "body": &out.TokenRequest.Body}, false); err != nil {
			return out, err
		}
		if field, present := members["static_headers"]; present {
			out.TokenRequest.StaticHeaders, err = toolValueTextMap(field)
			if err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

func projectToolModuleValue(value yamlsource.Value, fields map[string]yamlsource.Value, handler ToolHandlerKind, schemas map[string]ToolInputSchema) (PolicyModule, error) {
	for _, name := range []string{"http", "response_mapping", "response_success", "credentials", "managed_credential", "category", "permission", "effect_class", "rate_limit", "rate_limit_max_wait"} {
		if field, present := fields[name]; present {
			return PolicyModule{}, nodeValueError(field, fmt.Errorf("module tools cannot declare %s", name))
		}
	}
	out := PolicyModule{Kind: handler.String(), InputSchema: schemas["input_schema"].Projection(), OutputSchema: schemas["output_schema"].Projection()}
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"path": &out.Path, "abi": &out.ABI, "entry": &out.Entry, "digest": &out.Digest}); err != nil {
		return out, err
	}
	if err := schemaValueTexts(fields, map[string]*string{"source_path": &out.SourcePath, "source_hash": &out.SourceHash}, true); err != nil {
		return out, err
	}
	if handler == ToolHandlerPython && (out.SourcePath != "" || out.SourceHash != "") {
		return out, nodeValueError(value, fmt.Errorf("python module source authority is path/digest only"))
	}
	limits, present := fields["limits"]
	if !present {
		return out, nodeValueError(value, fmt.Errorf("module limits are required"))
	}
	members, err := schemaValueFields(limits, "limits", map[string]struct{}{"gas": {}, "memory_pages": {}, "output_bytes": {}}, nil, true)
	if err != nil {
		return out, err
	}
	for _, name := range []string{"gas", "memory_pages", "output_bytes"} {
		field, present := members[name]
		if !present {
			return out, nodeValueError(limits, fmt.Errorf("limits.%s is required", name))
		}
		scalar, err := field.Scalar()
		var integer uint64
		if err != nil || scalar.Tag != "!!int" || field.Project(&integer) != nil || integer == 0 {
			return out, nodeValueError(field, fmt.Errorf("limits.%s must be a positive integer", name))
		}
		switch name {
		case "gas":
			out.Limits.Gas = integer
		case "memory_pages":
			if integer > uint64(^uint32(0)) {
				return out, nodeValueError(field, fmt.Errorf("memory_pages overflows uint32"))
			}
			out.Limits.MemoryPages = uint32(integer)
		case "output_bytes":
			if integer > uint64(^uint(0)>>1) {
				return out, nodeValueError(field, fmt.Errorf("output_bytes overflows native int"))
			}
			out.Limits.OutputBytes = int(integer)
		}
	}
	if runtime, present := fields["runtime"]; present {
		members, err := schemaValueFields(runtime, "runtime", map[string]struct{}{"interpreter": {}, "interpreter_digest": {}, "snapshot_digest": {}, "harness_abi": {}}, nil, false)
		if err != nil {
			return out, err
		}
		if err := schemaValueTexts(members, map[string]*string{"interpreter": &out.Runtime.Interpreter, "interpreter_digest": &out.Runtime.InterpreterDigest, "snapshot_digest": &out.Runtime.SnapshotDigest, "harness_abi": &out.Runtime.HarnessABI}, true); err != nil {
			return out, err
		}
	}
	return out, nil
}
