package providerconnectors

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func admitGeneratorProfile(body []byte, file string) (GeneratorProfile, error) {
	root, err := connectorSource(body, file)
	if err != nil {
		return GeneratorProfile{}, err
	}
	fields, err := connectorFields(root, "schema_version", "provider", "source", "pack", "auth", "static_headers", "operations")
	if err != nil {
		return GeneratorProfile{}, err
	}
	out := GeneratorProfile{source: root}
	if err := connectorTextFields(root, fields, map[string]*string{"schema_version": &out.SchemaVersion, "provider": &out.Provider}); err != nil {
		return out, err
	}
	source, err := connectorRequired(root, fields, "source")
	if err != nil {
		return out, err
	}
	members, err := connectorFields(source, "path", "compression", "sha256", "openapi_version", "server")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(source, members, map[string]*string{"path": &out.Source.Path, "compression": &out.Source.Compression, "sha256": &out.Source.SHA256, "openapi_version": &out.Source.OpenAPIVersion, "server": &out.Source.Server}); err != nil {
		return out, err
	}
	pack, err := connectorRequired(root, fields, "pack")
	if err != nil {
		return out, err
	}
	members, err = connectorFields(pack, "id", "version", "platform_version", "provenance", "tests")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(pack, members, map[string]*string{"id": &out.Pack.ID, "version": &out.Pack.Version, "platform_version": &out.Pack.PlatformVersion, "provenance": &out.Pack.Provenance}); err != nil {
		return out, err
	}
	tests, err := connectorRequired(pack, members, "tests")
	if err != nil {
		return out, err
	}
	if out.Pack.Tests, err = connectorTextList(tests); err != nil {
		return out, err
	}
	auth, err := connectorRequired(root, fields, "auth")
	if err != nil {
		return out, err
	}
	if out.Auth, err = admitGeneratorAuth(auth); err != nil {
		return out, err
	}
	headers, err := connectorRequired(root, fields, "static_headers")
	if err != nil {
		return out, err
	}
	entries, err := headers.Mapping()
	if err != nil {
		return out, err
	}
	out.StaticHeaders = make(map[string]string, len(entries))
	for _, entry := range entries {
		if _, duplicate := out.StaticHeaders[entry.Name]; duplicate || entry.KeyTag != "!!str" || entry.Name == "" {
			return out, connectorError(entry.Value, "duplicate or non-text header name")
		}
		if out.StaticHeaders[entry.Name], err = connectorText(entry.Value); err != nil {
			return out, err
		}
	}
	operations, err := connectorRows(root, fields, "operations")
	if err != nil {
		return out, err
	}
	out.Operations = make([]GeneratorOperation, len(operations))
	for i, row := range operations {
		if out.Operations[i], err = admitGeneratorOperation(row); err != nil {
			return out, err
		}
	}
	return out, nil
}

func admitGeneratorAuth(value yamlsource.Value) (GeneratorAuth, error) {
	fields, err := connectorFields(value, "mode", "credentials", "managed_credential")
	if err != nil {
		return GeneratorAuth{}, err
	}
	var out GeneratorAuth
	if err := connectorTextFields(value, fields, map[string]*string{"mode": &out.Mode}); err != nil {
		return out, err
	}
	switch out.Mode {
	case profileAuthStatic:
		if forbidden, present := fields["managed_credential"]; present {
			return out, connectorError(forbidden, "static_credentials auth forbids managed_credential")
		}
		credentials, err := connectorRequired(value, fields, "credentials")
		if err != nil {
			return out, err
		}
		if out.Credentials, err = connectorTextList(credentials); err != nil {
			return out, err
		}
	case profileAuthManaged:
		if forbidden, present := fields["credentials"]; present {
			return out, connectorError(forbidden, "managed_credential auth forbids credentials")
		}
		credential, err := connectorRequired(value, fields, "managed_credential")
		if err != nil {
			return out, err
		}
		ref, err := runtimecontracts.ProjectToolManagedCredentialValue(credential)
		if err != nil {
			return out, err
		}
		out.ManagedCredential = &ref
	default:
		return out, connectorError(fields["mode"], "unsupported auth mode")
	}
	return out, nil
}

func admitGeneratorOperation(value yamlsource.Value) (GeneratorOperation, error) {
	fields, err := connectorFields(value, "operation_id", "path", "method", "tool_id", "description", "effect_class", "path_parameters", "request_body", "injected_inputs", "permissions", "output", "response_success", "fixture", "review_status")
	if err != nil {
		return GeneratorOperation{}, err
	}
	var out GeneratorOperation
	if err := connectorTextFields(value, fields, map[string]*string{"operation_id": &out.OperationID, "path": &out.Path, "method": &out.Method, "tool_id": &out.ToolID, "description": &out.Description, "effect_class": &out.EffectClass, "review_status": &out.ReviewStatus}); err != nil {
		return out, err
	}
	parameters, err := connectorRequired(value, fields, "path_parameters")
	if err != nil {
		return out, err
	}
	if out.PathParameters, err = admitGeneratorFields(parameters); err != nil {
		return out, err
	}
	if err := admitGeneratorRequestBody(value, fields, &out); err != nil {
		return out, err
	}
	injected, err := connectorRows(value, fields, "injected_inputs")
	if err != nil {
		return out, err
	}
	out.InjectedInputs = make([]GeneratorInjectedInput, len(injected))
	for i, row := range injected {
		members, err := connectorFields(row, "input", "type", "required", "purpose")
		if err != nil {
			return out, err
		}
		input := &out.InjectedInputs[i]
		if err := connectorTextFields(row, members, map[string]*string{"input": &input.Input, "type": &input.Type, "purpose": &input.Purpose}); err != nil {
			return out, err
		}
		required, err := connectorRequired(row, members, "required")
		if err != nil {
			return out, err
		}
		if input.Required, err = connectorBool(required); err != nil {
			return out, err
		}
	}
	if out.Permissions, err = connectorPermissions(value, fields); err != nil {
		return out, err
	}
	output, err := connectorRequired(value, fields, "output")
	if err != nil {
		return out, err
	}
	members, err := connectorFields(output, "type", "items_type")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(output, members, map[string]*string{"type": &out.Output.Type}); err != nil {
		return out, err
	}
	if out.Output.ItemsType, err = generatorItemsType(output, members, out.Output.Type); err != nil {
		return out, err
	}
	success, err := connectorRequired(value, fields, "response_success")
	if err != nil {
		return out, err
	}
	if out.ResponseSuccess, err = runtimecontracts.ProjectToolResponseSuccessValue(success); err != nil {
		return out, err
	}
	fixture, err := connectorRequired(value, fields, "fixture")
	if err != nil {
		return out, err
	}
	members, err = connectorFields(fixture, "id", "status")
	if err != nil {
		return out, err
	}
	if err := connectorTextFields(fixture, members, map[string]*string{"id": &out.Fixture.ID, "status": &out.Fixture.Status}); err != nil {
		return out, err
	}
	return out, nil
}

func admitGeneratorFields(value yamlsource.Value) ([]GeneratorField, error) {
	rows, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	out := make([]GeneratorField, len(rows))
	for i, row := range rows {
		members, err := connectorFields(row, "source", "input", "type", "items_type", "required")
		if err != nil {
			return nil, err
		}
		field := &out[i]
		if err := connectorTextFields(row, members, map[string]*string{"source": &field.Source, "input": &field.Input, "type": &field.Type}); err != nil {
			return nil, err
		}
		required, err := connectorRequired(row, members, "required")
		if err != nil {
			return nil, err
		}
		if field.Required, err = connectorBool(required); err != nil {
			return nil, err
		}
		if field.ItemsType, err = generatorItemsType(row, members, field.Type); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func generatorItemsType(parent yamlsource.Value, members map[string]yamlsource.Value, kind string) (string, error) {
	if kind == "array" {
		value, err := connectorRequired(parent, members, "items_type")
		if err != nil {
			return "", err
		}
		return connectorText(value)
	}
	if forbidden, present := members["items_type"]; present {
		return "", connectorError(forbidden, "non-array field forbids items_type")
	}
	return "", nil
}

func admitGeneratorRequestBody(value yamlsource.Value, fields map[string]yamlsource.Value, out *GeneratorOperation) error {
	body, err := connectorRequired(value, fields, "request_body")
	if err != nil {
		return err
	}
	members, err := connectorFields(body, "required", "variant", "fields")
	if err != nil {
		return err
	}
	required, err := connectorRequired(body, members, "required")
	if err != nil {
		return err
	}
	if out.RequestBody.Required, err = connectorBool(required); err != nil {
		return err
	}
	bodyFields, err := connectorRequired(body, members, "fields")
	if err != nil {
		return err
	}
	if out.RequestBody.Fields, err = admitGeneratorFields(bodyFields); err != nil {
		return err
	}
	variant, err := connectorRequired(body, members, "variant")
	if err != nil {
		return err
	}
	members, err = connectorFields(variant, "kind", "fields")
	if err != nil {
		return err
	}
	if err := connectorTextFields(variant, members, map[string]*string{"kind": &out.RequestBody.Variant.Kind}); err != nil {
		return err
	}
	variantFields, err := connectorRequired(variant, members, "fields")
	if err != nil {
		return err
	}
	if out.RequestBody.Variant.Fields, err = connectorTextList(variantFields); err != nil {
		return err
	}
	return nil
}
