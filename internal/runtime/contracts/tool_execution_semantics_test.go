package contracts

import (
	"strings"
	"testing"
)

func TestToolSchemaEntryRejectsUnadmittedExecutionSemantics(t *testing.T) {
	object := MustToolInputSchema(ToolSchemaObject)
	testCases := []struct {
		name   string
		option ToolSchemaEntryOption
	}{
		{name: "unknown category", option: WithToolCategory("connector-ish")},
		{name: "malformed permission", option: WithToolPermission("bad permission")},
		{name: "invalid rate policy", option: WithToolRateLimit("many/soon", "later")},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewToolSchemaEntry(
				tc.option,
				WithToolHandler(ToolHandlerPlatformBuiltin),
				WithToolSchemas(object, object),
			); err == nil {
				t.Fatal("NewToolSchemaEntry error = nil, want admission rejection")
			}
		})
	}
}

func TestToolSchemaEntryRejectsMalformedHTTPExecutionBeforeAuthority(t *testing.T) {
	object := MustToolInputSchema(ToolSchemaObject)
	testCases := []struct {
		name    string
		options []ToolSchemaEntryOption
		want    string
	}{
		{
			name: "unterminated URL template",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example/{{input.id"}),
			},
			want: "unterminated template",
		},
		{
			name: "relative URL",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "/provider"}),
			},
			want: "absolute URL host is required",
		},
		{
			name: "unsupported URL scheme",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "ftp://provider.example"}),
			},
			want: "scheme",
		},
		{
			name: "relative templated URL",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "/provider/{{input.id}}"}),
			},
			want: "literal http:// or https:// prefix",
		},
		{
			name: "templated URL without host",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https:///provider/{{input.id}}"}),
			},
			want: "absolute URL host is required",
		},
		{
			name: "unsupported header template root",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example", Headers: map[string]string{"X-ID": "{{event.id}}"}}),
			},
			want: "must start with input. or credentials.",
		},
		{
			name: "malformed response mapping",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolResponseMapping(map[string]any{"id": "{{response.body.id"}),
			},
			want: "unterminated template",
		},
		{
			name: "unsupported response success kind",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolResponseSuccess(HTTPResponseSuccess{Kind: "guess"}),
			},
			want: "unsupported",
		},
		{
			name: "non-scalar response equality",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolResponseSuccess(HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: map[string]any{"value": true}}),
			},
			want: "must be a scalar",
		},
		{
			name: "overlapping result targets",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolCompiledResult(CompiledResultProjection{
					Fields: map[string]CompiledResultField{
						"delivery":    {From: "result.delivery"},
						"delivery.id": {From: "result.id"},
					},
					OutputSchema: object,
				}),
			},
			want: "overlaps",
		},
		{
			name: "empty static credential",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolCredentials(""),
			},
			want: "credentials[0] is empty",
		},
		{
			name: "invalid managed header",
			options: []ToolSchemaEntryOption{
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolManagedCredential(ManagedCredentialRef{Key: "provider", Header: "bad header"}),
			},
			want: "header",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := []ToolSchemaEntryOption{
				WithToolHandler(ToolHandlerHTTP),
				WithToolSchemas(object, object),
			}
			options = append(options, tc.options...)
			if _, err := NewToolSchemaEntry(options...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewToolSchemaEntry error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestToolSchemaEntryRejectsStaticallyImpossibleResponseMappingsBeforeAuthority(t *testing.T) {
	closed := false
	output := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{
			"id": MustToolInputSchema(ToolSchemaString),
		}),
		ToolSchemaRequired("id"),
		ToolSchemaAdditionalPropertiesAllowed(closed),
	)
	testCases := []struct {
		name    string
		mapping map[string]any
		want    string
	}{
		{
			name:    "required output omitted",
			mapping: map[string]any{"other": "{{response.body.other}}"},
			want:    `omits required output property "id"`,
		},
		{
			name:    "static output has wrong type",
			mapping: map[string]any{"id": 42},
			want:    "must be string",
		},
		{
			name:    "closed output receives extra property",
			mapping: map[string]any{"id": "{{response.body.id}}", "extra": "{{response.body.extra}}"},
			want:    "forbidden by output_schema",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewToolSchemaEntry(
				WithToolHandler(ToolHandlerHTTP),
				WithToolResponseMapping(tc.mapping),
				WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
				WithToolSchemas(MustToolInputSchema(ToolSchemaObject), output),
			)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewToolSchemaEntry error = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := NewToolSchemaEntry(
		WithToolHandler(ToolHandlerHTTP),
		WithToolResponseMapping(map[string]any{"id": "{{response.body.id}}"}),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), output),
	); err != nil {
		t.Fatalf("dynamic response mapping admission: %v", err)
	}
}

func TestToolHTTPExecutionRejectsResolvedNonHTTPURLBeforeDispatch(t *testing.T) {
	execution, err := AdmitToolHTTPExecution(HTTPToolSpec{Method: "POST", URL: "{{input.url}}"})
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"/relative", "ftp://provider.example/path"} {
		if _, err := execution.Prepare(map[string]any{"url": invalid}, nil); err == nil {
			t.Fatalf("Prepare(%q) error = nil", invalid)
		}
	}
}

func TestToolSchemaEntryRejectsCompiledResultPathsOutsideAdmittedSchemas(t *testing.T) {
	allow := false
	stringSchema := MustToolInputSchema(ToolSchemaString)
	sourceSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": stringSchema}),
		ToolSchemaRequired("source_id"),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	targetSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"target_id": stringSchema}),
		ToolSchemaRequired("target_id"),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	base := []ToolSchemaEntryOption{
		WithToolHandler(ToolHandlerHTTP),
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), sourceSchema),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
	}
	for _, tc := range []struct {
		name   string
		fields map[string]CompiledResultField
		want   string
	}{
		{
			name:   "missing source",
			fields: map[string]CompiledResultField{"target_id": {From: "result.missing"}},
			want:   `source "result.missing" is not guaranteed`,
		},
		{
			name:   "missing target",
			fields: map[string]CompiledResultField{"missing": {From: "result.source_id"}},
			want:   `target "missing" is absent`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := append([]ToolSchemaEntryOption(nil), base...)
			options = append(options, WithToolCompiledResult(CompiledResultProjection{
				Fields: tc.fields, OutputSchema: targetSchema,
			}))
			if _, err := NewToolSchemaEntry(options...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewToolSchemaEntry error = %v, want %q", err, tc.want)
			}
		})
	}
	optionalSourceSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": stringSchema}),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	optionalOptions := []ToolSchemaEntryOption{
		WithToolHandler(ToolHandlerHTTP),
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), optionalSourceSchema),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
		WithToolCompiledResult(CompiledResultProjection{
			Fields: map[string]CompiledResultField{"target_id": {From: "result.source_id"}}, OutputSchema: targetSchema,
		}),
	}
	if _, err := NewToolSchemaEntry(optionalOptions...); err == nil || !strings.Contains(err.Error(), "not guaranteed") {
		t.Fatalf("NewToolSchemaEntry optional source error = %v", err)
	}
	incompatibleTarget := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"target_id": MustToolInputSchema(ToolSchemaInteger)}),
		ToolSchemaRequired("target_id"),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	incompatibleOptions := append([]ToolSchemaEntryOption(nil), base...)
	incompatibleOptions = append(incompatibleOptions, WithToolCompiledResult(CompiledResultProjection{
		Fields: map[string]CompiledResultField{"target_id": {From: "result.source_id"}}, OutputSchema: incompatibleTarget,
	}))
	if _, err := NewToolSchemaEntry(incompatibleOptions...); err == nil || !strings.Contains(err.Error(), "incompatible types") {
		t.Fatalf("NewToolSchemaEntry incompatible projection error = %v", err)
	}
	incompleteTarget := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{
			"target_id": stringSchema,
			"other":     stringSchema,
		}),
		ToolSchemaRequired("target_id", "other"),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	incompleteOptions := append([]ToolSchemaEntryOption(nil), base...)
	incompleteOptions = append(incompleteOptions, WithToolCompiledResult(CompiledResultProjection{
		Fields: map[string]CompiledResultField{"target_id": {From: "result.source_id"}}, OutputSchema: incompleteTarget,
	}))
	if _, err := NewToolSchemaEntry(incompleteOptions...); err == nil || !strings.Contains(err.Error(), `required target "other" is not assigned`) {
		t.Fatalf("NewToolSchemaEntry incomplete projection error = %v", err)
	}
	arrayTargetSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{
			"items": MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(stringSchema)),
		}),
		ToolSchemaRequired("items"),
		ToolSchemaAdditionalPropertiesAllowed(allow),
	)
	options := append([]ToolSchemaEntryOption(nil), base...)
	options = append(options, WithToolCompiledResult(CompiledResultProjection{
		Fields:       map[string]CompiledResultField{"items[0]": {From: "result.source_id"}},
		OutputSchema: arrayTargetSchema,
	}))
	if _, err := NewToolSchemaEntry(options...); err == nil || !strings.Contains(err.Error(), "cannot construct an array index") {
		t.Fatalf("NewToolSchemaEntry array target error = %v", err)
	}
}

func TestToolCompiledResultAdmissionIsIndependentOfOptionOrder(t *testing.T) {
	t.Parallel()

	stringSchema := MustToolInputSchema(ToolSchemaString)
	sourceSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": stringSchema}),
		ToolSchemaRequired("source_id"),
	)
	targetSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"target_id": stringSchema}),
		ToolSchemaRequired("target_id"),
	)
	entry, err := NewToolSchemaEntry(
		WithToolCompiledResult(CompiledResultProjection{
			Fields:       map[string]CompiledResultField{"target_id": {From: "result.source_id"}},
			OutputSchema: targetSchema,
		}),
		WithToolHandler(ToolHandlerHTTP),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), sourceSchema),
	)
	if err != nil {
		t.Fatalf("NewToolSchemaEntry with compiled result before schemas: %v", err)
	}
	if _, ok := entry.CompiledResult(); !ok {
		t.Fatal("compiled result is missing")
	}
}

func TestToolSchemaEntryWithSchemasRevalidatesCompiledResultSource(t *testing.T) {
	t.Parallel()

	stringSchema := MustToolInputSchema(ToolSchemaString)
	sourceSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": stringSchema}),
		ToolSchemaRequired("source_id"),
	)
	targetSchema := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"target_id": stringSchema}),
		ToolSchemaRequired("target_id"),
	)
	entry := MustToolSchemaEntry(
		WithToolHandler(ToolHandlerHTTP),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://provider.example"}),
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), sourceSchema),
		WithToolCompiledResult(CompiledResultProjection{
			Fields:       map[string]CompiledResultField{"target_id": {From: "result.source_id"}},
			OutputSchema: targetSchema,
		}),
	)
	incompatibleSource := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": MustToolInputSchema(ToolSchemaInteger)}),
		ToolSchemaRequired("source_id"),
	)
	if _, err := entry.WithSchemas(MustToolInputSchema(ToolSchemaObject), incompatibleSource); err == nil || !strings.Contains(err.Error(), "incompatible types") {
		t.Fatalf("WithSchemas incompatible source error = %v", err)
	}
	optionalSource := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"source_id": stringSchema}),
	)
	if _, err := entry.WithSchemas(MustToolInputSchema(ToolSchemaObject), optionalSource); err == nil || !strings.Contains(err.Error(), "not guaranteed") {
		t.Fatalf("WithSchemas optional source error = %v", err)
	}
}

func TestToolSchemaRelationHandlesAnyAndEqualityWithoutDrift(t *testing.T) {
	t.Parallel()

	stringSchema := MustToolInputSchema(ToolSchemaString)
	if err := stringSchema.ValidateAssignableTo("string to any", MustToolInputSchema(ToolSchemaAny)); err != nil {
		t.Fatalf("string -> any: %v", err)
	}
	enumSource := MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("value"))
	if err := enumSource.ValidateAssignableTo("finite any to string", stringSchema); err != nil {
		t.Fatalf("finite any -> string: %v", err)
	}
	sourceProperty := MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("value"))
	targetProperty := sourceProperty
	targetPropertyValue := *targetProperty.value
	targetPropertyValue.equalTo = "peer"
	targetProperty.value = &targetPropertyValue
	sourceObject := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"value": sourceProperty, "peer": stringSchema}),
		ToolSchemaRequired("value", "peer"),
	)
	targetObject := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"value": targetProperty, "peer": stringSchema}),
		ToolSchemaRequired("value", "peer"),
	)
	if err := sourceObject.ValidateAssignableTo("missing equality", targetObject); err == nil {
		t.Fatal("finite enum bypassed the target equality relation")
	}

	openSource := MustToolInputSchema(ToolSchemaObject)
	optionalStringTarget := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"optional": stringSchema}),
	)
	if err := openSource.ValidateAssignableTo("open source", optionalStringTarget); err == nil {
		t.Fatal("unconstrained source additional property bypassed target property schema")
	}
	integerAdditionalSource := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaAdditionalPropertiesSchema(MustToolInputSchema(ToolSchemaInteger)),
	)
	optionalNumberTarget := MustToolInputSchema(
		ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"optional": MustToolInputSchema(ToolSchemaNumber)}),
	)
	if err := integerAdditionalSource.ValidateAssignableTo("typed source additional", optionalNumberTarget); err != nil {
		t.Fatalf("integer additional -> optional number: %v", err)
	}
}
