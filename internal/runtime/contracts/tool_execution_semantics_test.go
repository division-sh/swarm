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
