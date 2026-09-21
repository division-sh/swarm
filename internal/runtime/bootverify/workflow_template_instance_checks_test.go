package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunValidatesScalarTemplateInstanceIdentity(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
mode: template
instance: account_id
`, `
account:
  tenant_id: text
  account_id: uuid
`)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
}

func TestRun_RejectsRootTemplateInstanceDeclaration(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Name:     "root",
			Instance: mustBootverifyTemplateInstanceField(t, "account_id"),
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "template_instance_validation", "root schema must not declare instance") {
		t.Fatalf("expected root template_instance_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsInvalidTemplateInstanceDeclarations(t *testing.T) {

	tests := []struct {
		name           string
		flowSchema     string
		flowEntities   string
		want           string
		admissionError string
	}{
		{
			name: "missing instance declaration",
			flowSchema: `
name: scoring
mode: template
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "instance: <field>",
		},
		{
			name: "undeclared key field",
			flowSchema: `
name: scoring
mode: template
instance: missing_id
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want:           "not declared",
			admissionError: "compile workflow semantics: compile flow scoring input pins: receiver instance field missing_id has no declaration",
		},
		{
			name: "unsupported key field type",
			flowSchema: `
name: scoring
mode: template
instance: tags
`,
			flowEntities: `
account:
  tags: [text]
`,
			want: "scalar or enum",
		},
		{
			name: "non template declares instance",
			flowSchema: `
name: scoring
mode: static
instance: account_id
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "not mode: template",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := admitPrimaryEntityFixtureBundle(t, tc.flowSchema, tc.flowEntities)
			if tc.admissionError != "" {
				if err == nil || err.Error() != tc.admissionError {
					t.Fatalf("admission error = %v, want %q", err, tc.admissionError)
				}
				// Admission rejects this authoring first. Corrupt a valid in-memory
				// projection separately to retain the verifier's original oracle.
				bundle = loadPrimaryEntityFixtureBundle(t, strings.Replace(tc.flowSchema, "instance: missing_id", "instance: account_id", 1), tc.flowEntities)
				schema := bundle.FlowSchemas["scoring"]
				schema.Instance = mustBootverifyTemplateInstanceField(t, "missing_id")
				bundle.FlowSchemas["scoring"] = schema
			} else if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "template_instance_validation", tc.want) {
				t.Fatalf("expected template_instance_validation containing %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func mustBootverifyTemplateInstanceField(t testing.TB, raw string) runtimecontracts.TemplateInstanceField {
	t.Helper()
	field, err := runtimecontracts.ParseTemplateInstanceField(raw)
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField(%q): %v", raw, err)
	}
	return field
}
