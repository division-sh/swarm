package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunValidatesScalarTemplateInstanceIdentity(t *testing.T) {
	bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
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
		name         string
		flowSchema   string
		flowEntities string
		want         string
	}{
		{
			name: "undeclared key field",
			flowSchema: `
name: scoring
instance: missing_id
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "not declared",
		},
		{
			name: "unsupported key field type",
			flowSchema: `
name: scoring
instance: tags
`,
			flowEntities: `
account:
  tags: [text]
`,
			want: "scalar or enum",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := admitPrimaryEntityFixtureBundle(t, tc.flowSchema, tc.flowEntities)
			if err != nil {
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
