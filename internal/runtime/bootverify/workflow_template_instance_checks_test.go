package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_ValidatesTemplateInstanceSingleAndCompositeKeys(t *testing.T) {
	tests := []struct {
		name       string
		instanceBy string
	}{
		{
			name:       "single key",
			instanceBy: "account_id",
		},
		{
			name:       "composite key",
			instanceBy: "[tenant_id, account_id]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadPrimaryEntityFixtureBundle(t, `
name: scoring
mode: template
instance:
  by: `+tc.instanceBy+`
pins:
  inputs:
    events: []
  outputs:
    events: []
`, `
account:
  tenant_id: text
  account_id: uuid
`)

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), "template_instance_validation", "") {
				t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
			}
		})
	}
}

func TestRun_RejectsRootTemplateInstanceDeclaration(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Name: "root",
			Instance: runtimecontracts.FlowTemplateInstanceDeclaration{
				By:         []string{"account_id"},
				OnMissing:  "create",
				OnConflict: "reject",
			},
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
			name: "missing instance declaration",
			flowSchema: `
name: scoring
mode: template
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "must declare instance.by",
		},
		{
			name: "duplicate composite key field",
			flowSchema: `
name: scoring
mode: template
instance:
  by: [account_id, account_id]
  on_missing: create
  on_conflict: reject
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "duplicated",
		},
		{
			name: "undeclared key field",
			flowSchema: `
name: scoring
mode: template
instance:
  by: missing_id
  on_missing: create
  on_conflict: reject
pins:
  inputs:
    events: []
  outputs:
    events: []
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
mode: template
instance:
  by: tags
  on_missing: create
  on_conflict: reject
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  tags: [text]
`,
			want: "scalar or enum",
		},
		{
			name: "invalid on_missing policy",
			flowSchema: `
name: scoring
mode: template
instance:
  by: account_id
  on_missing: ignore
  on_conflict: reject
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "on_missing",
		},
		{
			name: "explicit empty on_conflict policy",
			flowSchema: `
name: scoring
mode: template
instance:
  by: account_id
  on_missing: create
  on_conflict: ""
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "on_conflict",
		},
		{
			name: "non template declares instance",
			flowSchema: `
name: scoring
mode: static
instance:
  by: account_id
  on_missing: create
  on_conflict: reject
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: `
account:
  account_id: uuid
`,
			want: "not mode: template",
		},
		{
			name: "non template declares empty instance",
			flowSchema: `
name: scoring
mode: static
instance: {}
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
			flowEntities: "",
			want:         "not mode: template",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadPrimaryEntityFixtureBundle(t, tc.flowSchema, tc.flowEntities)

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "template_instance_validation", tc.want) {
				t.Fatalf("expected template_instance_validation containing %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}
