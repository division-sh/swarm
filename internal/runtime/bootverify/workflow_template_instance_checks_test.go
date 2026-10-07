package bootverify

import (
	"context"
	"path/filepath"
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

func TestA9RootConstructorAdmissionUsesSharedOwners(t *testing.T) {
	for _, test := range []struct {
		name, key, eventSchema, wantCheck string
	}{
		{"constructible root", "account_id", "account_id: text\n  region: text", ""},
		{"unknown root key", "missing_id", "account_id: text\n  region: text", "template_instance_validation"},
		{"ineligible root input", "account_id", "note: text", "flow_constructor_validation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, repo := t.TempDir(), repoRootForBootverifyTest(t)
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root\ninstance: "+test.key+"\npins:\n  inputs: [account.opened]\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "account.opened:\n  "+test.eventSchema+"\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "account:\n  account_id: text\n  region: text\n  count: {type: integer, initial: 0}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "receiver:\n  execution_type: system_node\n  subscribes_to: [account.opened]\n  event_handlers:\n    account.opened:\n      data_accumulation:\n        source_event: account.opened\n        writes:\n          - {target_field: count, value: size(entity.region)}\n")
			bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if test.wantCheck != "" {
				if !reportContains(report.Errors(), test.wantCheck, "") {
					t.Fatalf("missing %s refusal: %+v", test.wantCheck, report.Errors())
				}
				return
			}
			if report.HasErrors() {
				t.Fatalf("valid standalone keyed root was refused: %+v", report.Errors())
			}
		})
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
