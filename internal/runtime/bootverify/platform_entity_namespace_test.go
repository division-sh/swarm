package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWave1EntityResolverSplitsBusinessEntityAndPlatformEntity(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"record": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"current_state": {Type: "text"},
					"name":          {Type: "text"},
				},
			},
		},
	})

	if got, _, err := wave1ResolveEntityPathWithOwner(source, "", "current_state"); err != nil {
		t.Fatalf("entity.current_state declared business field failed to resolve: %v", err)
	} else if got.Kind != "scalar" || got.Type != "text" {
		t.Fatalf("entity.current_state = %#v, want scalar text", got)
	}

	for _, ref := range []string{"id", "current_state", "flow_instance", "gates.reviewed"} {
		got, err := wave1ResolvePlatformEntityPath(ref)
		if err != nil {
			t.Fatalf("_entity.%s failed to resolve: %v", ref, err)
		}
		if got.Kind == "" || got.Type == "" {
			t.Fatalf("_entity.%s resolved to empty type %#v", ref, got)
		}
	}
}

func TestWave1EntityResolverRejectsLegacyAndUnsupportedPlatformEntityFields(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"record": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"name": {Type: "text"},
				},
			},
		},
	})

	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "current_state"); err == nil {
		t.Fatal("expected undeclared entity.current_state platform metadata read to fail")
	} else if !strings.Contains(err.Error(), "use _entity.current_state") {
		t.Fatalf("entity.current_state error = %q, want _entity.current_state guidance", err)
	}
	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "entity_id"); err == nil {
		t.Fatal("expected undeclared entity.entity_id platform metadata read to fail")
	} else if !strings.Contains(err.Error(), "use _entity.id") {
		t.Fatalf("entity.entity_id error = %q, want _entity.id guidance", err)
	}

	for _, ref := range []string{"entity_id", "revision", "created_at", "updated_at", "workflow_name", "name"} {
		if _, err := wave1ResolvePlatformEntityPath(ref); err == nil {
			t.Fatalf("expected _entity.%s to be unsupported", ref)
		}
	}
}

func TestRun_RejectsSelectEntityByPlatformEntitySourceAuthority(t *testing.T) {
	for _, acquisition := range []string{"select_entity", "select_or_create_entity"} {
		t.Run(acquisition, func(t *testing.T) {
			root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      `+acquisition+`:
        by:
          vertical_id: _entity.id
`)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "select_entity_validation", "must resolve from payload.*") ||
				!reportContains(report.Errors(), "select_entity_validation", "_entity.id") {
				t.Fatalf("expected %s to reject _entity source authority, got %#v", acquisition, report.Errors())
			}
		})
	}
}
