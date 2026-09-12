package bootverify

import (
	"context"
	"fmt"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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

func TestWave1EntityResolverRejectsRetiredFanOutCountRead(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"record": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"fan_out_count": {Type: "integer"},
				},
			},
		},
	})

	_, _, err := wave1ResolveEntityPathWithOwner(source, "", "fan_out_count")
	if err == nil || !strings.Contains(err.Error(), "entity.fan_out_count is platform bookkeeping; use the handler-local fan_out.count") {
		t.Fatalf("entity.fan_out_count error = %v, want handler-local fan_out.count teaching error", err)
	}
}

func TestEntityContractDiagnosticsUseAuthorFacingVocabulary(t *testing.T) {
	t.Run("missing contract path", func(t *testing.T) {
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
		_, _, err := wave1ResolveEntityPathWithOwner(source, "", "summary")
		if err == nil || !strings.Contains(err.Error(), "no declared entity contract") {
			t.Fatalf("error = %v, want declared entity contract guidance", err)
		}
		if strings.Contains(err.Error(), "Wave 1") {
			t.Fatalf("error leaks internal rollout vocabulary: %v", err)
		}
	})

	t.Run("write coverage finding", func(t *testing.T) {
		source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				"node-1": {
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						"item.received": {
							Compute: &runtimecontracts.ComputeSpec{
								Operation: runtimecontracts.ComputeOpCount,
								StoreAs:   "entity.summary",
							},
						},
					},
				},
			},
		})
		report := Run(context.Background(), source, Options{})
		for _, finding := range report.Errors() {
			if finding.CheckID != "entity_write_target_compliance" || !strings.Contains(finding.Message, "missing from the declared entity contract") {
				continue
			}
			if strings.Contains(finding.Message, "Wave 1") {
				t.Fatalf("finding leaks internal rollout vocabulary: %#v", finding)
			}
			return
		}
		t.Fatalf("missing author-facing entity_write_target_compliance finding: %#v", report.Errors())
	})
}

func TestRetiredReceiverSelectorsRejectEveryIngressShape(t *testing.T) {
	for _, selector := range []canonicalrouting.SelectEntityAcquisition{canonicalrouting.SelectEntityAcquire, canonicalrouting.SelectOrCreateEntityAcquire} {
		for _, template := range []bool{false, true} {
			for _, external := range []bool{false, true} {
				for _, producer := range []bool{false, true} {
					for _, renamed := range []bool{false, true} {
						name := fmt.Sprintf("selector-%d/template-%t/external-%t/producer-%t/renamed-%t", selector, template, external, producer, renamed)
						t.Run(name, func(t *testing.T) {
							root := canonicalrouting.CopySelectEntityDemotion(t, canonicalrouting.SelectEntityDemotionOptions{TemplateReceiver: template, Acquisition: selector, External: external, WithProducer: producer, RenameReceiverPin: renamed})
							assertRetiredReceiverSelectorRejected(t, root)
						})
					}
				}
			}
		}
	}
}

func TestRun_RejectsSelectEntityByPlatformEntitySourceAuthority(t *testing.T) {
	for _, acquisition := range []string{"select_entity", "select_or_create_entity"} {
		t.Run(acquisition, func(t *testing.T) {
			root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      `+acquisition+`:
        by:
          vertical_id: _entity.id
`)
			assertRetiredReceiverSelectorRejected(t, root)
		})
	}
}
