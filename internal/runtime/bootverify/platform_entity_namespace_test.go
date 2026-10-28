package bootverify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestWave1EntityResolverSplitsBusinessEntityAndPlatformEntity(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"current_state": {Type: "text"},
					"name":          {Type: "text"},
				},
			},
		},
	})

	if got, _, err := wave1ResolveEntityPathWithOwner(source, "", "current_state"); err != nil {
		t.Fatalf("state.current_state declared business field failed to resolve: %v", err)
	} else if got.Kind != "scalar" || got.Type != "text" {
		t.Fatalf("state.current_state = %#v, want scalar text", got)
	}

	for _, ref := range []string{"id", "stage", "path"} {
		got, err := wave1ResolvePlatformEntityPath(ref)
		if err != nil {
			t.Fatalf("instance.%s failed to resolve: %v", ref, err)
		}
		if got.Kind == "" || got.Type == "" {
			t.Fatalf("instance.%s resolved to empty type %#v", ref, got)
		}
	}
}

func TestWave1EntityResolverRejectsLegacyAndUnsupportedPlatformEntityFields(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"name": {Type: "text"},
				},
			},
		},
	})

	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "current_state"); err == nil {
		t.Fatal("expected undeclared state.current_state platform metadata read to fail")
	} else if !strings.Contains(err.Error(), "use instance.stage") {
		t.Fatalf("state.current_state error = %q, want instance.stage guidance", err)
	}
	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "entity_id"); err == nil {
		t.Fatal("expected undeclared state.entity_id platform metadata read to fail")
	} else if !strings.Contains(err.Error(), "use instance.id") {
		t.Fatalf("state.entity_id error = %q, want instance.id guidance", err)
	}

	for _, ref := range []string{"entity_id", "current_state", "flow_instance", "state.reviewed", "revision", "created_at", "updated_at", "workflow_name", "name"} {
		if _, err := wave1ResolvePlatformEntityPath(ref); err == nil {
			t.Fatalf("expected instance.%s to be unsupported", ref)
		}
	}
}

func TestWave1EntityResolverRejectsRetiredFanOutCountRead(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"fan_out_count": {Type: "integer"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "fan_out_count"); err != nil {
		t.Fatalf("declared business field refused: %v", err)
	}
	delete(bundle.RootEntities["state"].Fields, "fan_out_count")

	_, _, err := wave1ResolveEntityPathWithOwner(source, "", "fan_out_count")
	if err == nil || !strings.Contains(err.Error(), "state.fan_out_count is platform bookkeeping; use the handler-local fan_out.count") {
		t.Fatalf("state.fan_out_count error = %v, want handler-local fan_out.count teaching error", err)
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
								StoreAs:   "state.summary",
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
	for _, selector := range []string{"select_entity", "select_or_create_entity"} {
		for _, template := range []bool{false, true} {
			for _, selectedRoot := range []bool{false, true} {
				for _, producer := range []bool{false, true} {
					for _, renamed := range []bool{false, true} {
						name := fmt.Sprintf("selector-%s/template-%t/selected-root-%t/producer-%t/renamed-%t", selector, template, selectedRoot, producer, renamed)
						t.Run(name, func(t *testing.T) {
							root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
							if !producer {
								writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: selector-retirement\n")
							}
							for _, file := range []string{"schema.yaml", "account/schema.yaml", "account/nodes.yaml"} {
								path := filepath.Join(root, file)
								data, err := os.ReadFile(path)
								if err != nil {
									t.Fatal(err)
								}
								text := string(data)
								if file == "account/nodes.yaml" {
									text = strings.Replace(text, "    account.ready: {}", "    account.ready:\n      "+selector+":\n        by: {account_id: payload.account_id}", 1)
								}
								if !template && file == "account/schema.yaml" {
									text = strings.Replace(text, "instance: account_id\n", "", 1)
								}
								if renamed {
									text = strings.ReplaceAll(text, "account.ready", "account.renamed")
								}
								writeBootverifyFixtureFile(t, path, text)
							}
							if selectedRoot {
								root = filepath.Join(root, "account")
							}
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
          vertical_id: instance.id
`)
			assertRetiredReceiverSelectorRejected(t, root)
		})
	}
}
