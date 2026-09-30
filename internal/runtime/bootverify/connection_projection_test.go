package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestConnectionMixedProjectionBoot(t *testing.T) {
	for _, intrinsic := range []string{"payload", "generated.uuid", "event.id"} {
		for _, reverse := range []bool{false, true} {
			t.Run(intrinsic+map[bool]string{true: "/reverse", false: "/forward"}[reverse], func(t *testing.T) {
				root := canonicalrouting.CopyConnectionPolicies(t, reverse)
				if intrinsic != "payload" {
					root = canonicalrouting.CopyMixedConnectionProjections(t, reverse, intrinsic)
				}
				repo := repoRootForBootverifyTest(t)
				b, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				base := semanticview.Wrap(b)
				composed, err := semanticview.CompileActivityToolBindings(base)
				if err != nil {
					t.Fatal(err)
				}
				for _, source := range []semanticview.Source{base, composed} {
					requireMixedProjectionBoot(t, source, intrinsic)
				}
			})
		}
	}
}

func requireMixedProjectionBoot(t *testing.T, source semanticview.Source, intrinsic string) {
	t.Helper()
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Plans()) != 2 || len(graph.Issues()) != 0 {
		t.Fatalf("plans=%d issues=%v", len(graph.Plans()), graph.Issues())
	}
	common, found, err := source.ResolveEffectiveCompiledFlowEventSchema("worker", "worker/inst-1/work.ready")
	if err != nil || !found {
		t.Fatalf("valid edge-local schema choices collapsed: %t %v", found, err)
	}
	if _, ok := common.StructuralField("worker_id"); ok {
		t.Fatal("one edge's synthetic key became a receiver-wide guarantee")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("missing admitted bundle")
	}
	for _, connect := range bundle.CompositionConnects() {
		input, found, err := source.ConnectionInputs().Input(connect)
		if err != nil || !found {
			t.Fatalf("edge evidence missing: %t %v", found, err)
		}
		schema, ok := input.ReceiverEventSchema()
		_, projected := schema.StructuralField("worker_id")
		wantProjection := intrinsic != "payload" && input.Mode() == contracts.FlowInputResolutionModeCreate
		if !ok || projected != wantProjection {
			t.Fatalf("edge projection=%t, want %t for %s", projected, wantProjection, connect.KeyFrom)
		}
	}
	if report := Run(context.Background(), source, Options{}); len(report.Errors()) != 0 {
		t.Fatalf("boot errors: %+v", report.Errors())
	}
}

func TestConnectionCommonGuaranteesRespectEveryArrival(t *testing.T) {
	for _, allProjected := range []bool{false, true} {
		t.Run(map[bool]string{false: "mixed-refuses-key", true: "all-projected-admits-key"}[allProjected], func(t *testing.T) {
			root := canonicalrouting.CopyMixedConnectionProjections(t, false, "generated.uuid")
			raw, err := os.ReadFile(filepath.Join(root, "worker/nodes.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			writeBootverifyFixtureFile(t, filepath.Join(root, "worker/nodes.yaml"), strings.ReplaceAll(string(raw), "payload.creation_id", "payload.worker_id"))
			if allProjected {
				raw, err = os.ReadFile(filepath.Join(root, "schema.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), strings.ReplaceAll(string(raw), "resolution: select-or-create, key_from: payload.reuse_id", "resolution: create, key_from: event.id"))
			}
			repo := repoRootForBootverifyTest(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if allProjected {
				if len(report.Errors()) != 0 {
					t.Fatalf("all-edge guarantee refused: %+v", report.Errors())
				}
			} else {
				found := false
				for _, problem := range report.Errors() {
					if problem.CheckID == "condition_expression_validation" && strings.Contains(problem.Message, "worker_id") {
						found = true
					}
				}
				if !found {
					t.Fatalf("non-guaranteed projection became handler authority: %+v", report.Errors())
				}
			}
		})
	}
}

func TestConnectionStaticInitializationRequiresCreatingAuthority(t *testing.T) {
	for _, connected := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-edge", true: "static-edge"}[connected], func(t *testing.T) {
			root := canonicalrouting.CopyNonCreatingInitialization(t, connected)
			repo := repoRootForBootverifyTest(t)
			_, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), "initialize requires a creating connection") {
				t.Fatalf("non-creating initialization admission = %v", err)
			}
		})
	}
}

func TestConnectionIntrinsicConcreteEventProof(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	b, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "examples/routing/template-create-minted-key"), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(b)
	for _, event := range []string{"validation.requested", "validator/validation.requested", "validator/inst-1/validation.requested"} {
		proof := semanticview.ResolveFlowEventProof(source, "validator", event)
		t.Logf("event=%s local=%s schema=%t", event, proof.Local, proof.HasSchema)
		if proof.Local != "validation.requested" || !proof.HasSchema {
			t.Errorf("intrinsic receiver occurrence lost exact local event: %+v", proof)
		}
	}
	route, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/inst-1", EntityID: "entity"})
	if err != nil {
		t.Fatal(err)
	}
	got := pinrouting.NewOutputConsumerResolver(source).Classify("validator/inst-1/validation.requested", route)
	if !got.Has(pinrouting.OutputConsumerSameFlow) {
		t.Error("concrete intrinsic receiver lost its actual same-flow consumer")
	}
}

func TestConnectionRenamedConcreteEventAndNodeProof(t *testing.T) {
	for _, intrinsic := range []string{"generated.uuid", "event.id"} {
		for _, rename := range []bool{false, true} {
			t.Run(intrinsic+map[bool]string{false: "/original", true: "/renamed"}[rename], func(t *testing.T) {
				root := canonicalrouting.CopyMixedConnectionProjections(t, false, intrinsic)
				local := "work.ready"
				if rename {
					local = "work.accepted"
					for _, name := range []string{"worker/schema.yaml", "worker/nodes.yaml"} {
						raw, err := os.ReadFile(filepath.Join(root, name))
						if err != nil {
							t.Fatal(err)
						}
						writeBootverifyFixtureFile(t, filepath.Join(root, name), strings.ReplaceAll(string(raw), "work.ready", local))
					}
					raw, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), strings.ReplaceAll(string(raw), "to: worker,", "to: worker, rename: "+local+","))
				}
				writeBootverifyFixtureFile(t, filepath.Join(root, "worker/child/schema.yaml"), "name: child\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "worker/child/events.yaml"), local+":\n  child_only: text\n")
				repo := repoRootForBootverifyTest(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				base := semanticview.Wrap(bundle)
				composed, err := semanticview.CompileActivityToolBindings(base)
				if err != nil {
					t.Fatal(err)
				}
				for _, source := range []semanticview.Source{base, composed} {
					node := identitytest.FlowNode(t, "worker", "worker")
					for _, event := range []string{local, "worker/" + local, "worker/inst-1/" + local} {
						for _, proof := range []semanticview.FlowEventProof{semanticview.ResolveFlowEventProof(source, "worker", event), semanticview.ResolveExecutableNodeEventProof(source, node, event)} {
							if proof.Local != local || !proof.HasSchema {
								t.Fatalf("exact receiver proof lost: %+v", proof)
							}
							if strings.HasPrefix(event, "worker/inst-1/") && proof.Canonical != event {
								t.Fatalf("concrete identity changed: %+v", proof)
							}
							if proof.CatalogKey != "work.ready" {
								t.Fatalf("producer declaration changed: %+v", proof)
							}
						}
					}
					route, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/inst-1", EntityID: "entity"})
					if err != nil {
						t.Fatal(err)
					}
					if !pinrouting.NewOutputConsumerResolver(source).Classify("worker/inst-1/"+local, route).Has(pinrouting.OutputConsumerSameFlow) {
						t.Fatal("actual same-flow consumer missing")
					}
					if event, owned := source.ConnectionInputs().ReceiverEvent("worker", "worker/child/"+local); owned {
						t.Fatalf("descendant borrowed receiver %s", event)
					}
					if proof := semanticview.ResolveFlowEventProof(source, "worker", "worker/child/"+local); proof.Local == local {
						t.Fatalf("descendant localized into parent: %+v", proof)
					}
					if event, owned := source.ConnectionInputs().ReceiverEvent("worker", "foreign/inst-1/"+local); owned {
						t.Fatalf("foreign scope borrowed receiver %s", event)
					}
				}
			})
		}
	}
}
