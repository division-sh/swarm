package pinrouting

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestPrivateOutputConsumerRelationSurvivesOnlyWhileConnectionExists(t *testing.T) {
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	schemaPath := filepath.Join(root, "schema.yaml")
	original, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(original, &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "connect")
	unwired, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		name      string
		raw       []byte
		connected bool
	}{
		{"declared", original, true},
		{"removed", unwired, false},
		{"restored", original, true},
	} {
		t.Run(step.name, func(t *testing.T) {
			if err := os.WriteFile(schemaPath, step.raw, 0600); err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			classification := ClassifyOutputConsumer(source, "producer", "work.ready")
			if classification.Has(OutputConsumerConnect) != step.connected || classification.Has(OutputConsumerSameFlow) {
				t.Fatalf("consumer relation = %#v, want only connect=%v", classification, step.connected)
			}
			envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: "complete-target"})
			result := ResolveEnvelope(ResolutionInput{Source: source, FlowID: "producer", EventType: "work.ready"}, envelope)
			if step.connected && !result.Failure.Empty() {
				t.Fatalf("declared relation rejected: %s", result.Failure.Code())
			}
			if !step.connected && result.Failure != FailureTargetRequiredMissing {
				t.Fatalf("complete address authorized removed relation: %#v", result)
			}
			if !result.Target.Empty() {
				t.Fatalf("consumer admission invented a materialized target: %#v", result.Target)
			}
		})
	}
}

func TestRetiredParentRouteFailureIsNotAdmitted(t *testing.T) {
	if _, err := ParseTargetFailure("parent_route_incomplete"); err == nil {
		t.Fatal("retired parent-address interpreter survived through its failure codec")
	}
}

func requireRootIngressPlans(t *testing.T, plans []ConnectRoutePlan, eventNames ...string) []ConnectRoutePlan {
	t.Helper()
	remaining := make(map[events.EventType]bool, len(eventNames))
	for _, event := range eventNames {
		remaining[events.EventType(event)] = true
	}
	var private []ConnectRoutePlan
	for _, plan := range plans {
		if !plan.source.IsRoot() {
			private = append(private, plan)
			continue
		}
		event := plan.source.event.value
		if !remaining[event] || plan.receiver.flowID.value != "producer" ||
			plan.receiver.event.value != event || plan.source.pinDigest == "" ||
			plan.receiver.pinDigest == "" || plan.resolutionKind != ConnectResolutionStatic {
			t.Fatalf("unexpected or incomplete root ingress plan: %#v", plan.Readback())
		}
		delete(remaining, event)
	}
	if len(remaining) != 0 {
		t.Fatalf("missing root ingress plans: %#v", remaining)
	}
	return private
}

func TestRequiredAgentRoleDoesNotEstablishOutputConsumer(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			RequiredAgents: []runtimecontracts.FlowRequiredAgent{{Role: "worker", SubscribesTo: []string{"work.ready"}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}},
	}
	classification := ClassifyOutputConsumer(semanticview.Wrap(bundle), ".", "work.ready")
	if classification.HasRuntimeConsumer() || classification.Has(OutputConsumerRootExport) {
		t.Fatalf("unfulfilled role became delivery authority: %#v", classification)
	}
}

func TestConcreteInstanceConsumerUsesExactAdmittedIdentity(t *testing.T) {
	for _, declared := range []bool{true, false} {
		source := testPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil)
		bundle, _ := semanticview.Bundle(source)
		child := &bundle.FlowTree.Root.Children[0]
		child.Events = map[string]runtimecontracts.EventCatalogEntry{"child.done": {}}
		if declared {
			child.Nodes = map[string]runtimecontracts.SystemNodeContract{"finish": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"child.done": {}},
			}}
		}
		bundle.FlowTree.ByID["child"], bundle.FlowTree.ByPath["child"] = child, child
		if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
			t.Fatal(err)
		}
		source = semanticview.Wrap(bundle)
		for _, tc := range []struct {
			name, event, owner, instance string
			want                         bool
		}{
			{"matching", "child/one/child.done", "child", "child/one", declared},
			{"other instance", "child/two/child.done", "child", "child/one", false},
			{"other owner", "child/one/child.done", "other", "child/one", false},
		} {
			classification := ClassifyRoutingSourceOutputConsumer(source, tc.event, mustConcreteRoutingSource(t, tc.owner, tc.instance))
			if got := classification.Has(OutputConsumerSameFlow); got != tc.want {
				t.Fatalf("%s declared=%v: consumer=%v, want %v", tc.name, declared, got, tc.want)
			}
		}
	}
}
