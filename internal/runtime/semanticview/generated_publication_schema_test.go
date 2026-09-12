package semanticview

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPublicationSubscriptionsConsumeRetainedBindings(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := map[string]string{"root": ".", "static": "source", "template": "source", "nested_template": "outer/source"}[mode]
			source := Wrap(bundle)
			for _, view := range bundle.FlowTree.ByID {
				view.Events, view.Nodes = nil, nil
			}
			bundle.Events, bundle.Nodes = nil, nil
			for _, kind := range []AuthoredSubscriptionConsumerKind{AuthoredSubscriptionConsumerNode, AuthoredSubscriptionConsumerAgent, AuthoredSubscriptionConsumerTimer} {
				for _, receiver := range []string{flow, "sink"} {
					for _, local := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
						req := AuthoredSubscriptionRequest{ConsumerKind: kind, FlowID: receiver, Authored: local}
						admitted := ClassifyAuthoredSubscription(source, req)
						if !admitted.Admitted() {
							t.Fatalf("%s/%s/%s lost retained declaration: %s", receiver, kind, local, admitted.Message())
						}
						if got, ok := admitted.LocalEventAt("selected/instance", "selected/instance/"+local); !ok || got != local {
							t.Fatalf("retained subscription lost execution projection: %q %t", got, ok)
						}
						req.LocalEvents = map[string]struct{}{"invented.event": {}}
						req.InputEvents = []string{"invented.event"}
						if got := ClassifyAuthoredSubscription(source, req); !reflect.DeepEqual(got.RoutePatterns(), admitted.RoutePatterns()) {
							t.Fatal("caller maps changed existing compiled binding")
						}
						req.Authored = "invented.event"
						if got := ClassifyAuthoredSubscription(source, req); got.Admitted() {
							t.Fatalf("%s/%s admitted forged local/input evidence", receiver, kind)
						}
						req.Authored, req.FlowID = local, "absent"
						req.LocalEvents[local] = struct{}{}
						if got := ClassifyAuthoredSubscription(source, req); got.Admitted() {
							t.Fatal("absent scope borrowed a generated declaration")
						}
					}
				}
			}
		})
	}
}

func TestGeneratedPublicationStructuralSchemaPreservesExactOwner(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := "source"
			if mode == "root" {
				flow = "."
			} else if mode == "nested_template" {
				flow = "outer/source"
			}
			source := Wrap(bundle)
			for _, local := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
				qualified := local
				if flow != "." {
					qualified = flow + "/" + local
				}
				t.Run("absent_flow/"+local, func(t *testing.T) {
					resolution := ResolveEventSchema(source, "absent-flow", qualified)
					if resolution.HasSchema || resolution.HasCompiled || resolution.HasStructural {
						t.Fatalf("absent flow borrowed generated schema: key=%q compiled_flow=%q class=%q", resolution.EventKey, resolution.CompiledSchema.FlowPath(), resolution.Classification)
					}
				})
				for _, receiver := range []string{flow, "sink"} {
					t.Run(receiver+"/"+local, func(t *testing.T) {
						resolution := ResolveEventSchema(source, receiver, local)
						field, ok := resolution.Field("activity_id")
						if !ok || field.Type.Kind != "text" || field.IsOptional || resolution.Classification != contracts.CompiledEventSchemaGenerated || resolution.CompiledSchema.FlowPath() != flow || resolution.CompiledSchema.EventName() != qualified {
							t.Fatalf("structural schema borrowed another owner: found=%t kind=%q class=%q compiled_flow=%q compiled_event=%q", ok, field.Type.Kind, resolution.Classification, resolution.CompiledSchema.FlowPath(), resolution.CompiledSchema.EventName())
						}
						properties := resolution.Schema.Schema["properties"].(map[string]any)
						if properties["activity_id"].(map[string]any)["type"] != "string" {
							t.Fatal("structural text did not preserve JSON string semantics")
						}
					})
				}
			}
		})
	}
}
