package bus_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPublicationWildcardProjectionPreservesExactCachedRoute(t *testing.T) {
	for _, mode := range []string{"root", "static", "template"} {
		t.Run(mode, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
				canonicalrouting.CopyPublicationDirectTextSite(t, mode), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			routes, err := runtimebus.DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			paths := []string{"source"}
			if mode == "root" {
				paths = []string{""}
			}
			if mode == "template" {
				paths = []string{"source/one", "source/two"}
				for _, instance := range []string{"one", "two"} {
					if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
						Identity: testRunScopedFlowRoute(flowidentity.DeriveRoute("source", instance)),
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for repeat := 0; repeat < 3; repeat++ {
				for _, path := range paths {
					name := "result.direct"
					if path != "" {
						name = path + "/" + name
					}
					got := routes.ResolveForRun(eventBusTestRunID, name)
					if len(got) != 2 {
						t.Fatalf("%s: subscribers=%+v, want exact and wildcard; connects are not pubsub aliases", name, got)
					}
					wildcards := 0
					for i, subscriber := range got {
						if subscriber.Recipient.LocalID() != "wildcard" {
							continue
						}
						wildcards++
						if subscriber.LocalizedEvent != "result.direct" || (path != "" && subscriber.Path != path) {
							t.Fatalf("wrong matched receiver projection: %+v", subscriber)
						}
						node, ok := subscriber.Recipient.Node()
						if !ok {
							t.Fatal("wildcard lost executable node identity")
						}
						resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, subscriber.LocalizedEvent)
						if !resolved.Matched || resolved.HandlerEventKey != "result.*" {
							t.Fatalf("matched route has no exact handler: %+v", resolved)
						}
						got[i].LocalizedEvent = "poisoned.cache"
					}
					if wildcards != 1 {
						t.Fatalf("wildcard routes=%d", wildcards)
					}
					if mode == "template" && len(routes.ResolveForRun(eventtest.UUID("foreign-run"), name)) != 0 {
						t.Fatal("another run acquired a concrete template subscriber")
					}
				}
			}
			for _, hostile := range []string{"foreign/result.direct", "source/three/result.direct", "source/one/result.direct/extra"} {
				if got := routes.ResolveForRun(eventBusTestRunID, hostile); len(got) != 0 {
					t.Fatalf("hostile %s acquired routes %+v", hostile, got)
				}
			}
			if mode == "template" {
				if err := routes.RemoveFlowInstanceRoute(testRunScopedFlowRoute(flowidentity.DeriveRoute("source", "one"))); err != nil {
					t.Fatal(err)
				}
				if got := routes.ResolveForRun(eventBusTestRunID, "source/one/result.direct"); len(got) != 0 {
					t.Fatalf("removed instance kept cached routes: %+v", got)
				}
				if got := routes.ResolveForRun(eventBusTestRunID, "source/two/result.direct"); len(got) != 2 {
					t.Fatalf("removing first instance damaged second: %+v", got)
				}
			}
		})
	}
}
