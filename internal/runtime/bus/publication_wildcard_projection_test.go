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

func TestPublicationWildcardProjectionPreservesDetachedBindings(t *testing.T) {
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
			flowID, ids := "source", []string{""}
			if mode == "root" {
				flowID = "."
			} else if mode == "template" {
				ids = []string{"one", "two"}
			}
			var instances []flowidentity.Instance
			for _, id := range ids {
				instances = append(instances, runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, id, eventBusTestRunID))
			}
			for repeat := 0; repeat < 3; repeat++ {
				for _, instance := range instances {
					key := instance.InstancePath + "/result.direct"
					got := routes.PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, instance, key)
					if len(got) != 2 {
						t.Fatalf("%s: subscribers=%+v, want exact and wildcard; connections are not pubsub aliases", key, got)
					}
					wildcards := 0
					for i, subscriber := range got {
						if subscriber.Recipient.LocalID() != "wildcard" {
							continue
						}
						wildcards++
						if subscriber.LocalizedEvent != "result.direct" || subscriber.Path != instance.InstancePath {
							t.Fatalf("wrong receiver projection: %+v", subscriber)
						}
						node, ok := subscriber.Recipient.Node()
						if !ok {
							t.Fatal("wildcard lost executable node identity")
						}
						resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, subscriber.LocalizedEvent)
						if !resolved.Matched || resolved.HandlerEventKey != "result.*" {
							t.Fatalf("matched binding has no exact handler: %+v", resolved)
						}
						got[i].LocalizedEvent, got[i].Path = "poisoned.binding", "foreign"
					}
					if wildcards != 1 {
						t.Fatalf("wildcard bindings=%d", wildcards)
					}
					if got, err := routes.PubsubReceiverDefinitions(eventtest.UUID("foreign-run"), instance, []string{key}); err == nil || len(got) != 0 {
						t.Fatalf("another run acquired this receiver: bindings=%+v err=%v", got, err)
					}
					for _, hostile := range []string{"foreign/result.direct", "source/three/result.direct", key + "/extra", instance.InstancePath + "/result.unknown"} {
						if got := routes.PubsubReceiverDefinitionsFixture(t, eventBusTestRunID, instance, hostile); len(got) != 0 {
							t.Fatalf("hostile %s acquired bindings %+v", hostile, got)
						}
					}
				}
			}
		})
	}
}
