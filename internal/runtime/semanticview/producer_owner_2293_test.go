package semanticview

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestExecutableNodeProducerOwnerMatchesGuardEmitSites(t *testing.T) {
	source := loadAuthoredEmitSiteFixture(t, authoredEmitSiteFixture{
		rootNodeID: "root-node", rootEmit: "root.ready", rootGuardEmit: "root.escalated", rootGuardObject: true,
	})
	var matched bool
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			t.Fatal(err)
		}
		if node.FlowPath() != "." || node.NodeID() != "root-node" {
			continue
		}
		matched = true
		bundle, ok := Bundle(source)
		if !ok {
			t.Fatal("loaded producer proof requires its admitted bundle")
		}
		want := []string{"root.escalated", "root.ready"}
		if got := runtimecontracts.EffectiveSystemNodeProduces(bundle, node, record.Entry); !reflect.DeepEqual(got, want) {
			t.Fatalf("contract producer owner = %v, want %v", got, want)
		}
		if got := source.ExecutableNodeEffectiveProduces(node); !reflect.DeepEqual(got, want) {
			t.Fatalf("source producer projection = %v, want %v", got, want)
		}
		census := BuildAuthoredEventEndpointCensus(source)
		for _, event := range want {
			if got := endpointCount(census.Producers(), EventEndpointNodeHandler, ".", event); got != 1 {
				t.Fatalf("%s producer census count = %d, want 1", event, got)
			}
		}
		if got := len(authoredEmitSitesByFlowNodeEvent(AuthoredEmitSites(source), ".", "root-node", "root.escalated")); got != 1 {
			t.Fatalf("canonical guard emit projection duplicated or lost the site: %d", got)
		}
	}
	if !matched {
		t.Fatal("loaded producer proof did not expose its root node")
	}
}
