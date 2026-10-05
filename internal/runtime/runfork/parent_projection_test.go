package runfork

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestProjectParentRoutePreservesExactConstruction(t *testing.T) {
	root := flowidentity.ParentRoute{FlowID: ".", FlowInstance: "source-run", EntityID: "source-run"}
	nested := flowidentity.ParentRoute{FlowID: "templ/child", FlowInstance: "templ/one/child", EntityID: "child-entity"}
	for _, parent := range []flowidentity.ParentRoute{{}, root, nested} {
		got, err := ProjectParentRoute("source-run", "fork-run", parent)
		if err != nil {
			t.Fatal(err)
		}
		want := parent
		if parent == root {
			want.FlowInstance, want.EntityID = "fork-run", "fork-run"
		}
		if got != want {
			t.Fatalf("parent rehomed: got=%+v want=%+v", got, want)
		}
	}
	for _, parent := range []flowidentity.ParentRoute{
		{FlowID: ".", FlowInstance: "source-run"},
		{FlowID: ".", FlowInstance: "other", EntityID: "source-run"},
		{FlowID: "templ", FlowInstance: "source-run", EntityID: "other"},
		{FlowID: "templ", FlowInstance: " templ/one ", EntityID: "other"},
	} {
		if _, err := ProjectParentRoute("source-run", "fork-run", parent); err == nil {
			t.Fatalf("accepted incomplete or contradictory parent: %+v", parent)
		}
	}
}
