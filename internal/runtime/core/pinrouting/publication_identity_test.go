package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestBusinessPublicationAdmittedSourceKinds(t *testing.T) {
	root, err := events.NewRootRoutingSource("entity")
	if err != nil {
		t.Fatal(err)
	}
	external, err := events.NewExternalIngressRoutingSource("child", "entity", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	static, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: "entity"})
	if err != nil {
		t.Fatal(err)
	}
	template, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child/ti-one", EntityID: "entity"})
	if err != nil {
		t.Fatal(err)
	}
	selectedRoot, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: "selected-run"})
	if err != nil {
		t.Fatal(err)
	}
	control, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child/ti-one", EntityID: "entity"})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		name, flow, want string
		source           events.RoutingSource
	}{
		{"root", ".", "work.done", root}, {"static", "child", "child/work.done", static},
		{"template", "child", "child/ti-one/work.done", template}, {"selected_root", ".", "work.done", selectedRoot},
		{"foreign_root", "child", "", root}, {"foreign_static", "sibling", "", static},
		{"foreign_template", "sibling", "", template}, {"foreign_selected_root", "child", "", selectedRoot},
		{"control", "child", "", control}, {"platform", ".", "", events.NewPlatformControlRoutingSource()},
		{"absent", ".", "", events.NoRoutingSource()},
		{"external", "child", "", external},
	} {
		t.Run(row.name, func(t *testing.T) {
			before := row.source
			got, err := AdmitPublicationIdentity(row.flow, "work.done", row.source)
			if row.want == "" {
				if err == nil || got != "" {
					t.Fatalf("source acquired business authority: %q %v", got, err)
				}
			} else if err != nil || string(got) != row.want {
				t.Fatalf("got %q %v; want %q", got, err, row.want)
			}
			if row.source != before {
				t.Fatal("projection changed source authority")
			}
		})
	}
}
