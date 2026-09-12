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
			if row.want != "" {
				declaration, err := PublicationDeclarationForSourceEvent(got, row.source)
				if err != nil || declaration.Flow() != row.flow || declaration.Local() != "work.done" {
					t.Fatalf("readback declaration = %#v, %v", declaration, err)
				}
			}
			if row.source != before {
				t.Fatal("projection changed source authority")
			}
		})
	}
}

func TestPublicationMetadataRejectsForeignAndUnqualifiedNames(t *testing.T) {
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "outer/child", FlowInstance: "outer/child/first"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []events.EventType{
		"work.done", "outer/child/work.done", "outer/child/second/work.done",
		"outer/other/first/work.done", "outer/child/first/deeper/work.done",
		"outer/child/first/outer/child/work.done", "outer/child/first/ work.done",
	} {
		if _, err := PublicationDeclarationForSourceEvent(name, source); err == nil {
			t.Fatalf("foreign/noncanonical publication %q selected metadata", name)
		}
	}
	for _, source := range []events.RoutingSource{events.NoRoutingSource(), events.NewPlatformControlRoutingSource()} {
		if _, err := PublicationDeclarationForSourceEvent("work.done", source); err == nil {
			t.Fatal("non-business source selected business metadata")
		}
	}
}
