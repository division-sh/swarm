package runforkadmission

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestFixedCutKeyedAncestorRequiresUniqueExactOccurrenceChoice(t *testing.T) {
	parent := flowidentity.Instance{TemplateID: "root", InstancePath: "root/first", EntityID: "first"}
	route := flowidentity.ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID}
	left := flowidentity.Instance{TemplateID: "account", InstancePath: "account/left", EntityID: "left", ParentRoute: route}
	right := flowidentity.Instance{TemplateID: "account", InstancePath: "account/right", EntityID: "right", ParentRoute: route}
	foreign := left
	foreign.ParentRoute.EntityID = "foreign-parent"

	for _, tc := range []struct {
		name    string
		choices []flowidentity.Instance
		want    flowidentity.Instance
	}{
		{name: "next occurrence starts empty"},
		{name: "same path but foreign parent entity", choices: []flowidentity.Instance{foreign}},
		{name: "competing sibling choices", choices: []flowidentity.Instance{left, right}},
		{name: "identical choices and unrelated candidate", choices: []flowidentity.Instance{foreign, left, left}, want: left},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := fixedCutConnectSelection{"account": tc.choices}
			got, err := selected.keyedChild(parent, "account")
			if got != tc.want || (err == nil) != (tc.want != (flowidentity.Instance{})) {
				t.Fatalf("selected ancestor = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}
