package bus_test

import (
	"reflect"
	"testing"
)

func TestExternalFlowFixturesHaveNoRetiredRouteAuthority(t *testing.T) {
	owner := reflect.TypeOf((*routePersistenceTestStore)(nil))
	for _, name := range []string{"ListActiveFlowInstanceDescriptors", "UpsertFlowInstanceRoute", "DeleteFlowInstanceRoute", "ListFlowInstanceRoutes"} {
		if _, present := owner.MethodByName(name); present {
			t.Errorf("%s retains retired route authority %s", owner, name)
		}
	}
	for _, name := range []string{"routes", "stagedRoutes", "upsertCalls", "replaceCalls", "deleteCalls"} {
		if _, present := owner.Elem().FieldByName(name); present {
			t.Errorf("%s retains retired fixture route mirror %s", owner, name)
		}
	}
	if _, present := reflect.TypeOf((*routeSetEventStore)(nil)).MethodByName("ListActiveFlowInstanceDescriptors"); present {
		t.Error("route settlement fixture retains retired descriptor reader")
	}
}
