package manager

import (
	"reflect"
	"testing"
)

func TestManagerDoesNotRequireInstanceRoutePersistence(t *testing.T) {
	if _, present := reflect.TypeOf(PersistenceRoles{}).FieldByName("FlowRoutes"); present {
		t.Fatal("AgentManager restores non-authoritative instance-route persistence")
	}
}
