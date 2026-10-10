package runtimepersistence

import (
	"reflect"
	"testing"
)

func TestR7SelectedStoresExposeNoAuthoredRoutingRuleAPI(t *testing.T) {
	for _, store := range []any{(*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		owner := reflect.TypeOf(store)
		for _, name := range []string{"LoadRoutingRules", "UpsertRoutingRule"} {
			if _, present := owner.MethodByName(name); present {
				t.Fatalf("%s exposes retired authored-route authority %s", owner, name)
			}
		}
	}
}

func TestR7SelectedStoresExposeNoStandaloneInstanceRouteMutation(t *testing.T) {
	for _, store := range []any{(*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		owner := reflect.TypeOf(store)
		for _, name := range []string{"ReplaceFlowInstanceRouteRecords", "RollbackFlowInstanceRoute", "UpsertFlowInstanceRoute", "DeleteFlowInstanceRoute", "ListFlowInstanceRoutes", "RequirePublicationRunActive"} {
			if _, present := owner.MethodByName(name); present {
				t.Errorf("%s exposes retired standalone route-planning API %s", owner, name)
			}
		}
	}
}
