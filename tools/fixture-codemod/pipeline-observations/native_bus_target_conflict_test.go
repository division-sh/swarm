package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusTargetConflictPreservesContradictionAndReplayCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-conflict" {
			continue
		}
		matched++
		switch row.Function {
		case "corruptPreparedDeliveryRow":
			want, _ := canonicalFunction("func corruptPreparedDeliveryRow(\n\tt testing.TB,\n\tctx context.Context,\n\tselected targetOwnerParityStore,\n\teventID string,\n\toriginalIdentity string,\n\tconflicting events.DeliveryRoute,\n) {\n\tt.Helper()\n\tchanged, err := storetest.SetPreparedTargetConflict(ctx, selected, eventID, originalIdentity, conflicting)\n\tif err != nil {\n\t\tt.Fatalf(\"corrupt prepared delivery row: %v\", err)\n\t}\n\tif err != nil || changed != 1 {\n\t\tt.Fatalf(\"corrupt prepared delivery rows = %d err=%v, want one\", changed, err)\n\t}\n}")
			got, err := canonicalFunction(row.After)
			if err != nil || want != got {
				t.Fatal("single-row conflict fault lost its exact original owner and typed route")
			}
		case "loadPreparedDeliveryCorruption":
			want, _ := canonicalFunction("func loadPreparedDeliveryCorruption(\n\tt testing.TB,\n\tctx context.Context,\n\tselected targetOwnerParityStore,\n\teventID string,\n\trouteIdentity string,\n) preparedDeliveryCorruption {\n\tt.Helper()\n\tout, err := storetest.ReadPreparedTargetConflict(ctx, selected, eventID, routeIdentity)\n\tif err != nil {\n\t\tt.Fatalf(\"load corrupt prepared delivery row: %v\", err)\n\t}\n\treturn preparedDeliveryCorruption{count: out.Count, subscriberID: out.SubscriberID, routeIdentity: out.RouteIdentity, targetEncoding: out.TargetEncoding}\n}")
			got, err := canonicalFunction(row.After)
			if err != nil || want != got {
				t.Fatal("conflict witness lost exact row/fields or read error")
			}
		case "TestPreparedPublishAggregateCorruptionRejectsBothStoreReadbackAndExactDuplicate":
			before := strings.Replace(row.Before, "\t\t\tdb := storetest.Database(selected)\n", "", 1)
			before = strings.Replace(before, "\t\t\tcorruptTarget, err := json.Marshal(corruptRoute.Target)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatalf(\"encode corrupt route target: %v\", err)\n\t\t\t}\n", "", 1)
			before = strings.Replace(before, "\t\t\tcorruptPreparedDeliveryRow(\n\t\t\t\tt, ctx, db, backend, evt.ID(),\n\t\t\t\tevents.EncodeDeliveryRouteIdentity(originalIdentity),\n\t\t\t\tcorruptRoute.Recipient.ID(),\n\t\t\t\tevents.EncodeDeliveryRouteIdentity(corruptIdentity),\n\t\t\t\tstring(corruptTarget),\n\t\t\t)\n", "\t\t\tcorruptPreparedDeliveryRow(\n\t\t\t\tt, ctx, selected, evt.ID(),\n\t\t\t\tevents.EncodeDeliveryRouteIdentity(originalIdentity),\n\t\t\t\tcorruptRoute,\n\t\t\t)\n", 1)
			before = strings.ReplaceAll(before, "loadPreparedDeliveryCorruption(t, ctx, db, backend,", "loadPreparedDeliveryCorruption(t, ctx, selected,")
			if before != row.After {
				t.Fatal("conflict keys/kinds, readback, restart, replay/preflight error or unchanged-row assertion changed")
			}
		default:
			t.Fatal("unnamed conflict consumer")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("conflict consumer diverged from finite migration")
		}
	}
	if matched != 3 {
		t.Fatalf("target conflict recipes=%d,want3", matched)
	}
}
func TestNativeBusTargetConflictKeepsEveryPhysicalDialectStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	owners := map[string][2]string{"corruptPreparedDeliveryRow": {"lifecycle.go", "SetPreparedTargetConflictTx"}, "loadPreparedDeliveryCorruption": {"read_projections.go", "ReadPreparedTargetConflictTx"}}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-conflict" || owners[row.Function][0] == "" {
			continue
		}
		matched++
		before := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(row.Before, "UPDATE event_deliveries", "SELECT event_deliveries"))
		owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/"+owners[row.Function][0], owners[row.Function][1])
		after := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(owner, "UPDATE event_deliveries", "SELECT event_deliveries"))
		if len(before) != 2 || !reflect.DeepEqual(before, after) {
			t.Fatalf("event/identity predicates, update columns, native JSON cast or COUNT/MIN projection changed: %v -> %v", before, after)
		}
	}
	if matched != 2 {
		t.Fatalf("physical conflict owners=%d,want2", matched)
	}
}
