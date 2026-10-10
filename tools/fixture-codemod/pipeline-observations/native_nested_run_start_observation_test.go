package main

import (
	"strings"
	"testing"
)

const nestedRunStartOldProjection = "\t\tvar eventID, eventEntityID, eventFlowInstance, targetRoute, targetSet string\n\t\tvar payload json.RawMessage\n\t\tif err := db.QueryRow(`\n\t\t\t\tSELECT event_id::text, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''),\n\t\t\t\t       COALESCE(target_route::text, '{}'), COALESCE(target_set::text, '[]'), payload\n\t\t\t\tFROM events\n\t\t\t\tWHERE run_id = $1::uuid AND event_name = 'scan.requested'\n\t\t\t`, runID).Scan(&eventID, &eventEntityID, &eventFlowInstance, &targetRoute, &targetSet, &payload); err != nil {\n\t\t\tt.Fatalf(\"load run.start event row: %v\", err)\n\t\t}\n\t\tassertStoredEventProjectionMatchesDeliveries(t, eventEntityID, eventFlowInstance, targetRoute, targetSet, runID, postgresEventDeliveryTargetRoutes(t, db, eventID))\n"
const nestedRunStartNativeProjection = "\t\teventID, err := storetest.ReadLatestNamedEventIdentityStorage(context.Background(), pg, \"scan.requested\", \"\")\n\t\tif err != nil {\n\t\t\tt.Fatalf(\"load run.start event row: %v\", err)\n\t\t}\n\t\tevidence := storetest.ReadSemanticEventFixtureEvidence(t, context.Background(), pg, runID, eventID)\n\t\tif !evidence.RecordFound || evidence.Record.EventID != eventID || evidence.Record.RunID != runID || evidence.Record.EventName != \"scan.requested\" {\n\t\t\tt.Fatalf(\"run.start physical event identity changed: %+v\", evidence.Record)\n\t\t}\n\t\tvar deliveryTargets []events.RouteIdentity\n\t\tfor _, projection := range evidence.DeliveryProjections {\n\t\t\tvar target events.DeliveryTargetOwnership\n\t\t\t// Column 9 is the original physical delivery_target_route encoding.\n\t\t\tif err := json.Unmarshal([]byte(projection[9]), &target); err != nil {\n\t\t\t\tt.Fatalf(\"decode physical delivery target: %v\", err)\n\t\t\t}\n\t\t\tdeliveryTargets = append(deliveryTargets, target.Route())\n\t\t}\n\t\tassertStoredEventProjectionMatchesDeliveries(t, evidence.Record.EntityID, evidence.Record.FlowInstance,\n\t\t\tstring(evidence.Record.TargetRoute), string(evidence.Record.TargetSet), runID, deliveryTargets)\n"

func nativeNestedRunStartObservationSource(source string) string {
	source = strings.Replace(source, "runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),", "storetest.RequireRun(t, context.Background(), pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),", 1)
	source = strings.Replace(source, nestedRunStartOldProjection, nestedRunStartNativeProjection, 1)
	return strings.Replace(source, "json.Unmarshal(payload, &decoded)", "json.Unmarshal(evidence.Record.Payload, &decoded)", 1)
}

func TestNativeNestedRunStartPreservesPhysicalProjectionAgainstEveryDelivery(t *testing.T) {
	source := selectedCausalObservationBody(t, "internal/apiv1/operator_run_start_test.go", "TestOperatorRunStartHandlersFailClosedBeforePersistence")
	for _, cut := range []string{
		"storetest.RequireRun(t, context.Background(), pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),",
		"ReadSemanticEventFixtureEvidence(t, context.Background(), pg, runID, eventID)",
		"!evidence.RecordFound", "evidence.Record.EventID != eventID", "evidence.Record.RunID != runID", "evidence.Record.EventName != \"scan.requested\"",
		"range evidence.DeliveryProjections", "json.Unmarshal([]byte(projection[9]), &target)", "deliveryTargets = append(deliveryTargets, target.Route())",
		"evidence.Record.EntityID, evidence.Record.FlowInstance", "string(evidence.Record.TargetRoute), string(evidence.Record.TargetSet), runID, deliveryTargets",
		"json.Unmarshal(evidence.Record.Payload, &decoded)", "decoded[\"entity_id\"] != payloadEntityID", "decoded[\"topic\"] != \"medicine\"",
	} {
		if !strings.Contains(source, cut) {
			t.Fatalf("run-start projection lost exact physical cut: %s", cut)
		}
	}
	if strings.Contains(source, "db.Query") || strings.Contains(source, "postgresEventDeliveryTargetRoutes(") || strings.Contains(source, "runlifecyclefixture.Require") {
		t.Fatal("nested run-start fixture regained raw construction/projection")
	}
}
