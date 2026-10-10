package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var nativeSemanticFixtureEvidenceShapes = map[string]string{
	"TestUnrevisionedSemanticEventFixtureMatchesCanonicalMutationProjection": "func TestUnrevisionedSemanticEventFixtureMatchesCanonicalMutationProjection(t *testing.T) {\n\tfor _, backend := range []struct {\n\t\tname string\n\t\topen func(*testing.T) RunFixtureStore\n\t}{\n\t\t{\"sqlite\", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},\n\t\t{\"postgres\", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},\n\t} {\n\t\tt.Run(backend.name, func(t *testing.T) {\n\t\t\tctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())\n\t\t\trawStore := backend.open(t)\n\t\t\tcanonicalStore := backend.open(t)\n\t\t\trunID, eventID := uuid.NewString(), uuid.NewString()\n\t\t\tat := time.Now().UTC().Truncate(time.Microsecond)\n\t\t\tfor _, selected := range []RunFixtureStore{rawStore, canonicalStore} {\n\t\t\t\tRequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})\n\t\t\t}\n\t\t\tnode, err := runtimeidentity.ParseExecutableNode(\"flow_a\", \"fixture-node\")\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\troute := events.DeliveryRoute{\n\t\t\t\tRecipient: events.MustNodeDeliveryRecipient(node),\n\t\t\t\tTarget:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: \"flow_a\", FlowInstance: \"flow_a\"}),\n\t\t\t}\n\t\t\tname, err := agentidentity.DeclaredName(\"fixture-agent\", \"fixture-owner\")\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tidentity, err := agentidentity.New(runID, name, agentidentity.RootRoute())\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\troutes := []events.DeliveryRoute{route, {\n\t\t\t\tRecipient:     events.MustAgentDeliveryRecipient(\"fixture-agent\"),\n\t\t\t\tAgentIdentity: identity,\n\t\t\t}}\n\t\t\tevent := eventtest.ExistingRunRootIngressWithRoutingSource(\n\t\t\t\teventID, \"fixture.ready\", \"fixture\", \"\", []byte(`{\"value\":1}`), 0, runID,\n\t\t\t\tevents.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,\n\t\t\t)\n\t\t\tbeforeRevision := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID).RevisionCount\n\t\t\tif got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {\n\t\t\t\tt.Fatalf(\"raw fixture outcome = %v, want inserted\", got)\n\t\t\t}\n\t\t\trawEvidence := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID)\n\t\t\tif got := rawEvidence.RevisionCount; got != beforeRevision {\n\t\t\t\tt.Fatalf(\"raw fixture revisions = %d, want unchanged %d\", got, beforeRevision)\n\t\t\t}\n\n\t\t\tbound, err := eventfixture.BindPayload(event)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tadmitted, err := events.AdmitForPublish(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tcanonicalRevision := ReadSemanticEventFixtureEvidence(t, ctx, canonicalStore, runID, eventID).RevisionCount\n\t\t\tinserted, err := private.CommitRevisionedSemanticEventFixtureForTest(\n\t\t\t\tctx, canonicalStore, admitted, canonicalFixtureSettlement(t, admitted.Event(), routes),\n\t\t\t\troutes, runtimepipelineobligation.ScopeSubscribed, nil,\n\t\t\t)\n\t\t\tif err == nil && !inserted {\n\t\t\t\tt.Fatal(\"canonical mutation did not insert the fixture\")\n\t\t\t}\n\t\t\tif err != nil {\n\t\t\t\tt.Fatalf(\"canonical mutation fixture: %v\", err)\n\t\t\t}\n\t\t\tcanonicalEvidence := ReadSemanticEventFixtureEvidence(t, ctx, canonicalStore, runID, eventID)\n\t\t\tif got := canonicalEvidence.RevisionCount; got <= canonicalRevision {\n\t\t\t\tt.Fatalf(\"canonical mutation revisions = %d, want after %d\", got, canonicalRevision)\n\t\t\t}\n\n\t\t\tif !rawEvidence.RecordFound || !canonicalEvidence.RecordFound {\n\t\t\t\tt.Fatal(\"fixture comparison requires both complete persisted records\")\n\t\t\t}\n\t\t\trawRecord := rawEvidence.Record\n\t\t\tcanonicalRecord := canonicalEvidence.Record\n\t\t\tif !rawRecord.Equal(canonicalRecord) {\n\t\t\t\tt.Fatalf(\"unrevisioned event record differs from canonical mutation projection: raw=%#v canonical=%#v\", rawRecord, canonicalRecord)\n\t\t\t}\n\t\t\tfor _, selectedRoute := range routes {\n\t\t\t\tdeliveryID, err := runtimedelivery.DeliveryID(eventID, selectedRoute)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\traw, rawFound := rawEvidence.DeliveryProjections[deliveryID]\n\t\t\t\tcanonical, canonicalFound := canonicalEvidence.DeliveryProjections[deliveryID]\n\t\t\t\tif !rawFound || !canonicalFound {\n\t\t\t\t\tt.Fatalf(\"exact delivery projection missing: raw=%v canonical=%v\", rawFound, canonicalFound)\n\t\t\t\t}\n\t\t\t\tif !reflect.DeepEqual(raw, canonical) {\n\t\t\t\t\tt.Fatalf(\"unrevisioned delivery projection differs from canonical mutation: raw=%#v canonical=%#v\", raw, canonical)\n\t\t\t\t}\n\t\t\t}\n\t\t\tif got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendExactDuplicate {\n\t\t\t\tt.Fatalf(\"exact duplicate outcome = %v, want exact duplicate\", got)\n\t\t\t}\n\t\t\tduplicateEvidence := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID)\n\t\t\tif got := duplicateEvidence.RevisionCount; got != beforeRevision {\n\t\t\t\tt.Fatalf(\"duplicate fixture revisions = %d, want unchanged %d\", got, beforeRevision)\n\t\t\t}\n\t\t\tif got := len(duplicateEvidence.DeliveryProjections); got != len(routes) {\n\t\t\t\tt.Fatalf(\"duplicate fixture delivery count = %d, want %d\", got, len(routes))\n\t\t\t}\n\t\t})\n\t}\n}",
	"TestUnrevisionedSemanticDeliveryFixtureImmediateTerminalization":        "func TestUnrevisionedSemanticDeliveryFixtureImmediateTerminalization(t *testing.T) {\n\ttype terminalizingStore interface {\n\t\tRunFixtureStore\n\t\tTerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error)\n\t\tSnapshot(context.Context, string) (runtimedelivery.Snapshot, error)\n\t}\n\tfor _, backend := range []struct {\n\t\tname string\n\t\topen func(*testing.T) terminalizingStore\n\t}{\n\t\t{\"sqlite\", func(t *testing.T) terminalizingStore { return StartSQLiteRuntimeStore(t) }},\n\t\t{\"postgres\", func(t *testing.T) terminalizingStore { return StartPostgresRuntimeStore(t) }},\n\t} {\n\t\tt.Run(backend.name, func(t *testing.T) {\n\t\t\tctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())\n\t\t\tselected := backend.open(t)\n\t\t\trunID, eventID := uuid.NewString(), uuid.NewString()\n\t\t\tat := time.Now().UTC()\n\t\t\tRequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})\n\t\t\tnode, err := runtimeidentity.ParseExecutableNode(\"flow_a\", \"fixture-node\")\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\troute := events.DeliveryRoute{\n\t\t\t\tRecipient: events.MustNodeDeliveryRecipient(node),\n\t\t\t\tTarget:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: \"flow_a\", FlowInstance: \"flow_a\"}),\n\t\t\t}\n\t\t\tevent := eventtest.ExistingRunRootIngressWithRoutingSource(\n\t\t\t\teventID, \"fixture.ready\", \"fixture\", \"\", []byte(`{\"value\":1}`), 0, runID,\n\t\t\t\tevents.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,\n\t\t\t)\n\t\t\tif got := CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {\n\t\t\t\tt.Fatalf(\"fixture outcome = %v, want inserted\", got)\n\t\t\t}\n\t\t\tdeliveryID, err := runtimedelivery.DeliveryID(eventID, route)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore, err := selected.Snapshot(ctx, deliveryID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tif backend.name == \"sqlite\" && before.CreatedAt.Nanosecond()%int(time.Millisecond) != 0 {\n\t\t\t\tt.Fatalf(\"SQLite fixture created_at = %s, want database-clock millisecond precision\", before.CreatedAt)\n\t\t\t}\n\t\t\ttransitions, err := selected.TerminalizeRun(ctx, runID, \"run_terminal\")\n\t\t\tif err != nil {\n\t\t\t\tt.Fatalf(\"immediate terminalization: %v\", err)\n\t\t\t}\n\t\t\tif len(transitions) != 1 || transitions[0].Current.DeliveryID != deliveryID || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {\n\t\t\t\tt.Fatalf(\"terminalization transitions = %#v, want one dead-lettered delivery\", transitions)\n\t\t\t}\n\t\t\tif transitions[0].Current.UpdatedAt.Before(before.CreatedAt) {\n\t\t\t\tt.Fatalf(\"terminalized updated_at %s precedes created_at %s\", transitions[0].Current.UpdatedAt, before.CreatedAt)\n\t\t\t}\n\t\t})\n\t}\n}",
}

func TestNativeSemanticFixtureEvidencePreservesRecordProjectionRevisionAndTerminalization(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-semantic-fixture-evidence" {
			continue
		}
		matched++
		want, err := canonicalFunction(nativeSemanticFixtureEvidenceShapes[row.Function])
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || actual != got {
			t.Fatalf("semantic fixture proof diverged: %s", row.Function)
		}
		for _, old := range []string{"rawDB", "canonicalDB", "DatabaseForTest(", "AdmitPostgresRuntimeStore(", "fixtureRevisionCount(", "fixtureDeliveryCount(", "fixtureLoadedEventRecord(", "fixtureDeliveryProjection("} {
			if strings.Contains(row.After, old) {
				t.Fatalf("shared raw fixture survives: %s", old)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("semantic evidence recipes=%d,want2", matched)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/store/storetest/event_unrevisioned_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	retired := map[string]bool{"fixtureRevisionCount": true, "fixtureDeliveryCount": true, "fixtureLoadedEventRecord": true, "fixtureDeliveryProjection": true}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && retired[fn.Name.Name] {
			t.Fatalf("retired raw fixture helper survives: %s", fn.Name.Name)
		}
	}
	root := nativeSemanticFixtureEvidenceShapes["TestUnrevisionedSemanticEventFixtureMatchesCanonicalMutationProjection"]
	for _, pair := range [][2]string{
		{"if !rawEvidence.RecordFound || !canonicalEvidence.RecordFound", "if false"},
		{"!rawRecord.Equal(canonicalRecord)", "false"},
		{"if !rawFound || !canonicalFound", "if false"},
		{"!reflect.DeepEqual(raw, canonical)", "false"},
		{"got <= canonicalRevision", "got < canonicalRevision"},
		{"got != beforeRevision", "got < beforeRevision"},
		{"len(duplicateEvidence.DeliveryProjections)", "len(canonicalEvidence.DeliveryProjections)"},
		{"ctx, rawStore, runID, eventID", "ctx, canonicalStore, runID, eventID"},
	} {
		mutant := strings.Replace(root, pair[0], pair[1], 1)
		want, _ := canonicalFunction(root)
		got, err := canonicalFunction(mutant)
		if mutant == root || (err == nil && got == want) {
			t.Fatalf("weak/shared-owner evidence admitted: %v", pair)
		}
	}
	projection := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/read_projections.go", "ReadSemanticEventDeliveryStorage")
	for _, field := range []string{
		"route_identity, subscriber_type, subscriber_id", "agent_name_owner, agent_name_source, agent_route_presence",
		"agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path", "CAST(delivery_target_route AS TEXT)",
		"CAST(delivery_context AS TEXT)", "CAST(delivery_payload_projection AS TEXT)", "CAST(connect_execution_claim AS TEXT)",
		"CAST(receiver_materialization_plan AS TEXT)", "execution_authority_kind", "authority_bundle_hash, execution_authority_id",
		"CAST(execution_authority_generation AS TEXT)", "FROM event_deliveries WHERE event_id=$1",
		"var projection [18]string", "projections[deliveryID], statuses[deliveryID] = projection, status",
	} {
		if !strings.Contains(projection, field) {
			t.Fatalf("closed physical projection lost %s", field)
		}
	}
}
