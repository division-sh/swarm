package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusTargetConstructionPreservesSourceAndEveryRoundTripAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-construction" {
			continue
		}
		matched++
		before := row.Before
		switch row.Function {
		case "newTargetOwnerParityStore":
			before = strings.Replace(before, "_, db, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\treturn storetest.AdmitPostgresRuntimeStore(t, db)", "return storetest.StartPostgresRuntimeStore(t)", 1)
		case "TestCrossFlowConstructedTargetOwnershipRoundTripOnBothBackends":
			before = strings.Replace(before, "\t\t\trun := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: testSourceArtifactFact(source), Artifact: bundle.SourceArtifact, StartedAt: at}\n\t\t\tif backend == \"postgres\" {\n\t\t\t\trunlifecyclefixture.RequirePostgres(t, ctx, storetest.DatabaseForTest(selected), run)\n\t\t\t} else {\n\t\t\t\trunlifecyclefixture.RequireSQLite(t, ctx, storetest.DatabaseForTest(selected), run)\n\t\t\t}", "\t\t\tstoretest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: testSourceArtifactFact(source).BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at})", 1)
		default:
			t.Fatal("unknown target construction consumer")
		}
		if before != row.After {
			t.Fatal("target identity, run/source/clock, flow construction, route, readback or duplicate workload changed")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("target construction diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("target construction recipes=%d,want2", matched)
	}
}
