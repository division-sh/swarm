package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const nativeComputeReplayRootShape = "func TestExecuteWithPersistedComputeModuleReplayEvidenceLoadsAndFailsClosedOnStoredDivergence(t *testing.T) {\n\tfor _, backend := range []string{\"sqlite\", \"postgres\"} {\n\t\tt.Run(backend, func(t *testing.T) {\n\t\t\tctx := testAuthorActivityContext(context.Background())\n\t\t\trunID := uuid.NewString()\n\t\t\tctx = runtimecorrelation.WithRunID(ctx, runID)\n\t\t\tfixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC(), BundleHash: authorActivityTestSourceArtifactFact.BundleHash()}\n\t\t\tvar persistence interface {\n\t\t\t\tcomputeReplayPersistence\n\t\t\t\tstoretest.RunFixtureStore\n\t\t\t}\n\t\t\tif backend == \"sqlite\" {\n\t\t\t\tpersistence = newComputeModuleReplaySQLiteStore(t)\n\t\t\t} else {\n\t\t\t\tpersistence = storetest.StartPostgresRuntimeStore(t)\n\t\t\t}\n\t\t\tprobe := storetest.CollectTransactions(t, persistence, storetest.TransactionProbeOptions{})\n\t\t\tstoretest.RequireRun(t, ctx, persistence, fixture)\n\t\t\tif counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {\n\t\t\t\tt.Fatalf(\"compute replay run setup escaped original selected owner: %+v\", counts)\n\t\t\t}\n\t\t\tprovePersistedComputeModuleReplay(t, ctx, persistence, runID)\n\t\t})\n\t}\n}"
const nativeComputeReplayConstructorShape = "func newComputeModuleReplaySQLiteStore(t *testing.T) *store.SQLiteRuntimeStore {\n\tt.Helper()\n\treturn storetest.StartSQLiteRuntimeStore(t)\n}"

func TestNativeComputeReplayConstructionPreservesStoredDivergenceAndOriginalRunOwner(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-compute-replay-construction" {
			continue
		}
		matched++
		expected := nativeComputeReplayRootShape
		if row.Function == "newComputeModuleReplaySQLiteStore" {
			expected = nativeComputeReplayConstructorShape
		}
		want, err := canonicalFunction(expected)
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || actual != got {
			t.Fatalf("native compute setup diverged: %s", row.Function)
		}
		for _, old := range []string{"DatabaseForTest(", "testutil.StartPostgres(", "AdmitPostgresRuntimeStore(", "runlifecyclefixture.Require", "store.NewSQLiteRuntimeStore("} {
			if strings.Contains(row.After, old) {
				t.Fatalf("raw compute construction survives: %s", old)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("compute construction recipes=%d,want2", matched)
	}
	source := selectedCausalObservationBody(t, "internal/runtime/engine/compute_module_replay_test.go", "provePersistedComputeModuleReplay")
	for _, fact := range []string{
		"persistComputeModuleReplayEvidenceForExecution(t, ctx, persistence",
		"ExecuteWithPersistedComputeModuleReplayEvidence(ctx, persistence, runID, req)",
		"moduleErr.Code != computemodule.CodeReplay",
		"moduleErr.Finding.Kind != computemodule.ReplayFindingResultDivergence",
		"moduleErr.Finding.Field != \"output_hash\"",
		"failed.Committed || len(failed.EmitIntents) != 0",
	} {
		if !strings.Contains(source, fact) {
			t.Fatalf("stored replay proof lost %s", fact)
		}
	}
	for _, pair := range [][2]string{
		{"storetest.RequireRun(t, ctx, persistence, fixture)", "storetest.RequireRun(t, ctx, foreignOwner, fixture)"},
		{"StartedAt: time.Now().UTC()", "StartedAt: time.Time{}"},
		{"BundleHash: authorActivityTestSourceArtifactFact.BundleHash()", "BundleHash: foreignFact.BundleHash()"},
		{"counts.Total.WriteCommits == 0 || counts.Active != 0", "false"},
		{"provePersistedComputeModuleReplay(t, ctx, persistence, runID)", "provePersistedComputeModuleReplay(t, ctx, foreignOwner, runID)"},
	} {
		mutant := strings.Replace(nativeComputeReplayRootShape, pair[0], pair[1], 1)
		value, err := canonicalFunction(mutant)
		expected, _ := canonicalFunction(nativeComputeReplayRootShape)
		if mutant == nativeComputeReplayRootShape || (err == nil && value == expected) {
			t.Fatalf("foreign/weak compute setup admitted: %v", pair)
		}
	}
}
