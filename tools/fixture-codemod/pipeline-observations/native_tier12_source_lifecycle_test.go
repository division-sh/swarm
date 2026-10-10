package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const tier12SourceLifecycleShape = `func assertSourceRunLifecycle(t testing.TB,selected runtimebus.RunLifecycleReadPersistence,runID,wantStatus string,wantEnded bool) {
t.Helper()
snapshot,err := selected.LoadRunLifecycleSnapshot(testAuthorActivityContext(context.Background()),runID)
if err != nil { t.Fatalf("load source run lifecycle: %v",err) }
if snapshot.RunID != runID { t.Fatalf("source run lifecycle borrowed identity: got %q want %q",snapshot.RunID,runID) }
status,ended := snapshot.Status,snapshot.EndedAt != nil
if strings.TrimSpace(status) != wantStatus || ended != wantEnded {
t.Fatalf("source run lifecycle = status:%q ended:%v, want status:%q ended:%v",status,ended,wantStatus,wantEnded)
}
}`

func tier12SourceLifecyclePreserved(source string) bool {
	want, err := canonicalFunction(tier12SourceLifecycleShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeTier12SourceLifecycleUsesCanonicalReaderAndExactFacts(t *testing.T) {
	var recipes []recipe
	if err := json.Unmarshal(recipeBytes, &recipes); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range recipes {
		if row.Family != "native-tier12-source-lifecycle" {
			continue
		}
		matched++
		if row.Function == "assertSourceRunLifecycle" {
			if !tier12SourceLifecyclePreserved(row.After) {
				t.Fatal("source lifecycle changed canonical owner, exact run, NULL-ended semantics or status assertion")
			}
			for _, pair := range [][2]string{
				{"context.Background()), runID", "context.Background()), otherRun"},
				{"snapshot.RunID != runID", "false"},
				{"snapshot.Status", "\"paused\""}, {"snapshot.EndedAt != nil", "false"},
				{"ended != wantEnded", "false"}, {"strings.TrimSpace(status) != wantStatus", "false"},
				{"if err != nil", "if false"},
			} {
				mutant := strings.Replace(row.After, pair[0], pair[1], 1)
				if mutant == row.After || tier12SourceLifecyclePreserved(mutant) {
					t.Fatalf("weakened source lifecycle admitted: %v", pair)
				}
			}
			continue
		}
		if row.Function != "TestTier12RuntimeFork_SelectedContractForkExecutionFixture" ||
			strings.Count(row.Before, "assertSourceRunLifecycle(t, h.db,") != 2 ||
			strings.ReplaceAll(row.Before, "assertSourceRunLifecycle(t, h.db,", "assertSourceRunLifecycle(t, h.pg,") != row.After {
			t.Fatal("Tier12 source/fork setup, PostgreSQL scope, cleanup, execution or source freeze assertions changed")
		}
	}
	if matched != 2 {
		t.Fatalf("source lifecycle recipes=%d, want2", matched)
	}
}
