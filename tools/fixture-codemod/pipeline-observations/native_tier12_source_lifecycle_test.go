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
		row, source := requireCatchUp2672MasterPair(t, row, selectedCausalObservationBody(t, row.File, row.Function))
		// Q2 excludes diagnostic history (review 6183283144). Preserve that
		// historical rewrite; its successor adds only explicit business setup.
		const anchor = "\tforkAt := sourceEventID\n"
		const setup = `	// Source advancement needs a business event, not incidental runtime logs.
	storetest.InsertRevisionedExistingRunRootEventRecord(t, h.ctx, h.pg, uuid.NewString(), sourceRunID, "source.after",
		eventtest.Producer(events.EventProducerExternal, "cataloge2e"), []byte(` + "`{}`" + `),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, sourceRunID), sourceRunID), time.Now().UTC())
`
		if strings.Count(row.After, anchor) != 1 {
			t.Fatal("historical cut anchor changed")
		}
		expected, err := canonicalFunction(strings.Replace(row.After, anchor, anchor+setup, 1))
		if err != nil {
			t.Fatal(err)
		}
		matches := func(candidate string) bool {
			actual, err := canonicalFunction(candidate)
			return err == nil && actual == expected
		}
		if !matches(row.Successor) || !matches(source) {
			t.Fatal("source-advanced successor changed more than explicit post-cut business-event setup")
		}
		for _, edit := range [][2]string{
			{setup, ""},
			{"InsertRevisionedExistingRunRootEventRecord", "InsertExistingRunRootEventRecord"},
			{"h.ctx, h.pg, uuid.NewString(), sourceRunID", "h.ctx, h.pg, uuid.NewString(), otherRunID"},
			{`"source.after"`, `"platform.runtime_log"`},
			{"!result.Activation.SourceAdvancedAfterFork", "false"},
			{"sourceBefore, selectedContractSourceRunCounts", "sourceBefore, foreignRunCounts"},
			{"assertUnsupportedHistoricalReplayFailsClosed(t, negativeFixtureRoot)", ""},
			{"10 * time.Second", "11 * time.Second"},
		} {
			changed := strings.Replace(row.Successor, edit[0], edit[1], 1)
			if changed == row.Successor || matches(changed) {
				t.Fatalf("altered source-advanced setup or assertion accepted: %q", edit[0])
			}
		}
	}
	if matched != 2 {
		t.Fatalf("source lifecycle recipes=%d, want2", matched)
	}
}
