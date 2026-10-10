package main

import (
	"strings"
	"testing"
)

func TestNativeTargetRefusalSnapshotsPreserveExactReadersAndAssertions(t *testing.T) {
	for _, cut := range []struct {
		family, before, after string
	}{
		{
			"native-wrong-run-target",
			"\t\t\t\tif !reflect.DeepEqual(before.State, persisted) {\n\t\t\t\t\tt.Fatal(\"wrong-run refusal changed unasserted field facts or clocks\")\n\t\t\t\t}",
			"\t\t\t\t// Compare exact snapshots from one reader, not driver-local times against UTC.\n\t\t\t\tafter, err := fixture.Persistence.store.LoadTargetPersistence(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))\n\t\t\t\tif err != nil || !reflect.DeepEqual(before, after) {\n\t\t\t\t\tt.Fatalf(\"wrong-run refusal changed unasserted field facts or clocks: before=%#v after=%#v err=%v\", before, after, err)\n\t\t\t\t}",
		},
		{
			"native-missing-header-target",
			"\tif !reflect.DeepEqual(prestate.State, persisted) {\n\t\tt.Fatal(\"rejected child changed an unasserted field or lifecycle clock\")\n\t}",
			"\tafter, err := fixture.Persistence.store.LoadTargetPersistence(ctx, childOwner, runtimeidentity.NormalizeEntityID(entityID))\n\tif err != nil || !reflect.DeepEqual(prestate, after) {\n\t\tt.Fatalf(\"rejected child changed an unasserted field or lifecycle clock: before=%#v after=%#v err=%v\", prestate, after, err)\n\t}",
		},
	} {
		t.Run(cut.family, func(t *testing.T) {
			row := nativeMissingHeaderRecipe(t, cut.family)
			if strings.Count(row.After, cut.before) != 1 || strings.Count(row.Successor, cut.after) != 1 {
				t.Fatal("exact no-mutation comparison cut is missing or ambiguous")
			}
			original, err := canonicalFunction(row.After)
			restored, restoreErr := canonicalFunction(strings.Replace(row.Successor, cut.after, cut.before, 1))
			if err != nil || restoreErr != nil || original != restored {
				t.Fatal("snapshot repair changed original construction, refusal, field assertions or counts")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current exact-snapshot successor diverged: %v", err)
			}
			for _, token := range []string{"LoadTargetPersistence", "runtimeidentity.NormalizeEntityID(entityID)", "!reflect.DeepEqual", "err != nil"} {
				mutant := strings.Replace(row.Successor, cut.after, strings.Replace(cut.after, token, "foreignCut", 1), 1)
				if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), row); err == nil || changed {
					t.Fatalf("weakened snapshot owner, identity, equality or read-error check accepted: %s", token)
				}
			}
		})
	}
}
