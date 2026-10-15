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
			restoredSource := strings.Replace(row.Successor, cut.after, cut.before, 1)
			if cut.family == "native-missing-header-target" {
				ancestry := `root := constructorUnitIdentity(t, source, testPipelineRunID, ".")
	if err := fixture.Construct(ctx, constructedScenarioInstanceForTest(t, source, ctx, ".")); err != nil {
		t.Fatal(err)
	}
	parent, err := runtimeflowidentity.KeyedChild(source, root, "review", "chosen-parent")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForSource(t, source, ctx, WorkflowInstance{
		InstanceID: parent.InstanceID, StorageRef: parent.InstancePath, EntityID: parent.EntityID,
		WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), CurrentState: "active",
		EntityType: "review_entity", Fields: map[string]any{"instance_key": "chosen-parent"},
	})); err != nil {
		t.Fatal(err)
	}
	child, err := runtimeflowidentity.KeylessChild(source, parent, "review/child")
	if err != nil {
		t.Fatal(err)
	}
	instancePath := child.InstancePath`
				fields := `WorkflowName: "review/child", WorkflowVersion: source.WorkflowVersion(), Mode: "static",
		ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID,`
				if strings.Count(restoredSource, ancestry) != 1 || strings.Count(restoredSource, fields) != 1 {
					t.Fatal("reviewed exact root/parent construction or captured ancestry is missing")
				}
				restoredSource = strings.Replace(restoredSource, ancestry, `instancePath := "review/child/instance"`, 1)
				restoredSource = strings.Replace(restoredSource, fields, `WorkflowName: "review/child", WorkflowVersion: source.WorkflowVersion(),`, 1)
			}
			restored, restoreErr := canonicalFunction(restoredSource)
			if err != nil || restoreErr != nil || original != restored {
				t.Fatal("snapshot repair changed original construction, refusal, field assertions or counts")
			}
			current := row.Successor
			var currentEdits [][2]string
			switch cut.family {
			case "native-wrong-run-target":
				currentEdits = [][2]string{
					{"persisted, found, err := fixture.Persistence.store.LoadEntityState(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))", "persisted, err := fixture.Persistence.store.LoadTargetPersistence(ctx, testRunScopedWorkflowInstanceFromContext(ctx, wrongRunID), runtimeidentity.NormalizeEntityID(entityID))"},
					{"marker := string(persisted.Fields)", "marker := string(persisted.State.Fields)"},
					{"err != nil || !found || !strings.Contains(marker, \"unchanged\")", "err != nil || persisted.Presence == WorkflowTargetPersistenceAbsent || !strings.Contains(marker, \"unchanged\")"},
				}
			case "native-missing-header-target":
				currentEdits = [][2]string{
					{"persisted, found, err := fixture.Persistence.store.LoadEntityState(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, route.InstancePath), runtimeidentity.NormalizeEntityID(entityID))", "persisted, err := fixture.Persistence.store.LoadTargetPersistence(ctx, testRunScopedWorkflowInstanceForRun(testPipelineRunID, route.InstancePath), runtimeidentity.NormalizeEntityID(entityID))"},
					{"fieldsErr := json.Unmarshal(persisted.Fields, &fields)", "fieldsErr := json.Unmarshal(persisted.State.Fields, &fields)"},
					{"err != nil || !found || persisted.CurrentState != \"active\" || persisted.Revision != 1 || fieldsErr != nil || fields[\"marker\"] != \"unchanged\"", "err != nil || persisted.Presence != WorkflowTargetPersistenceStateOnly || fieldsErr != nil || fields[\"marker\"] != \"unchanged\""},
					{"t.Fatalf(\"rejected child state changed: found=%t err=%v state=%#v\", found, err, persisted)", "t.Fatalf(\"rejected child state changed or gained lifecycle: err=%v state=%#v\", err, persisted)"},
				}
			}
			for _, edit := range currentEdits {
				if strings.Count(current, edit[0]) != 1 {
					t.Fatalf("missing or ambiguous historical target read: %q", edit[0])
				}
				current = strings.Replace(current, edit[0], edit[1], 1)
			}
			currentGuard := row
			currentGuard.Before, currentGuard.After, currentGuard.Successor = current, current, ""
			if files, changes, err := prepareFiles("../../..", []recipe{currentGuard}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current exact-snapshot successor diverged: %v", err)
			}
			if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+row.Successor), currentGuard); err == nil || changed {
				t.Fatal("retired data-row lifecycle reader accepted as current")
			}
			for _, edit := range currentEdits {
				mutant := strings.Replace(current, edit[1], edit[0], 1)
				if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), currentGuard); err == nil || changed {
					t.Fatalf("lost paired target/presence/business-field read accepted: %q", edit[1])
				}
			}
			for _, token := range []string{"LoadTargetPersistence", "runtimeidentity.NormalizeEntityID(entityID)", "!reflect.DeepEqual", "err != nil"} {
				mutant := strings.Replace(row.Successor, cut.after, strings.Replace(cut.after, token, "foreignCut", 1), 1)
				if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), row); err == nil || changed {
					t.Fatalf("weakened snapshot owner, identity, equality or read-error check accepted: %s", token)
				}
				mutant = strings.Replace(current, cut.after, strings.Replace(cut.after, token, "foreignCut", 1), 1)
				if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), currentGuard); err == nil || changed {
					t.Fatalf("current snapshot lost owner, identity, equality or error check: %s", token)
				}
			}
		})
	}
}
