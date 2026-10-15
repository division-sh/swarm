package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

var retainedJoinSuccessorEdits = []struct{ before, block string }{
	{"\t\t\tif len(joins) != 1 || first.Attempt != 1 {\n\t\t\t\tt.Fatalf(\"first ordinary join: loop=%+v joins=%+v\", first, joins)\n\t\t\t}\n", "\t\t\tfirstRef := joins[0].JoinRef()\n\t\t\tnativeEvents, hasNativeEvents := rt.ReceiverStateReader.(runtimebus.EventStore)\n\t\t\tif !hasNativeEvents || nativeEvents == nil {\n\t\t\t\tt.Fatal(\"retained join outcome requires its original selected event reader\")\n\t\t\t}\n\t\t\tnativeHandoff := servedWorkspaceProofRuntime{Events: nativeEvents}\n\t\t\tfirstCtx, cancelFirst := context.WithTimeout(t.Context(), servedProofPollDeadline)\n\t\t\tfirstWaitErr := waitServedRetainedJoinOutcome(firstCtx, started.RunID, firstRef, func() (loopruntime.Activation, []joinruntime.Activation) {\n\t\t\t\treturn readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t})\n\t\t\tcancelFirst()\n\t\t\tif firstWaitErr != nil {\n\t\t\t\tt.Fatalf(\"first exact retained join outcome: %v\", firstWaitErr)\n\t\t\t}\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n\t\t\tfirst, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n"},
	{"\t\t\tif current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {\n\t\t\t\tt.Fatalf(\"ordinary repeat lost retained joins: loop=%+v joins=%+v\", current, sourceJoins)\n\t\t\t}\n", "\t\t\tvar currentRef timeridentity.JoinRef\n\t\t\tcurrentMatches := 0\n\t\t\tfor _, join := range sourceJoins {\n\t\t\t\tif join.Generation() == current.Generation() {\n\t\t\t\t\tcurrentRef = join.JoinRef()\n\t\t\t\t\tcurrentMatches++\n\t\t\t\t}\n\t\t\t}\n\t\t\tif currentMatches != 1 {\n\t\t\t\tt.Fatalf(\"current retained join reference is not unique: %d\", currentMatches)\n\t\t\t}\n\t\t\tcurrentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)\n\t\t\tcurrentWaitErr := waitServedRetainedJoinOutcome(currentCtx, started.RunID, currentRef, func() (loopruntime.Activation, []joinruntime.Activation) {\n\t\t\t\treturn readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t})\n\t\t\tcancelCurrent()\n\t\t\tif currentWaitErr != nil {\n\t\t\t\tt.Fatalf(\"current exact retained join outcome: %v\", currentWaitErr)\n\t\t\t}\n\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)\n\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n\t\t\tcurrent, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n"},
}

func proveRetainedJoinTypedReaderSource(t *testing.T) {
	t.Helper()
	const expected = `func readNativeRetainedRootJoins(t *testing.T, reader pipeline.WorkflowTargetPersistenceReader, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	if reader == nil {
		t.Fatal("retained root join observation requires its original workflow reader")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", flowidentity.LogicalInstanceID(runID), runID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.LoadWorkflowTargetPersistence(t.Context(), owner, identity.NormalizeEntityID(runID))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := record.DecodeComplete(owner.Route, identity.NormalizeEntityID(runID))
	if err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
	if err != nil || !found {
		t.Fatalf("missing exact root loop: found=%t err=%v", found, err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return loop, joins
}`
	file, name := "internal/serveapp/retained_join_outcome_wait_test.go", "readNativeRetainedRootJoins"
	actual := selectedCausalObservationBody(t, file, name)
	current, currentErr := canonicalFunction(actual)
	pinned, pinErr := canonicalFunction(expected)
	if currentErr != nil || pinErr != nil || current != pinned {
		t.Fatal("typed retained join adapter changed its exact constructed-target/decode owner")
	}
	for _, mutation := range []struct{ name, before, after string }{
		{"foreign_run", "NewRunScopedFlowInstance(runID,", "NewRunScopedFlowInstance(foreignRun,"},
		{"foreign_route", "flowidentity.StoredRoute(\".\",", "flowidentity.StoredRoute(\"foreign\","},
		{"foreign_entity", "identity.NormalizeEntityID(runID)", "identity.NormalizeEntityID(foreignEntity)"},
		{"foreign_reader", "reader.LoadWorkflowTargetPersistence(", "foreignReader.LoadWorkflowTargetPersistence("},
		{"missing_decode", "record.DecodeComplete(", "unvalidatedDecode("},
		{"missing_loop", "loopruntime.Load(", "foreignLoopLoad("},
		{"missing_join", "joinruntime.List(", "foreignJoinList("},
		{"error_to_empty", "\treturn loop, joins", "\treturn loopruntime.Activation{}, nil"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			mutant := strings.Replace(expected, mutation.before, mutation.after, 1)
			if mutant == expected {
				t.Fatal("typed adapter hostile mutation did not change source")
			}
			guard := recipe{File: file, Function: name, Before: expected, After: expected}
			if _, changed, err := rewriteFunction(file, []byte("package probe\n"+mutant), guard); err == nil || changed {
				t.Fatal("foreign/unvalidated retained join owner admitted")
			}
		})
	}
}

//go:embed testdata/retained_join_r7_reviewed.go.txt
var retainedJoinR7Reviewed string

func validateRetainedJoinSuccessor(row recipe, candidate string) error {
	// The one native handoff is reversible to the previously checked caller.
	// Authenticate its complete finite endpoint before using that predecessor.
	if _, err := fixtureSetupCurrentEndpoint2617(row, candidate); err != nil {
		return err
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return err
	}
	if len(pins) != 47 || !sameReviewedRecipeKey(row, pins[43].Original) {
		return fmt.Errorf("native retained join has no exact predecessor")
	}
	candidate = pins[43].Original.Successor
	predecessor := strings.TrimSuffix(retainedJoinR7Reviewed, "\n")
	if fmt.Sprintf("%x", sha256.Sum256([]byte(predecessor))) != "9fe6d8918d3fc2716ec8db18d7415b6445afa8da23014a4b20965f6a85155867" {
		return fmt.Errorf("reviewed R7 predecessor changed")
	}
	restored, err := restoreRetainedJoinRefusalBranch(candidate)
	if err != nil {
		return err
	}
	expected, current := predecessor, restored
	const superseded = "\n\t\t\twaitForServeStory(t, func() bool {\n\t\t\t\tcurrent, sourceJoins = readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n\t\t\t\treturn len(sourceJoins) == 2 && sourceJoins[0].OutcomeFired && sourceJoins[1].OutcomeFired\n\t\t\t})\n\t\t\twaitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)\n"
	if strings.Count(expected, superseded) != 1 {
		return fmt.Errorf("reviewed R7 wait changed")
	}
	expected = strings.Replace(expected, superseded, "", 1)
	for _, edit := range retainedJoinSuccessorEdits {
		if strings.Count(expected, edit.before) != 1 || strings.Count(current, edit.before+edit.block) != 1 {
			return fmt.Errorf("exact join checkpoint/wait block missing, duplicated or moved")
		}
		expected = strings.Replace(expected, edit.before, edit.before+edit.block, 1)
		restored = strings.Replace(restored, edit.before+edit.block, edit.before, 1)
	}
	actual, actualErr := canonicalFunction(current)
	want, wantErr := canonicalFunction(expected)
	checkpoint := "\t\t\tif current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {"
	restored = strings.Replace(restored, checkpoint, superseded+checkpoint, 1)
	original, originalErr := canonicalFunction(predecessor)
	reversed, reversedErr := canonicalFunction(restored)
	if actualErr != nil || wantErr != nil || originalErr != nil || reversedErr != nil || actual != want || reversed != original {
		return fmt.Errorf("join successor changed work beyond exact outcome composition")
	}
	return nil
}

// Remove only the added positive branch before checking the original refusal
// and exact outcome waits. Historical Before/After pins remain unchanged.
func restoreRetainedJoinRefusalBranch(source string) (string, error) {
	const signature = "func testServedJoinWriterCompletedHistoryRefusal(t *testing.T, separateCheckpoint, success bool)"
	const original = "func testServedJoinWriterCompletedHistoryRefusal(t *testing.T, separateCheckpoint bool)"
	const request = "\t\t\trequest := runfork.RunForkMaterializeRequest{SourceRunID: started.RunID, At: frontier, OriginalLoopCarriage: carriage}\n"
	const branch = request + "\t\t\tif success {\n\t\t\t\trequireServedRetainedJoinForkSuccess(t, rt, selected, request, current, sourceJoins)\n\t\t\t\treturn\n\t\t\t}\n"
	const start = "\t\t\t// Delivery settlement does not join the lifecycle candidate executor. Stop"
	const finish = "\t\t\tconst refusal = \"fork materialization requires execution-ready plan; blockers: timer_history_unproven\""
	if strings.Count(source, signature) != 1 || strings.Count(source, branch+start) != 1 || strings.Count(source, finish) != 1 {
		return "", fmt.Errorf("retained join success branch changed the original refusal boundary")
	}
	source = strings.Replace(source, signature, original, 1)
	source = strings.Replace(source, branch+start, start, 1)
	return strings.Replace(source, finish, request+finish, 1), nil
}

func TestRetainedJoinOutcomeSuccessorPreservesReviewedCaller(t *testing.T) {
	for _, target := range []struct{ file, function string }{
		{"internal/serveapp/fork_retained_join_test.go", "testServedJoinWriterCompletedHistoryRefusal"},
	} {
		t.Run(target.function, func(t *testing.T) {
			var rows []recipe
			if err := json.Unmarshal(recipeBytes, &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 893 {
				t.Fatalf("recipe census=%d,want893", len(rows))
			}
			var row *recipe
			for i := range rows {
				if rows[i].File == target.file && rows[i].Function == target.function {
					if row != nil {
						t.Fatal("ambiguous retained-join recipe")
					}
					row = &rows[i]
				}
			}
			if row == nil || row.Family != "native-served-delivery-quiescence-caller" || row.Successor == "" || row.Removed || row.Mechanical != nil {
				t.Fatal("retained-join historical recipe identity changed")
			}
			if fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "9e8f41f028da9f650675a673db43a5c5671a3c88bd490bc091674fae34619aa8" ||
				fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "fcdf44feff728df7a784859bfa4d3d1a9345866013b5ea8a291530871478d474" {
				t.Fatal("retained-join Before/After pins changed")
			}
			if err := validateRetainedJoinSuccessor(*row, row.Successor); err != nil {
				t.Fatal(err)
			}
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			current, currentErr := canonicalFunction(actual)
			pinned, pinErr := canonicalFunction(row.Successor)
			if currentErr != nil || pinErr != nil || current != pinned {
				t.Fatal("current caller differs from exact Successor")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{*row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("reviewed caller is not inert: %v", err)
			}
			archive, err := os.ReadFile("../../../internal/serveapp/testdata/future_capabilities/fork_retained_join_success_test.go.txt")
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(archive)) != "a24e4b0a28a643c56bda44ae35805588dae1e6cc1a90438efa9e07c15055ba59" {
				t.Fatal("archived future-capability oracle changed")
			}
			for _, mutation := range []struct{ name, before, after string }{
				{"missing_success_return", "requireServedRetainedJoinForkSuccess(t, ctx, rt.ReceiverStateReader, rt.BundleHash, rt.ForkRuntime, selected, request, current, sourceJoins)", "omitSuccessAssertions()"},
				{"foreign_success_reader", "requireServedRetainedJoinForkSuccess(t, ctx, rt.ReceiverStateReader,", "requireServedRetainedJoinForkSuccess(t, ctx, foreignReader,"},
				{"foreign_success_scope", "servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash)", "servedRuntimeProofAuthorActivityContext(t, foreignRuntime, rt.BundleHash)"},
				{"wrong_success_selection", "if success {", "if !success {"},
				{"missing_first", retainedJoinSuccessorEdits[0].block, ""},
				{"duplicate_first", retainedJoinSuccessorEdits[0].block, strings.Repeat(retainedJoinSuccessorEdits[0].block, 2)},
				{"missing_second", retainedJoinSuccessorEdits[1].block, ""},
				{"duplicate_second", retainedJoinSuccessorEdits[1].block, strings.Repeat(retainedJoinSuccessorEdits[1].block, 2)},
				{"wrong_run", "firstCtx, started.RunID, firstRef", "firstCtx, foreignRun, firstRef"},
				{"wrong_first_reference", "firstRef := joins[0].JoinRef()", "firstRef := joins[0].JoinRef().Declaration()"},
				{"reused_first_at_second", "currentCtx, started.RunID, currentRef", "currentCtx, started.RunID, firstRef"},
				{"wrong_generation", "join.Generation() == current.Generation()", "join.Generation() == first.Generation()"},
				{"missing_first_reread", "\t\t\tfirst, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n", ""},
				{"missing_second_reread", "\t\t\tcurrent, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)\n", ""},
				{"foreign_native_reader", "readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)", "readNativeRetainedRootJoins(t, foreignReader, started.RunID)"},
				{"foreign_event_role", "rt.ReceiverStateReader.(runtimebus.EventStore)", "foreignReader.(runtimebus.EventStore)"},
				{"missing_native_role", "\t\t\tif !hasNativeEvents || nativeEvents == nil {\n\t\t\t\tt.Fatal(\"retained join outcome requires its original selected event reader\")\n\t\t\t}\n", ""},
				{"missing_first_handoff", "\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n", ""},
				{"missing_second_handoff", "\t\t\twaitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)\n\t\t\tcurrent, sourceJoins", "\t\t\tcurrent, sourceJoins"},
				{"weakened_assertion", "len(join.Outputs) != 1", "len(join.Outputs) != 0"},
				{"changed_setup", "canonicalrouting.CopyForkLoopRetainedJoin(t)", "canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint(t)"},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					mutant := strings.Replace(row.Successor, mutation.before, mutation.after, 1)
					if mutant == row.Successor {
						t.Fatal("hostile mutation did not change source")
					}
					if err := validateRetainedJoinSuccessor(*row, mutant); err == nil {
						t.Fatal("hostile join composition admitted")
					}
					guard := *row
					guard.Before, guard.After, guard.Successor = row.Successor, row.Successor, ""
					if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
						t.Fatal("hostile caller accepted by finite rewrite")
					}
				})
			}
			t.Run("late_first_wait", func(t *testing.T) {
				mutant := strings.Replace(row.Successor, retainedJoinSuccessorEdits[0].block, "", 1)
				mutant = strings.Replace(mutant, retainedJoinSuccessorEdits[1].before, retainedJoinSuccessorEdits[1].before+retainedJoinSuccessorEdits[0].block, 1)
				if mutant == row.Successor || validateRetainedJoinSuccessor(*row, mutant) == nil {
					t.Fatal("late creating-generation wait admitted")
				}
			})
			t.Run("typed_reader_owner", proveRetainedJoinTypedReaderSource)
		})
	}
}
