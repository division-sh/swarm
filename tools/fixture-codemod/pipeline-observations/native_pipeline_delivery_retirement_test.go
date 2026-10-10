package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func nativePipelineReopenObservationOrder(source string) error {
	remaining := source
	for _, step := range []string{
		"recoverNativePipelineRetryForTest(t, next, nextCtx)",
		"beforeReplay := next.Transactions()",
		"for attempt := 0; attempt < 2; attempt++",
		"next.PublishNode(nextCtx, event, route)",
		"next.JoinExecution(join)",
		"nextWork.ActiveCount() == 0",
		"next.Continuations.Retire(join)",
		"nextWork.ActiveCount(); active != 0",
		"afterReplay := next.Transactions()",
		"afterReplay.Active != 0",
		"afterReplay.Claims != beforeReplay.Claims",
	} {
		index := strings.Index(remaining, step)
		if index < 0 {
			return fmt.Errorf("native reopen observation omitted or reordered %s", step)
		}
		remaining = remaining[index+len(step):]
	}
	return nil
}

func TestNativePipelineReopenCountersFollowStandingWorkerJoin(t *testing.T) {
	source := selectedCausalObservationBody(t, "internal/runtime/pipeline/delivery_native_fixture_bridge_test.go", "VerifyNativePipelineDeliveryReopenUsesFreshOccurrenceAndOriginalReceiptsForTest")
	if err := nativePipelineReopenObservationOrder(source); err != nil {
		t.Fatal(err)
	}
	for _, step := range []string{
		"next.PublishNode(nextCtx, event, route)",
		"next.JoinExecution(join)",
		"next.Continuations.Retire(join)",
		"afterReplay.Active != 0",
		"afterReplay.Claims != beforeReplay.Claims",
	} {
		t.Run(step, func(t *testing.T) {
			if err := nativePipelineReopenObservationOrder(strings.Replace(source, step, "omittedProof", 1)); err == nil {
				t.Fatal("missing replay/join/counter obligation accepted")
			}
		})
	}
	sample := "afterReplay := next.Transactions()"
	early := strings.Replace(source, sample, "", 1)
	early = strings.Replace(early, "next.Continuations.Retire(join)", sample+"\nnext.Continuations.Retire(join)", 1)
	if err := nativePipelineReopenObservationOrder(early); err == nil {
		t.Fatal("global counter observation before standing-worker join accepted")
	}
}

func TestNativePipelineDeliverySharedTailRecipesStaySourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-pipeline-delivery-shared-tail-semantic" {
			continue
		}
		count++
		source := []byte("package probe\n" + row.Before)
		after, changed, err := rewriteFunction(row.File, source, row)
		if err != nil || !changed {
			t.Fatalf("finite native repair failed: %s/%v", row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, row); err != nil || changed {
			t.Fatalf("native repair is not idempotent: %s/%v", row.Function, err)
		}
		if _, _, err := rewriteFunction(row.File, bytes.Replace(source, []byte("{"), []byte("{ unreviewedEscape();"), 1), row); err == nil {
			t.Fatalf("changed native source admitted: %s", row.Function)
		}
		if row.Removed {
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("retired shared-tail capability survived: %s/%v", row.Function, err)
			}
			continue
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, nativePipelineRecipeReplacementName(t, row)))
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		want, afterErr := canonicalFunction(current)
		if err != nil || afterErr != nil || actual != want {
			t.Fatalf("native repair differs from its reviewed snapshot: %s/%v/%v", row.Function, err, afterErr)
		}
	}
	if count != 28 {
		t.Fatalf("native delivery shared-tail recipes=%d, want 28", count)
	}
}

func retainedPipelineJournalUnits(source string) (map[string]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "journal.go", "package probe\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return nil, fmt.Errorf("invalid journal declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Name.Name != "TestMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTable" {
		return nil, fmt.Errorf("retained journal declaration identity changed")
	}
	units := map[string]string{}
	duplicate := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "t.Run" || len(call.Args) != 2 {
			return true
		}
		name := formattedNativeReadNode(call.Args[0])
		if name != `"gate mutation"` && name != `"accumulator append"` {
			return true
		}
		if _, exists := units[name]; exists {
			duplicate = true
		}
		units[name] = formattedNativeReadNode(call)
		return true
	})
	if duplicate || len(units) != 2 {
		return nil, fmt.Errorf("retained journal subtest inventory changed")
	}
	return units, nil
}

func TestNativePipelineJournalSplitPreservesRetainedUnitsAndNativeFailure(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row recipe
	for _, candidate := range rows {
		if candidate.File == "internal/runtime/pipeline/mutation_logging_test.go" && candidate.Function == "TestMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTable" {
			row = candidate
		}
	}
	if row.Function == "" {
		t.Fatal("journal migration recipe missing")
	}
	before, err := retainedPipelineJournalUnits(row.Before)
	if err != nil {
		t.Fatal(err)
	}
	after, err := retainedPipelineJournalUnits(row.After)
	if err != nil {
		t.Fatal(err)
	}
	for name, original := range before {
		if after[name] != original {
			t.Fatalf("unmigrated journal unit or assertion changed: %s", name)
		}
	}
	mutant := strings.Replace(row.After, "assertEntityGates(t, db, entityID, map[string]any{})", "omitGateAssertion()", 1)
	changed, err := retainedPipelineJournalUnits(mutant)
	if err != nil || changed[`"gate mutation"`] == before[`"gate mutation"`] {
		t.Fatal("changed retained assertion escaped preservation proof")
	}
	if _, err := retainedPipelineJournalUnits(strings.Replace(row.After, row.Function, "RenamedJournalUnit", 1)); err == nil {
		t.Fatal("renamed retained debt identity accepted")
	}
	actual := selectedCausalObservationBody(t, row.File, "VerifyNativeMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTableForTest")
	for _, required := range []string{
		`[]string{"sqlite", "postgres"}`, "fixture.PublishNode(ctx, event, route)",
		"fixture.HideMutationTable(ctx, true)", "fixture.HideMutationTable(ctx, false)",
		"t.Cleanup(func()", "executeNativeClaimedPipelineHandlerForTest",
		`strings.Contains(err.Error(), "entity_mutations")`, `loaded.CurrentState != "queued"`,
		"len(upserts)+len(cancellations) != 0",
	} {
		if !strings.Contains(actual, required) {
			t.Fatalf("native journal failure cut or assertion absent: %s", required)
		}
	}
	root := selectedCausalObservationBody(t, "internal/runtime/pipeline/join_identity_native_external_test.go", "TestNativeMutationLoggedStateTransitionFailsClosedWithoutJournalBothStores")
	if !strings.Contains(root, "pipeline.VerifyNativeMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTableForTest(t, pipelineDeliveryNativeFixture)") {
		t.Fatal("both-store native journal execution root is disconnected")
	}
}

func pipelineDeliveryCapabilityEscapes(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "probe.go", source, 0)
	if err != nil {
		return nil, err
	}
	retired := map[string]bool{}
	for _, name := range strings.Fields("pipelineTestDeliveryOwner pipelineTestContinuationOwner pipelineTestContinuation newPipelineTestDeliveryOwner newPipelineTestDeliveryOwnerForDB openPipelineTestDeliveryOwner newPipelineTestContinuationOwner configurePipelineTestDeliveryOwner seedPipelineTestNodeDelivery installedWorkflowJoinDeliveryOwnerForTest withClaimedWorkflowNodePublicationForTest persistWorkflowJoinPublicationForTest executeClaimedWorkflowJoinForTest executePublishedWorkflowJoinForTest executeResolvedJoinForTest drivePublishedJoinCompletionForTest admitJoinTransitionOccurrenceForTest persistAdmittedJoinTransitionForTest newWorkflowJoinPipelineCoordinator prepareConstructorUnitDelivery newEmitPersistenceTestCoordinator seedQueryEntitiesGuardInstance assertCreatedChildFlowIdentityCoherent") {
		retired[name] = true
	}
	var escapes []string
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && retired[id.Name] {
			escapes = append(escapes, id.Name)
		}
		return true
	})
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		receiver := fn.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		id, ok := receiver.(*ast.Ident)
		if !ok || id.Name != "recordingPipelineBus" {
			continue
		}
		switch fn.Name.Name {
		case "DeliveryAuthority", "AcquireDeliveryContinuation", "RetainDeliveryContinuation", "ReleaseDeliveryContinuation":
			escapes = append(escapes, "recordingPipelineBus."+fn.Name.Name)
		}
	}
	return escapes, nil
}

func TestPipelineDeliveryFixtureCapabilityIsRetired(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "runtime", "pipeline"), func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return failure
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		escapes, err := pipelineDeliveryCapabilityEscapes(source)
		if len(escapes) != 0 {
			t.Errorf("retired delivery authority remains: %s/%v", path, escapes)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"type pipelineTestDeliveryOwner struct{}",
		"type pipelineTestContinuationOwner struct{}",
		"func probe(){ openPipelineTestDeliveryOwner(nil,false) }",
		"func probe(){ configurePipelineTestDeliveryOwner(nil,nil) }",
		"func (b *recordingPipelineBus) DeliveryAuthority() {}",
		"func (b *recordingPipelineBus) AcquireDeliveryContinuation() {}",
		"func (b recordingPipelineBus) RetainDeliveryContinuation() {}",
		"func (b recordingPipelineBus) ReleaseDeliveryContinuation() {}",
	} {
		if escapes, err := pipelineDeliveryCapabilityEscapes([]byte("package pipeline\n" + source)); err != nil || len(escapes) != 1 {
			t.Fatalf("delivery capability mutant escaped: %s/%v/%v", source, escapes, err)
		}
	}
	if _, err := pipelineDeliveryCapabilityEscapes([]byte("package pipeline\nfunc")); err == nil {
		t.Fatal("incomplete capability source accepted")
	}
}
