package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func family145CompetingRevisionPairOwner(file, function string) string {
	if function != "CommitWorkflowEngineMutation" {
		return ""
	}
	switch file {
	case "internal/runtime/pipeline/selection_retry_cas_external_test.go":
		return "AdvanceSelectionRevisionPair"
	case "internal/runtime/pipeline/prospective_publication_external_test.go":
		return "AdvanceProspectiveRevisionPair"
	default:
		return ""
	}
}

func family145CompetingRevisionPairProofError(owner, before, after string) error {
	indent, successor := "\t\t", "p.fired = true"
	if owner == "AdvanceProspectiveRevisionPair" {
		indent, successor = "\t\t\t", "p.raced = true"
	}
	start := strings.Index(before, indent+"tx, err := p.db.BeginTx(ctx, nil)")
	end := strings.Index(before, indent+successor)
	if start < 0 || end <= start {
		return fmt.Errorf("competing writer lost original transaction cut: %s", owner)
	}
	resultType := "pipeline.CommittedWorkflowEngineMutation"
	if owner == "AdvanceProspectiveRevisionPair" {
		resultType = "runtimepipeline.CommittedWorkflowEngineMutation"
	}
	replacement := indent + "if err := storetest." + owner + "(ctx, p.selected, command.State); err != nil {\n" + indent + "\treturn " + resultType + "{}, err\n" + indent + "}\n"
	if before[:start]+replacement+before[end:] != after {
		return fmt.Errorf("competing writer changed original conditions, counts, capture, call order or settlement: %s", owner)
	}
	return nil
}

func TestFamily145CompetingRevisionPairsPreserveExactStateMachineAndSQL(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		owner := family145CompetingRevisionPairOwner(row.File, row.Function)
		if owner == "" {
			continue
		}
		found++
		if err := family145CompetingRevisionPairProofError(owner, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"ctx, p.selected, command.State", "p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)", "p.err = err"} {
			if family145CompetingRevisionPairProofError(owner, row.Before, strings.Replace(row.After, cut, "foreignCut", 1)) == nil {
				t.Fatalf("changed original owner/state/window passed: %s", cut)
			}
		}
		assertFamily145CompetingRevisionPhysicalCut(t, owner, row.Before)
	}
	if found != 2 {
		t.Fatalf("competing-writer consumer inventory=%d, want 2", found)
	}
	for _, row := range []struct{ path, function, cut string }{
		{"../runtime/pipeline/selection_retry_cas_external_test.go", "TestSelectionRetryAfterRealCASConflictBothStores", "&selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), selected: selected.events}"},
		{"../runtime/pipeline/prospective_publication_external_test.go", "TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores", "&observedProspectivePersistence{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner), mode: name, selected: selected.events}"},
	} {
		source := family145ConversationForkOwnerSource(t, row.path, row.function)
		if strings.Count(source, row.cut) != 1 {
			t.Fatalf("competing writer lost original selected owner: %s", row.function)
		}
	}
}

func assertFamily145CompetingRevisionPhysicalCut(t *testing.T, owner, before string) {
	t.Helper()
	start := strings.Index(before, "result, err := tx.ExecContext")
	end := strings.Index(before, "if err := tx.Commit()")
	if start < 0 || end <= start {
		t.Fatalf("missing original paired writer: %s", owner)
	}
	expected := strings.ReplaceAll(before[start:end], "command.State.", "state.")
	expected = strings.ReplaceAll(expected, "return pipeline.CommittedWorkflowEngineMutation{}, ", "return ")
	expected = strings.ReplaceAll(expected, "return runtimepipeline.CommittedWorkflowEngineMutation{}, ", "return ")
	expected = family145CompetingRevisionStatements(t, "func proof(){\n"+expected+"return nil\n}")
	physical := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/competing_revision_pair_fixture.go", owner+"Tx")
	if actual := family145CompetingRevisionStatements(t, physical); expected != actual {
		t.Fatalf("%s changed SQL, arguments, cardinality/error or statement ordering\nwant:\n%s\ngot:\n%s", owner, expected, actual)
	}
	for _, hostile := range []string{
		strings.Replace(physical, "revision=revision+1", "revision=revision+2", 1),
		strings.Replace(physical, "state.EntityID", "state.Identity.RunID", 1),
		strings.Replace(physical, "result.RowsAffected()", "foreignCardinality()", 1),
	} {
		if expected == family145CompetingRevisionStatements(t, hostile) {
			t.Fatal("changed paired SQL scope, clock or cardinality passed")
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_competing_revision_pair.go", owner+"ForTest")
	if strings.Count(bridge, "return runWorkflowProjectionFault(ctx, selected,") != 1 || strings.Count(bridge, "return pipelinepersistence."+owner+"Tx(ctx, tx, state)") != 1 {
		t.Fatalf("%s escaped original private selected fault coordinator", owner)
	}
	public := family145ConversationForkOwnerSource(t, "storetest/competing_revision_pair.go", owner)
	if strings.Count(public, "return private."+owner+"ForTest(ctx, selected, state)") != 1 {
		t.Fatalf("%s public port changed exact operation or coordinates", owner)
	}
}

func family145CompetingRevisionStatements(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "paired.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	var out []string
	for _, statement := range fn.Body.List {
		out = append(out, family145NodeString(statement))
	}
	return strings.Join(out, "\n")
}
