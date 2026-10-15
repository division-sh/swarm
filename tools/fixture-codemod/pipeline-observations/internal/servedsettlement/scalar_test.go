package servedsettlement

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func scalarProofCall(t *testing.T, expression string) (*token.FileSet, *ast.CallExpr) {
	t.Helper()
	set := token.NewFileSet()
	node, err := parser.ParseExprFrom(set, "scalar.go", expression, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	call, ok := node.(*ast.CallExpr)
	if !ok {
		t.Fatal("counterexample is not a call")
	}
	return set, call
}

func TestServedScalarExactOwnersPreserveScopeDestinationAndContext(t *testing.T) {
	for _, proof := range []struct{ sql, args, result, port string }{
		{"SELECT COUNT(*) FROM runs", "", "int", "CountGlobalRunStorage"},
		{"SELECT COUNT(*) FROM entity_state WHERE run_id=$1", ",runID", "int", "CountWorkflowRunFieldRows"},
		{"SELECT COUNT(*) FROM events WHERE run_id = $1 AND event_id = $2", ",runID,eventID", "int", "CountRunExactEventStorage"},
		{"SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND current_state='Done.Case'", ",runID", "int", "CountRunStateEntityStorage"},
		{"SELECT status FROM runs WHERE run_id=$1", ",runID", "string", "ReadRunStatusStorage"},
	} {
		set, call := scalarProofCall(t, "rt.DB.QueryRowContext(originalContext,`"+proof.sql+"`"+proof.args+").Scan(&observed)")
		edit, port, err := ScalarCall(set, call, proof.result)
		if err != nil || port != proof.port {
			t.Fatal("exact query refused:", proof, err)
		}
		for _, cut := range []string{"rt.ReceiverStateReader.InspectSnapshot(originalContext", "observed,err=storetest." + port, "ctx,rt.ReceiverStateReader", "return err"} {
			if !strings.Contains(edit.After, cut) {
				t.Fatal("scalar splice lost context/owner/destination/error:", cut, edit.After)
			}
		}
		if strings.Contains(proof.sql, "Done.Case") && !strings.Contains(edit.After, `"Done.Case"`) {
			t.Fatal("literal state changed case")
		}
	}
}

func TestServedScalarRefusesDifferentQueriesDestinationsAndCarriers(t *testing.T) {
	for _, expression := range []string{
		"rt.DB.QueryRowContext().Scan(&count)",
		"rt.DB.QueryRow(query,run).Scan(&count)",
		"rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE status='running'`).Scan(&count)",
		"rt.DB.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&count,&other)",
		"rt.DB.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(count)",
		"rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`,run).Scan(&count)",
		"db.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&count)",
		"rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND current_state='done' OR 1=1`,run).Scan(&count)",
	} {
		set, call := scalarProofCall(t, expression)
		if _, _, err := ScalarCall(set, call, "int"); err == nil {
			t.Fatal("unclassified scalar accepted:", expression)
		}
	}
	set, call := scalarProofCall(t, "rt.DB.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&count)")
	if _, _, err := ScalarCall(set, call, "int64"); err == nil {
		t.Fatal("destination conversion hidden")
	}
}
