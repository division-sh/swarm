package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func catalogReceiptRecipePreservesAssertions(row recipe) bool {
	source, valid := catalogDiagnosticProjectionPredecessor(row)
	if !valid {
		return false
	}
	row.After = source
	indent, first, contextExpr, refusal := "\t\t", "var subscriberID, outcome, sideEffects string", "testAuthorActivityContext(context.Background())", "h.t.Fatal(err)"
	values := "subscriberID, outcome := receipt.SubscriberID, receipt.Outcome\nrawFailure := []byte(receipt.Failure)\nif rawFailure == nil { rawFailure = []byte(\"null\") }"
	if row.Function == "assertHandlerOutcomeForEntity" {
		first, refusal = "var outcome string", "t.Fatal(err)"
		values = "outcome := receipt.Outcome\nfailure := []byte(receipt.Failure)\nif failure == nil { failure = []byte(\"{}\") }"
	} else if row.Function == "loadCatalogReceipt" {
		indent, first, contextExpr, refusal = "\t", "var subscriberID, outcome string", "h.ctx", "return nil, err"
		values = "subscriberID, outcome := receipt.SubscriberID, receipt.Outcome\nsideEffects := []byte(receipt.SideEffects)\nrawFailure := []byte(receipt.Failure)\nif rawFailure == nil { rawFailure = []byte(\"null\") }"
	} else if row.Function != "assertTriggerReceipt" {
		return false
	}
	bind := "\treader, err := h.catalogOperatorEventLister()\n\tif err != nil {\n\t\t" + refusal + "\n\t}\n"
	if strings.Count(row.After, bind) != 1 {
		return false
	}
	endToken := indent + "if err == sql.ErrNoRows"
	oldStart, oldEnd := strings.Index(row.Before, indent+first), strings.Index(row.Before, endToken)
	newStart, newEnd := strings.Index(row.After, indent+"receipt, err :="), strings.Index(row.After, endToken)
	if oldStart < 0 || oldEnd < oldStart || newStart < 0 || newEnd < newStart {
		return false
	}
	expected := "func witness() { receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(" + contextExpr + ", reader, strings.TrimSpace(eventID))\nif err == nil && !receipt.Found { err = sql.ErrNoRows }\n" + values + "\n}"
	want, err := canonicalFunction(expected)
	actual, parseErr := canonicalFunction("func witness() {" + row.After[newStart:newEnd] + "}")
	if err != nil || parseErr != nil || want != actual {
		return false
	}
	restored := strings.Replace(row.After, row.After[newStart:newEnd], row.Before[oldStart:oldEnd], 1)
	restored = strings.Replace(restored, bind, "", 1)
	want, err = canonicalFunction(row.Before)
	actual, parseErr = canonicalFunction(restored)
	return err == nil && parseErr == nil && want == actual
}

func TestNativeCatalogPipelineReceiptRecipesPreserveCompleteAssertionAndReplayTails(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-pipeline-receipt" {
			continue
		}
		count++
		if !catalogReceiptRecipePreservesAssertions(row) {
			t.Fatalf("%s changed exact read, NULL formatting, assertions or replay", row.Function)
		}
		for _, pair := range [][2]string{
			{"reader, strings.TrimSpace(eventID)", "foreignOwner, otherEventID"},
			{"if err == nil && !receipt.Found", "if !receipt.Found"},
			{"failure.Class", "failure.Detail.Code"}, {"failure.Detail.Attributes[key]", "failure.Detail.Attributes[otherKey]"},
			{"canonicalJSONBytes(sideEffects)", "canonicalJSONBytes(nil)"}, {"got != \"success\"", "got == \"success\""},
			{"if err != nil", "if false"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After != row.After && catalogReceiptRecipePreservesAssertions(mutant) {
				t.Fatalf("lost owner, refusal, failure attributes or replay data accepted: %s %v", row.Function, pair)
			}
		}
	}
	if count != 3 {
		t.Fatalf("pipeline receipt recipes=%d, want all three", count)
	}
}

func TestNativeCatalogPipelineReceiptOwnerPreservesExactLatestSelection(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "pipelinepersistence", "owner_operations.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"SELECT subscriber_id,outcome,COALESCE(side_effects::text,''),failure",
		"SELECT subscriber_id,outcome,COALESCE(side_effects,''),failure",
		"FROM event_receipts WHERE event_id=$1::uuid AND subscriber_type='platform'",
		"FROM event_receipts WHERE event_id=? AND subscriber_type='platform'",
		"AND (subscriber_id='pipeline' OR subscriber_id LIKE 'pipeline:%') ORDER BY processed_at DESC LIMIT 1",
		"var failure sql.NullString", "out.Found = true", "if failure.Valid", "out.Failure = make(json.RawMessage, len(failure.String))", "copy(out.Failure, failure.String)",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("latest receipt selection/raw NULL distinction missing %q", fragment)
		}
	}
}
