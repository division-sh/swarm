package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func family145SupplementalHistoryProofError(before, after string) error {
	oldAssertions, oldErr := family145NestedBusinessAssertions(before)
	newAssertions, newErr := runSetupAssertions(after)
	if oldErr != nil || newErr != nil || !slices.Equal(oldAssertions, newAssertions) {
		return fmt.Errorf("supplemental history lost a business assertion: %v/%v", oldErr, newErr)
	}
	for _, cut := range []string{
		"storetest.ReadSupplementalFanOutObligationHistory(ctx, selected, runID)",
		"key, raw := row.Key, row.Fact", "if fact.Kind != \"intent\"", "if key != previous",
		"previous != \"\" && versions != 2", "if versions == 1", "wantCursor = cardinality",
		"versions >= 2 || fact.Cursor != wantCursor || fact.Cardinality != cardinality",
		"observed != intents || versions != 2",
	} {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("supplemental history lost exact owner/key/order/cardinality cut: %s", cut)
		}
	}
	return nil
}

func TestFamily145SupplementalHistoryRetainsKeyMajorOrderAndCompleteVersionAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.File != "internal/runtime/conformance/fan_out_supplemental_helpers_test.go" || row.Function != "assertSupplementalHistory" {
			continue
		}
		if err := family145SupplementalHistoryProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{
			"ReadSupplementalFanOutObligationHistory", "versions != 2", "fact.Cursor != wantCursor",
			"fact.Cardinality != cardinality", "observed != intents", "key, raw := row.Key, row.Fact",
		} {
			if err := family145SupplementalHistoryProofError(row.Before, strings.ReplaceAll(row.After, cut, "foreignCut")); err == nil {
				t.Fatalf("lost physical history proof accepted: %s", cut)
			}
		}
		return
	}
	t.Fatal("finite supplemental history mapping disappeared")
}

func TestFamily145SupplementalFixtureAndEveryCallerHaveNoPoolOrQuerySelector(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	for _, path := range []string{
		"fan_out_supplemental_helpers_test.go", "fan_out_two_ten_suffix_test.go",
		"fan_out_twenty_intent_contention_test.go", "fan_out_pressure_reservation_test.go",
		"fan_out_pressure_soak_test.go", "fan_out_serving_capacity_conformance_test.go",
		"fan_out_staged_soak_test.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, "internal/runtime/conformance", path))
		if err != nil {
			t.Fatal(err)
		}
		if err := family145SupplementalEscape(data); err != nil {
			t.Fatalf("supplemental caller retains a pool/query escape: %s/%v", path, err)
		}
	}
	for _, escape := range []string{
		"f.db", "db.QueryRow(query)", "db.QueryContext(ctx, query)",
		"storetest.AdmitPostgresRuntimeStore(t, db)", "storetest.DatabaseForTest(selected)", "(*sql.DB)(nil)",
	} {
		if family145SupplementalEscape([]byte("package probe; func unsafe() { _ = "+escape+" }")) == nil {
			t.Fatalf("restored pool/query escape passed the family guard: %s", escape)
		}
	}
}

func family145SupplementalEscape(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "supplemental.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		member, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if slices.Contains([]string{"QueryRow", "QueryRowContext", "Query", "QueryContext", "AdmitPostgresRuntimeStore", "DatabaseForTest"}, member.Sel.Name) {
			err = fmt.Errorf("raw member %s", member.Sel.Name)
		}
		if owner, ok := member.X.(*ast.Ident); ok && ((owner.Name == "f" && member.Sel.Name == "db") || (owner.Name == "sql" && member.Sel.Name == "DB")) {
			err = fmt.Errorf("raw carrier %s", family145NodeString(member))
		}
		return true
	})
	return err
}
