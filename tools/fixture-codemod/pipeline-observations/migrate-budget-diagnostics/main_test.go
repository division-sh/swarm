package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func budgetRepository(t *testing.T) string {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir("../../../.."); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(old); err != nil {
			t.Error(err)
		}
	})
	return old
}

func TestBudgetDiagnosticWholeRulePreservesEveryQueryAndCallerAssertion(t *testing.T) {
	budgetRepository(t)
	if err := inventory("."); err != nil {
		t.Fatal(err)
	}
	before := signed(sourcePath)
	current, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	after, pins := transformSource(before, current)
	if string(after) != string(current) || len(pins) != 2 {
		t.Fatal("current caller escaped its complete finite transform")
	}
	queries := sqlLiterals(declaration(before, "logBudgetNoticeFailure"))
	if len(queries) != 6 {
		t.Fatal("original diagnostic inventory changed")
	}
	for i, p := range projections {
		raw, err := os.ReadFile(p.File)
		if err != nil {
			t.Fatal(err)
		}
		name := "ReadChannelBudget" + p.Name + "DiagnosticsStorageTx"
		if got := sqlLiterals(declaration(raw, name)); !reflect.DeepEqual(got, []string{queries[i]}) {
			t.Fatalf("diagnostic scope/grouping/bounds changed: %s %q", name, got)
		}
		if p.ExistingScanner {
			raw, err = os.ReadFile("internal/store/internal/backend/effectpersistence/channel_settlement_diagnostics.go")
			if err != nil {
				t.Fatal(err)
			}
		}
		assertOriginalNullScanner(t, raw, p.ExistingScanner)
	}
	positive := sqlLiterals(declaration(before, "TestChannelDeliveryBudgetNoticePublicJourney"))
	for i, pair := range [][2]string{
		{"internal/store/internal/mailboxpersistence/channel_budget_diagnostics.go", "ReadChannelBudgetEmergencyNoticeStorageTx"},
		{"internal/store/internal/budgetspend/channel_budget_diagnostics.go", "CountChannelBudgetLiveCompletionsStorageTx"},
	} {
		raw, err := os.ReadFile(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		if got := sqlLiterals(declaration(raw, pair[1])); !reflect.DeepEqual(got, []string{positive[i]}) {
			t.Fatal("positive witness changed its original scope:", pair[1])
		}
	}
}

func assertOriginalNullScanner(t *testing.T, current []byte, existing bool) {
	t.Helper()
	name := "scanChannelBudgetDiagnosticRows"
	if existing {
		name = "scanChannelSettlementDiagnosticRows"
	}
	prior := declaration(signed("internal/serveapp/channel_delivery_settlement_diagnostic_test.go"), "readChannelDeliveryDiagnosticRows")
	after := declaration(current, name)
	prior = prior[strings.Index(prior, "defer rows.Close()"):]
	after = after[strings.Index(after, "defer rows.Close()"):]
	if prior != after {
		t.Fatal("diagnostic NULL omission, error, ordering or close behavior changed")
	}
}

func TestBudgetDiagnosticFiniteRuleRejectsChangedOrMissingSources(t *testing.T) {
	budgetRepository(t)
	before := signed(sourcePath)
	for _, cut := range []string{"20 * time.Second", "20 * time.Millisecond", "5*time.Second", "Budget emergency:%", "cost_usd >= 3", "TestChannelDeliveryBudgetNoticePublicJourney", "logBudgetNoticeFailure"} {
		t.Run(cut, func(t *testing.T) {
			changed := strings.Replace(string(before), cut, "foreignCut", 1)
			if changed == string(before) {
				t.Fatal("hostile control did not mutate its source")
			}
			defer func() {
				if recover() == nil {
					t.Fatal("foreign source received a finite rewrite")
				}
			}()
			transformSource(before, []byte(changed))
		})
	}
}

func TestBudgetDiagnosticInventoryIncludesExcludedAliasesAndPackageInitializers(t *testing.T) {
	for _, bad := range []string{
		"var alias = logBudgetNoticeFailure",
		"var initValue = func() int { logBudgetNoticeFailure(nil,nil,0);return 0 }()",
		"func init(){ logBudgetNoticeFailure(nil,nil,0) }",
	} {
		t.Run(bad, func(t *testing.T) {
			root := t.TempDir()
			writeBudgetTestFile(t, root, sourcePath, "package serveapp\nfunc logBudgetNoticeFailure(){}\nfunc TestChannelDeliveryBudgetNoticePublicJourney(){logBudgetNoticeFailure()}\n")
			if err := inventory(root); err != nil {
				t.Fatal(err)
			}
			writeBudgetTestFile(t, root, "internal/serveapp/excluded_darwin.go", "//go:build darwin\n\npackage serveapp\n"+bad)
			if err := inventory(root); err == nil {
				t.Fatal("owned excluded/indirect source disappeared from the caller inventory")
			}
		})
	}
}

func writeBudgetTestFile(t *testing.T, root, path, source string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
}
