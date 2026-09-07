package worklifetime

import (
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func inventoryFixture(t *testing.T, source string) map[string][]int {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]int{}
	collectAsyncSites(fset, file, "fixture.go", found)
	return found
}

func TestAsyncSiteInventoryIgnoresLineMovement(t *testing.T) {
	const source = `package fixture
func launch() {
	go work()
}
`
	ledger := map[string]asyncSiteLedgerEntry{
		"go|fixture.go|launch": {1, asyncSiteSynchronousJoin, "caller joins the worker"},
	}
	original := inventoryFixture(t, source)
	moved := inventoryFixture(t, "// An unrelated comment.\n\n"+source)
	if reflect.DeepEqual(original, moved) {
		t.Fatal("fixture did not move the diagnostic source lines")
	}
	for _, found := range []map[string][]int{original, moved} {
		if err := compareAsyncSiteLedger(found, ledger); err != nil {
			t.Fatalf("line-only change invalidated inventory: %v", err)
		}
	}
}

func TestAsyncSiteInventoryRequiresExactMultiplicity(t *testing.T) {
	ledger := map[string]asyncSiteLedgerEntry{
		"go|fixture.go|launch": {2, asyncSiteSynchronousJoin, "caller joins both workers"},
	}
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{"separate lines", "go first()\ngo second()", ""},
		{"same line", "go first(); go second()", ""},
		{"added launch", "go first(); go second(); go third()", "expected 2, found 3 (lines [2 2 2])"},
		{"removed launch", "go first()", "expected 2, found 1 (lines [2])"},
		{"removed all launches", "", "missing ledger sites:\ngo|fixture.go|launch: expected 2, found 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			found := inventoryFixture(t, "package fixture\nfunc launch() { "+test.body+" }")
			err := compareAsyncSiteLedger(found, ledger)
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("inventory error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAsyncSiteInventoryKeepsFunctionsAndReceiversDistinct(t *testing.T) {
	found := inventoryFixture(t, `package fixture
func launch() { go work() }
func (a *First) launch() { go work(); go work() }
func (b Second) launch() { go work() }
`)
	ledger := map[string]asyncSiteLedgerEntry{
		"go|fixture.go|launch":        {1, asyncSiteSynchronousJoin, "function joins worker"},
		"go|fixture.go|First.launch":  {2, asyncSiteCanonicalOwner, "First owns both workers"},
		"go|fixture.go|Second.launch": {1, asyncSiteCanonicalOwner, "Second owns worker"},
	}
	if err := compareAsyncSiteLedger(found, ledger); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncSiteInventoryCountsSupportedCallbacks(t *testing.T) {
	const source = `package fixture
import ctx "context"
func launch() {
	ctx.AfterFunc(parent, callback); ctx.AfterFunc(parent, callback)
	QueuePipelinePostCommitAction(callback)
	queuePipelinePostCommitAction(callback)
	owner.QueuePipelineRollbackAction(callback)
	owner.queuePipelineRollbackAction(callback)
}
`
	ledger := map[string]asyncSiteLedgerEntry{
		"after_func|fixture.go|launch":   {2, asyncSiteCanonicalOwner, "owner releases cancellation bridges"},
		"owner_action|fixture.go|launch": {4, asyncSiteCanonicalOwner, "transaction owns registered actions"},
	}
	if err := compareAsyncSiteLedger(inventoryFixture(t, source), ledger); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		remove string
		want   string
	}{
		{"cancellation callback", "ctx.AfterFunc(parent, callback); ", "expected 2, found 1"},
		{"owner action", "owner.queuePipelineRollbackAction(callback)", "expected 4, found 3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := strings.Replace(source, test.remove, "", 1)
			err := compareAsyncSiteLedger(inventoryFixture(t, changed), ledger)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("inventory error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAsyncSiteInventoryRejectsStaleAndInvalidClassifications(t *testing.T) {
	const key = "go|fixture.go|launch"
	found := inventoryFixture(t, "package fixture\nfunc launch() { go work() }")
	for _, test := range []struct {
		name  string
		entry asyncSiteLedgerEntry
	}{
		{"zero count", asyncSiteLedgerEntry{0, asyncSiteCanonicalOwner, "owner joins work"}},
		{"negative count", asyncSiteLedgerEntry{-1, asyncSiteCanonicalOwner, "owner joins work"}},
		{"unknown class", asyncSiteLedgerEntry{1, asyncSiteClass("unknown"), "owner joins work"}},
		{"missing rationale", asyncSiteLedgerEntry{1, asyncSiteCanonicalOwner, " \t\n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := compareAsyncSiteLedger(found, map[string]asyncSiteLedgerEntry{key: test.entry})
			if err == nil || !strings.Contains(err.Error(), "invalid ledger entries (require positive count, known class, and rationale):\n"+key) {
				t.Fatalf("invalid classification error = %v", err)
			}
		})
	}
	err := compareAsyncSiteLedger(found, map[string]asyncSiteLedgerEntry{
		"go|fixture.go|retired": {1, asyncSiteCanonicalOwner, "retired worker"},
	})
	for _, want := range []string{
		"unclassified async sites:\ngo|fixture.go|launch: found 1 (lines [2])",
		"missing ledger sites:\ngo|fixture.go|retired: expected 1, found 0",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("stale inventory error = %v, want %q", err, want)
		}
	}
}
