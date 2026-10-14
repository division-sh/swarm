package main

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedeventcount"
)

//go:embed family145-served-event-count-preimages.json
var family145ServedEventCountBytes []byte

func servedEventCountPins(t *testing.T) []servedeventcount.Pin {
	t.Helper()
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ServedEventCountPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	return servedeventcount.Previous(servedEventCountPins(t), file, function, current)
}

func family145ServedEventCountCallerInverse(function, current string) string {
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return servedeventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckServedEventCountWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range servedEventCountPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("served event count escaped exact identity/assertion/cut:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ServedEventCountWholeRuleRetainsEveryScopeAndAssertion(t *testing.T) {
	calls, seen := 0, map[string]bool{}
	for _, pin := range servedEventCountPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("duplicate or changed served event count source:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", servedeventcount.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("served event count predecessor is not exact:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedObservationEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("served event count current source escaped its rule:", key, err)
		}
		after, count, err := servedeventcount.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("served event count transform changed its reviewed source:", key, err)
		}
		if again, count, err := servedeventcount.Transform(after); err != nil || count != 0 || again != after {
			t.Fatal("served event count transform is not idempotent:", key, err)
		}
		calls += count
		servedEventCountHostiles(t, pin)
	}
	if calls != 6 || len(seen) != 4 {
		t.Fatalf("served event count inventory=%d/%d want6/4", calls, len(seen))
	}
}

func servedEventCountHostiles(t *testing.T, pin servedeventcount.Pin) {
	t.Helper()
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	pins := servedEventCountPins(t)
	for _, cut := range []string{"readServedEventNameCountForRun", "ReceiverStateReader", "seed.RunID", "fork.ForkRunID", "runID", "name", "selected", "ack", "work.seeded", "mailbox.card_decided", "work.completed", "out.DecisionEvents", "out.OutcomeEvents", "publications", "copied", "count", " != ", " == ", "t.Fatal", "time.After", "time.Sleep", "kill()", "restart()", "stop()", "context.Background()"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignCountCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || servedeventcount.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("foreign context/identity/assertion/lifetime received inverse:", pin.Function, cut)
		}
	}
	if servedeventcount.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || servedeventcount.Previous(pins, pin.File, pin.Function+"Foreign", pin.After) != pin.After {
		t.Fatal("served event count inverse accepted a foreign complete key")
	}
}

func TestFamily145ServedEventCountTransformRefusesForeignBindingAndKeepsOuterDestination(t *testing.T) {
	const statement = "if err := recovered.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, runID, name).Scan(&count); err != nil || count != 2 { t.Fatal(err) }"
	const prefix = "func TestServedSelectionProcessCutsBothStores() { var count int; "
	before := prefix + statement + "; consume(count) }"
	after, calls, err := servedeventcount.Transform(before)
	if err != nil || calls != 1 || !strings.Contains(after, "count, err = ") || strings.Contains(after, "count, err := ") || !strings.Contains(after, "consume(count)") {
		t.Fatal("count result no longer updates its original destination:", after, err)
	}
	for _, mutant := range []string{
		strings.Replace(before, "recovered.DB.QueryRow", "foreign.DB.QueryRow", 1),
		strings.Replace(before, "&count", "&row.Count", 1),
		strings.Replace(before, "runID, name).Scan", "runID, name, extra).Scan", 1),
		strings.Replace(before, "err :=", "_ :=", 1),
		strings.Replace(before, "TestServedSelectionProcessCutsBothStores", "foreignConsumer", 1),
	} {
		if _, _, err := servedeventcount.Transform(mutant); err == nil {
			t.Fatal("unreviewed carrier/destination/arity/consumer accepted:", mutant)
		}
	}
}

func TestFamily145ServedEventCountWholeOwnedInventoryHasNoOldQuery(t *testing.T) {
	root, counts := "../../..", map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		return servedEventCountInventoryFile(path, file, counts)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range servedEventCountPins(t) {
		if counts[pin.Function] != pin.Calls {
			t.Fatalf("served event count calls %s=%d want%d", pin.Function, counts[pin.Function], pin.Calls)
		}
		delete(counts, pin.Function)
	}
	if len(counts) != 0 {
		t.Fatal("unreviewed served event-count callers:", counts)
	}
}

func servedEventCountInventoryFile(path string, file *ast.File, counts map[string]int) error {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		_, oldQueries, err := servedeventcount.Transform(requestkey.Node(fn))
		if err != nil || oldQueries != 0 {
			return fmt.Errorf("served event count raw query survived: %s/%s: %v", path, fn.Name.Name, err)
		}
		if servedeventcount.Consumers[fn.Name.Name] != 0 {
			counts[fn.Name.Name] += servedEventCountCalls(fn)
		}
	}
	return nil
}

func servedEventCountCalls(fn *ast.FuncDecl) int {
	count := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok && function.Name == "readServedEventNameCountForRun" {
				count++
			}
		}
		return true
	})
	return count
}

func TestFamily145ServedEventCountUsesOriginalOwnerAndUnchangedCanonicalQuery(t *testing.T) {
	const path = "internal/serveapp/mailbox_completion_test.go"
	original, err := exec.Command("git", "show", servedeventcount.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	before := requestKeyDeclaration(t, original, "gateCompletionHarness")
	const old = "SQLite: selectedSQLite}"
	const next = "SQLite: selectedSQLite, ReceiverStateReader: requireReceiverProofStateReader(t, projected)}"
	if strings.Count(before, old) != 1 {
		t.Fatal("original mailbox harness identity changed")
	}
	after, err := requestkey.Normalize(strings.Replace(before, old, next, 1))
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil || requestKeyDeclaration(t, current, "gateCompletionHarness") != after {
		t.Fatal("mailbox fixture substituted or reconstructed the selected reader:", err)
	}
	for _, query := range []string{
		"internal/store/internal/backend/eventrecord/run_name_cardinality.go",
		"internal/store/internal/runtimepersistence/test_event_support.go",
		"internal/store/storetest/run_fork.go",
	} {
		before, err := exec.Command("git", "show", servedeventcount.Predecessor+":"+query).Output()
		if err != nil {
			t.Fatal(err)
		}
		after, err := os.ReadFile(filepath.Join("../../..", query))
		if err == nil {
			after = servedObservationEarlierToolOwnerSource(t, query, after)
		}
		if err != nil || string(before) != string(after) {
			t.Fatal("canonical physical count was reinterpreted:", query, err)
		}
	}
}
