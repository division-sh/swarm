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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
)

//go:embed family145-run-scalar-preimages.json
var family145RunScalarBytes []byte

func runScalarPins(t *testing.T) []runscalar.Pin {
	t.Helper()
	pins, err := runscalar.Decode(family145RunScalarBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145RunScalarCallerInverse(function, current string) string {
	current = family145DeliveryStatusCallerInverse(function, current)
	pins, err := runscalar.Decode(family145RunScalarBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return runscalar.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckRunScalarWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145DeliveryStatusCallerInverse(function, current)
	for _, pin := range runScalarPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("run scalar observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145RunScalarWholeChainPreservesPredicatesAssertionsAndCuts(t *testing.T) {
	calls := 0
	seen := map[string]bool{}
	for _, pin := range runScalarPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate scalar predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", runscalar.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("scalar source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		entry := servedObservationEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))
		if family145DeliveryStatusCallerInverse(pin.Function, entry) != pin.After {
			t.Fatal("scalar current source escaped its whole rule:", key, err)
		}
		after, count, err := runscalar.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("scalar finite transform escaped its rule:", key, err)
		}
		calls += count
		runScalarHostiles(t, pin)
	}
	if calls != 9 || len(seen) != 6 {
		t.Fatalf("scalar inventory=%d/%d want9/6", calls, len(seen))
	}
}

func runScalarHostiles(t *testing.T, pin runscalar.Pin) {
	t.Helper()
	pins := runScalarPins(t)
	if runscalar.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || runscalar.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("scalar inverse admitted foreign/missing source")
	}
	if pin.After == "" {
		if pin.Function != "servedEventPublishRowCount" {
			t.Fatal("unexpected retired scalar helper")
		}
		return
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "CountPhysicalRunIdentity", "CountApplicationRunEvents", "CountExactRunEventDeliveries", "reader", "captureSelectedRuntimePersistence", "observer", "eventID", "runID", "backend", "scope", "t.Fatal", "got !=", " != ", "return", "requireServedPublicationPossessionLossAbort", "commitLost"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedScalarCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || runscalar.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed scalar predicate/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145RunScalarOwnedInventoryRetiresAliasAndEveryRawCaller(t *testing.T) {
	root := "../../.."
	calls, definitions := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		n, d := runScalarOwnedFile(t, filepath.ToSlash(rel), file)
		calls += n
		definitions += d
		return nil
	})
	if err != nil || calls != 8 || definitions != 1 {
		t.Fatalf("scalar complete owned inventory=%d/%d want8/1 err=%v", calls, definitions, err)
	}
}

func TestFamily145RunScalarTransformRefusesMissingReadOwnerAndShadowedCalls(t *testing.T) {
	for _, source := range []string{
		`func probe(t *testing.T, db *sql.DB) { servedEventPublishScalarCount(t, db, "sqlite", "runs", run, "") }`,
		`func probe(t *testing.T, reader receiverProofStateReader, db *sql.DB) { servedEventPublishScalarCount := foreign; servedEventPublishScalarCount(t, db, "sqlite", "runs", run, "") }`,
		`func probe(t *testing.T, reader receiverProofStateReader, db *sql.DB) { servedEventPublishScalarCount(t, db, "sqlite", "runs", run) }`,
		`func probe(t *testing.T, reader receiverProofStateReader, db *sql.DB) { servedEventPublishRowCount(other, db, "sqlite", "runs", run, "") }`,
		`func unrelated(t *testing.T, observer *sql.DB) { var reader receiverProofStateReader; servedEventPublishScalarCount(t, observer, "postgres", "runs", run, "") }`,
	} {
		if _, _, err := runscalar.Transform(source); err == nil {
			t.Fatal("unclassified scalar authority became a mechanical transform:", source)
		}
	}
}

func runScalarOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "servedEventPublishScalarCount" {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && id.Name == "servedEventPublishScalarCount" {
				calls++
				allowed[id.Pos()] = true
				current := servedObservationEarlierSource(t, path, fn.Name.Name, requestkey.Node(fn))
				current = family145DeliveryStatusCallerInverse(fn.Name.Name, current)
				if runscalar.Previous(runScalarPins(t), path, fn.Name.Name, current) == current {
					t.Error("scalar caller escaped complete source-pinned chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && (id.Name == "servedEventPublishRowCount" || id.Name == "servedEventPublishScalarCount" && !allowed[id.Pos()]) {
			t.Error("scalar helper escaped as retired reference/alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

func TestFamily145RunScalarSharedRunOwnerRetainsValidationAndExactQuery(t *testing.T) {
	for path, name := range map[string]string{"internal/store/internal/runtimepersistence/test_api_event_storage.go": "CountPhysicalRunIdentityForTest", "internal/store/internal/runtimepersistence/test_notify_execution_storage.go": "ReadNotifyRunPresenceForTest"} {
		raw, err := os.ReadFile(filepath.Join("../../..", path))
		before, priorErr := exec.Command("git", "show", runscalar.Predecessor+":"+path).Output()
		if err != nil || priorErr != nil {
			t.Fatal(err, priorErr)
		}
		expected := requestKeyDeclaration(t, before, name)
		if name == "CountPhysicalRunIdentityForTest" {
			expected = strings.Replace(expected, "return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&count)", "var err error\n\t\tcount, err = runlifecycle.CountRunIdentityStorageTx(ctx, tx, runID)\n\t\treturn err", 1)
		} else {
			expected = strings.Replace(expected, "return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&out)", "var err error\n\t\tout, err = runlifecycle.CountRunIdentityStorageTx(ctx, tx, runID)\n\t\treturn err", 1)
		}
		if expected != requestKeyDeclaration(t, raw, name) {
			t.Fatal("shared run-count bridge changed validation/transaction/errors:", path)
		}
	}
	for path, predicate := range map[string]string{
		"internal/store/internal/backend/runlifecycle/run_identity_cardinality.go": "SELECT COUNT(*) FROM runs WHERE run_id=$1",
		"internal/store/internal/backend/eventrecord/run_name_cardinality.go":      "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name NOT LIKE 'platform.%'",
		"internal/store/internal/backend/delivery/read_projections.go":             "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2",
	} {
		raw, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil || !strings.Contains(string(raw), predicate) {
			t.Fatal("exact scalar predicate missing:", path, err)
		}
	}
}
