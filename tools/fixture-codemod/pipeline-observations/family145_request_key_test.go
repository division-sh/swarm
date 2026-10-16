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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

//go:embed family145-request-key-preimages.json
var family145RequestKeyBytes []byte

func requestKeyPins(t *testing.T) []requestkey.Pin {
	t.Helper()
	pins, err := requestkey.Decode(family145RequestKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145RequestKeyCallerInverse(function, current string) string {
	pins, err := requestkey.Decode(family145RequestKeyBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			current = family145RequestKeyCurrentPrevious(pin.File, function, current)
			return requestkey.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckRequestKeyWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145RequestKeyCurrentPrevious(file, function, current)
	for _, pin := range requestKeyPins(t) {
		if pin.File == file && strings.Split(pin.Function, "/")[0] == function {
			if current != pin.After {
				t.Fatal("request-key observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145RequestKeyWholeChainRetainsPredicatesAssertionsAndCuts(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	calls := 0
	seen := map[string]bool{}
	for _, pin := range requestKeyPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate request-key predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", requestkey.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("request-key source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || family145RequestKeyCurrentPrevious(pin.File, pin.Function, proof.previous(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))) != pin.After {
			t.Fatal("request-key current source escaped its whole rule:", key, err)
		}
		after, count, err := requestkey.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("request-key finite transform escaped its rule:", key, err)
		}
		calls += count
		requestKeyHostiles(t, pin)
	}
	if calls != 39 || len(seen) != 28 {
		t.Fatalf("request-key inventory=%d/%d want39/28", calls, len(seen))
	}
}

func requestKeyDeclaration(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "request-key.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && requestkey.Key(fn) == name {
			return requestkey.Node(fn)
		}
	}
	return ""
}

func requestKeyHostiles(t *testing.T, pin requestkey.Pin) {
	t.Helper()
	pins := requestKeyPins(t)
	if requestkey.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || requestkey.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("request-key inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "ReadAPIIdempotencyCardinality", "ReadEventIdempotencyCardinality", "CountSourceEventPublications", "ReceiverStateReader", "reader", "captureSelectedRuntimePersistence", "requireReceiverProofStateReader", "p.deps.EventStore", "idempotencyKey", "key", "method", "got !=", "count !=", "n !=", "t.Fatal", "0", "want", "requirePublicInputRollbackNoResidue", "startPublicInputRollbackRuntime"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedRequestKeyCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || requestkey.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed request-key predicate/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145RequestKeyRollbackLiftPreservesSevenRawCountsAndImmediateCut(t *testing.T) {
	for _, pin := range requestKeyPins(t) {
		switch pin.Function {
		case "requirePublicInputRollbackNoResidue":
			before := strings.Replace(pin.Before, "idempotencyKey, eventID, runID string", "eventID, runID string", 1)
			at := strings.Index(before, "\tif got := servedEventPublishAPIIdempotencyCount(")
			if at < 0 || before[:at]+"}" != pin.After || !strings.Contains(pin.After, "db.QueryRowContext(") {
				t.Fatal("rollback changed retained raw counts/arguments/assertions")
			}
		case "TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores", "TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores":
			verifyRequestKeyRollbackAdjacency(t, pin.After)
		}
	}
}

func TestFamily145RequestKeyOwnedInventoryRetiresEveryRawCountCaller(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	root := "../../.."
	pins := requestKeyPins(t)
	repairSource, err := os.ReadFile(filepath.Join(root, requestKeyRepairCallerPath))
	if err != nil || !requestKeyRepairCaller(t, requestKeyRepairCallerPath, requestKeyRepairCallerName, requestKeyDeclaration(t, repairSource, requestKeyRepairCallerName)) {
		t.Fatal("exact new wrong-template rollback counterexample disappeared:", err)
	}
	helpers := map[string]bool{"servedEventPublishAPIIdempotencyCount": true, "servedEventPublishEventCountByIdempotencyKey": true,
		"servedEventPublishSourcedEventCount": true, "requireServedControlAPIIdempotencyRows": true,
		"requireServedReplayNoDeliveryHistoryNoMutation": true, "requirePublicInputRollbackNoResidue": true}
	calls, definitions := 0, 0
	err = checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fileCalls, fileDefinitions := requestKeyOwnedFile(t, pins, helpers, filepath.ToSlash(relative), file, proof)
		calls += fileCalls
		definitions += fileDefinitions
		return nil
	})
	if err != nil || calls != 40+2 || definitions != 6 {
		t.Fatalf("request-key complete owned inventory=%d/%d want40 historical + 2 exact new rollback calls / 6 owners err=%v", calls, definitions, err)
	}
}

func requestKeyOwnedFile(t *testing.T, pins []requestkey.Pin, helpers map[string]bool, path string, file *ast.File, proof sourceBlobDeclarationInvocation) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				if requestKeyRepairCaller(t, path, fn.Name.Name, requestkey.Node(fn)) {
					return true
				}
				current := family145RequestKeyCurrentPrevious(path, fn.Name.Name, proof.previous(t, path, fn.Name.Name, requestkey.Node(fn)))
				if requestkey.Previous(pins, path, fn.Name.Name, current) == current {
					t.Error("request-key caller escaped complete source-pinned owner chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("request-key helper escaped as alias/initializer/unclassified reference:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

const requestKeyRepairCallerPath = "internal/serveapp/scenario_selected_fixture_test.go"
const requestKeyRepairCallerName = "TestPublicInputPublicationFaultRefusesWrongTemplateBothStores"

func requestKeyRepairCaller(t *testing.T, path, name, current string) bool {
	t.Helper()
	if path != requestKeyRepairCallerPath || name != requestKeyRepairCallerName {
		return false
	}
	signed, err := exec.Command("git", "show", selectedreadrole.Repair2698After+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return current != "" && current == requestKeyDeclaration(t, signed, name)
}

func TestFamily145RequestKeyNewRollbackCounterexampleCannotHideAnExtraOrChangedCaller(t *testing.T) {
	raw, err := os.ReadFile("../../../" + requestKeyRepairCallerPath)
	if err != nil {
		t.Fatal(err)
	}
	current := requestKeyDeclaration(t, raw, requestKeyRepairCallerName)
	if !requestKeyRepairCaller(t, requestKeyRepairCallerPath, requestKeyRepairCallerName, current) {
		t.Fatal("named physical rollback counterexample lost its complete signed source")
	}
	for _, cut := range []string{"requirePublicInputRollbackNoResidue", "servedEventPublishAPIIdempotencyCount", "wrong-template-proof", "count != 0", "stage.marker"} {
		mutant := strings.Replace(current, cut, "unreviewedRollbackCut", 1)
		if mutant == current || requestKeyRepairCaller(t, requestKeyRepairCallerPath, requestKeyRepairCallerName, mutant) {
			t.Fatal("changed native rollback owner/oracle accepted:", cut)
		}
	}
	extra := strings.Replace(current, "{", "{ servedEventPublishAPIIdempotencyCount(nil,nil,\"\",\"\",\"\");", 1)
	if requestKeyRepairCaller(t, requestKeyRepairCallerPath, requestKeyRepairCallerName, extra) ||
		requestKeyRepairCaller(t, "foreign.go", requestKeyRepairCallerName, current) ||
		requestKeyRepairCaller(t, requestKeyRepairCallerPath, "foreignFunction", current) {
		t.Fatal("extra or foreign canonical helper call hid behind the new counterexample")
	}
}

func verifyRequestKeyRollbackAdjacency(t *testing.T, source string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "cut.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, stmt := range block.List {
			if requestkey.Node(stmt) == "requirePublicInputRollbackNoResidue(t, db, backend, eventID, runID)" {
				if i+1 >= len(block.List) || requestkey.Node(block.List[i+1]) != requestkey.RollbackAssertion {
					t.Error("API-zero observation crossed a fault/cleanup/lifecycle cut")
				}
				found++
			}
		}
		return true
	})
	if found != 1 {
		t.Fatal("missing/duplicate retained rollback helper:", found)
	}
}
