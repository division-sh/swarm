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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-event-name-preimages.json
var family145EventNameBytes []byte

func eventNamePins(t *testing.T) []eventname.Pin {
	t.Helper()
	pins, err := eventname.Decode(family145EventNameBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145EventNameCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := eventname.Decode(family145EventNameBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return eventname.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckEventNameWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range eventNamePins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("event name observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145EventNameWaitStatementsAndCanonicalCountOwnersArePreserved(t *testing.T) {
	for _, pin := range eventNamePins(t) {
		if pin.Function != "requireServedEventNameCount" {
			continue
		}
		after := strings.Replace(pin.Before, "servedEventNameCount(t, db,", "servedEventNameCount(t, reader,", 1)
		after = strings.Replace(after, "db *sql.DB", "reader receiverProofStateReader", 1)
		if after != pin.After {
			t.Fatal("event-name polling/deadline/assertion changed")
		}
	}
	const eventSupport = "internal/store/internal/runtimepersistence/test_event_support.go"
	const apiSupport = "internal/store/internal/runtimepersistence/test_api_event_storage.go"
	oldRun := eventNameOriginalDeclaration(t, eventSupport, "CountRunEventNameStorageForTest")
	newRun := eventNameCurrentDeclaration(t, eventSupport, "CountRunEventNameStorageForTest")
	if strings.Replace(oldRun, "eventrecord.CountRunEventNameStorage", "eventrecord.CountRunNamedEventStorageTx", 1) != newRun {
		t.Fatal("run/name bridge changed its original validation/transaction/error contract")
	}
	oldGlobal := eventNameOriginalDeclaration(t, apiSupport, "CountEventNameStorageForTest")
	newGlobal := eventNameCurrentDeclaration(t, eventSupport, "CountEventNameStorageForTest")
	expected := strings.Replace(oldGlobal,
		"return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name=$1`, eventName).Scan(&count)",
		"var err error\ncount, err = eventrecord.CountGlobalNamedEventStorageTx(ctx, tx, eventName)\nreturn err", 1)
	expected, err := requestkey.Normalize(expected)
	if err != nil || expected != newGlobal {
		t.Fatal("global/name bridge changed its original validation/transaction/error contract:", err)
	}
	const query = "internal/store/internal/backend/eventrecord/run_name_cardinality.go"
	if eventNameOriginalDeclaration(t, query, "CountRunNamedEventStorageTx") != eventNameCurrentDeclaration(t, query, "CountRunNamedEventStorageTx") {
		t.Fatal("existing exact run/name canonical query changed")
	}
	oldLifecycle := eventNameOriginalDeclaration(t, "internal/store/internal/runtimepersistence/test_lifecycle_storage.go", "ReadLifecycleEventCardinalityForTest")
	newLifecycle := eventNameCurrentDeclaration(t, eventSupport, "ReadLifecycleEventCardinalityForTest")
	expected = strings.Replace(oldLifecycle,
		"return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1 AND event_name = $2`, runID, eventName).Scan(&count)",
		"var err error\ncount, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)\nreturn err", 1)
	expected, err = requestkey.Normalize(expected)
	if err != nil || expected != newLifecycle {
		t.Fatal("lifecycle/name bridge changed its original validation/transaction/error contract:", err)
	}
	if _, err := os.Stat(filepath.Join("../../..", "internal/store/internal/runtimepersistence/test_lifecycle_storage.go")); !os.IsNotExist(err) {
		t.Fatal("independent singleton lifecycle/name query remains:", err)
	}
	for _, path := range []string{apiSupport, "internal/store/internal/backend/eventrecord/causal_observation.go"} {
		raw, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil || strings.Contains(string(raw), "func CountEventNameStorageForTest(") || strings.Contains(string(raw), "func CountRunEventNameStorage(") {
			t.Fatal("duplicate/raw event-name query remains:", path, err)
		}
	}
}

func eventNameOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", eventname.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func eventNameCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	return sourceBlobPreviousDeclaration(t, path, function, requestKeyDeclaration(t, raw, function))
}

func TestFamily145EventNameTransformRefusesForeignCarriersAndShadowedCalls(t *testing.T) {
	for _, before := range []string{
		`func foreign(t *testing.T) { servedEventNameCount(t, foreign.DB, "sqlite", "x") }`,
		`func foreign(t *testing.T) { servedEventNameCount(t, db, "sqlite", "x") }`,
		`func foreign(t *testing.T) { servedEventNameCount(t, rt.DB, "sqlite", "x", "extra") }`,
		`func foreign(t *testing.T) { servedEventNameCount := foreign; servedEventNameCount(t, rt.DB, "sqlite", "x") }`,
	} {
		if _, _, err := eventname.Transform(before); err == nil {
			t.Fatal("unclassified carrier/arity/owner accepted:", before)
		}
	}
}

func TestFamily145EventNameWholeChainPreservesEveryPollAndRefusalCut(t *testing.T) {
	calls := 0
	seen := map[string]bool{}
	for _, pin := range eventNamePins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate event name predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", eventname.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("event name source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || sourceBlobPreviousDeclaration(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("event name current source escaped its whole rule:", key, err)
		}
		after, count, err := eventname.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("event name finite transform escaped its rule:", key, err)
		}
		calls += count
		eventNameHostiles(t, pin)
	}
	if calls != 14 || len(seen) != 6 {
		t.Fatalf("event name inventory=%d/%d want14/6", calls, len(seen))
	}
}

func eventNameHostiles(t *testing.T, pin eventname.Pin) {
	t.Helper()
	pins := eventNamePins(t)
	if eventname.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || eventname.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("event name inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "CountEventNameStorage", "CountRunNamedEventStorage", "reader", "ReceiverStateReader", "eventID", "runID", "statuses...", "status", "subscriberID", "subscriberType", "t.Fatal", " != ", " == ", "got == want", "servedProofPollDeadline", "duration", "25 * time.Millisecond", "servedInspectionRunDebugSummary", "waitForReady", "captureSelectedRuntimePersistence", "releaseOnce.Do", "250*time.Millisecond"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || eventname.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed event name identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145EventNameOwnedInventoryHasNoRawCallerOrAlias(t *testing.T) {
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
		n, d := eventNameOwnedFile(t, filepath.ToSlash(rel), file)
		calls += n
		definitions += d
		return nil
	})
	if err != nil || calls != 14 || definitions != 3 {
		t.Fatalf("event name complete inventory=%d/%d want14/3 err=%v", calls, definitions, err)
	}
}

func eventNameOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if eventname.Helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && eventname.Helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				current := sourceBlobPreviousDeclaration(t, path, fn.Name.Name, requestkey.Node(fn))
				if eventname.Previous(eventNamePins(t), path, fn.Name.Name, current) == current {
					t.Error("event name caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && eventname.Helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("event name helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}
