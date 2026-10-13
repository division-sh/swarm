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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-delivery-status-preimages.json
var family145DeliveryStatusBytes []byte

func deliveryStatusPins(t *testing.T) []deliverystatus.Pin {
	t.Helper()
	pins, err := deliverystatus.Decode(family145DeliveryStatusBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145DeliveryStatusCallerInverse(function, current string) string {
	current = family145EventNameCallerInverse(function, current)
	pins, err := deliverystatus.Decode(family145DeliveryStatusBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return deliverystatus.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckDeliveryStatusWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145EventNameCallerInverse(function, current)
	for _, pin := range deliveryStatusPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("delivery status observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145DeliveryStatusWholeChainPreservesEveryPollAndRefusalCut(t *testing.T) {
	calls := 0
	seen := map[string]bool{}
	for _, pin := range deliveryStatusPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate delivery status predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", deliverystatus.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("delivery status source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || family145EventNameCallerInverse(pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("delivery status current source escaped its whole rule:", key, err)
		}
		after, count, err := deliverystatus.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("delivery status finite transform escaped its rule:", key, err)
		}
		calls += count
		deliveryStatusHostiles(t, pin)
	}
	if calls != 28 || len(seen) != 14 {
		t.Fatalf("delivery status inventory=%d/%d want28/14", calls, len(seen))
	}
}

func deliveryStatusHostiles(t *testing.T, pin deliverystatus.Pin) {
	t.Helper()
	pins := deliveryStatusPins(t)
	if deliverystatus.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || deliverystatus.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("delivery status inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "ReadServedDeliveryStatusCount", "reader", "ReceiverStateReader", "eventID", "runID", "statuses...", "status", "subscriberID", "subscriberType", "t.Fatal", " != ", " == ", "got == want", "servedProofPollDeadline", "duration", "25 * time.Millisecond", "servedInspectionRunDebugSummary", "waitForReady", "captureSelectedRuntimePersistence", "releaseOnce.Do", "250*time.Millisecond"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || deliverystatus.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed delivery status identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145DeliveryStatusOwnedInventoryHasNoRawCallerOrAlias(t *testing.T) {
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
		n, d := deliveryStatusOwnedFile(t, filepath.ToSlash(rel), file)
		calls += n
		definitions += d
		return nil
	})
	if err != nil || calls != 28 || definitions != 4 {
		t.Fatalf("delivery status complete inventory=%d/%d want28/4 err=%v", calls, definitions, err)
	}
}

func deliveryStatusOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if deliverystatus.Helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && deliverystatus.Helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				current := family145EventNameCallerInverse(fn.Name.Name, requestkey.Node(fn))
				if deliverystatus.Previous(deliveryStatusPins(t), path, fn.Name.Name, current) == current {
					t.Error("delivery status caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && deliverystatus.Helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("delivery status helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

func TestFamily145DeliveryStatusPollingStatementsAndExistingQueryOwnerAreUnchanged(t *testing.T) {
	for _, pin := range deliveryStatusPins(t) {
		if !deliverystatus.Helpers[pin.Function] || pin.Function == "servedEventPublishDeliveryStatusCount" {
			continue
		}
		after := strings.Replace(pin.Before, "servedEventPublishDeliveryStatusCount(t, db,", "servedEventPublishDeliveryStatusCount(t, reader,", 1)
		if pin.Function == "waitServedEventPublishDeliveryStatusCountForRun" {
			after = strings.Replace(after, "db *sql.DB, reader receiverProofStateReader", "reader receiverProofStateReader", 1)
		} else {
			after = strings.Replace(after, "db *sql.DB", "reader receiverProofStateReader", 1)
		}
		if after != pin.After {
			t.Fatal("polling/deadline/window/debug/assertion changed:", pin.Function)
		}
	}
	for _, path := range []string{"internal/store/internal/backend/delivery/read_projections.go", "internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "internal/store/storetest/served_delivery_observation.go", "internal/store/internal/runtimepersistence/served_delivery_status_count_test.go"} {
		original, err := exec.Command("git", "show", deliverystatus.Predecessor+":"+path).Output()
		current, currentErr := os.ReadFile(filepath.Join("../../..", path))
		if err == nil && currentErr == nil {
			current = deliveryStatusEarlierOwnerSource(t, path, current, original)
		}
		if err != nil || currentErr != nil || string(original) != string(current) {
			t.Fatal("existing delivery status canonical owner/hostile proof changed:", path, err, currentErr)
		}
	}
}
