package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/receiverreadback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-receiver-public-readback-preimages.json
var family145ReceiverPublicReadbackBytes []byte

type receiverReadbackPin = receiverreadback.Pin

func family145ReceiverReadbackCallerInverse(function, current string) string {
	pins, err := receiverreadback.Decode(family145ReceiverPublicReadbackBytes)
	if err != nil {
		panic(err)
	}
	return receiverreadback.Previous(pins, function, current)
}

func receiverReadbackPins(t *testing.T) []receiverReadbackPin {
	t.Helper()
	if fmt.Sprintf("%x", sha256.Sum256(family145ReceiverPublicReadbackBytes)) != "a75db13f04646436e22522c83c9a04d929d12e384d9b48bfa585fe5bdf499709" {
		t.Fatal("complete receiver public-readback preimages changed")
	}
	var pins []receiverReadbackPin
	if err := json.Unmarshal(family145ReceiverPublicReadbackBytes, &pins); err != nil || len(pins) != 15 {
		t.Fatal("receiver readback complete declaration inventory changed:", err)
	}
	return pins
}

func family145CheckReceiverPublicReadbackWholeRule(t *testing.T, file, function, after string) bool {
	t.Helper()
	for _, pin := range receiverReadbackPins(t) {
		if pin.File == file && pin.Function == function {
			if after != pin.After {
				t.Fatal("receiver readback escaped its complete reviewed rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ReceiverPublicReadbackWholeRulePreservesExistingNativeOwnerAnd15Calls(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	pins := receiverReadbackPins(t)
	seen, calls := map[string]bool{}, 0
	var rawBefore, nativeBefore, nativeAfter string
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("duplicate or changed immutable receiver preimage:", key)
		}
		seen[key] = true
		calls += pin.Calls
		source, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		current := proof.previous(t, pin.File, pin.Function, family145PublicA2CurrentDeclaration(t, source, pin.Function))
		current = runphysical.Previous(runPhysicalPins(t), pin.File, pin.Function, current)
		current = family145PublicJoinCallerInverse(pin.Function, current)
		current = family145PublicA2EndpointCallerInverse(pin.Function, current)
		if current != pin.After {
			t.Fatal("current receiver caller differs from the complete transform:", key)
		}
		if pin.Function == "requireReceiverPublicReadback" {
			rawBefore, nativeAfter = pin.Before, pin.After
		}
		if pin.Removed {
			if pin.Function != "requireWorkspaceProofReceiverPublicReadback" || pin.After != "" || pin.Source != "" {
				t.Fatal("unexpected receiver owner/helper retirement")
			}
			nativeBefore = pin.Before
			if previous := receiverreadback.Previous(pins, pin.Function, pin.After); previous != pin.After {
				t.Fatal("retired helper normalized into a live historical body")
			}
			continue
		}
		receiverReadbackPinHostiles(t, pin)
	}
	if calls != 15 || rawBefore == "" || nativeBefore == "" || nativeAfter == "" {
		t.Fatal("whole receiver helper/caller family is incomplete")
	}
	expected := strings.Replace(nativeBefore, "requireWorkspaceProofReceiverPublicReadback", "requireReceiverPublicReadback", 1)
	expected = strings.Replace(expected, "owner runtimebus.EventStore", "owner receiverProofStateReader", 1)
	if expected != nativeAfter {
		t.Fatal("native query/decoder/assertions changed beyond the exact typed role and name")
	}
	backend, err := os.ReadFile("../../../internal/store/internal/backend/delivery/read_projections.go")
	if err != nil {
		t.Fatal(err)
	}
	queryOwner := family145PublicA2CurrentDeclaration(t, backend, "ReadReceiverDeliveryStorage")
	if receiverReadbackSQL(t, rawBefore) != receiverReadbackSQL(t, queryOwner) {
		t.Fatal("native physical reader changed selected columns/run scope/SQL")
	}
}

func receiverReadbackSQL(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "query.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Query" && selector.Sel.Name != "QueryContext" {
			return true
		}
		index := 0
		if selector.Sel.Name == "QueryContext" {
			index = 1
		}
		literal, ok := call.Args[index].(*ast.BasicLit)
		if !ok {
			t.Fatal("exact receiver physical query became dynamic")
		}
		query, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, query)
		return true
	})
	if len(queries) != 1 {
		t.Fatal("exact receiver query census changed")
	}
	return queries[0]
}

func receiverReadbackPinHostiles(t *testing.T, pin receiverReadbackPin) {
	t.Helper()
	pins := receiverReadbackPins(t)
	if previous := receiverreadback.Previous(pins, pin.Function, pin.After); previous != pin.Before {
		t.Fatal("exact complete successor lost its original preimage")
	}
	if previous := receiverreadback.Previous(pins, pin.Function+"Foreign", pin.After); previous != pin.After {
		t.Fatal("foreign declaration entered the finite inverse")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	after, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
	if err != nil || !changed {
		t.Fatal("exact receiver rule failed:", pin.Function, err)
	}
	if _, changed, err := rewriteFunction(pin.File, after, finite); err != nil || changed {
		t.Fatal("receiver rule replay changed the successor")
	}
	for _, cut := range []string{"rt.ReceiverStateReader", "endpoint", "reader", "p.deps.EventStore", "ReadReceiverDeliveryStorage", "json.Unmarshal", "record.owner", "t.Fatal", "reflect.DeepEqual", "second.stop", "claim.DeliveryID()", "idempotency_key", "requireReceiverPublicReadbackEvidence"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignReceiverCut", 1)
		if previous := receiverreadback.Previous(pins, pin.Function, mutant); previous != mutant {
			t.Fatal("altered complete successor normalized:", pin.Function, cut)
		}
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("lost receiver scope/decoder/public assertion/lifecycle cut accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145ReceiverPublicReadbackOwnedInventoryRetiresTheRawDelegate(t *testing.T) {
	root := "../../.."
	calls, helpers := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		foundCalls, foundHelpers := receiverReadbackInventoryFile(t, path, file)
		calls += foundCalls
		helpers += foundHelpers
		return nil
	})
	if err != nil || calls != 15 || helpers != 1 {
		t.Fatalf("owned receiver inventory calls=%d helpers=%d err=%v", calls, helpers, err)
	}
}

func receiverReadbackInventoryFile(t *testing.T, path string, file *ast.File) (calls, helpers int) {
	t.Helper()
	allowed := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncDecl:
			if node.Name.Name == "requireReceiverPublicReadback" {
				helpers++
				allowed[node.Name.Pos()] = true
				if !strings.HasPrefix(family145NodeString(node), "func requireReceiverPublicReadback(t *testing.T, endpoint string, owner receiverProofStateReader, runID string)") {
					t.Error("receiver helper restored a broad/raw carrier")
				}
			}
		case *ast.CallExpr:
			if name, ok := node.Fun.(*ast.Ident); ok && name.Name == "requireReceiverPublicReadback" {
				allowed[name.Pos()] = true
				calls++
				if len(node.Args) != 4 || node.Ellipsis.IsValid() {
					t.Errorf("receiver call restored an old or unknown carrier: %s", path)
				}
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok {
			if id.Name == "requireWorkspaceProofReceiverPublicReadback" || id.Name == "requireReceiverPublicReadback" && !allowed[id.Pos()] {
				t.Errorf("retired or escaped receiver helper reference: %s", path)
			}
		}
		return true
	})
	return calls, helpers
}
