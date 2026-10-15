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
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/serveddebug"
)

//go:embed family145-served-debug-preimages.json
var family145ServedDebugPreimages []byte

type servedDebugPreimage struct {
	File, Function, SHA256, Before, After string
	DebugCalls, HelperCalls               int
}

var family145ServedDebugRows = func() []servedDebugPreimage {
	var rows []servedDebugPreimage
	if err := json.Unmarshal(family145ServedDebugPreimages, &rows); err != nil {
		panic(err)
	}
	return rows
}()

// Only the exact reviewed current body composes back to the signed predecessor.
// Mutated arguments, assertions, captures or control flow never enter this path.
func family145ServedDebugCallerInverse(function, after string) string {
	after = family145RunPhysicalCallerInverse(function, after)
	after = family145RequestKeyCallerInverse(function, after)
	after = family145ChannelIntentCallerInverse(function, after)
	after = family145ForkNoticeCallerInverse(function, after)
	after = family145ForkCompanionCallerInverse(function, after)
	after = family145PublicJoinCallerInverse(function, after)
	after = family145ReceiverReadbackCallerInverse(function, after)
	after = family145PublicA2EndpointCallerInverse(function, after)
	for _, row := range family145ServedDebugRows {
		if row.Function == function && row.After != "" && after == row.After {
			return row.Before
		}
	}
	return after
}

func family145CheckServedDebugWholeRule(t *testing.T, file, function, after string) bool {
	t.Helper()
	after = family145RunPhysicalPrevious(file, function, after)
	after = family145RequestKeyCallerInverse(function, after)
	after = family145ChannelIntentCallerInverse(function, after)
	after = family145ForkNoticeCallerInverse(function, after)
	after = family145ForkCompanionCallerInverse(function, after)
	after = family145ReceiverReadbackCallerInverse(function, after)
	after = publicendpoint.Previous(file, function, after)
	for _, row := range family145ServedDebugRows {
		if row.File == file && row.Function == function {
			if after != row.After {
				t.Fatalf("served diagnostic propagation escaped complete signed rule: %s/%s", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ServedDebugWholeRulePreservesEveryBodyAndOwner(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSuffix(string(family145ServedDebugPreimages), "\n")))) != "31f62531eda187d22fa6d08b1379ab1df88a4d4f75aa6c997b00fdb06d89700e" {
		t.Fatal("served debug signed predecessor ledger changed")
	}
	seen := map[string]bool{}
	debug, helpers, retired, carriers := 0, 0, 0, 0
	for _, row := range family145ServedDebugRows {
		key := row.File + "/" + row.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != row.SHA256 {
			t.Fatalf("duplicate/changed complete predecessor: %s", key)
		}
		seen[key] = true
		if actual := family145ServedDebugCurrentDeclaration(t, row); actual != row.After {
			t.Errorf("current declaration differs from closed rule: %s", key)
		}
		if row.After == "" {
			if row.Function != "servedEventPublishDebugSummary" && row.Function != "servedEventPublishDebugQuery" {
				t.Fatal("unexpected diagnostic retirement")
			}
			retired++
			continue
		}
		if strings.HasPrefix(row.Function, "type/") {
			if family145ServedDebugNormalize(t, serveddebug.TransformCarrier(row.Before)) != row.After {
				t.Fatal("session carrier changed beyond its original read role")
			}
			carriers++
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "before.go", "package proof\n"+row.Before, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		function := file.Decls[0].(*ast.FuncDecl)
		after, n, calls, needs := serveddebug.TransformFunction(fset, function, row.Before)
		if needs || n != row.DebugCalls || calls != row.HelperCalls || family145ServedDebugNormalize(t, after) != row.After {
			t.Fatalf("finite transform changed assertions/waits/identity/lifecycle/capture cuts: %s", key)
		}
		debug += n
		helpers += calls
		family145ServedDebugHostiles(t, row)
	}
	if len(seen) != 85 || debug != 63 || retired != 2 || carriers != 1 || helpers == 0 {
		t.Fatalf("served debug complete inventory bodies=%d direct=%d helper-calls=%d retired=%d carriers=%d", len(seen), debug, helpers, retired, carriers)
	}
}

func family145ServedDebugNormalize(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "rule.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatalf("invalid finite declaration: %v", err)
	}
	return family145NodeString(file.Decls[0])
}

func family145ServedDebugCurrentDeclaration(t *testing.T, row servedDebugPreimage) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", row.File))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), row.File, raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil && function.Name.Name == row.Function {
			current := sourceBlobPreviousDeclaration(t, row.File, row.Function, family145NodeString(function))
			current = runphysical.Previous(runIdentityPins, row.File, row.Function, current)
			current = runphysical.Previous(runDeliveryLivePins(t), row.File, row.Function, current)
			current = runphysical.Previous(sourceForkPins(t), row.File, row.Function, current)
			current = family145RunScalarCallerInverse(row.Function, current)
			current = family145WorkflowStateCallerInverse(row.Function, current)
			current = family145RunPhysicalPrevious(row.File, row.Function, current)
			current = family145RequestKeyCallerInverse(row.Function, current)
			current = family145ChannelIntentCallerInverse(row.Function, current)
			current = family145ForkNoticeCallerInverse(row.Function, current)
			current = family145ForkCompanionCallerInverse(row.Function, current)
			current = family145ReceiverReadbackCallerInverse(row.Function, current)
			return publicendpoint.Previous(row.File, row.Function, current)
		}
		if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 && "type/"+group.Specs[0].(*ast.TypeSpec).Name.Name == row.Function {
			return family145NodeString(group)
		}
	}
	return ""
}

func family145ServedDebugHostiles(t *testing.T, row servedDebugPreimage) {
	t.Helper()
	finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
	for _, cut := range []string{"ReceiverStateReader", "p.deps.EventStore", "t.Fatal", "t.Errorf", "reflect.DeepEqual", "releaseOnce.Do", "first.kill", "45 * time.Second", "50 * time.Millisecond", "ReadServedEventRunIDStorage", "return err", "idempotency_key"} {
		if !strings.Contains(row.After, cut) {
			continue
		}
		hostile := strings.Replace(row.After, cut, "foreignDiagnosticCut()", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("diagnostic rule accepted lost identity/assertion/temporal/owner cut: %s/%s", row.Function, cut)
		}
	}
}

func TestFamily145ServedDebugWholeOwnedInventoryHasNoRawInterpreter(t *testing.T) {
	root := "../../.."
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if function, ok := node.(*ast.FuncDecl); ok && (function.Name.Name == "servedEventPublishDebugSummary" || function.Name.Name == "servedEventPublishDebugQuery") {
				t.Errorf("retired diagnostic interpreter returned: %s", path)
			}
			if identifier, ok := node.(*ast.Ident); ok && (identifier.Name == "servedEventPublishDebugSummary" || identifier.Name == "servedEventPublishDebugQuery") {
				t.Errorf("owned source retains raw diagnostic capability: %s", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
