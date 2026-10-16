package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func assertSelectedForkExecutionFiniteRewrite(t *testing.T, pin selectedreadrole.EntityToolPin) {
	t.Helper()
	row := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	after, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), row)
	if err != nil || !changed {
		t.Fatalf("finite source rewrite failed: %s/%v", pin.Function, err)
	}
	if _, changed, err := rewriteFunction(pin.File, after, row); err != nil || changed {
		t.Fatalf("finite source replay changed native endpoint: %s/%v", pin.Function, err)
	}
	hostile := strings.Replace(pin.Before, "{", "{ foreignAuthority();", 1)
	if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+hostile), row); err == nil {
		t.Fatalf("finite source rewrite accepted changed input: %s", pin.Function)
	}
}

func assertSelectedForkFieldHelperRetired(t *testing.T, root string, pin selectedreadrole.EntityToolPin) {
	t.Helper()
	if pin.Function != "selectedExecutionTestAgentFields" || pin.After != "" {
		t.Fatalf("unrelated helper retirement entered the wave: %s", pin.Function)
	}
	before, err := selectedreadrole.EarlierSelectedForkExecutionSource(root, pin.File, pin.Function, "")
	if err != nil || before != pin.Before {
		t.Fatalf("dead helper inverse lost its complete predecessor: %v", err)
	}
	retired := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.Before, Removed: true}
	if _, err := checkpointRecipes([]byte("package proof\n"+pin.Before), []recipe{retired}); err == nil {
		t.Fatal("dead field-format helper was resurrected")
	}
	if _, err := checkpointRecipes([]byte("package proof\n"), []recipe{retired}); err != nil {
		t.Fatalf("exact dead helper retirement refused: %v", err)
	}
}

func selectedForkFailureMessages(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "Fatal" && method.Sel.Name != "Fatalf" && method.Sel.Name != "Error" && method.Sel.Name != "Errorf" {
			return true
		}
		if literal, ok := call.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
			out = append(out, method.Sel.Name+":"+literal.Value)
		}
		return true
	})
	return out
}

func assertSelectedForkRetiredRoutingDebtRetained(t *testing.T, source string) {
	t.Helper()
	for _, exact := range []string{
		"_, db, _ := testutil.StartPostgres(t)",
		"pg := storetest.AdmitPostgresRuntimeStore(t, db)",
		"if copiedCurrentRoutes != 0",
		"FROM routing_rules", "WHERE flow_instance = 'flow-a/1'", "AND is_materialized = true",
	} {
		if strings.Count(source, exact) != 1 {
			t.Fatalf("retired-routing residual disappeared or widened: %s", exact)
		}
	}
	if strings.Count(source, "db.QueryRowContext(") != 1 || strings.Count(source, ".Scan(") != 1 {
		t.Fatal("the retained carrier gained another raw reader")
	}
}

func assertSelectedForkHistoryOriginals(t *testing.T, before, after []map[string]any, recipes bool) {
	t.Helper()
	appended := 14
	if recipes {
		appended = 2
	}
	if len(after) != len(before)+appended {
		t.Fatalf("history append count=%d, want%d", len(after)-len(before), appended)
	}
	unchanged, advanced := 0, 0
	for i, row := range before {
		for key, value := range row {
			if !reflect.DeepEqual(value, after[i][key]) {
				t.Fatalf("historical row%d %s was overwritten", i, key)
			}
		}
		if !recipes && !reflect.DeepEqual(row, after[i]) {
			t.Fatalf("original transition%d was edited", i)
		}
		if recipes && row["File"] == selectedreadrole.SelectedForkExecutionFile {
			if reflect.DeepEqual(row, after[i]) {
				unchanged++
			} else {
				advanced++
			}
		}
	}
	if recipes && (unchanged != 35 || advanced != 12) {
		t.Fatalf("original 47 endpoints changed outside12+35 boundary: %d/%d", advanced, unchanged)
	}
}

func assertSelectedForkHistoryHostiles(t *testing.T, root string, actual []byte, recipes bool) {
	t.Helper()
	for _, cut := range []string{"lost", "order", "before", "foreign"} {
		var rows []map[string]any
		if err := json.Unmarshal(actual, &rows); err != nil {
			t.Fatal(err)
		}
		switch cut {
		case "lost":
			rows = rows[:len(rows)-1]
		case "order":
			rows[len(rows)-1], rows[len(rows)-2] = rows[len(rows)-2], rows[len(rows)-1]
		case "before":
			rows[len(rows)-1]["Before"] = "foreign original"
		case "foreign":
			rows[len(rows)-1]["After"] = "foreign endpoint"
		}
		bad, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selectedreadrole.EarlierSelectedForkExecutionHistory(root, bad, recipes); err == nil {
			t.Fatalf("changed history accepted: %s", cut)
		}
	}
}

func selectedForkActualSourceMutation(t *testing.T, raw []byte, function, cut string) string {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "current.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != function {
			continue
		}
		start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
		body := string(raw[start:end])
		switch cut {
		case "changed":
			return string(raw[:start]) + strings.Replace(body, "{", "{ erasedProof();", 1) + string(raw[end:])
		case "missing":
			return string(raw[:start]) + string(raw[end:])
		case "duplicate":
			return string(raw) + "\n" + body
		}
	}
	t.Fatalf("actual-source mutation did not find %s/%s", function, cut)
	return ""
}

func selectedForkExecutionProbeCheckout(t *testing.T, root string) string {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{selectedreadrole.SelectedForkExecutionPath, selectedreadrole.SelectedForkExecutionFile} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copy, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return copy
}
