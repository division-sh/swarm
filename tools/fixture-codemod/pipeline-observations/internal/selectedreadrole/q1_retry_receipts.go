package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const q1RetrySource = "internal/runtime/node_delivery_startup_recovery_test.go"

// Q1RetryDecisionHistoricalSource peels only Q1's authenticated retry setup.
// Older complete-file validators still check every other byte themselves.
func Q1RetryDecisionHistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path != q1RetrySource {
		return append([]byte(nil), current...), nil
	}
	packet, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/testdata/fixture_setup_successors_2617.json"))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(packet)) != "03c030fb2a6a3d1b2b372ab76b184410cca88ae045040c900aec3bb9109cbb89" {
		return nil, fmt.Errorf("Q1 retry source history changed: %v", err)
	}
	var pins []struct {
		Original  struct{ File, Function, Successor string }
		Candidate string
	}
	if err := json.Unmarshal(packet, &pins); err != nil || len(pins) != 114 {
		return nil, fmt.Errorf("Q1 retry source history shape changed: %v", err)
	}
	pin := pins[39]
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, current, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	start, end, matched := 0, 0, 0
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != pin.Original.Function {
			continue
		}
		start, end = set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
		matched++
	}
	body := string(current[start:end])
	canonical, err := q1RetryCanonicalFunction(body)
	if pin.Original.File != path || matched != 1 || err != nil || canonical != pin.Candidate {
		return nil, fmt.Errorf("Q1 current source or multiplicity changed: %s", path)
	}
	due := regexp.MustCompile(`(\bRetryBase:\s*)time\.Millisecond\b`)
	if len(due.FindAllStringIndex(body, -1)) != 1 || strings.Count(body, "storetest.WaitManagerRetryDecision(") != 1 {
		return nil, fmt.Errorf("Q1 current retry edits changed: %s", path)
	}
	body = due.ReplaceAllString(body, "${1}time.Hour")
	body = strings.Replace(body, "storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible(", 1)
	canonical, err = q1RetryCanonicalFunction(body)
	if err != nil || canonical != pin.Original.Successor {
		return nil, fmt.Errorf("Q1 exact source inverse changed: %s", path)
	}
	prior := append(append(append([]byte(nil), current[:start]...), body...), current[end:]...)
	return format.Source(prior)
}

func q1RetryCanonicalFunction(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "receipt.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", fmt.Errorf("Q1 source is not one complete function: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return "", fmt.Errorf("Q1 source is not a function")
	}
	var out bytes.Buffer
	err = format.Node(&out, token.NewFileSet(), fn)
	return out.String(), err
}
