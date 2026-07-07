package bootverify

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestHardInvalidityFindingLiteralsHaveRemediationAuthority(t *testing.T) {
	files := parseBootverifyProductionFiles(t)
	constants := bootverifyStringConstants(files)

	var problems []string
	for _, parsed := range files {
		inspectFindingCompositeLiterals(parsed.file, func(lit *ast.CompositeLit) {
			fields := findingLiteralFields(lit)
			if !findingLiteralIsHardInvalidity(fields, constants) {
				return
			}
			if exprHasNonEmptyStringAuthority(fields["Remediation"], constants) {
				return
			}
			checkID := resolvedFindingCheckID(fields["CheckID"], constants)
			if findingCheckIDHasCanonicalRemediationAuthority(checkID) {
				return
			}
			pos := parsed.fset.Position(lit.Pos())
			problems = append(problems, fmt.Sprintf("%s:%d: hard/error Finding literal check_id=%q has no local remediation and no approved remediation authority", filepath.Base(pos.Filename), pos.Line, checkID))
		})
	}
	if len(problems) > 0 {
		t.Fatalf("hard/error Finding literals without remediation authority:\n%s", strings.Join(problems, "\n"))
	}
}

func TestDynamicSeverityFindingLiteralsFailClosed(t *testing.T) {
	lit := parseSingleFindingLiteral(t, `package bootverify

func example(severity string) Finding {
	return Finding{
		CheckID: "missing_dynamic_authority",
		Severity: severity,
		Message: "dynamic severity must not bypass hard-invalidity remediation authority",
		Location: "global",
	}
}
`)
	fields := findingLiteralFields(lit)
	if !findingLiteralIsHardInvalidity(fields, nil) {
		t.Fatalf("dynamic severity Finding literal should be treated as potential hard invalidity")
	}
	if findingCheckIDHasCanonicalRemediationAuthority(resolvedFindingCheckID(fields["CheckID"], nil)) {
		t.Fatalf("test fixture unexpectedly has canonical remediation authority")
	}
}

type parsedBootverifyFile struct {
	fset *token.FileSet
	file *ast.File
}

func parseBootverifyProductionFiles(t *testing.T) []parsedBootverifyFile {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob bootverify go files: %v", err)
	}
	out := make([]parsedBootverifyFile, 0, len(paths))
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		out = append(out, parsedBootverifyFile{fset: fset, file: file})
	}
	return out
}

func bootverifyStringConstants(files []parsedBootverifyFile) map[string]string {
	out := map[string]string{}
	for _, parsed := range files {
		for _, decl := range parsed.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range valueSpec.Names {
					if i >= len(valueSpec.Values) {
						continue
					}
					if value, ok := stringValue(valueSpec.Values[i], out); ok {
						out[name.Name] = value
					}
				}
			}
		}
	}
	return out
}

func parseSingleFindingLiteral(t *testing.T, src string) *ast.CompositeLit {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	var findings []*ast.CompositeLit
	inspectFindingCompositeLiterals(file, func(lit *ast.CompositeLit) {
		findings = append(findings, lit)
	})
	if len(findings) != 1 {
		t.Fatalf("finding literals = %d, want 1", len(findings))
	}
	return findings[0]
}

func inspectFindingCompositeLiterals(node ast.Node, visit func(*ast.CompositeLit)) {
	ast.Inspect(node, func(node ast.Node) bool {
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if isFindingCompositeLiteral(lit) {
			if len(lit.Elts) > 0 {
				visit(lit)
			}
			return true
		}
		if !isFindingSliceOrArrayCompositeLiteral(lit) {
			return true
		}
		for _, elt := range lit.Elts {
			inner, ok := elt.(*ast.CompositeLit)
			if !ok || inner.Type != nil || len(inner.Elts) == 0 {
				continue
			}
			visit(inner)
		}
		return true
	})
}

func isFindingCompositeLiteral(lit *ast.CompositeLit) bool {
	return isFindingTypeExpr(lit.Type)
}

func isFindingSliceOrArrayCompositeLiteral(lit *ast.CompositeLit) bool {
	array, ok := lit.Type.(*ast.ArrayType)
	return ok && isFindingTypeExpr(array.Elt)
}

func isFindingTypeExpr(expr ast.Expr) bool {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name == "Finding"
	case *ast.SelectorExpr:
		return typ.Sel.Name == "Finding"
	default:
		return false
	}
}

func findingLiteralFields(lit *ast.CompositeLit) map[string]ast.Expr {
	out := map[string]ast.Expr{}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		out[key.Name] = kv.Value
	}
	return out
}

func findingLiteralIsHardInvalidity(fields map[string]ast.Expr, constants map[string]string) bool {
	raw, ok := stringValue(fields["Severity"], constants)
	if !ok {
		return true
	}
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", legacySeverityError, SeverityHardInvalidity:
		return true
	default:
		return false
	}
}

func exprHasNonEmptyStringAuthority(expr ast.Expr, constants map[string]string) bool {
	if expr == nil {
		return false
	}
	value, ok := stringValue(expr, constants)
	return !ok || strings.TrimSpace(value) != ""
}

func resolvedFindingCheckID(expr ast.Expr, constants map[string]string) string {
	if value, ok := stringValue(expr, constants); ok {
		return strings.TrimSpace(value)
	}
	if expr == nil {
		return "workflow_contract_validation"
	}
	return ""
}

func findingCheckIDHasCanonicalRemediationAuthority(checkID string) bool {
	checkID = strings.TrimSpace(checkID)
	if checkID == "" {
		return false
	}
	if _, ok := stableHardInvalidityRemediation[checkID]; ok {
		return true
	}
	_, ok := routingRemediationSplitCheckIDs[checkID]
	return ok
}

func stringValue(expr ast.Expr, constants map[string]string) (string, bool) {
	switch value := expr.(type) {
	case nil:
		return "", false
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		unquoted, err := strconv.Unquote(value.Value)
		if err != nil {
			return "", false
		}
		return unquoted, true
	case *ast.Ident:
		resolved, ok := constants[value.Name]
		return resolved, ok
	default:
		return "", false
	}
}
