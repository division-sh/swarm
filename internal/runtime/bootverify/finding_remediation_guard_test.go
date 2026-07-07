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
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok || !isFindingCompositeLiteral(lit) {
				return true
			}
			if len(lit.Elts) == 0 {
				return true
			}
			fields := findingLiteralFields(lit)
			if !findingLiteralIsHardInvalidity(fields, constants) {
				return true
			}
			if exprHasNonEmptyStringAuthority(fields["Remediation"], constants) {
				return true
			}
			checkID := resolvedFindingCheckID(fields["CheckID"], constants)
			if findingCheckIDHasCanonicalRemediationAuthority(checkID) {
				return true
			}
			pos := parsed.fset.Position(lit.Pos())
			problems = append(problems, fmt.Sprintf("%s:%d: hard/error Finding literal check_id=%q has no local remediation and no approved remediation authority", filepath.Base(pos.Filename), pos.Line, checkID))
			return true
		})
	}
	if len(problems) > 0 {
		t.Fatalf("hard/error Finding literals without remediation authority:\n%s", strings.Join(problems, "\n"))
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

func isFindingCompositeLiteral(lit *ast.CompositeLit) bool {
	switch typ := lit.Type.(type) {
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
		return fields["Severity"] == nil
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
