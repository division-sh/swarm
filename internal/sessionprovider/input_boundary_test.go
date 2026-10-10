package sessionprovider

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func nativeInputIssuerViolations(path string, source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	aliases := map[string]bool{}
	readOnlyAliases := map[*ast.SelectorExpr]bool{}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, spec := range general.Specs {
			alias, ok := spec.(*ast.TypeSpec)
			if !ok || !alias.Assign.IsValid() {
				continue
			}
			selected, ok := alias.Type.(*ast.SelectorExpr)
			if ok && (selected.Sel.Name == "Admission" || selected.Sel.Name == "Account" || selected.Sel.Name == "Claim" || selected.Sel.Name == "NonClaim") {
				readOnlyAliases[selected] = true
			}
		}
	}
	for _, imported := range file.Imports {
		name, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return nil, err
		}
		if name != "github.com/division-sh/swarm/internal/sessionprovider/internal/inputfact" && name != "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact" {
			continue
		}
		alias := "inputfact"
		if strings.HasSuffix(name, "/authorityfact") {
			alias = "authorityfact"
		}
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		aliases[alias] = true
		if path != "input_issuer.go" && path != "input/admission.go" && path != "authority_issuer_unix.go" && path != "claim_issuer.go" && path != "authority/admission.go" {
			violations = append(violations, "foreign input issuer import: "+path)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		root, ok := selector.X.(*ast.Ident)
		facade := path == "input/admission.go" || path == "authority/admission.go"
		if ok && aliases[root.Name] && facade && !readOnlyAliases[selector] {
			violations = append(violations, "read-only facade exposes native construction: "+path)
		}
		return true
	})
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.IsExported() && function.Type.Results != nil && (path == "input_issuer.go" || path == "authority_issuer_unix.go" || path == "claim_issuer.go") {
			violations = append(violations, "exported native issuer: "+function.Name.Name)
		}
	}
	return violations, nil
}

func TestNativeInputIssuerCensusIsClosed(t *testing.T) {
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate native issuer census")
	}
	root := filepath.Dir(here)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		violations, err := nativeInputIssuerViolations(filepath.ToSlash(relative), source)
		if err != nil {
			return err
		}
		if len(violations) != 0 {
			t.Errorf("native issuance boundary: %v", violations)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNativeInputIssuerCensusRejectsRawReexport(t *testing.T) {
	for _, source := range []string{
		`package input
import "github.com/division-sh/swarm/internal/sessionprovider/internal/inputfact"
var New = inputfact.SealOwnedCapture
`,
		`package input
import "github.com/division-sh/swarm/internal/sessionprovider/internal/inputfact"
	type Raw = inputfact.Capture
`,
		`package input
import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
var New = authorityfact.SealOwnedAccount
`,
		`package input
import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
var New = authorityfact.SealOwnedNonClaim
`,
		`package input
import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
var Raw = authorityfact.NonClaim{}
`,
	} {
		violations, err := nativeInputIssuerViolations("input/admission.go", []byte(source))
		if err != nil || len(violations) == 0 {
			t.Fatalf("raw constructor/DTO reexport accepted: %v %v", violations, err)
		}
	}
}

func TestNativeInputIssuerCensusAdmitsOnlyOpaqueReadOnlyNonClaimAlias(t *testing.T) {
	violations, err := nativeInputIssuerViolations("authority/admission.go", []byte(`package authority
import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
type NonClaim = authorityfact.NonClaim
`))
	if err != nil || len(violations) != 0 {
		t.Fatal("opaque read-only alias classified as a constructor", violations, err)
	}
}
