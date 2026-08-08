package store_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const authorityRegistryUpdateEnv = "SWARM_UPDATE_PERSISTENCE_AUTHORITY_REGISTRY"

type authorityFinding struct {
	Kind        string
	File        string
	Enclosing   string
	Member      string
	Resolved    string
	RawSQL      bool
	Disposition string
}

func (f authorityFinding) key() string {
	raw := "typed"
	if f.RawSQL {
		raw = "raw-sql"
	}
	return strings.Join([]string{f.Kind, raw, f.File, f.Enclosing, f.Member, f.Resolved}, "\t")
}

func (f authorityFinding) registryLine() string {
	return f.Disposition + "\t" + f.key()
}

func TestPersistenceAuthorityFindingRegistry(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	findings := loadPersistenceAuthorityFindings(t, root)
	registryPath := filepath.Join(root, "internal", "store", "testdata", "persistence_authority_findings.tsv")
	expected := readAuthorityRegistryIfPresent(t, registryPath)
	if os.Getenv(authorityRegistryUpdateEnv) == "1" {
		writeAuthorityRegistry(t, registryPath, findings, expected)
	}
	expected = readAuthorityRegistry(t, registryPath)
	actual := make(map[string]authorityFinding, len(findings))
	for _, finding := range findings {
		key := finding.key()
		if prior, exists := actual[key]; exists {
			t.Fatalf("duplicate resolved authority finding:\n%s\n%s", prior.registryLine(), finding.registryLine())
		}
		actual[key] = finding
	}

	var unknown, stale, invalid []string
	for key, finding := range actual {
		disposition, ok := expected[key]
		if !ok {
			unknown = append(unknown, "unclassified\t"+key)
			continue
		}
		if disposition == "unclassified" {
			invalid = append(invalid, "unclassified\t"+key)
		}
		if finding.RawSQL && !rawSQLDispositionAllowed(disposition) {
			invalid = append(invalid, disposition+"\t"+key)
		}
	}
	for key, disposition := range expected {
		if _, ok := actual[key]; !ok {
			stale = append(stale, disposition+"\t"+key)
		}
	}
	sort.Strings(unknown)
	sort.Strings(stale)
	sort.Strings(invalid)
	if len(unknown)+len(stale)+len(invalid) != 0 {
		t.Fatalf("persistence authority registry mismatch (set %s=1 only after classifying every changed finding):\nunknown:\n%s\nstale:\n%s\ninvalid raw authority:\n%s",
			authorityRegistryUpdateEnv,
			strings.Join(unknown, "\n"),
			strings.Join(stale, "\n"),
			strings.Join(invalid, "\n"),
		)
	}
	t.Logf("verified %d exact resolved authority findings", len(findings))
}

func TestPersistenceAuthorityRegistryRejectsHostileResolvedTypes(t *testing.T) {
	fixtures := map[string]string{
		"postgres-field": `package fixture
import "database/sql"
type Store struct { DB *sql.DB }
`,
		"sqlite-alias": `package fixture
import "database/sql"
type Alias = sql.DB
type Store struct { DB *Alias }
`,
		"embedded-handle": `package fixture
import "database/sql"
type Store struct { *sql.DB }
`,
		"interface-result": `package fixture
import "database/sql"
type Store interface { Transaction() *sql.Tx }
`,
		"function-field": `package fixture
import (
  "context"
  "database/sql"
)
type Store struct { Mutate func(context.Context, *sql.Tx) error }
`,
		"context-authority": `package fixture
import (
  "context"
  "database/sql"
)
func Bad(ctx context.Context, db *sql.DB) {
  _ = context.WithValue(ctx, "db", db)
  _ = ctx.Value((*sql.Tx)(nil))
}
`,
	}
	for name, source := range fixtures {
		t.Run(name, func(t *testing.T) {
			findings := authorityFindingsFromSource(t, "internal/store/fixture/"+name+".go", source)
			if len(findings) == 0 {
				t.Fatal("hostile fixture produced no resolved finding")
			}
			var hasRelevant bool
			for _, finding := range findings {
				if finding.RawSQL || strings.HasPrefix(finding.Kind, "context-") {
					hasRelevant = true
					break
				}
			}
			if !hasRelevant {
				t.Fatalf("hostile fixture was not resolved as raw/context authority: %+v", findings)
			}
		})
	}
}

func loadPersistenceAuthorityFindings(t *testing.T, root string) []authorityFinding {
	t.Helper()
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, "./internal/persistence/...", "./internal/runtime/...", "./internal/store/...")
	if err != nil {
		t.Fatalf("load authority packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load authority packages reported type errors")
	}
	var findings []authorityFinding
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatalf("relativize %s: %v", pkg.CompiledGoFiles[index], err)
			}
			findings = append(findings, collectAuthorityFindings(filepath.ToSlash(rel), file, pkg.TypesInfo)...)
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].key() < findings[j].key() })
	return findings
}

func authorityFindingsFromSource(t *testing.T, path, source string) []authorityFinding {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse hostile fixture: %v", err)
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: importer.Default()}
	if _, err := conf.Check("github.com/division-sh/swarm/internal/store/fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatalf("type-check hostile fixture: %v", err)
	}
	return collectAuthorityFindings(path, file, info)
}

func collectAuthorityFindings(path string, file *ast.File, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	for _, decl := range file.Decls {
		switch node := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch typed := spec.(type) {
				case *ast.TypeSpec:
					findings = append(findings, collectTypeAuthorityFindings(path, typed, info)...)
				case *ast.ValueSpec:
					for index, name := range typed.Names {
						valueType := info.TypeOf(typed.Type)
						if valueType == nil && index < len(typed.Values) {
							valueType = info.TypeOf(typed.Values[index])
						}
						if finding, ok := authorityTypeFinding(path, "package", "value:"+name.Name, valueType); ok {
							findings = append(findings, finding)
						}
					}
				}
			}
		case *ast.FuncDecl:
			enclosing := functionAuthorityName(node, info)
			findings = append(findings, collectFieldAuthorityFindings(path, enclosing, "param", node.Type.Params, info)...)
			findings = append(findings, collectFieldAuthorityFindings(path, enclosing, "result", node.Type.Results, info)...)
			findings = append(findings, collectContextAuthorityFindings(path, enclosing, node.Body, info)...)
		}
	}
	return findings
}

func collectTypeAuthorityFindings(path string, spec *ast.TypeSpec, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	switch node := spec.Type.(type) {
	case *ast.FuncType:
		if finding, ok := authorityTypeFinding(path, spec.Name.Name, "named-function-type", info.TypeOf(node)); ok {
			findings = append(findings, finding)
		}
	case *ast.StructType:
		for index, field := range node.Fields.List {
			member := authorityFieldName("field", index, field)
			if finding, ok := authorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				findings = append(findings, finding)
			}
		}
	case *ast.InterfaceType:
		for index, field := range node.Methods.List {
			member := authorityFieldName("method", index, field)
			if finding, ok := authorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				finding.Kind = "interface-method"
				findings = append(findings, finding)
			}
		}
	}
	return findings
}

func collectFieldAuthorityFindings(path, enclosing, prefix string, list *ast.FieldList, info *types.Info) []authorityFinding {
	if list == nil {
		return nil
	}
	var findings []authorityFinding
	for index, field := range list.List {
		if finding, ok := authorityTypeFinding(path, enclosing, authorityFieldName(prefix, index, field), info.TypeOf(field.Type)); ok {
			findings = append(findings, finding)
		}
	}
	return findings
}

func authorityTypeFinding(path, enclosing, member string, valueType types.Type) (authorityFinding, bool) {
	if valueType == nil {
		return authorityFinding{}, false
	}
	raw := containsRawSQLType(valueType)
	callback := callableType(valueType)
	if !raw && !callback {
		return authorityFinding{}, false
	}
	kind := "raw-type"
	if callback {
		kind = "callback-type"
	}
	return authorityFinding{
		Kind:      kind,
		File:      path,
		Enclosing: enclosing,
		Member:    member,
		Resolved:  resolvedTypeString(valueType),
		RawSQL:    raw,
	}, true
}

func collectContextAuthorityFindings(path, enclosing string, body *ast.BlockStmt, info *types.Info) []authorityFinding {
	if body == nil {
		return nil
	}
	counts := map[string]int{}
	var findings []authorityFinding
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		operation := ""
		if object := info.Uses[selector.Sel]; object != nil && object.Pkg() != nil && object.Pkg().Path() == "context" && selector.Sel.Name == "WithValue" {
			operation = "WithValue"
		} else if selection := info.Selections[selector]; selection != nil && selection.Obj().Name() == "Value" && selection.Obj().Pkg() != nil && selection.Obj().Pkg().Path() == "context" {
			operation = "Value"
		}
		if operation == "" {
			return true
		}
		counts[operation]++
		resolved := resolvedTypeString(info.TypeOf(call.Fun))
		raw := false
		for _, arg := range call.Args {
			argType := info.TypeOf(arg)
			raw = raw || containsRawSQLType(argType)
			resolved += "|arg=" + resolvedTypeString(argType)
		}
		findings = append(findings, authorityFinding{
			Kind:      "context-" + strings.ToLower(operation),
			File:      path,
			Enclosing: enclosing,
			Member:    fmt.Sprintf("context:%s#%d", operation, counts[operation]),
			Resolved:  resolved,
			RawSQL:    raw,
		})
		return true
	})
	return findings
}

func authorityFieldName(prefix string, index int, field *ast.Field) string {
	if len(field.Names) == 0 {
		return fmt.Sprintf("%s:#%d", prefix, index+1)
	}
	names := make([]string, 0, len(field.Names))
	for _, name := range field.Names {
		names = append(names, name.Name)
	}
	return prefix + ":" + strings.Join(names, ",")
}

func functionAuthorityName(decl *ast.FuncDecl, info *types.Info) string {
	object, _ := info.Defs[decl.Name].(*types.Func)
	if object == nil {
		return decl.Name.Name
	}
	signature, _ := object.Type().(*types.Signature)
	if signature == nil || signature.Recv() == nil {
		return decl.Name.Name
	}
	return "(" + resolvedTypeString(signature.Recv().Type()) + ")." + decl.Name.Name
}

func callableType(valueType types.Type) bool {
	if valueType == nil {
		return false
	}
	_, ok := types.Unalias(valueType).Underlying().(*types.Signature)
	return ok
}

func containsRawSQLType(valueType types.Type) bool {
	return containsRawSQLTypeSeen(valueType, make(map[types.Type]struct{}))
}

func containsRawSQLTypeSeen(valueType types.Type, seen map[types.Type]struct{}) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if _, ok := seen[valueType]; ok {
		return false
	}
	seen[valueType] = struct{}{}
	switch typed := valueType.(type) {
	case *types.Named:
		object := typed.Obj()
		if object != nil && object.Pkg() != nil && object.Pkg().Path() == "database/sql" {
			switch object.Name() {
			case "DB", "Tx", "Conn", "Rows", "Row", "Result":
				return true
			}
		}
		// A non-alias named type is an opaque authority boundary. Its own fields
		// and methods are inspected where that type is declared.
		return false
	case *types.Pointer:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Slice:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Array:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Map:
		return containsRawSQLTypeSeen(typed.Key(), seen) || containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Chan:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Signature:
		return tupleContainsRawSQL(typed.Params(), seen) || tupleContainsRawSQL(typed.Results(), seen)
	case *types.Struct:
		for index := 0; index < typed.NumFields(); index++ {
			if containsRawSQLTypeSeen(typed.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < typed.NumMethods(); index++ {
			if containsRawSQLTypeSeen(typed.Method(index).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func tupleContainsRawSQL(tuple *types.Tuple, seen map[types.Type]struct{}) bool {
	if tuple == nil {
		return false
	}
	for index := 0; index < tuple.Len(); index++ {
		if containsRawSQLTypeSeen(tuple.At(index).Type(), seen) {
			return true
		}
	}
	return false
}

func resolvedTypeString(valueType types.Type) string {
	if valueType == nil {
		return "<nil>"
	}
	return types.TypeString(valueType, func(pkg *types.Package) string {
		if pkg == nil {
			return ""
		}
		return pkg.Path()
	})
}

func rawSQLDispositionAllowed(disposition string) bool {
	switch disposition {
	case "private-backend", "private-runtime-adapter", "private-domain-adapter", "construction-owner", "fixture-2151", "adjacent-2149":
		return true
	default:
		return false
	}
}

func readAuthorityRegistryIfPresent(t *testing.T, path string) map[string]string {
	t.Helper()
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return map[string]string{}
	} else if err != nil {
		t.Fatalf("stat persistence authority registry: %v", err)
	}
	return readAuthorityRegistry(t, path)
}

func readAuthorityRegistry(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open persistence authority registry: %v", err)
	}
	defer file.Close()
	registry := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid persistence authority registry line %q", line)
		}
		if _, exists := registry[parts[1]]; exists {
			t.Fatalf("duplicate persistence authority registry finding %q", parts[1])
		}
		registry[parts[1]] = parts[0]
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read persistence authority registry: %v", err)
	}
	return registry
}

func writeAuthorityRegistry(t *testing.T, path string, findings []authorityFinding, prior map[string]string) {
	t.Helper()
	var buffer bytes.Buffer
	buffer.WriteString("# disposition\tkind\tauthority\tfile\tenclosing\tmember\tresolved-type\n")
	for _, finding := range findings {
		disposition := prior[finding.key()]
		if disposition == "" {
			disposition = "unclassified"
		}
		buffer.WriteString(disposition + "\t" + finding.key())
		buffer.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create authority registry directory: %v", err)
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0o644); err != nil {
		t.Fatalf("write persistence authority registry: %v", err)
	}
}

func persistenceAuthorityRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve persistence authority test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
