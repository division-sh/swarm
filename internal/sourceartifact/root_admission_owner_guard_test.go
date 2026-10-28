package sourceartifact

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestS1RootAdmissionHasClosedProductionOwners(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "test-results", "worktrees", "node_modules", ".swarm":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		if !bytes.Contains(body, []byte("RootAdmission")) && !bytes.Contains(body, []byte("RootedSource")) {
			return nil
		}
		path, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		violations, err := rootAdmissionOwnerViolations(filepath.ToSlash(path), body)
		if err != nil {
			return err
		}
		for _, violation := range violations {
			t.Errorf("%s: %s", path, violation)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func rootAdmissionOwnerViolations(name string, body []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), name, body, 0)
	if err != nil {
		return nil, err
	}
	aliases := map[string]bool{}
	for _, declaration := range file.Imports {
		path, err := strconv.Unquote(declaration.Path.Value)
		if err != nil || path != "github.com/division-sh/swarm/internal/sourceartifact" {
			continue
		}
		alias := "sourceartifact"
		if declaration.Name != nil {
			alias = declaration.Name.Name
		}
		aliases[alias] = true
	}
	var violations []string
	if aliases["."] {
		violations = append(violations, "root admission owner cannot use a dot import")
	}
	localOwner := strings.HasPrefix(name, "internal/sourceartifact/") && !strings.Contains(strings.TrimPrefix(name, "internal/sourceartifact/"), "/")
	rootTypes := rootAdmissionTypeAliases(file)
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			if localOwner && value.Name == "RestoreRootedSource" && name != "internal/sourceartifact/root_persistence.go" {
				violations = append(violations, "retained root restoration bypasses the selected-store source owner")
			}
		case *ast.SelectorExpr:
			base, identifier := value.X.(*ast.Ident)
			if identifier && aliases[base.Name] && value.Sel.Name == "RestoreRootedSource" && name != "internal/store/internal/sourceartifact/root_admission.go" {
				violations = append(violations, "retained root restoration bypasses the selected-store source owner")
			}
		case *ast.CompositeLit:
			typ, identifier := value.Type.(*ast.Ident)
			if !identifier || len(value.Elts) == 0 || !localOwner {
				break
			}
			if kind := rootTypes[typ.Name]; kind != "" && !rootAdmissionMintFile(name, kind) {
				violations = append(violations, "root evidence construction bypasses its finite owners")
			}
		case *ast.CallExpr:
			typ, identifier := value.Fun.(*ast.Ident)
			if identifier && localOwner {
				if kind := rootTypes[typ.Name]; kind != "" && !rootAdmissionMintFile(name, kind) {
					violations = append(violations, "root evidence conversion bypasses its finite owners")
				}
			}
		case *ast.AssignStmt:
			if localOwner && name != "internal/sourceartifact/admit_unix.go" {
				for _, target := range value.Lhs {
					if field, ok := target.(*ast.SelectorExpr); ok && (field.Sel.Name == "rootDirectory" || field.Sel.Name == "rootAdmission") {
						violations = append(violations, "root evidence mutation bypasses its finite owner")
					}
				}
			}
		}
		return true
	})
	return violations, nil
}

func rootAdmissionMintFile(name, kind string) bool {
	return name == "internal/sourceartifact/root_persistence.go" ||
		kind == "RootAdmission" && name == "internal/sourceartifact/admit_unix.go" ||
		kind == "RootedSource" && name == "internal/sourceartifact/root_admission.go"
}

func rootAdmissionTypeAliases(file *ast.File) map[string]string {
	types := map[string]string{"RootAdmission": "RootAdmission", "RootedSource": "RootedSource"}
	changed := true
	for changed {
		changed = false
		ast.Inspect(file, func(node ast.Node) bool {
			declaration, ok := node.(*ast.TypeSpec)
			if !ok || types[declaration.Name.Name] != "" {
				return true
			}
			if target, ok := declaration.Type.(*ast.Ident); ok && types[target.Name] != "" {
				types[declaration.Name.Name] = types[target.Name]
				changed = true
			}
			return true
		})
	}
	return types
}

func TestS1RootAdmissionOwnerGuardRejectsNewMintAndRetainedBypass(t *testing.T) {
	for _, row := range []struct {
		name, source string
	}{
		{"internal/runtime/foreign.go", `package fixture; import a "github.com/division-sh/swarm/internal/sourceartifact"; var restore = a.RestoreRootedSource`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; var root = RootAdmission{rootDirectory: "invented"}`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; var source = RootedSource{artifact: nil}`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; type Alias = RootAdmission; var root = Alias{rootDirectory: "invented"}`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; func replace(root *RootAdmission) { root.rootDirectory = "invented" }`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; var restore = RestoreRootedSource`},
		{"internal/sourceartifact/foreign.go", `package sourceartifact; type Alias RootAdmission; func convert(root Alias) RootAdmission { return RootAdmission(root) }`},
	} {
		violations, err := rootAdmissionOwnerViolations(row.name, []byte(row.source))
		if err != nil || len(violations) != 1 {
			t.Fatalf("foreign root owner accepted: %s: %v, %v", row.source, violations, err)
		}
	}
	for _, row := range []struct {
		name, source string
	}{
		{"internal/store/internal/sourceartifact/root_admission.go", `package sourceartifactstore; import a "github.com/division-sh/swarm/internal/sourceartifact"; var restore = a.RestoreRootedSource`},
		{"internal/sourceartifact/admit_unix.go", `package sourceartifact; var root = RootAdmission{rootDirectory: "shop"}`},
		{"internal/sourceartifact/root_persistence.go", `package sourceartifact; var source = RootedSource{artifact: nil}`},
		{"internal/runtime/foreign.go", `package fixture; import a "github.com/division-sh/swarm/internal/sourceartifact"; var ref = a.RootAdmissionReference{RootDirectory: "shop"}`},
	} {
		violations, err := rootAdmissionOwnerViolations(row.name, []byte(row.source))
		if err != nil || len(violations) != 0 {
			t.Fatalf("canonical root owner rejected: %s: %v, %v", row.source, violations, err)
		}
	}
}
