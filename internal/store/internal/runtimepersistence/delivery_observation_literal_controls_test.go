package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func parseDeliveryObservationControl(t *testing.T, path, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestReviewedDeliveryObservationSitesAreExact(t *testing.T) {
	root := eventBoundaryRepositoryRoot(t)
	ownerPath := "internal/store/internal/runtimepersistence/test_selected_storage_observation.go"
	ownerBytes, err := os.ReadFile(filepath.Join(root, ownerPath))
	if err != nil {
		t.Fatal(err)
	}
	owner := parseDeliveryObservationControl(t, ownerPath, string(ownerBytes))
	total := 0
	for _, site := range reviewedDeliveryObservations {
		data, err := os.ReadFile(filepath.Join(root, site.path))
		if err != nil {
			t.Fatal(err)
		}
		file := parseDeliveryObservationControl(t, site.path, string(data))
		approved := reviewedDeliveryObservationLiterals(site.path, file, owner)
		fn := deliveryObservationDeclaration(file, site.declaration)
		if fn == nil {
			t.Fatal("missing reviewed declaration", site.declaration)
		}
		for _, literal := range deliveryObservationLiterals(fn) {
			if approved[literal.Pos()] != site.path+"/"+site.declaration {
				t.Fatalf("reviewed literal lost exact declaration custody: %s", site.declaration)
			}
			total++
		}
		if _, allowed := executableDeliverySQLOwners[site.path]; allowed {
			t.Fatal("whole-file permission substituted for exact sites")
		}
	}
	if len(reviewedDeliveryObservations) != 3 || total != 8 {
		t.Fatalf("reviewed declaration/literal counts changed: %d/%d", len(reviewedDeliveryObservations), total)
	}
}

func TestReviewedDeliveryObservationHostilesStayUnowned(t *testing.T) {
	root := eventBoundaryRepositoryRoot(t)
	ownerPath := "internal/store/internal/runtimepersistence/test_selected_storage_observation.go"
	ownerBytes, err := os.ReadFile(filepath.Join(root, ownerPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, site := range reviewedDeliveryObservations {
		data, err := os.ReadFile(filepath.Join(root, site.path))
		if err != nil {
			t.Fatal(err)
		}
		for _, attack := range []string{"mutation", "multi-mutation", "multi-select", "same-count-query", "extra", "moved", "foreign-declaration", "foreign-path", "imports", "delegation", "owner-delegation", "owner-imports", "duplicate-declaration", "removed-declaration", "extra-foreign-declaration"} {
			for index := range site.literals {
				t.Run(site.declaration+"/"+attack+"/"+strconv.Itoa(index), func(t *testing.T) {
					path := site.path
					file := parseDeliveryObservationControl(t, path, string(data))
					owner := parseDeliveryObservationControl(t, ownerPath, string(ownerBytes))
					fn := deliveryObservationDeclaration(file, site.declaration)
					literals := deliveryObservationLiterals(fn)
					literal := literals[index]
					query, err := strconv.Unquote(literal.Value)
					if err != nil {
						t.Fatal(err)
					}
					switch attack {
					case "mutation":
						literal.Value = strconv.Quote("UPDATE event_deliveries SET status='delivered'")
					case "multi-mutation":
						literal.Value = strconv.Quote(query + "; DELETE FROM event_deliveries")
					case "multi-select":
						literal.Value = strconv.Quote(query + "; SELECT 1")
					case "same-count-query":
						literal.Value = strconv.Quote(strings.Replace(query, "$1", "$2", 1))
						if literal.Value == strconv.Quote(query) {
							t.Fatal("hostile query was not changed")
						}
					case "extra":
						fn.Body.List = append(fn.Body.List, deliveryObservationControlStatement(literal.Value))
					case "moved":
						literal.Value = strconv.Quote("SELECT 1")
						file.Decls = append(file.Decls, deliveryObservationControlDeclaration(strconv.Quote(query)))
					case "foreign-declaration":
						fn.Name.Name += "Foreign"
					case "foreign-path":
						path = "internal/store/internal/runtimepersistence/foreign_observation.go"
					case "imports":
						file.Imports[0].Name = ast.NewIdent("foreign")
					case "delegation", "owner-delegation":
						target, name := ast.Node(fn), "readServedDeliveryObservation"
						if attack == "owner-delegation" {
							target, name = deliveryObservationDeclaration(owner, "readServedDeliveryObservation"), "RunReadTransaction"
						}
						changed := false
						ast.Inspect(target, func(node ast.Node) bool {
							if ident, ok := node.(*ast.Ident); ok && ident.Name == name {
								ident.Name += "Foreign"
								changed = true
							}
							return true
						})
						if !changed {
							t.Fatal("hostile delegation was not changed")
						}
					case "owner-imports":
						owner.Imports[0].Name = ast.NewIdent("foreign")
					case "duplicate-declaration":
						file.Decls = append(file.Decls, fn)
					case "removed-declaration":
						for i, decl := range file.Decls {
							if decl == fn {
								file.Decls = append(file.Decls[:i], file.Decls[i+1:]...)
								break
							}
						}
					case "extra-foreign-declaration":
						file.Decls = append(file.Decls, deliveryObservationControlDeclaration(literal.Value))
					}
					approved := reviewedDeliveryObservationLiterals(path, file, owner)
					matched, unowned := 0, 0
					ast.Inspect(file, func(node ast.Node) bool {
						lit, ok := node.(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							return true
						}
						text, err := strconv.Unquote(lit.Value)
						if err == nil && executableDeliverySQL.MatchString(text) {
							if approved[lit.Pos()] == site.path+"/"+site.declaration {
								matched++
							} else if approved[lit.Pos()] == "" {
								unowned++
							}
						}
						return true
					})
					if matched == len(site.literals) && unowned == 0 {
						t.Fatal("hostile source retained complete exact-site permission")
					}
				})
			}
		}
	}
}

func deliveryObservationControlStatement(query string) ast.Stmt {
	return &ast.ExprStmt{X: &ast.CallExpr{Fun: ast.NewIdent("unreviewedRead"), Args: []ast.Expr{&ast.BasicLit{Kind: token.STRING, Value: query}}}}
}

func deliveryObservationControlDeclaration(query string) *ast.FuncDecl {
	return &ast.FuncDecl{Name: ast.NewIdent("unreviewedDeliveryRead"), Type: &ast.FuncType{Params: &ast.FieldList{}}, Body: &ast.BlockStmt{List: []ast.Stmt{deliveryObservationControlStatement(query)}}}
}
