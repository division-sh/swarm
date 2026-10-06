package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestConformanceMutationProjectionRewritePreservesScopeOrderingAndRefusals(t *testing.T) {
	for _, mutation := range []string{"none", "foreign-query", "foreign-scan", "changed-scope", "changed-order", "changed-null", "effectful-db", "unknown-root", "already-migrated"} {
		t.Run(mutation, func(t *testing.T) {
			source := `package conformance
func trackedMutationStateMatchesEntityState(db *sql.DB,runID,entityID string)error{
 var currentState string;var fieldsRaw,bookRaw,gatesRaw,accRaw []byte
 if err:=db.QueryRowContext(testAuthorActivityContext(context.Background()), ` + "`SELECT COALESCE(current_state, ''), COALESCE(fields, '{}'::jsonb), COALESCE(bookkeeping, '{}'::jsonb), COALESCE(gates, '{}'::jsonb), COALESCE(accumulator, '{}'::jsonb) FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`" + `,runID,entityID).Scan(&currentState,&fieldsRaw,&bookRaw,&gatesRaw,&accRaw);err!=nil{return fmt.Errorf("load entity_state projection: %w",err)}
 want:=runtimemutationlog.EntityStateProjection{CurrentState:strings.TrimSpace(currentState)}
 var err error
 if want.Fields,err=decodeJSONMapErr(fieldsRaw);err!=nil{return fmt.Errorf("decode entity_state fields: %w",err)}
 records:=make([]runtimemutationlog.ProjectionMutation,0,8)
 rows,err:=db.QueryContext(testAuthorActivityContext(context.Background()), ` + "`SELECT domain, path, new_value FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid ORDER BY created_at ASC, mutation_id ASC`" + `,runID,entityID);if err!=nil{return err};defer rows.Close()
 rowCount:=0
 for rows.Next(){var domain,path string;var value []byte;if err:=rows.Scan(&domain,&path,&value);err!=nil{return err};rowCount++;records=append(records,originalMutation)}
 if rowCount==0{return fmt.Errorf("entity_mutations is empty; canonical mutation surface is missing")}
 got,err:=runtimemutationlog.ReconstructEntityStateProjection(records);if err!=nil{return fmt.Errorf("reconstruct mutation state: %w",err)}
 if !trackedStatesEqual(got,want){return fmt.Errorf("mutation reconstruction mismatch")};return nil
}`
			switch mutation {
			case "changed-scope":
				source = strings.ReplaceAll(source, "AND entity_id = $2::uuid", "AND entity_id = $2::uuid AND current_state='active'")
			case "changed-order":
				source = strings.ReplaceAll(source, "created_at ASC, mutation_id ASC", "created_at DESC, mutation_id DESC")
			case "changed-null":
				source = strings.ReplaceAll(source, "SELECT domain, path, new_value", "SELECT domain, path, COALESCE(new_value,'null')")
			case "effectful-db":
				source = strings.ReplaceAll(source, "db.QueryContext", "openDB().QueryContext")
			case "unknown-root":
				source = strings.ReplaceAll(source, "trackedMutationStateMatchesEntityState", "unknownProjection")
			case "already-migrated":
				source = strings.ReplaceAll(source, "db *sql.DB", "selected any")
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture_test.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selector.Sel.Name {
				case "QueryRowContext", "QueryContext", "Scan":
				default:
					return true
				}
				pkg := "database/sql"
				if mutation == "foreign-query" && selector.Sel.Name != "Scan" || mutation == "foreign-scan" && selector.Sel.Name == "Scan" {
					pkg = "example/foreign"
				}
				info.Uses[selector.Sel] = types.NewFunc(0, types.NewPackage(pkg, "fixture"), selector.Sel.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))
				return true
			})
			updated, ok := rewriteConformanceMutationProjectionOwner(fset, info, file.Decls[0].(*ast.FuncDecl))
			if ok != (mutation == "none") {
				t.Fatalf("matched=%t output=%s", ok, updated)
			}
			if ok && (strings.Contains(updated, "db.Query") || strings.Contains(updated, "*sql.DB") || strings.Contains(updated, "rows.Next") || !strings.Contains(updated, "ReadTrackedEntityMutationProjectionStorage") || !strings.Contains(updated, "rowCount == 0") || !strings.Contains(updated, "mutation reconstruction mismatch") || !strings.Contains(updated, "decode mutation value: %w")) {
				t.Fatal("native snapshot or reconstruction refusal assertions changed")
			}
		})
	}
}
