package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func inventory(root string) error {
	definitions, references := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		if file.Name.Name != "serveapp" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		allowed := allowedReferences(file, filepath.ToSlash(relative), &definitions, &references)
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Name == "logBudgetNoticeFailure" && !allowed[id] {
				err = fmt.Errorf("unclassified budget logger reference in owned source: %s", relative)
			}
			return true
		})
		return err
	})
	if err != nil {
		return err
	}
	if definitions != 1 || references != 1 {
		return fmt.Errorf("budget logger inventory changed: definitions=%d references=%d", definitions, references)
	}
	return nil
}

func allowedReferences(file *ast.File, path string, definitions, references *int) map[*ast.Ident]bool {
	allowed := map[*ast.Ident]bool{}
	if path != sourcePath {
		return allowed
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if fn.Name.Name == "logBudgetNoticeFailure" {
			*definitions++
			allowed[fn.Name] = true
		}
		if fn.Name.Name == "TestChannelDeliveryBudgetNoticePublicJourney" {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "logBudgetNoticeFailure" {
						allowed[id] = true
						*references++
					}
				}
				return true
			})
		}
	}
	return allowed
}
