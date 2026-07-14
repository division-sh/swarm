package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEventBusSubscribedRecipientIDsRemainReadOnlyObserverData(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read bus package: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range parsed.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			receiverName, eventBusReceiver := eventBusMethodReceiver(method)
			if !ok || !eventBusReceiver {
				continue
			}
			if method.Name.Name == "deliverByType" {
				t.Errorf("%s: deliverByType restores tokenless subscribed live dispatch", name)
			}
			if method.Name.Name == "ResolveSubscribedRecipients" {
				continue
			}
			ast.Inspect(method.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				calledReceiver, receiverOK := selector.X.(*ast.Ident)
				if receiverOK && calledReceiver.Name == receiverName &&
					(selector.Sel.Name == "resolveSubscribedRecipients" || selector.Sel.Name == "ResolveSubscribedRecipients") {
					t.Errorf("%s: EventBus.%s consumes tokenless subscribed-recipient IDs; use exact route candidates", name, method.Name.Name)
				}
				return true
			})
		}
	}
}

func eventBusMethodReceiver(method *ast.FuncDecl) (string, bool) {
	if method == nil || method.Recv == nil || len(method.Recv.List) != 1 {
		return "", false
	}
	receiverField := method.Recv.List[0]
	receiver := receiverField.Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	identifier, ok := receiver.(*ast.Ident)
	if !ok || identifier.Name != "EventBus" || len(receiverField.Names) != 1 {
		return "", false
	}
	return receiverField.Names[0].Name, true
}
