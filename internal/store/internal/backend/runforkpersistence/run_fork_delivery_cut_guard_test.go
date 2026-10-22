package runforkpersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func replayCutConsumerGuard(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "replay.go", source, 0)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	for _, decl := range file.Decls {
		if types, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range types.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || typeSpec.Name.Name != "runForkDeliveryEventReplayAdapter" {
					continue
				}
				structure, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					return fmt.Errorf("replay adapter ceased to be explicit")
				}
				for _, field := range structure.Fields.List {
					for _, name := range field.Names {
						if name.Name == "loadSource" {
							return fmt.Errorf("current-source replay callback restored")
						}
					}
				}
			}
		}
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "applyRunForkDeliveryEventReplay" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch callee := call.Fun.(type) {
			case *ast.Ident:
				counts[callee.Name]++
			case *ast.SelectorExpr:
				if receiver, ok := callee.X.(*ast.Ident); ok && receiver.Name == "cut" {
					counts["cut."+callee.Sel.Name]++
				}
				if callee.Sel.Name == "Snapshot" || callee.Sel.Name == "loadSource" {
					counts["current-source-read"]++
				}
			}
			return true
		})
	}
	for _, required := range []string{"newRunForkReplayCut", "cut.event", "cut.delivery"} {
		if counts[required] != 1 {
			return fmt.Errorf("replay must consume exactly one %s path", required)
		}
	}
	if counts["current-source-read"] != 0 || counts["loadRunForkReplaySourceEvent"] != 0 || counts["loadSQLiteRunForkReplaySourceEvent"] != 0 {
		return fmt.Errorf("replay source must come from the admitted cut, not current rows")
	}
	return nil
}

func TestRunForkReplayConsumesFixedCutWithoutCurrentSourceFallback(t *testing.T) {
	source, err := os.ReadFile("run_fork_delivery_event_replay.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := replayCutConsumerGuard(source); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []struct{ name, old, replacement string }{
		{"live_publication", "cut.event(sourceEventID)", "store.loadSource(ctx, tx, lineage.SourceRunID, sourceEventID)"},
		{"live_delivery", "cut.delivery(sourceEventID, sourceDeliveryID)", "store.deliveries.Snapshot(ctx, tx, sourceDeliveryID)"},
		{"unbound_cut", "newRunForkReplayCut(snapshot)", "otherCut(snapshot)"},
		{"source_callback", "type runForkDeliveryEventReplayAdapter struct {", "type runForkDeliveryEventReplayAdapter struct {\nloadSource func(context.Context, *sql.Tx, string, string) (events.Event, error)"},
	} {
		t.Run(fault.name, func(t *testing.T) {
			if strings.Count(string(source), fault.old) != 1 {
				t.Fatalf("restoration anchor %q must stay exact", fault.old)
			}
			hostile := strings.Replace(string(source), fault.old, fault.replacement, 1)
			if err := replayCutConsumerGuard([]byte(hostile)); err == nil {
				t.Fatal("restored live interpretation escaped the guard")
			}
		})
	}
}
