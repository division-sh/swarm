package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

func TestFamily145DeliveryStatusInventoryTraversalControls(t *testing.T) {
	pins := deliveryStatusPins(t)
	var bodies []string
	for _, pin := range pins {
		if pin.File != "internal/serveapp/main_runtime_test.go" {
			t.Fatal("finite delivery-status controls require the original file identity")
		}
		bodies = append(bodies, pin.After)
	}
	source := "package serveapp\n" + strings.Join(bodies, "\n")
	// These fixtures are at the pinned delivery-status stage. The real checkout
	// inventory separately runs the complete, unchanged inverse chain.
	exactSource := func(path, function, current string) bool {
		return deliverystatus.Previous(pins, path, function, current) != current
	}
	secondCallSource := deliveryStatusSecondCallMutation(t, source)
	for _, control := range []struct {
		name, filename, source string
		calls, definitions     int
		accepted               bool
	}{
		{"all_original_pins", "main_runtime_test.go", source, 28, 4, true},
		{"second_call_identity", "main_runtime_test.go", secondCallSource, 28, 4, false},
		{"same_name_second_declaration", "main_runtime_test.go", source + "\n" + requestKeyDeclaration(t, []byte(secondCallSource), "runServedDynamicAutoEmitProof"), 32, 4, false},
		{"foreign_source_path", "foreign.go", source, 28, 4, false},
		{"source_only_poll_change", "main_runtime_test.go", strings.Replace(source, "25 * time.Millisecond", "26 * time.Millisecond", 1), 28, 4, false},
		{"package_alias", "main_runtime_test.go", source + "\nvar escaped = servedEventPublishDeliveryStatusCount\n", 28, 4, false},
		{"initializer_call", "main_runtime_test.go", source + "\nvar escaped = servedEventPublishDeliveryStatusCount(nil, nil, \"sqlite\", \"event\", \"run\", \"id\")\n", 28, 4, false},
		{"local_alias_without_calls", "main_runtime_test.go", source + "\nfunc foreignAlias() { escaped := servedEventPublishDeliveryStatusCount; _ = escaped }\n", 28, 4, false},
		{"foreign_caller", "main_runtime_test.go", source + "\nfunc foreignCaller() { servedEventPublishDeliveryStatusCount(nil, nil, \"sqlite\", \"event\", \"run\", \"id\") }\n", 29, 4, false},
		{"extra_helper_definition", "main_runtime_test.go", source + "\nfunc servedEventPublishDeliveryStatusCount() {}\n", 28, 5, false},
		{"missing_helper_definition", "main_runtime_test.go", strings.Replace(source, pins[len(pins)-1].After, "", 1), 28, 3, false},
	} {
		t.Run(control.name, func(t *testing.T) {
			root := t.TempDir()
			writeDeliveryStatusInventorySource(t, root, control.filename, control.source)
			legacy, current := compareDeliveryStatusInventory(t, root, exactSource)
			if current.calls != control.calls || current.definitions != control.definitions || deliveryStatusInventoryAccepted(current) != control.accepted {
				t.Fatalf("inventory=%+v accepted=%t want %d/%d accepted=%t", current, deliveryStatusInventoryAccepted(current), control.calls, control.definitions, control.accepted)
			}
			wantCompositions := 13
			if control.name == "foreign_caller" || control.name == "same_name_second_declaration" {
				wantCompositions++
			}
			if legacy.compositions != control.calls || current.compositions != wantCompositions {
				t.Fatalf("complete declarations composed %d/%d want%d/%d", legacy.compositions, current.compositions, control.calls, wantCompositions)
			}
			if control.name == "second_call_identity" && !strings.Contains(fmt.Sprint(current.err), "escaped exact chain") {
				t.Fatal("second call escaped the whole-declaration source check", current.err)
			}
			if control.name == "package_alias" || control.name == "initializer_call" || control.name == "local_alias_without_calls" {
				if !strings.Contains(fmt.Sprint(current.err), "alias/initializer") {
					t.Fatal("alias/initializer escaped its independent identifier check", current.err)
				}
			}
		})
	}

	t.Run("same_path_size_mtime_changed_source_is_reread", func(t *testing.T) {
		root := t.TempDir()
		path := writeDeliveryStatusInventorySource(t, root, "main_runtime_test.go", source)
		before, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		_, first := compareDeliveryStatusInventory(t, root, exactSource)
		if !deliveryStatusInventoryAccepted(first) {
			t.Fatal("original source did not pass", first.err)
		}
		changed := strings.Replace(source, "25 * time.Millisecond", "26 * time.Millisecond", 1)
		if changed == source || len(changed) != len(source) {
			t.Fatal("re-read control must change bytes without changing size")
		}
		writeDeliveryStatusInventorySource(t, root, "main_runtime_test.go", changed)
		if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
			t.Fatal(err)
		}
		after, err := os.Stat(path)
		if err != nil || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
			t.Fatal("re-read control changed its path/size/mtime envelope", err)
		}
		_, second := compareDeliveryStatusInventory(t, root, exactSource)
		if deliveryStatusInventoryAccepted(second) || second.calls != 28 || second.definitions != 4 || second.compositions != 13 || second.err == nil {
			t.Fatal("subsequent scan reused a prior source verdict", second)
		}
	})

	for _, control := range []string{"parse_failure", "missing_scan_root", "source_symlink"} {
		t.Run(control, func(t *testing.T) {
			root := t.TempDir()
			if control != "missing_scan_root" {
				writeDeliveryStatusInventorySource(t, root, "main_runtime_test.go", source)
			}
			switch control {
			case "parse_failure":
				writeDeliveryStatusInventorySource(t, root, "z_bad.go", "package serveapp\nfunc broken(")
			case "source_symlink":
				outside := filepath.Join(t.TempDir(), "outside.go")
				if err := os.WriteFile(outside, []byte("package foreign\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(root, "internal/serveapp/z_bad.go")); err != nil {
					t.Fatal(err)
				}
			}
			_, current := compareDeliveryStatusInventory(t, root, exactSource)
			if current.err == nil || deliveryStatusInventoryAccepted(current) {
				t.Fatal("parse/walk failure was not retained", current)
			}
		})
	}
}

func deliveryStatusSecondCallMutation(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "main_runtime_test.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "runServedDynamicAutoEmitProof" {
			continue
		}
		calls := 0
		ast.Inspect(fn, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && deliverystatus.Helpers[id.Name] {
					calls++
					if calls == 2 {
						call.Args[0] = ast.NewIdent("foreignCallerIdentity")
					}
				}
			}
			return true
		})
		if calls != 4 {
			t.Fatalf("second-call control has %d calls, want4", calls)
		}
		return requestkey.Node(file)
	}
	t.Fatal("missing original multi-call declaration")
	return ""
}

func writeDeliveryStatusInventorySource(t *testing.T, root, filename, source string) string {
	t.Helper()
	dir := filepath.Join(root, "internal/serveapp")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func deliveryStatusInventoryAccepted(result deliveryStatusInventoryResult) bool {
	return result.err == nil && result.calls == 28 && result.definitions == 4
}

func compareDeliveryStatusInventory(t *testing.T, root string, exactSource func(string, string, string) bool) (deliveryStatusInventoryResult, deliveryStatusInventoryResult) {
	t.Helper()
	started := time.Now()
	legacy := deliveryStatusLegacyInventory(root, exactSource)
	legacyElapsed := time.Since(started)
	started = time.Now()
	current := deliveryStatusOwnedInventory(root, exactSource)
	if legacy.calls != current.calls || legacy.definitions != current.definitions || (legacy.err == nil) != (current.err == nil) || deliveryStatusInventoryAccepted(legacy) != deliveryStatusInventoryAccepted(current) {
		t.Fatalf("inventory verdict/count drift: legacy=%+v current=%+v", legacy, current)
	}
	t.Logf("legacy=%d/%d accepted=%t compositions=%d elapsed=%s current=%d/%d accepted=%t compositions=%d elapsed=%s", legacy.calls, legacy.definitions, deliveryStatusInventoryAccepted(legacy), legacy.compositions, legacyElapsed, current.calls, current.definitions, deliveryStatusInventoryAccepted(current), current.compositions, time.Since(started))
	return legacy, current
}

// Reference the original per-call traversal, not the optimized enumerator, so
// the controls independently compare every call/definition/alias and verdict.
func deliveryStatusLegacyInventory(root string, exactSource func(string, string, string) bool) deliveryStatusInventoryResult {
	var result deliveryStatusInventoryResult
	walkErr := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		allowed := map[token.Pos]bool{}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if deliverystatus.Helpers[fn.Name.Name] {
				result.definitions++
				allowed[fn.Name.Pos()] = true
			}
			ast.Inspect(fn, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && deliverystatus.Helpers[id.Name] {
						result.calls++
						allowed[id.Pos()] = true
						result.compositions++
						if !exactSource(filepath.ToSlash(rel), fn.Name.Name, requestkey.Node(fn)) {
							result.err = errors.Join(result.err, fmt.Errorf("delivery status caller escaped exact chain: %s %s", rel, fn.Name.Name))
						}
					}
				}
				return true
			})
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && deliverystatus.Helpers[id.Name] && !allowed[id.Pos()] {
				result.err = errors.Join(result.err, fmt.Errorf("delivery status helper escaped as alias/initializer: %s %s", rel, id.Name))
			}
			return true
		})
		return nil
	})
	result.err = errors.Join(result.err, walkErr)
	return result
}
