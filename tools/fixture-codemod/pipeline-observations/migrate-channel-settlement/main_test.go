package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"golang.org/x/tools/go/packages"
)

func TestChannelSettlementWholeTransformMatchesEverySignedDeclaration(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	pins, err := channelsettlement.ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := channelsettlement.ValidateCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	original := originalOverlay(root)
	queries := projectionQueries(root)
	for _, pin := range pins {
		before, err := channelsettlement.Body(original[filepath.Join(root, pin.File)], pin.Function)
		if err != nil || before != pin.Before {
			t.Fatal("not the exact signed predecessor:", pin.Function, err)
		}
		after, calls := transform(pin.Before, queries)
		normal, err := requestkey.Normalize(after)
		if err != nil || normal != pin.After || calls != pin.Calls {
			t.Fatal("finite rule/source correspondence changed:", pin.Function, err)
		}
	}
}

func TestChannelSettlementTransformRejectsUnknownQueriesAndUnsafeCalls(t *testing.T) {
	for _, source := range []string{
		`func waitObjectCardReceipt(t *testing.T,db *sql.DB,card string)string{var raw string;db.QueryRow("SELECT foreign_value FROM foreign_table").Scan(&raw);return raw}`,
		`func caller(t *testing.T,h *channelOnboardingE2EHarness,card string){waitObjectCardReceipt(t,openRawPool(),card)}`,
		`func caller(t *testing.T,h *channelOnboardingE2EHarness,db *sql.DB,card string){waitObjectCardReceipt:=func(a,b,c any){};waitObjectCardReceipt(t,db,card)}`,
		`func caller(t *testing.T,h *channelOnboardingE2EHarness,db *sql.DB){waitChannelDeliverySendsSettled(t,db,sideEffect())}`,
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("unsafe channel source accepted: %s", source)
				}
			}()
			transform(source, map[string]projection{})
		}()
	}
}

func TestChannelSettlementInventoryRejectsExcludedOwnedConsumers(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "internal/serveapp")
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "internal/store/storetest"), 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "excluded_test.go")
	if err := os.WriteFile(path, []byte("//go:build never\n\npackage serveapp\nfunc hidden(){waitObjectCardReceipt()}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := checkExcludedSources(root, nil); err == nil {
		t.Fatal("excluded channel helper escaped the inventory")
	}
	if err := checkExcludedSources(root, map[string]bool{path: true}); err != nil {
		t.Fatal("active source was not left to its typed owner:", err)
	}
}

func TestChannelSettlementTypedInventoryRejectsAliasesAndInitializers(t *testing.T) {
	for _, test := range []struct {
		name, source string
		want         int
		refuse       bool
	}{
		{"direct", "func caller(){waitObjectCardReceipt()}", 1, false},
		{"shadowed", "func caller(){waitObjectCardReceipt:=func(){};waitObjectCardReceipt()}", 0, false},
		{"alias", "func caller(){alias:=waitObjectCardReceipt;alias()}", 0, true},
		{"callback", "func caller(){consume(waitObjectCardReceipt)};func consume(func()){}", 0, true},
		{"initializer", "var alias=waitObjectCardReceipt;func caller(){alias()}", 0, true},
		{"initializer-call", "var value=func()int{waitObjectCardReceipt();return 1}()", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "owned.go", "package serveapp\nfunc waitObjectCardReceipt(){}\n"+test.source, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}}
			checked, err := new(types.Config).Check("github.com/division-sh/swarm/internal/serveapp", set, []*ast.File{file}, info)
			if err != nil {
				t.Fatal(err)
			}
			pkg := &packages.Package{Fset: set, Types: checked, TypesInfo: info, Syntax: []*ast.File{file}}
			covered := map[*ast.Ident]bool{}
			count := 0
			for _, node := range file.Decls {
				if fn, ok := node.(*ast.FuncDecl); ok {
					calls, callErr := references(pkg, fn, covered)
					count += len(calls)
					if callErr != nil {
						err = callErr
					}
				}
			}
			if coverageErr := completeReferences(pkg, covered); coverageErr != nil {
				err = coverageErr
			}
			if (err != nil) != test.refuse || !test.refuse && count != test.want {
				t.Fatalf("typed census count=%d error=%v, want=%d refusal=%t", count, err, test.want, test.refuse)
			}
		})
	}
}
