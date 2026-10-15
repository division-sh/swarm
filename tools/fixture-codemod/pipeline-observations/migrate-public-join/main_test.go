package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicjoin"
)

func TestFinitePublicJoinTransformMatchesEveryReviewedBodyAndRejectsForeignAuthority(t *testing.T) {
	raw, err := os.ReadFile("../family145-public-join-preimages.json")
	if err != nil {
		t.Fatal(err)
	}
	pins, err := publicjoin.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		t.Run(pin.Function, func(t *testing.T) {
			fset, fn := parsedFunction(t, pin.Before)
			after, count, err := transform(fset, fn, pin.Before)
			if err != nil || count != pin.Calls {
				t.Fatalf("reviewed transformation count=%d err=%v", count, err)
			}
			_, parsed := parsedFunction(t, after)
			if nodeString(parsed) != pin.After {
				t.Fatal("transform lost complete public join request/oracle/temporal source")
			}
			if helpers[pin.Function] == "" {
				return
			}
			for _, mutant := range []string{
				strings.Replace(pin.Before, "rt servedControlProofRuntime", "rt foreignCarrier", 1),
				strings.Replace(pin.Before, "t.Helper()", "t.Helper(); _ = rt.DB", 1),
				strings.Replace(pin.Before, "t.Helper()", "t.Helper(); _ = rt.Runtime", 1),
				strings.Replace(pin.Before, "t.Helper()", "t.Helper(); alias := rt; _ = alias", 1),
			} {
				fset, fn := parsedFunction(t, mutant)
				if _, _, err := transform(fset, fn, mutant); err == nil {
					t.Fatal("foreign/raw/runtime/alias authority accepted")
				}
			}
		})
	}
}

func parsedFunction(t *testing.T, source string) (*token.FileSet, *ast.FuncDecl) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	return fset, file.Decls[0].(*ast.FuncDecl)
}
