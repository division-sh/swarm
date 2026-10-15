package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestNoticeTransformPreservesEveryOtherByteAndRejectsForeignCarriers(t *testing.T) {
	const body = `func proof(t *testing.T) {
	before := readForkReceiverNoticeDomain(t, rt, seed.RunID)
	if !reflect.DeepEqual(before, readForkReceiverNoticeDomain(t, rt, fork.ForkRunID)) { t.Fatal("notice mutation") }
	release()
}`
	after, count := noticeTransform(t, body)
	if count != 2 || after != strings.ReplaceAll(body, "readForkReceiverNoticeDomain(t, rt,", "readForkReceiverNoticeDomain(t, rt.ReceiverStateReader,") {
		t.Fatal("notice transform changed assertions/predicates/temporal cuts")
	}
	for _, mutant := range []string{
		strings.Replace(body, "(t, rt,", "(t, foreign,", 1),
		strings.Replace(body, "(t, rt, seed.RunID)", "(t, rt, seed.RunID, foreign)", 1),
		strings.Replace(body, "(t, rt, seed.RunID)", "(t, rt, seed.RunID...)", 1),
		strings.Replace(body, "before :=", "readForkReceiverNoticeDomain := foreign; before :=", 1),
	} {
		t.Run(mutant, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("unclassified notice call accepted")
				}
			}()
			noticeTransform(t, mutant)
		})
	}
}

func noticeTransform(t *testing.T, source string) (string, int) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	return transform(fset, file.Decls[0].(*ast.FuncDecl), source)
}
