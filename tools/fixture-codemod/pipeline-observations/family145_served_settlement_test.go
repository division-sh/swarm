package main

import (
	_ "embed"
	"go/ast"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-served-settlement-preimages.json
var servedSettlementBytes []byte

func assertServedSettlementHostileCuts(t *testing.T, file, function, source string, finite recipe) {
	t.Helper()
	for _, cut := range []string{"reader", "ReceiverStateReader", "context.Background()", "ctx", "runID", "eventID", "subscriberType", "subscriberID", "outcome", "wantScope", " == want", " != ", "time.Sleep", "deadline", "probe.Expect", "RequireNodePending", "HandlerStarted", "PostCommitDispatch", "once.Do", "<-release", "proofs <-", "close(release)", "t.Fatal"} {
		if !strings.Contains(source, cut) {
			continue
		}
		mutant := strings.Replace(source, cut, "foreignServedSettlementCut", 1)
		if _, _, err := rewriteFunction(file, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("changed owner/argument/poll/probe/barrier/assertion cut accepted:", function, cut)
		}
	}
}

func servedSettlementDeclarationUsesOwner(fn *ast.FuncDecl) bool {
	return servedDeclarationUsesChanges(fn, servedsettlement.Changes)
}

func servedDeclarationUsesChanges(fn *ast.FuncDecl, changes map[string]servedsettlement.Change) bool {
	if _, owner := changes[fn.Name.Name]; owner {
		return true
	}
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			if _, owner := changes[ident.Name]; owner {
				found = true
			}
		}
		return true
	})
	return found
}

func servedSettlementEarlierDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedSetupEarlierSource(t, file, function, current)
	for _, pin := range servedPublicationLookupPins(t) {
		if pin.File == file && pin.Function == function {
			return servedPublicationLookupEarlierDeclaration(t, file, function, current)
		}
	}
	return current
}
