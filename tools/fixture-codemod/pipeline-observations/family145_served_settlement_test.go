package main

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-served-settlement-preimages.json
var servedSettlementBytes []byte

func TestFamily145ServedSettlementWholeTypedChainPreservesEveryCallerAndTemporalCut(t *testing.T) {
	pins, err := servedsettlement.Decode(servedSettlementBytes)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, pin := range pins {
		old, err := exec.Command("git", "show", servedsettlement.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, old, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("served settlement signed complete predecessor changed:", pin.Function, err)
		}
		after, count, err := servedsettlement.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("served settlement escaped typed argument/body transformations:", pin.Function, count, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedSettlementEarlierDeclaration(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("served settlement current unique declaration changed:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("served settlement finite rewrite refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("served settlement rewrite is not inert:", pin.Function, err)
		}
		assertServedSettlementHostileCuts(t, pin.File, pin.Function, pin.After, finite)
		calls += pin.Calls
	}
	if calls != 46 {
		t.Fatal("served settlement complete typed caller inventory changed:", calls)
	}
}

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

func TestFamily145ServedSettlementTransformationRefusesUnknownCarriersAndCallableAliases(t *testing.T) {
	for _, source := range []string{
		`func foreign(){servedEventPublishNodeDeliveryCount(t,foreignOwner,backend,run,event,node)}`,
		`func foreign(){saved:=servedEventPublishNodeDeliveryCount; saved(t,db,backend,run,event,node)}`,
		`func foreign(){servedEventPublishNodeDeliveryCount:=foreign;servedEventPublishNodeDeliveryCount(t,db,backend,run,event,node)}`,
		`func foreign(){waitServedDeliveryOutcomeCount(t,db)}`,
		`func servedEventPublishNodeDeliveryCount(t *testing.T,db *foreign.DB,backend,run,event,node string)int{return 1}`,
	} {
		if _, _, err := servedsettlement.Transform(source); err == nil {
			t.Fatal("unknown/shadowed/raw alias shape passed finite preflight:", source)
		}
	}
	if _, err := servedsettlement.Decode(append(append([]byte{}, servedSettlementBytes...), ' ')); err == nil {
		t.Fatal("served settlement accepted changed complete manifest bytes")
	}
}

func TestFamily145ServedSettlementCompleteOwnerAndCallerInventoryHasNoSQLCarrier(t *testing.T) {
	pins, err := servedsettlement.Decode(servedSettlementBytes)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{}
	for _, pin := range pins {
		expected[pin.File+"/"+pin.Function] = pin.After
	}
	err = checkoutsource.WalkDir("../../..", "../../../internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel("../../..", path)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if !servedSettlementDeclarationUsesOwner(fn) {
				continue
			}
			key := filepath.ToSlash(relative) + "/" + fn.Name.Name
			if expected[key] == "" || servedSettlementEarlierDeclaration(t, filepath.ToSlash(relative), fn.Name.Name, family145NodeString(fn)) != expected[key] {
				return fmt.Errorf("unrecorded/changed served settlement caller or owner: %s", key)
			}
			if _, owner := servedsettlement.Changes[fn.Name.Name]; owner && strings.Contains(family145NodeString(fn.Type), "sql.DB") {
				return fmt.Errorf("served settlement owner retains raw SQL carrier: %s", key)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
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
