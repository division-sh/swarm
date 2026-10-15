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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-served-publication-lookup-preimages.json
var servedPublicationLookupBytes []byte

func servedPublicationLookupPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.DecodeLookup(servedPublicationLookupBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedPublicationLookupEarlierDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	for _, pin := range servedPublicationLookupPins(t) {
		if pin.File != file || pin.Function != function {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("../../..", file))
		if err != nil || servedSetupEarlierSource(t, file, function, requestKeyDeclaration(t, raw, function)) != pin.After || current != pin.After {
			t.Fatal("lookup composition requires the actual unique final source:", function, err)
		}
		return pin.Before
	}
	return current
}

func TestFamily145ServedPublicationLookupWholeTypedChainKeepsEveryPredicateAndAssertion(t *testing.T) {
	calls := 0
	for _, pin := range servedPublicationLookupPins(t) {
		old, err := exec.Command("git", "show", servedsettlement.LookupPredecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, old, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("publication lookup signed predecessor changed:", pin.Function, err)
		}
		after, count, err := servedsettlement.TransformLookup(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("publication lookup escaped the typed finite rule:", pin.Function, count, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedPublicationLookupEarlierDeclaration(t, pin.File, pin.Function, servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))) != pin.Before {
			t.Fatal("publication lookup actual current declaration changed:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("publication lookup finite rewrite refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("publication lookup rewrite is not inert:", pin.Function, err)
		}
		assertServedSettlementHostileCuts(t, pin.File, pin.Function, pin.After, finite)
		for _, cut := range []string{"json.Unmarshal", "eventName", "subscriberIDs", "Payload", "Entity", "TrimSpace", "sql.ErrNoRows", "installServeRuntimeEmptyPostgresTestStores"} {
			if !strings.Contains(pin.After, cut) {
				continue
			}
			mutant := strings.Replace(pin.After, cut, "foreignPublicationLookupCut", 1)
			if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
				t.Fatal("changed lookup decode/null/name/disjunction/wait/setup accepted:", pin.Function, cut)
			}
		}
		calls += pin.Calls
	}
	if calls != 17 {
		t.Fatal("publication lookup complete caller multiplicity changed:", calls)
	}
}

func TestFamily145ServedPublicationLookupRefusesUnknownCarrierAndCallableAlias(t *testing.T) {
	for _, source := range []string{
		`func foreign(){servedEventPayloadObject(t,foreignOwner,backend,event)}`,
		`func foreign(){saved:=servedEventPayloadObject; saved(t,db,backend,event)}`,
		`func foreign(){servedEventPayloadObject:=foreign;servedEventPayloadObject(t,db,backend,event)}`,
		`func foreign(){waitServedEventPublishEventID(t,db)}`,
		`func servedEventPayloadObject(t *testing.T,db *foreign.DB,backend,event string)int{return 1}`,
	} {
		if _, _, err := servedsettlement.TransformLookup(source); err == nil {
			t.Fatal("unknown/shadowed/raw alias lookup shape passed preflight:", source)
		}
	}
	if _, err := servedsettlement.DecodeLookup(append(append([]byte{}, servedPublicationLookupBytes...), ' ')); err == nil {
		t.Fatal("publication lookup accepted changed complete manifest bytes")
	}
}

func TestFamily145ServedPublicationLookupCompleteCallerInventoryHasNoSQLCarrier(t *testing.T) {
	expected := map[string]string{}
	for _, pin := range servedPublicationLookupPins(t) {
		expected[pin.File+"/"+pin.Function] = pin.After
	}
	err := checkoutsource.WalkDir("../../..", "../../../internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
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
			if !ok || fn.Recv != nil || !servedDeclarationUsesChanges(fn, servedsettlement.LookupChanges) {
				continue
			}
			key := filepath.ToSlash(relative) + "/" + fn.Name.Name
			if expected[key] == "" || family145NodeString(fn) != expected[key] {
				return fmt.Errorf("unrecorded/changed publication lookup caller or owner: %s", key)
			}
			if _, owner := servedsettlement.LookupChanges[fn.Name.Name]; owner && strings.Contains(family145NodeString(fn.Type), "sql.DB") {
				return fmt.Errorf("publication lookup owner retains SQL carrier: %s", key)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
