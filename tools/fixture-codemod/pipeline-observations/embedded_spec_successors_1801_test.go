package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

// USER6197217312 and reviewer-g6201157565 retire only the spec selector.
// Every earlier transformation and successor remains in this frozen packet.
//
//go:embed testdata/embedded_spec_successors_1801.json
var embeddedSpecSuccessorBytes1801 []byte

type embeddedSpecSuccessorPacket1801 struct {
	Source, Authorization, PopulationSHA256 string
	Rows                                    []fixtureSetupSuccessor2617
}

func loadEmbeddedSpecSuccessors1801(raw []byte) (embeddedSpecSuccessorPacket1801, error) {
	var packet embeddedSpecSuccessorPacket1801
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "461fec6b4bf08382294ffa6120f66ac9cd92b999a6c493fc06657a3d30576ace" {
		return packet, fmt.Errorf("embedded-spec successor evidence changed")
	}
	if err := json.Unmarshal(raw, &packet); err != nil {
		return packet, err
	}
	if packet.Source != "53788da1332cbecaf805b13722316257d7ac6829" ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/1801#issuecomment-6201157565" || len(packet.Rows) != 28 {
		return packet, fmt.Errorf("wrong embedded-spec source, authorization or finite population")
	}
	return packet, nil
}

func embeddedSpecPriorEndpoint1801(row recipe, actual string) (recipe, string, error) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		return recipe{}, "", err
	}
	for _, pin := range packet.Rows {
		if sameReviewedRecipeKey(row, pin.Original) {
			return reverseEmbeddedSpecSuccessor1801(row, actual, pin)
		}
	}
	return recipe{}, "", fmt.Errorf("unlisted embedded-spec successor")
}

func reverseEmbeddedSpecSuccessor1801(row recipe, actual string, pin fixtureSetupSuccessor2617) (recipe, string, error) {
	history := row
	history.Successor = pin.Original.Successor
	if !reflect.DeepEqual(history, pin.Original) || row.Successor == "" ||
		!canonicalComparatorEqual2617(row.Successor, pin.Candidate) || !canonicalComparatorEqual2617(actual, pin.Candidate) {
		return recipe{}, "", fmt.Errorf("embedded-spec current source or historical receipt differs")
	}
	prior := pin.Original.After
	if pin.Original.Successor != "" {
		prior = pin.Original.Successor
	}
	expected, err := embeddedSpecFieldDeletion1801(prior)
	if err != nil || !canonicalComparatorEqual2617(expected, pin.Candidate) {
		return recipe{}, "", fmt.Errorf("embedded-spec successor changes more than selector plumbing: %v", err)
	}
	reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || !canonicalComparatorEqual2617(reversed, prior) {
		return recipe{}, "", fmt.Errorf("embedded-spec successor lost its prior current endpoint: %v", err)
	}
	return pin.Original, prior, nil
}

func embeddedSpecFieldDeletion1801(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "prior.go", "package proof\n"+source, 0)
	if err != nil {
		return "", err
	}
	astutil.Apply(file, func(cursor *astutil.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *ast.Ident:
			if node.Name == "CLISourcePlatformSpecPaths" {
				node.Name = "CLISourcePaths"
			}
		case *ast.CompositeLit:
			if node.Type == nil {
				return true
			}
			switch formattedNativeReadNode(node.Type) {
			case "cliapp.ServeOptions", "cliapp.CLISourcePlatformSpecPaths",
				"admittedFixtureSelectedContractSourceLoader", "SourceArtifactSelectedContractSourceLoader",
				"runforkexecution.SourceArtifactSelectedContractSourceLoader", "runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader":
				var kept []ast.Expr
				for _, field := range node.Elts {
					if kv, ok := field.(*ast.KeyValueExpr); ok && formattedNativeReadNode(kv.Key) == "PlatformSpecPath" {
						continue
					}
					kept = append(kept, field)
				}
				node.Elts = kept
			}
		case *ast.CallExpr:
			switch formattedNativeReadNode(node.Fun) {
			case "initializeServePlatformStateStores", "cliapp.NewSwarmWorkflowModule":
				if len(node.Args) == 3 {
					node.Args = node.Args[:2]
				}
			case "loadServePlatformSpecDocument":
				if len(node.Args) == 1 {
					node.Args = nil
				}
			}
		case *ast.AssignStmt:
			if node.Tok == token.ASSIGN && len(node.Lhs) == 1 && len(node.Rhs) == 1 &&
				formattedNativeReadNode(node.Lhs[0]) == "opts.PlatformSpecPath" && formattedNativeReadNode(node.Rhs[0]) == "defaultPlatformSpecPath" {
				cursor.Delete()
				return false
			}
		}
		return true
	}, nil)
	return canonicalFunction(formattedNativeReadNode(file.Decls[0]))
}

func TestEmbeddedSpecSuccessorsPreserveFiniteHistoryAndAllOtherWork(t *testing.T) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 893 {
		t.Fatal("embedded-spec deletion changed the finite recipe population")
	}
	restored := append([]recipe(nil), rows...)
	seen := map[string]bool{}
	priorSuccessors := 0
	for _, pin := range packet.Rows {
		key := pin.Original.Family + "/" + pin.Original.File + "/" + pin.Original.Function
		if seen[key] {
			t.Fatal("duplicate embedded-spec successor key")
		}
		seen[key] = true
		if pin.Original.Successor != "" {
			priorSuccessors++
		}
		matches := 0
		for i, row := range rows {
			if !sameReviewedRecipeKey(row, pin.Original) {
				continue
			}
			matches++
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			original, _, err := embeddedSpecPriorEndpoint1801(row, actual)
			if err != nil {
				t.Fatalf("%s: %v", key, err)
			}
			restored[i] = original
			assertEmbeddedSpecSuccessorRefusals1801(t, row, actual, pin)
		}
		if matches != 1 {
			t.Fatalf("%s recipe multiplicity=%d", key, matches)
		}
	}
	population, err := json.Marshal(restored)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(population)) != packet.PopulationSHA256 || priorSuccessors != 12 {
		t.Fatal("embedded-spec successor overwrote historical evidence, prior chains or an unrelated row")
	}
}

func assertEmbeddedSpecSuccessorRefusals1801(t *testing.T, row recipe, actual string, pin fixtureSetupSuccessor2617) {
	t.Helper()
	for _, field := range []string{"family", "file", "function", "before", "after", "mechanical", "removed", "missing-successor", "prior-endpoint", "current-source", "paired-source"} {
		mutant, source := row, actual
		switch field {
		case "family":
			mutant.Family += "-unlisted"
		case "file":
			mutant.File = "unlisted.go"
		case "function":
			mutant.Function += "Unlisted"
		case "before":
			mutant.Before += " "
		case "after":
			mutant.After += " "
		case "mechanical":
			mutant.Mechanical = &mechanicalSnapshot{}
		case "removed":
			mutant.Removed = true
		case "missing-successor":
			mutant.Successor = ""
		case "prior-endpoint":
			source = pin.Original.After
			if pin.Original.Successor != "" {
				source = pin.Original.Successor
			}
		case "current-source":
			source = strings.Replace(actual, "{", "{ unreviewedAuthority();", 1)
		case "paired-source":
			mutant.Successor = strings.Replace(row.Successor, "{", "{ unreviewedAuthority();", 1)
			source = mutant.Successor
		}
		if _, _, err := embeddedSpecPriorEndpoint1801(mutant, source); err == nil {
			t.Fatalf("%s: changed %s admitted", row.Function, field)
		}
	}
	changedPin := pin
	changedPin.Original.Successor += "\nfunc foreignEndpoint() {}"
	if _, _, err := reverseEmbeddedSpecSuccessor1801(row, actual, changedPin); err == nil {
		t.Fatal("altered prior endpoint admitted")
	}
}

func TestEmbeddedSpecSuccessorPacketRejectsChangedHistoryAndSemanticEdits(t *testing.T) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"Source", "Authorization", "PopulationSHA256", "Before", "After", "Successor", "Candidate", "Edits"} {
		mutant := strings.Replace(string(embeddedSpecSuccessorBytes1801), `"`+change+`"`, `"foreign`+change+`"`, 1)
		if mutant == string(embeddedSpecSuccessorBytes1801) {
			t.Fatalf("missing packet field %s", change)
		}
		if _, err := loadEmbeddedSpecSuccessors1801([]byte(mutant)); err == nil {
			t.Fatalf("changed packet %s admitted", change)
		}
	}
	pin := packet.Rows[0]
	row := pin.Original
	for _, cut := range []struct{ before, after string }{
		{"t.Fatalf(", "t.Logf("}, {"family.Execute(", "foreign.Execute("},
		{"servedparity.BackendExplicitPostgres", "servedparity.BackendDefaultSQLite"},
		{"rt.DB.QueryRow(", "foreign.QueryRow("}, {"t.Cleanup(", "t.Run("},
	} {
		mutant := strings.Replace(pin.Candidate, cut.before, cut.after, 1)
		if mutant == pin.Candidate {
			t.Fatalf("semantic negative stopped exercising %s", cut.before)
		}
		row.Successor = mutant
		if _, _, err := embeddedSpecPriorEndpoint1801(row, mutant); err == nil {
			t.Fatalf("changed assertion/execution/store/cleanup admitted: %s", cut.before)
		}
	}
}
