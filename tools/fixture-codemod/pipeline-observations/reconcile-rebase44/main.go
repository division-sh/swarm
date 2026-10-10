package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
)

const frozen = "8f061ebf486a0581230f49b568f941ccdde2ff6b"
const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const targetDigest = "562ddf479e99775d7fcd702b2c65970c6247e0b56b4d9f0e54d239a9644da059"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string          `json:"Successor,omitempty"`
	Removed                               bool            `json:"Removed,omitempty"`
	Mechanical                            json.RawMessage `json:"Mechanical,omitempty"`
}

type target struct{ File, Function string }
type transition struct{ File, Function, Before, After string }

var targets = []target{
	{"internal/runtime/tools/entity_sparse_mutation_test.go", "TestEntitySparseGeneratedToolMutation"},
	{"internal/apiv1/operator_runtime_context_test.go", "TestOperatorRuntimeContextManagerRejectsExistingRunUnavailableSourceStates"},
	{"internal/runtime/cataloge2e/creation_settlement_test.go", "requireCatalogCreationHandlerOrders"},
	{"internal/runtime/cataloge2e/replay_clean_test.go", "assertCatalogReplayFixtureOutcome"},
	{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogTest"},
	{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogHarness"},
	{"internal/runtime/pipeline/selection_retry_cas_external_test.go", "TestSelectionRetryAfterRealCASConflictBothStores"},
	{"internal/serveapp/managed_emit_publication_test.go", "TestManagedEmitPublicationExactScopeBothStores"},
	{"internal/runtime/manager/receipts_test.go", "TestProcessEventCancellationRetainsFinalReceipt"},
	{"internal/runtime/runforkexecution/execution_test.go", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority"},
	{"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go", "TestRunDebugReadSurface_ListRunDebugRuns_UsesCanonicalRunScope"},
	{"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go", "TestRunDebugReadSurface_ResolveLatestRunDebugRunID_UsesLatestPersistedRun"},
}

func main() {
	b, err := exec.Command("git", "show", frozen+":"+ledger).Output()
	must(err)
	var rows []recipe
	must(json.Unmarshal(b, &rows))
	if len(rows) != 893 {
		panic("frozen recipe inventory changed")
	}
	var changes []transition
	for _, t := range targets {
		changes = append(changes, reconcileTarget(rows, t))
	}
	raw, err := json.Marshal(changes)
	must(err)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	fmt.Printf("transitions=%d digest=%s\n", len(changes), digest)
	for _, change := range changes {
		fmt.Printf("%s\t%s\n", change.File, change.Function)
	}
	if len(os.Args) > 1 && os.Args[1] == "-write" {
		if digest != targetDigest {
			panic("unreviewed rebase successor bodies")
		}
		data, err := json.MarshalIndent(rows, "", "  ")
		must(err)
		must(os.WriteFile(ledger, append(data, '\n'), 0644))
	}
}

func frozenRecipe(rows []recipe, t target) *recipe {
	var found *recipe
	for i := range rows {
		if rows[i].File != t.File || rows[i].Function != t.Function {
			continue
		}
		if found != nil {
			panic("ambiguous frozen recipe: " + t.Function)
		}
		found = &rows[i]
	}
	if found == nil || found.Removed {
		panic("missing live frozen recipe: " + t.Function)
	}
	return found
}

func reconcileTarget(rows []recipe, t target) transition {
	row := frozenRecipe(rows, t)
	before := row.After
	if row.Successor != "" {
		before = row.Successor
	}
	pin, err := parser.ParseFile(token.NewFileSet(), "pin.go", "package pin\n"+before, parser.AllErrors)
	must(err)
	name := pin.Decls[0].(*ast.FuncDecl).Name.Name
	after := currentBody(t.File, name)
	if after == before {
		panic("unchanged rebase target: " + name)
	}
	row.Successor = after
	return transition{t.File, t.Function, before, after}
}

func currentBody(path, name string) string {
	source, err := os.ReadFile(path)
	must(err)
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, parser.AllErrors)
	must(err)
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		if found != nil {
			panic("ambiguous current recipe: " + name)
		}
		found = fn
	}
	if found == nil {
		panic("missing current recipe: " + name)
	}
	return string(source[set.Position(found.Pos()).Offset:set.Position(found.End()).Offset])
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
