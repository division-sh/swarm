package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

// Source composition under6192298462/6192732056; no accounting or collector change.
func debtServedScenarioSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtCatchUpLaneDeclaration(root, file, function, current)
	if err != nil {
		return "", err
	}
	if file != "internal/serveapp/test_command_runtime_test.go" ||
		(function != "TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores" && function != "TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores" && function != "runServedGeneratedInputFixtureBackendProof") {
		return current, nil
	}
	// Older controlled source copies still authenticate against their original
	// immutable frame. Only the new live endpoint receives this extra inverse.
	for _, pin := range debtPublicRollbackPins {
		if pin.File == file && pin.Function == function &&
			(debtBaselineChecksum([]byte(current)) == pin.BeforeSHA256 || debtBaselineChecksum([]byte(current)) == pin.CurrentSHA256) {
			return current, nil
		}
	}
	if function == "runServedGeneratedInputFixtureBackendProof" && debtBaselineChecksum([]byte(current)) == "e0e63fc9adf03c98dd2b8d464ba6d529ee01000c6fa6f51fd38b27015f327807" {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-served-scenario-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "4040570dee0ab72b903aeebaeb4d10fee10b40f4428a5172765aaf3beba459c8" {
		return "", fmt.Errorf("served scenario complete signed inventory changed: %v", err)
	}
	var rows []debtRunIdentitySourceBody
	const digest = "4040570dee0ab72b903aeebaeb4d10fee10b40f4428a5172765aaf3beba459c8"
	if cached, ok := debtSelectedReadRolePinCache.Load(digest); ok {
		rows = cached.([]debtRunIdentitySourceBody)
	} else {
		if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 22 {
			return "", fmt.Errorf("served scenario complete declaration inventory changed: %v", err)
		}
		debtSelectedReadRolePinCache.Store(digest, rows)
	}
	for _, row := range rows {
		if row.File != file || row.Function != function {
			continue
		}
		live, err := debtCatchUpLaneDeclarations(root, file)
		if err != nil || current != row.After || live[function] != row.After {
			return "", fmt.Errorf("served scenario supplied/live declaration is not its exact endpoint: %v", err)
		}
		return row.Before, nil
	}
	return "", fmt.Errorf("served scenario exact source pair is missing")
}

func debtServedScenarioNativePrevious(path string, raw []byte) ([]byte, error) {
	if path != "internal/store/internal/runtimepersistence/test_served_delivery_observation.go" ||
		debtBaselineChecksum(raw) != "9268956572bb05d5f62c6ed5402e208e0c60c3ab17e7a5be0205d488c8591565" {
		return raw, nil
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var kept []ast.Decl
	seen := 0
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && (fn.Name.Name == "CountGlobalDeliveredNodeRowsStorageForTest" || fn.Name.Name == "ReadCanonicalRoutingSQLiteDiagnosticForTest") {
			seen++
			continue
		}
		kept = append(kept, declaration)
	}
	file.Decls = kept
	var out bytes.Buffer
	if err := format.Node(&out, set, file); err != nil || seen != 2 {
		return nil, fmt.Errorf("served scenario native inverse lost its exact two additions: %v", err)
	}
	if debtBaselineChecksum(out.Bytes()) != "256de06dd68f2c2fe45eac7c02ae8726e022696e6d977db3ca74f981d3e865a7" {
		return nil, fmt.Errorf("served scenario native inverse lost its complete signed predecessor")
	}
	return out.Bytes(), nil
}

func TestPersistenceAuthorityDebtServedScenarioCompositionIsExactAndSourceOnly(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	file := "internal/serveapp/test_command_runtime_test.go"
	live, err := debtServedSetupDeclarations(filepath.Join(root, file))
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range debtPublicRollbackPins[1:] {
		current := live[pin.Function]
		prior, err := debtServedScenarioSourcePrevious(root, file, pin.Function, current)
		if err != nil || debtBaselineChecksum([]byte(prior)) != pin.CurrentSHA256 {
			t.Fatalf("exact live source did not restore signed predecessor: %v", err)
		}
		for _, bad := range []string{current + "\n", "foreign", ""} {
			if _, err := debtServedScenarioSourcePrevious(root, file, pin.Function, bad); err == nil {
				t.Fatal("foreign source received exact scenario inverse")
			}
		}
		if got, err := debtServedScenarioSourcePrevious(root, "foreign.go", pin.Function, current); err != nil || got != current {
			t.Fatal("source inverse escaped its finite file scope")
		}
	}
	path := "internal/store/internal/runtimepersistence/test_served_delivery_observation.go"
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := debtServedScenarioNativePrevious(path, raw)
	if err != nil || debtBaselineChecksum(prior) != "256de06dd68f2c2fe45eac7c02ae8726e022696e6d977db3ca74f981d3e865a7" {
		t.Fatalf("native owner inverse lost complete prior bytes: %v", err)
	}
	if _, err := debtServedObservationNativePrevious(path, append(raw, '\n')); err == nil {
		t.Fatal("native owner inverse accepted noncanonical retained bytes")
	}
}
