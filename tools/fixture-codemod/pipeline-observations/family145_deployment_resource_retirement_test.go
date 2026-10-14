package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func TestFamily145DeploymentResourceCarrierRetirementHasCompleteNativeCallers(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateDeploymentResourceCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, pin := range pins {
		paths[pin.File] = true
		if strings.Contains(pin.After, ".db") || strings.Contains(pin.After, "AdmitPostgresRuntimeStore") || strings.Contains(pin.After, "DatabaseForTest") {
			t.Fatal("raw carrier capability survived:", pin.Function)
		}
	}
	if len(paths) != 17 {
		t.Fatal("whole caller/source inventory changed:", len(paths))
	}
	path := filepath.Join(root, "internal/runtime/conformance/fan_out_resource_source_proof_test.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := deploymentResourceCarrierClosed(raw); err != nil {
		t.Fatal(err)
	}
	for _, addition := range []string{"db *sql.DB", "db any", "pool interface{}"} {
		bad := strings.Replace(string(raw), "type deploymentResourceFixture struct {", "type deploymentResourceFixture struct {\n"+addition, 1)
		if err := deploymentResourceCarrierClosed([]byte(bad)); err == nil {
			t.Fatal("carrier admitted a new escape field:", addition)
		}
	}
	for _, pin := range pins {
		if pin.Function != "newDeploymentResourceFixtureWithSource" && pin.Function != "selectedDeploymentResourceFixtureConfigured" {
			continue
		}
		if !strings.Contains(pin.After, "StartSQLiteRuntimeStore") || !strings.Contains(pin.After, "StartPostgresRuntimeStore") || !strings.Contains(pin.After, "newNotifyAllChildrenProcessTopology") {
			t.Fatal("native constructor lost source/topology or backend:", pin.Function)
		}
	}
}

func deploymentResourceCarrierClosed(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, declaration := range file.Decls {
		decl, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range decl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "deploymentResourceFixture" {
				continue
			}
			shape, ok := typeSpec.Type.(*ast.StructType)
			if !ok || len(shape.Fields.List) != 6 {
				return os.ErrInvalid
			}
			want := []string{"selected", "postgresDSN", "source", "runtime", "topology", "ctx"}
			for index, field := range shape.Fields.List {
				if len(field.Names) != 1 || field.Names[0].Name != want[index] {
					return os.ErrInvalid
				}
			}
			return nil
		}
	}
	return os.ErrNotExist
}

func TestFamily145DeploymentResourceFiniteRecipesKeepHistoricalAssertionsAndOneOwner(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		input := []byte("package conformance\n" + pin.Before)
		output, changed, err := rewriteFunction(pin.File, input, finite)
		if err != nil || !changed || !strings.Contains(string(output), pin.Function) {
			t.Fatal("finite exact transform failed:", pin.Function, err)
		}
		if _, again, err := rewriteFunction(pin.File, output, finite); err != nil || again {
			t.Fatal("finite transform was not idempotent:", pin.Function, err)
		}
		bad := strings.Replace(pin.Before, pin.Function, pin.Function+"Foreign", 1)
		if _, changed, err := rewriteFunction(pin.File, []byte("package conformance\n"+bad), finite); err == nil || changed {
			t.Fatal("foreign declaration accepted:", pin.Function)
		}
	}
}

type deploymentResourceOwnerAddition struct {
	file, digest, retainedCut, scopeCut string
	names                               []string
}

// These two older whole-file controls are already red at the signed predecessor.
// Prove only this batch's exact additions; do not rotate their historical pins.
func TestFamily145DeploymentResourceOwnerAdditionsPreserveCompleteSignedPreBatch(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, pin := range []deploymentResourceOwnerAddition{
		{
			file:        "internal/store/internal/runtimepersistence/test_event_support.go",
			digest:      "92ac32d1a35e98b728ca73f66867897328f6bb3c7dbdd23c22d0b0e2ce8d7def",
			retainedCut: "ReadAgentSettledAttemptCountForTest",
			scopeCut:    "readDeploymentStorageForTest",
			names:       []string{"CountPinnedPortfolioSourceEventsStorageForTest"},
		},
		{
			file:        "internal/store/internal/backend/delivery/read_projections.go",
			digest:      "4fc05261307acae25bf49b2484e92a7cfc9d07ad65f3284a7b22637324cf7097",
			retainedCut: "CountCurrentEventDeliveryOutcomeStorageTx",
			scopeCut:    "current_attempt_version",
			names: []string{
				"CountRunDeliveredRowsStorageTx", "DeploymentEventDeliveryCutStorage",
				"ReadDeploymentPreReceiptDeliveryCutStorageTx", "ReadDeploymentPendingReceiverCutStorageTx",
				"DeploymentRunEventDeliveryCutStorage", "ReadDeploymentRunEventDeliveryCutStorageTx",
				"CountDeploymentRunDeliveryAttemptsStorageTx", "SelectedDeploymentClaimStorage",
				"ReadSelectedDeploymentClaimStorageTx", "SelectedDeliveryExecutionStorage",
				"ReadSelectedDeliveryExecutionStorageTx", "CountDeliveredClaimVersionStorageTx",
				"CountDeliveredSuccessorClaimVersionsStorageTx", "CountPinnedPortfolioSettledDeliveriesStorageTx",
				"PinnedPortfolioDeliveryDiagnosticStorage", "ReadPinnedPortfolioDeliveryDiagnosticsStorageTx",
			},
		},
	} {
		t.Run(filepath.Base(pin.file), func(t *testing.T) {
			current, err := os.ReadFile(filepath.Join(root, pin.file))
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("git", "show", "1c4cb4917703cda30faa17de2f093332e7a38c36:"+pin.file)
			cmd.Dir = root
			previous, err := cmd.Output()
			if err != nil {
				t.Fatal(err)
			}
			if err := deploymentResourceOwnerAdditionMatches(pin, current, previous); err != nil {
				t.Fatal(err)
			}
			for _, cut := range []string{pin.names[0], pin.retainedCut, pin.scopeCut} {
				if !bytes.Contains(current, []byte(cut)) {
					t.Fatal("missing added/retained/scope counterexample:", cut)
				}
				mutant := bytes.Replace(current, []byte(cut), []byte("foreignOwnerCut"), 1)
				if deploymentResourceOwnerAdditionMatches(pin, mutant, previous) == nil {
					t.Fatal("changed added or retained owner accepted:", cut)
				}
			}
			if deploymentResourceOwnerAdditionMatches(pin, current, append(append([]byte{}, previous...), '\n')) == nil {
				t.Fatal("incomplete signed predecessor accepted")
			}
		})
	}
}

func deploymentResourceOwnerAdditionMatches(pin deploymentResourceOwnerAddition, current, previous []byte) error {
	if fmt.Sprintf("%x", sha256.Sum256(current)) != pin.digest {
		return fmt.Errorf("deployment-resource whole owner changed: %s", pin.file)
	}
	for _, name := range pin.names {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return err
		}
	}
	restored, err := format.Source(current)
	if err != nil || !bytes.Equal(restored, previous) {
		return fmt.Errorf("deployment-resource addition changed retained signed owner: %s: %v", pin.file, err)
	}
	return nil
}
