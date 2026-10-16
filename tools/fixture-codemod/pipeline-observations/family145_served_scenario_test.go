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

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const servedScenarioReadOwnerAfter = "80e9bcafe29741da3cf094e017b4a973b88e30e3dd3dcc2ef27d27446c52f0a3"

var servedScenarioReadDeclarations = []string{
	"CountGlobalDeliveredNodeRowsStorageTx", "ReplayScopePriorDeliveryWitnessSQL", "APICompletionPriorDeliveryWitnessSQL",
	"CountPublicationMaterializationFaultObjectsTx", "ReadCanonicalRoutingSQLiteDiagnosticTx",
}

func earlierServedScenarioOwnerFile(root, file string, current []byte) ([]byte, error) {
	if file != deliveryRecoveryReadOwner {
		return current, nil
	}
	var err error
	current, err = selectedreadrole.CatchUp2542HistoricalSource(root, file, current)
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(current))
	if digest == deliveryRecoveryReadOwnerAfter || digest == deliveryRecoveryReadOwnerBefore {
		return current, nil
	}
	if digest != servedScenarioReadOwnerAfter {
		return nil, fmt.Errorf("served scenario complete read owner changed")
	}
	for _, name := range servedScenarioReadDeclarations {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return nil, err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return nil, err
		}
	}
	for _, comment := range []string{
		"// These two closed expressions are consumed only by the private replay/API\n// fault installers. Their trigger-local identities cannot be supplied by callers.\n",
		"// Preserve the original best-effort diagnostic, including its ordered prefix\n// when a later diagnostic query fails. This text is never execution authority.\n",
	} {
		current = bytes.ReplaceAll(current, []byte(comment), nil)
	}
	restored, err := format.Source(current)
	if err != nil {
		return nil, err
	}
	prior, err := exec.Command("git", "-C", root, "show", selectedreadrole.ServedScenarioPredecessor+":"+file).Output()
	if err != nil || !bytes.Equal(restored, prior) {
		return nil, fmt.Errorf("served scenario read owner lost complete signed predecessor: %v", err)
	}
	return restored, nil
}

func TestFamily145ServedScenarioReadOwnerAndFixedWitnessConsumersAreClosed(t *testing.T) {
	root := filepath.Clean("../../..")
	current, err := os.ReadFile(filepath.Join(root, deliveryRecoveryReadOwner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := earlierServedScenarioOwnerFile(root, deliveryRecoveryReadOwner, current); err != nil {
		t.Fatal(err)
	}
	if _, err := earlierServedScenarioOwnerFile(root, deliveryRecoveryReadOwner, append(current, '\n')); err == nil {
		t.Fatal("owner inverse accepted unrelated bytes")
	}
	for _, cut := range []struct{ path, witness string }{
		{"internal/store/internal/backend/pipelinepersistence/test_publication_replay_failure.go", "delivery.ReplayScopePriorDeliveryWitnessSQL(postgres)"},
		{"internal/store/internal/apiidempotency/test_publication_completion_failure.go", "delivery.APICompletionPriorDeliveryWitnessSQL(postgres)"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, cut.path))
		if err != nil || strings.Count(string(raw), cut.witness) != 1 || strings.Contains(string(raw), "FROM event_deliveries") {
			t.Fatalf("private trigger rebuilt/lost the canonical delivery witness: %s %v", cut.path, err)
		}
	}
}

func TestFamily145ServedScenarioFixedWitnessesNeverEscapePrivateFaultOwners(t *testing.T) {
	root := filepath.Clean("../../..")
	allowed := map[string]string{
		"ReplayScopePriorDeliveryWitnessSQL":   "internal/store/internal/backend/pipelinepersistence/test_publication_replay_failure.go",
		"APICompletionPriorDeliveryWitnessSQL": "internal/store/internal/apiidempotency/test_publication_completion_failure.go",
	}
	seen := map[string]int{}
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || allowed[selector.Sel.Name] == "" {
				return true
			}
			seen[selector.Sel.Name]++
			if filepath.ToSlash(relative) != allowed[selector.Sel.Name] {
				t.Errorf("fixed private witness escaped its exact owner: %s in %s", selector.Sel.Name, relative)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range allowed {
		if seen[name] != 1 {
			t.Fatalf("private witness %s consumers=%d want1", name, seen[name])
		}
	}
}
