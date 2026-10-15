package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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

func TestFamily145ServedScenarioCompleteSourceAndAppendOnlyHistory(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadServedScenarioPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 22 {
		t.Fatalf("scenario declarations=%d want22", len(pins))
	}
	if err := selectedreadrole.ValidateServedScenarioCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, recipes := range []bool{true, false} {
		path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
		if recipes {
			path = "tools/fixture-codemod/pipeline-observations/recipes.json"
		}
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		prior, err := selectedreadrole.PriorServedScenarioHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := selectedreadrole.EarlierServedScenarioHistory(root, current, recipes)
		if err != nil || !bytes.Equal(restored, prior) {
			t.Fatalf("scenario inverse lost immutable predecessor: %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(current, &rows); err != nil {
			t.Fatal(err)
		}
		rows[0]["Before"] = "foreign historical text"
		bad, _ := json.Marshal(rows)
		if _, err := selectedreadrole.EarlierServedScenarioHistory(root, bad, recipes); err == nil {
			t.Fatal("scenario inverse accepted unrelated history changes")
		}
	}
	bad := append([]selectedreadrole.EntityToolPin(nil), pins...)
	bad[0].After += "\n"
	if err := selectedreadrole.ValidateServedScenarioCurrent(root, bad); err == nil || reflect.DeepEqual(pins, bad) {
		t.Fatal("scenario source accepted a foreign endpoint")
	}
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
