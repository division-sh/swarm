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

const agentSnapshotEventOwner = "internal/store/internal/runtimepersistence/test_event_support.go"
const agentSnapshotEventOwnerAfter = "1e72b7d932872d481c5f2f3ae163d966ad4f028e85f867a3f8d93c485644b1cc"

var agentSnapshotSupportSources = map[string]string{
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_constraints.go": "0d5e4bcdbc9b318c4fcfdf6d18fef105c85d1acf7a6b51382a01685df1f6ef69",
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_corruption.go":  "edd7b67bd27026bac3ac685557a67662ac503d30418f879bd37801b58b9879a0",
	"internal/store/internal/backend/agentpersistence/test_operator_snapshot_mutations.go":   "e6c7a3773436da64e46ffb9437434403b3b1caef9d99d3036b3c93f8c6839b2f",
	"internal/store/internal/backend/llmpersistence/test_operator_snapshot_barrier.go":       "45e20cc0b4447b06a753a161dd718caaea4521a25138302b658a6253541125ae",
	"internal/store/internal/backend/llmpersistence/test_operator_snapshot_mutations.go":     "7787d35d5b52d6b4239db03e3bb37215b29d99d0e0d1f54dcfad619ae4536dfb",
	"internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go":               "4644bdc2d88b4888df88afaac3541d67d4d8e18c034212316a7f5d888231abd3",
	"internal/store/internal/runtimepersistence/test_operator_snapshot_corruption.go":        "6559be5b69121990e7a4d1f09851d51eb79ea12e044c4b0379bafeac8dff3496",
	"internal/store/internal/runtimepersistence/test_operator_snapshot_cut.go":               "4c57be61fa068f6ad67a62f54b32d2eddbd3013d78c959cba523b07dd6603e1e",
	"internal/store/internal/runtimepersistence/test_operator_snapshot_read_barrier.go":      "3e0e28f853b0ed9dc50e82be99c36421fedebdb048bd5420011a157f224baaba",
	"internal/store/storetest/operator_snapshot_corruption.go":                               "43ba82d999caeaddf3748302ef92601fa0d6d3802702ae1abd7f615ce0ba8625",
	"internal/store/storetest/operator_snapshot_cut.go":                                      "f4143ee96b1f5e78033aca73a92f1e373b06d6678a30a5973a414c5ea38447ae",
}

func validateAgentSnapshotSupport(path string, source []byte) error {
	expected, ok := agentSnapshotSupportSources[path]
	if !ok || fmt.Sprintf("%x", sha256.Sum256(source)) != expected {
		return fmt.Errorf("operator snapshot support is not its exact reviewed source: %s", path)
	}
	return nil
}

func TestFamily145AgentSnapshotSupportEntriesStayFinite(t *testing.T) {
	root := filepath.Clean("../../..")
	for path := range agentSnapshotSupportSources {
		source, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateAgentSnapshotSupport(path, source); err != nil {
			t.Fatal(err)
		}
		for _, bad := range [][]byte{nil, append(bytes.Clone(source), '\n'), bytes.Replace(source, []byte("func "), []byte("func Foreign"), 1)} {
			if err := validateAgentSnapshotSupport(path, bad); err == nil {
				t.Fatal("missing/changed/foreign support source accepted:", path)
			}
		}
		if err := validateAgentSnapshotSupport(path+".foreign", source); err == nil {
			t.Fatal("foreign support identity accepted:", path)
		}
	}
}

func earlierAgentSnapshotEventOwnerFile(root string, current []byte) ([]byte, error) {
	var err error
	current, err = earlierChannelProcessEventOwnerFile(root, current)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != agentSnapshotEventOwnerAfter {
		return nil, fmt.Errorf("agent snapshot event owner is not its complete current endpoint")
	}
	for _, name := range []string{"corruptOperatorSnapshotEventTx", "CorruptOperatorSnapshotEventForTest"} {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return nil, err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return nil, err
		}
	}
	restored, err := format.Source(current)
	if err != nil {
		return nil, err
	}
	prior, err := exec.Command("git", "-C", root, "show", selectedreadrole.AgentSnapshotPredecessor+":"+agentSnapshotEventOwner).Output()
	if err != nil || !bytes.Equal(restored, prior) {
		return nil, fmt.Errorf("agent snapshot event additions changed earlier retained bytes: %v", err)
	}
	return restored, nil
}

func TestFamily145AgentSnapshotCompleteSourceAndFiniteTransforms(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadAgentSnapshotPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateAgentSnapshotCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	removed := 0
	for _, pin := range pins {
		if pin.Removed {
			removed++
			before, err := selectedreadrole.EarlierAgentSnapshotSource(root, pin.File, pin.Function, "")
			if err != nil || before != pin.Before {
				t.Fatal("retired declaration lost signed source:", pin.Function, err)
			}
			if _, err := selectedreadrole.EarlierAgentSnapshotSource(root, pin.File, pin.Function, pin.Before); err == nil {
				t.Fatal("retired authority resurrected:", pin.Function)
			}
			continue
		}
		row := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package apiv1\n"+pin.Before), row)
		if err != nil || !changed {
			t.Fatal("exact snapshot transform failed:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, row); err != nil || changed {
			t.Fatal("snapshot transform repeated:", pin.Function, err)
		}
		foreign := strings.Replace(pin.Before, pin.Function, pin.Function+"Foreign", 1)
		if _, changed, err := rewriteFunction(pin.File, []byte("package apiv1\n"+foreign), row); err == nil || changed {
			t.Fatal("foreign source accepted:", pin.Function)
		}
		before, err := selectedreadrole.EarlierAgentSnapshotSource(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatal("source inverse lost predecessor:", pin.Function, err)
		}
		if _, err := selectedreadrole.EarlierAgentSnapshotSource(root, pin.File, pin.Function, pin.After+"\n"); err == nil {
			t.Fatal("source inverse accepted foreign endpoint:", pin.Function)
		}
	}
	if removed != 27 {
		t.Fatalf("retired authority declarations=%d, want27", removed)
	}
}

func TestFamily145AgentSnapshotCompleteHistoryPreservesAllEarlierRows(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := selectedreadrole.PriorAgentSnapshotHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		next, err := selectedreadrole.ComposeAgentSnapshotHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		actual := family145TransitionBytes
		if recipes {
			actual = recipeBytes
		}
		actual, err = selectedreadrole.BeforeChannelProcessHistory(root, actual, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var have, want any
		if json.Unmarshal(next, &want) != nil || json.Unmarshal(actual, &have) != nil || !reflect.DeepEqual(have, want) {
			t.Fatal("current history differs from complete signed composition")
		}
		old, err := selectedreadrole.EarlierAgentSnapshotHistory(root, actual, recipes)
		if err != nil || !bytes.Equal(old, prior) {
			t.Fatal("history lost signed predecessor:", err)
		}
		if _, err := selectedreadrole.EarlierAgentSnapshotHistory(root, prior, recipes); err == nil {
			t.Fatal("history inverse repeated")
		}
		var rows []map[string]any
		if err := json.Unmarshal(actual, &rows); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func([]map[string]any) []map[string]any{
			func(rows []map[string]any) []map[string]any { return rows[:len(rows)-1] },
			func(rows []map[string]any) []map[string]any { return append(rows, rows[0]) },
			func(rows []map[string]any) []map[string]any { rows[0]["Before"] = "foreign"; return rows },
		} {
			bad, err := json.Marshal(mutate(cloneRollbackHistory(t, rows)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := selectedreadrole.EarlierAgentSnapshotHistory(root, bad, recipes); err == nil {
				t.Fatal("history admitted missing/duplicate/foreign row")
			}
		}
	}
}

func TestFamily145AgentSnapshotEventAdditionPreservesCompletePredecessor(t *testing.T) {
	root := filepath.Clean("../../..")
	current, err := os.ReadFile(filepath.Join(root, agentSnapshotEventOwner))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := earlierAgentSnapshotEventOwnerFile(root, current)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{prior, append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("CorruptOperatorSnapshotEventForTest"), []byte("ForeignEventFault"), 1)} {
		if _, err := earlierAgentSnapshotEventOwnerFile(root, bad); err == nil {
			t.Fatal("event owner inverse accepted a stale/foreign endpoint")
		}
	}
}
