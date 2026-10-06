package testplanning

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"testing"
)

const originalRuntime02Selection = `^Test([D-E].*|F($|[^ao].*|a($|[^n].*|n($|[^O].*|O($|[^u].*|u($|[^t].*))))|o($|[^r].*|r($|[^k].*|k($|[^G].*)))))$`
const originalRuntime02RequiredEvidence = "6abc0e274dae375592588471f77b26a9293470bb13489491e27429e0b9c4b81b"

func TestRuntime02RequiredEvidenceRemainsExact(t *testing.T) {
	if err := validateRuntime02RequiredEvidence(loadPersistenceDebtPolicy(t)); err != nil {
		t.Fatal(err)
	}
}

func validateRuntime02RequiredEvidence(policy Policy) error {
	children := map[string][]string{}
	for _, id := range []string{"store-runtime-full-02", "store-runtime-flow-lifecycle"} {
		unit := policy.Units[id]
		selected := regexp.MustCompile(unit.Run)
		for root, cells := range unit.RequiredChildren {
			if _, duplicate := children[root]; duplicate || !selected.MatchString(root) {
				return fmt.Errorf("%s required evidence has duplicate or unselected root %s", id, root)
			}
			children[root] = cells
		}
	}
	// This source-pinned digest preserves every original named backend/fault
	// cell and its order without maintaining another execution inventory.
	data, err := json.Marshal(children)
	if err != nil {
		return err
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != originalRuntime02RequiredEvidence {
		return fmt.Errorf("original full-02 required evidence changed: %s", actual)
	}
	return nil
}

func TestRuntimeFlowSplitPreservesOriginalCompleteRootPartition(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validateRuntimeFanOutEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntime02RequiredEvidence(policy); err != nil {
		t.Fatal(err)
	}
	if policy.Units["store-runtime-flow-lifecycle"].Run != "^TestFlow.*$" {
		t.Fatal("the complete flow lifecycle family changed selection")
	}
	if err := validateRuntime02Partition(policy); err != nil {
		t.Fatal(err)
	}
}

func validateRuntime02Partition(policy Policy) error {
	old := regexp.MustCompile(originalRuntime02Selection)
	matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
	for _, id := range []string{"store-runtime-full-02", "store-runtime-flow-lifecycle"} {
		matchers = append(matchers, regexp.MustCompile(policy.Units[id].Run).MatchString)
	}
	return validateGoProofMatchers(filepath.Join("..", "..", "internal", "store", "internal", "runtimepersistence"), matchers)
}

func TestRuntimeFlowSplitRejectsCoverageAndEvidenceDrift(t *testing.T) {
	for _, run := range []string{"^$", originalRuntime02Selection, "^TestFlow.*$/sqlite", "^Test.*$"} {
		t.Run(run, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["store-runtime-flow-lifecycle"]
			unit.Run = run
			policy.Units["store-runtime-flow-lifecycle"] = unit
			if validateRuntime02Partition(policy) == nil {
				t.Fatal("changed complete-root ownership was accepted")
			}
		})
	}
	for _, change := range []string{"omit_backend", "extra_backend", "duplicate_owner"} {
		t.Run(change, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["store-runtime-flow-lifecycle"]
			root := "TestFlowAttachmentNativeLostAckAfterRebindBothStores"
			switch change {
			case "omit_backend":
				unit.RequiredChildren[root] = []string{"sqlite"}
			case "extra_backend":
				unit.RequiredChildren[root] = append(unit.RequiredChildren[root], "postgres/extra")
			case "duplicate_owner":
				policy.Units["store-runtime-full-02"].RequiredChildren[root] = unit.RequiredChildren[root]
			}
			if validateRuntime02RequiredEvidence(policy) == nil {
				t.Fatal("changed backend/fault execution obligations were accepted")
			}
		})
	}
}
