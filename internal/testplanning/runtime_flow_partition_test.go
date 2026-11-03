package testplanning

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"
)

const originalRuntime02Selection = `^Test([D-E].*|F($|[^ao].*|a($|[^n].*|n($|[^O].*|O($|[^u].*|u($|[^t].*))))|o($|[^r].*|r($|[^k].*|k($|[^G].*)))))$`

func validateRuntime02RequiredEvidence(policy Policy) error {
	children := map[string]struct{}{}
	for _, id := range []string{"store-runtime-full-02", "store-runtime-flow-lifecycle"} {
		unit := policy.Units[id]
		selected := regexp.MustCompile(unit.Run)
		for root := range unit.RequiredChildren {
			if _, duplicate := children[root]; duplicate || !selected.MatchString(root) {
				return fmt.Errorf("%s required evidence has duplicate or unselected root %s", id, root)
			}
			children[root] = struct{}{}
		}
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
	t.Run("duplicate_owner", func(t *testing.T) {
		policy := loadPersistenceDebtPolicy(t)
		unit := policy.Units["store-runtime-flow-lifecycle"]
		root := "TestFlowAttachmentNativeLostAckAfterRebindBothStores"
		policy.Units["store-runtime-full-02"].RequiredChildren[root] = unit.RequiredChildren[root]
		if validateRuntime02RequiredEvidence(policy) == nil {
			t.Fatal("duplicate required-child owner was accepted")
		}
	})
}
