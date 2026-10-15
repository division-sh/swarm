package testplanning

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
)

const originalRuntime02Selection = `^Test([D-E].*|F($|[^ao].*|a($|[^n].*|n($|[^O].*|O($|[^u].*|u($|[^t].*))))|o($|[^r].*|r($|[^k].*|k($|[^G].*)))))$`

// R7 replaces route acquisition with exact activation verification, retains
// native parent/receipt corruption cuts, and requires the real cleanup sinks.
const runtime02RequiredEvidence = "a4efe0a79d06cb8f0ebaba69cfd16d5ba8c4931b361b322c5375018716a8dbfd"

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
	if err := separateNewForkWitnessEvidence(children); err != nil {
		return err
	}
	// This source-pinned digest preserves every original named backend/fault
	// cell and its order without maintaining another execution inventory.
	data, err := json.Marshal(children)
	if err != nil {
		return err
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != runtime02RequiredEvidence {
		return fmt.Errorf("approved full-02 required evidence changed: %s", actual)
	}
	return nil
}

// New owner proofs add obligations without changing the original split's
// immutable evidence digest. Only these exact cells leave that comparison.
func separateNewForkWitnessEvidence(children map[string][]string) error {
	for _, root := range []string{
		"TestDynamicFlowRuntimeReadinessStorageFaultsKeepNativeScopeBothStores",
		"TestForkReceiverCompanionStoragePreservesWholeRowsAndSeparateCutsBothStores",
		"TestForkReceiverCompanionStorageRefusesRawCancelledClosedAndPartialReadsBothStores",
		"TestForkReceiverNoticeStoragePreservesCompleteSourceScopedRowsBothStores",
		"TestForkReceiverNoticeStorageRefusesInvalidCancelledClosedAndPartialEvidenceBothStores",
	} {
		if !slices.Equal(children[root], []string{"sqlite", "postgres"}) {
			return fmt.Errorf("new fork witness proof lost exact both-store evidence: %s", root)
		}
		delete(children, root)
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
	for _, change := range []string{"omit_backend", "extra_backend", "duplicate_owner", "restore_route_cleanup", "omit_agent_fence"} {
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
			case "restore_route_cleanup":
				root = "TestFlowAttachmentCleanupRetainsExactPredecessorBothStores"
				unit.RequiredChildren[root] = append(unit.RequiredChildren[root], "sqlite/route/fail_once")
			case "omit_agent_fence":
				root = "TestFlowAttachmentCleanupRetainsExactPredecessorBothStores"
				children := append([]string(nil), unit.RequiredChildren[root]...)
				unit.RequiredChildren[root] = nil
				for _, child := range children {
					if child != "postgres/agent_fence/fail_once" {
						unit.RequiredChildren[root] = append(unit.RequiredChildren[root], child)
					}
				}
			}
			policy.Units["store-runtime-flow-lifecycle"] = unit
			if validateRuntime02RequiredEvidence(policy) == nil {
				t.Fatal("changed backend/fault execution obligations were accepted")
			}
		})
	}
	for _, root := range []string{
		"TestForkReceiverCompanionStoragePreservesWholeRowsAndSeparateCutsBothStores",
		"TestForkReceiverCompanionStorageRefusesRawCancelledClosedAndPartialReadsBothStores",
		"TestForkReceiverNoticeStoragePreservesCompleteSourceScopedRowsBothStores",
		"TestForkReceiverNoticeStorageRefusesInvalidCancelledClosedAndPartialEvidenceBothStores",
	} {
		for _, cells := range [][]string{nil, {"sqlite"}, {"postgres"}, {"sqlite", "postgres", "foreign"}} {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["store-runtime-full-02"]
			unit.RequiredChildren[root] = cells
			policy.Units["store-runtime-full-02"] = unit
			if validateRuntime02RequiredEvidence(policy) == nil {
				t.Fatal("new fork witness obligation lost or broadened backend evidence:", root, cells)
			}
		}
	}
}
