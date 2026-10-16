package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/format"
	"os/exec"

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
	var err error
	source, err = agentSnapshotSupportHistoricalSource(path, source)
	if err != nil {
		return err
	}
	expected, ok := agentSnapshotSupportSources[path]
	if !ok || fmt.Sprintf("%x", sha256.Sum256(source)) != expected {
		return fmt.Errorf("operator snapshot support is not its exact reviewed source: %s", path)
	}
	return nil
}

// #6200510991's exact E packet adds recovery-write custody to this original
// fixture. #2698/6205327685 authorizes composing that support consumer while
// keeping the original support inventory, bytes and foreign-edit controls.
func agentSnapshotSupportHistoricalSource(path string, source []byte) ([]byte, error) {
	if path != "internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go" ||
		fmt.Sprintf("%x", sha256.Sum256(source)) != "9c47a40cc1093c0fa7f6e19de539b93264a2cc8bd9ed8865e8d0bc3668236ccc" {
		return source, nil
	}
	prior, err := exec.Command("git", "-C", "../../..", "show", "7333184bec:"+path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != agentSnapshotSupportSources[path] {
		return nil, fmt.Errorf("snapshot support lost its exact signed predecessor: %v", err)
	}
	return prior, nil
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
