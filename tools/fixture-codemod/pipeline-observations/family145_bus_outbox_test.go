package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

type outboxOwnerAddition struct {
	file, predecessor, before, after string
	names                            []string
}

var outboxOwnerAdditions = []outboxOwnerAddition{
	{agentSnapshotEventOwner, selectedreadrole.DeliveryRecoveryPredecessor,
		"a5cc858f28c9f48b638ad6608b42be4c538e11d144a3b845ac3d9f97eada5e5b",
		"ca20ab60177b3b4c07cea02338068eb2d538658b802483b32c230cf810f7fb90",
		[]string{"SetOutboxDeadLetterInsertFaultForTest", "SetOutboxAfterFirstEventWriteFaultForTest"}},
	{agentSnapshotEventOwner, selectedreadrole.SelectedForkExecutionPredecessor,
		channelProcessEventOwnerAfter,
		"a5cc858f28c9f48b638ad6608b42be4c538e11d144a3b845ac3d9f97eada5e5b",
		[]string{"ReadPostgresSelectedForkEventTimelineStorageForTest", "ReadPostgresRunRuntimeLogArrayStorageForTest", "CountPostgresSelectedForkLocalRuntimeDiagnosticsStorageForTest", "SelectedExecutionEventStorage", "SelectedExecutionFollowUpCounts", "ReadSelectedExecutionEventStorageForTest", "ReadSelectedExecutionFollowUpCountsForTest", "ReadSelectedExecutionPayloadsStorageForTest", "CountSelectedAgentFollowUpStorageForTest"}},
	{deliveryRecoveryReadOwner, selectedreadrole.SelectedForkExecutionPredecessor,
		"1a3ad1edca16216db20a5741c66f6c35800554c2ba4399e09126483c17903db5",
		deliveryRecoveryReadOwnerBefore,
		[]string{"SelectedNodeDeliveryFailureStorage", "ReadSelectedNodeDeliveryFailureStorageTx", "CountRunEventSubscriberDeliveriesStorageTx", "CountForkSourceSubscriberActiveDeliveriesStorageTx", "CountRunEventAgentDeliveriesStorageTx", "CountDeliveredAgentAttemptsStorageTx", "ReadLatestDeadLetterFailureStorageTx", "ReadPostgresSelectedAgentDeliveryFailureStorageTx", "ReadPostgresRunDeliveryAttemptArrayStorageTx", "ReadPostgresSelectedForkDeliveryTimelineStorageTx", "ReadPostgresSelectedForkRouteTimelineStorageTx"}},
}

// These exact additive source inverses only compose immutable proof copies.
// They do not classify, move, forgive or retire any debt occurrence.
func earlierOutboxAndSelectedForkOwner(root, file string, current []byte) ([]byte, error) {
	for _, pin := range outboxOwnerAdditions {
		if pin.file != file {
			continue
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(current))
		if digest != pin.after {
			continue
		}
		for _, name := range pin.names {
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
		prior, err := exec.Command("git", "-C", root, "show", pin.predecessor+":"+file).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != pin.before || !bytes.Equal(restored, prior) {
			return nil, fmt.Errorf("outbox/selected-fork retained owner bytes changed: %s: %v", file, err)
		}
		current = restored
	}
	return current, nil
}

func TestBusOutboxAndSelectedForkAdditionsPreserveCompleteEarlierOwners(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, file := range []string{agentSnapshotEventOwner, deliveryRecoveryReadOwner} {
		t.Run(filepath.Base(filepath.Dir(file)), func(t *testing.T) {
			current, err := os.ReadFile(filepath.Join(root, file))
			if err != nil {
				t.Fatal(err)
			}
			if file == deliveryRecoveryReadOwner {
				current, err = earlierDeliveryRecoveryOwnerFile(root, file, current)
				if err != nil {
					t.Fatal(err)
				}
			}
			previous, err := earlierOutboxAndSelectedForkOwner(root, file, current)
			if err != nil || bytes.Equal(current, previous) {
				t.Fatal("exact owner additions were not composed:", err)
			}
			for _, bad := range [][]byte{append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("func "), []byte("func Foreign"), 1)} {
				if actual, err := earlierOutboxAndSelectedForkOwner(root, file, bad); err == nil && !bytes.Equal(actual, bad) {
					t.Fatal("altered complete source entered a signed inverse")
				}
			}
		})
	}
}
