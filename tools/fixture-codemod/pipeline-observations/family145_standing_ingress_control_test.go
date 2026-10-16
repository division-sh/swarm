package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func standingIngressHelperNames() map[string]bool {
	out := map[string]bool{}
	for _, name := range strings.Fields(`loadServedStandingOwner assertServedStandingState assertServedStandingResetAndShutdownJoin assertServedStandingInvalidChildCommands waitForStandingDeliveryQuiescence servedRunControlState requireServedRunControlState servedDeliveryStatusReason requireServedStoppedPendingDelivery servedRuntimeIngressState requireServedRuntimeIngressState servedEventNameByID lifecycleAdmissionDomainCounts semanticNumericDomainCounts terminalizeStandingRunFromRuntime`) {
		out[name] = true
	}
	return out
}

func standingIngressReferenceCounts(raw []byte) (map[string]int, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	want, counts := standingIngressHelperNames(), map[string]int{}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && want[id.Name] {
			counts[id.Name]++
		}
		return true
	})
	return counts, nil
}

func standingIngressHelperReferences(root string, pins []selectedreadrole.EntityToolPin) error {
	want, actual := map[string]int{}, map[string]int{}
	for _, pin := range pins {
		counts, err := standingIngressReferenceCounts([]byte("package serveapp\n" + pin.After))
		if err != nil {
			return err
		}
		for name, count := range counts {
			want[name] += count
		}
	}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		counts, err := standingIngressReferenceCounts(raw)
		if err != nil {
			return err
		}
		for name, count := range counts {
			actual[name] += count
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, actual) {
		return fmt.Errorf("standing-ingress live caller inventory changed: have %v want %v", actual, want)
	}
	return nil
}

type standingIngressOwnerAddition struct {
	file, before, after string
	names               []string
}

var standingIngressOwnerAdditions = []standingIngressOwnerAddition{
	{"internal/store/internal/runtimepersistence/test_event_support.go", "92ac32d1a35e98b728ca73f66867897328f6bb3c7dbdd23c22d0b0e2ce8d7def", "936652dc04a748c0d01ff9683520b90a63e74e7c8ff5bf3b70bfeabddee3edec", []string{"StandingTelegramLineageStorage", "ReadEventNameStorageForTest", "CountInboundNamedEventsStorageForTest", "ReadStandingTelegramLineageStorageForTest"}},
	{"internal/store/internal/backend/delivery/read_projections.go", "4fc05261307acae25bf49b2484e92a7cfc9d07ad65f3284a7b22637324cf7097", "1a3ad1edca16216db20a5741c66f6c35800554c2ba4399e09126483c17903db5", []string{"ServedDeliveryStatusReasonStorage", "ReadServedDeliveryStatusReasonStorageTx", "CountUnsettledStandingDeliveriesStorageTx"}},
	{"internal/store/internal/runtimepersistence/test_selected_source_outcome.go", "555221d42d3a0346057a53b6ea23ebb4171889350e223609b355b59113c8f7c6", "e720022ed07f729533088d7a62960bce6cf49413b4832464253de8d13dcd160c", []string{"CountPhysicalSourceArtifactsStorageForTest"}},
}

func earlierStandingIngressOwnerFile(root, file string, current []byte) ([]byte, error) {
	var err error
	current, err = earlierDeliveryRecoveryOwnerFile(root, file, current)
	if err != nil {
		return nil, err
	}
	current, err = earlierOutboxAndSelectedForkOwner(root, file, current)
	if err != nil {
		return nil, err
	}
	if file == agentSnapshotEventOwner {
		var err error
		current, err = earlierChannelProcessEventOwnerFile(root, current)
		if err != nil {
			return nil, err
		}
	}
	if file == agentSnapshotEventOwner && fmt.Sprintf("%x", sha256.Sum256(current)) == agentSnapshotEventOwnerAfter {
		var err error
		current, err = earlierAgentSnapshotEventOwnerFile(root, current)
		if err != nil {
			return nil, err
		}
	}
	for _, pin := range standingIngressOwnerAdditions {
		if pin.file != file {
			continue
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(current))
		if digest == pin.before {
			return current, nil
		}
		if digest != pin.after {
			return nil, fmt.Errorf("standing-ingress complete owner changed: %s", file)
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
		if file == "internal/store/internal/runtimepersistence/test_event_support.go" {
			restored = bytes.Replace(restored,
				[]byte("type PublicationResultSourceStorage = eventrecord.PublicationResultSourceStorage\n\ntype ReceiverFinishedEffectStorage"),
				[]byte("type PublicationResultSourceStorage = eventrecord.PublicationResultSourceStorage\ntype ReceiverFinishedEffectStorage"), 1)
		}
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(restored)) != pin.before {
			return nil, fmt.Errorf("standing-ingress addition changed retained owner: %s: %v", file, err)
		}
		return restored, nil
	}
	return current, nil
}
