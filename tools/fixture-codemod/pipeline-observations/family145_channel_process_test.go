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

const channelProcessEventOwnerAfter = "f18887031dea8a98c73666305a23e48b1125d0641f72c802c7f68aaff23097fd"

var channelProcessNativeControls = map[string]struct{ function, digest string }{
	"internal/serveapp/channel_current_render_fault_owner_test.go": {"TestChannelCurrentRenderFaultPreservesScopeCancellationAndRestorationBothStores", "fa08c7ea241f025383957914f4ad6bcbe009d3af0efecef248540906eeda5e6d"},
	"internal/serveapp/channel_input_binding_fault_owner_test.go":  {"TestChannelInputBindingFaultKeepsOrdinaryRunAndExactReceiptBothStores", "2bc566196cd4d9f5eb159758df8298c39ad82c443e8d1a8fa69d8ca424785364"},
	"internal/serveapp/channel_process_history_owner_test.go":      {"TestChannelProcessHistoryKeepsNativeFactsAndIndependentTemporalCutsBothStores", "9896600d7df74fa50531bd499deb3b5e17e8b5977d220a2402a105a27d2c21a4"},
}

func channelProcessNativeControl(root, path, function, current string) (bool, error) {
	pin, ok := channelProcessNativeControls[path]
	if !ok || pin.function != function {
		return false, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.digest {
		return false, fmt.Errorf("channel native hostile-control complete source changed: %s: %v", path, err)
	}
	body, err := selectedreadrole.Body(raw, function)
	if err != nil || current != body {
		return false, fmt.Errorf("channel native hostile-control declaration changed: %s: %v", function, err)
	}
	return true, nil
}

func earlierChannelProcessEventOwnerFile(root string, current []byte) ([]byte, error) {
	var err error
	current, err = earlierOutboxAndSelectedForkOwner(root, agentSnapshotEventOwner, current)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != channelProcessEventOwnerAfter {
		return current, nil
	}
	for _, name := range []string{"CountChannelForkSupersessionLineageStorageForTest", "ReadChannelForkSupersessionPayloadStorageForTest"} {
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
	prior, err := exec.Command("git", "-C", root, "show", selectedreadrole.ChannelProcessPredecessor+":"+agentSnapshotEventOwner).Output()
	if err != nil || !bytes.Equal(restored, prior) {
		return nil, fmt.Errorf("channel fork event bridges changed retained event-owner bytes: %v", err)
	}
	return restored, nil
}

func assertChannelProcessHistoryHostileControls(t *testing.T, root string, actual []byte, recipes bool) {
	t.Helper()
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
		if _, err := selectedreadrole.EarlierChannelProcessHistory(root, bad, recipes); err == nil {
			t.Fatal("missing/duplicate/foreign historical record accepted")
		}
	}
}
