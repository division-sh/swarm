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
	"internal/serveapp/channel_current_render_fault_owner_test.go": {"TestChannelCurrentRenderFaultPreservesScopeCancellationAndRestorationBothStores", "81e7523f7ea30f22c4e5adee5d57d9fa700dfcc40e74bc6c1cbdc48ce3788ae4"},
	"internal/serveapp/channel_input_binding_fault_owner_test.go":  {"TestChannelInputBindingFaultKeepsOrdinaryRunAndExactReceiptBothStores", "c2c96e344117cdb942aa3ced65a3b83b1e531a3eac9046d9f3eaed10ede7479c"},
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

func TestFamily145ChannelProcessNativeControlInventoryIsExact(t *testing.T) {
	root := filepath.Clean("../../..")
	for path, pin := range channelProcessNativeControls {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		body, err := selectedreadrole.Body(raw, pin.function)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := channelProcessNativeControl(root, path, pin.function, body); !ok || err != nil {
			t.Fatal("native control not accounted for:", path, err)
		}
		if ok, _ := channelProcessNativeControl(root, path, pin.function, body+"\n"); ok {
			t.Fatal("altered native assertion or callback was admitted")
		}
		if ok, _ := channelProcessNativeControl(root, path, pin.function+"Foreign", body); ok {
			t.Fatal("unclassified sibling declaration was admitted")
		}
		if ok, _ := channelProcessNativeControl(root, path+".foreign", pin.function, body); ok {
			t.Fatal("foreign native control path was admitted")
		}
	}
}

func earlierChannelProcessEventOwnerFile(root string, current []byte) ([]byte, error) {
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

func TestFamily145ChannelProcessFiniteSourcesAndEveryExactInverse(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadChannelProcessPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateChannelProcessCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if !pin.Removed {
			row := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
			output, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), row)
			if err != nil || !changed {
				t.Fatal("finite channel transformation refused its signed source:", pin.Function, err)
			}
			if _, changed, err := rewriteFunction(pin.File, output, row); err != nil || changed {
				t.Fatal("channel transformation was not inert:", pin.Function, err)
			}
			foreign := strings.Replace(pin.Before, pin.Function, pin.Function+"Foreign", 1)
			if _, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+foreign), row); err == nil || changed {
				t.Fatal("foreign source accepted:", pin.Function)
			}
		}
		old, err := selectedreadrole.EarlierChannelProcessSource(root, pin.File, pin.Function, pin.After)
		if err != nil || old != pin.Before {
			t.Fatal("exact source inverse lost predecessor:", pin.Function, err)
		}
		if _, err := selectedreadrole.EarlierChannelProcessSource(root, pin.File, pin.Function, pin.After+"\n"); err == nil {
			t.Fatal("source inverse accepted altered endpoint:", pin.Function)
		}
	}
}

func TestFamily145ChannelProcessCompleteHistoryAndEventBoundary(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := selectedreadrole.PriorChannelProcessHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := selectedreadrole.ComposeChannelProcessHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		actual := family145TransitionBytes
		if recipes {
			actual = recipeBytes
		}
		var have, want any
		if json.Unmarshal(actual, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
			t.Fatal("history changed outside the exact channel composition")
		}
		old, err := selectedreadrole.EarlierChannelProcessHistory(root, actual, recipes)
		if err != nil || !bytes.Equal(old, prior) {
			t.Fatal("channel history lost signed predecessor:", err)
		}
		assertChannelProcessHistoryHostileControls(t, root, actual, recipes)
	}
	current, err := os.ReadFile(filepath.Join(root, agentSnapshotEventOwner))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != channelProcessEventOwnerAfter {
		t.Fatal("channel event bridge owner changed")
	}
	if _, err := earlierChannelProcessEventOwnerFile(root, current); err != nil {
		t.Fatal(err)
	}
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
