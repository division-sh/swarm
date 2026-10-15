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

func TestFamily145StandingIngressControlFiniteNativeSourcesAndAllCallers(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadStandingIngressPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateStandingIngressCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		row := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		output, changed, err := rewriteFunction(pin.File, []byte("package serveapp\n"+pin.Before), row)
		if err != nil || !changed {
			t.Fatal("finite native transform refused signed source:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, output, row); err != nil || changed {
			t.Fatal("finite native transform repeated:", pin.Function, err)
		}
		bad := strings.Replace(pin.Before, pin.Function, pin.Function+"Foreign", 1)
		if _, changed, err := rewriteFunction(pin.File, []byte("package serveapp\n"+bad), row); err == nil || changed {
			t.Fatal("finite transform admitted foreign declaration:", pin.Function)
		}
	}
	if err := standingIngressHelperReferences(root, pins); err != nil {
		t.Fatal(err)
	}
}

func standingIngressHelperNames() map[string]bool {
	out := map[string]bool{}
	for _, name := range strings.Fields(`loadServedStandingOwner assertServedStandingState assertServedStandingResetAndShutdownJoin assertServedStandingInvalidChildCommands waitForStandingDeliveryQuiescence servedRunControlState requireServedRunControlState servedDeliveryStatusReason requireServedStoppedPendingDelivery servedRuntimeIngressState requireServedRuntimeIngressState servedEventNameByID lifecycleAdmissionDomainCounts semanticNumericDomainCounts terminalizeStandingRunFromRuntime`) {
		out[name] = true
	}
	return out
}

func TestFamily145StandingIngressControlNewHelpersKeepExactInspectionAndClose(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, pin := range []struct{ file, function, digest string }{
		{"internal/serveapp/source_admission_fork_proof_test.go", "countServedPhysicalSourceArtifacts", "5f812267bf162a940ede6589253a7359041abe1da9046c3bf887339f68267b7c"},
		{"internal/serveapp/standing_ingress_supported_surface_test.go", "openServedStandingInspection", "8ad8eff43f49a09d28f90528d348470486e644823e8eb0e92c3276a09059e282"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin.file))
		if err != nil {
			t.Fatal(err)
		}
		body, err := selectedreadrole.Body(raw, pin.function)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(body))) != pin.digest {
			t.Fatal("new helper changed its exact named observation/inspection/teardown:", pin.function, err)
		}
	}
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

func TestFamily145StandingIngressControlOneWaySource(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadStandingIngressPins(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		before, err := selectedreadrole.EarlierStandingIngressSource(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatal("complete earlier source changed:", pin.Function, err)
		}
		for _, bad := range []string{pin.Before, pin.After + "\n", pin.After + " // foreign"} {
			if _, err := selectedreadrole.EarlierStandingIngressSource(root, pin.File, pin.Function, bad); err == nil {
				t.Fatal("source inverse accepted stale or foreign endpoint:", pin.Function)
			}
		}
	}
}

func TestFamily145StandingIngressControlCompleteHistory(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := selectedreadrole.PriorStandingIngressHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		next, err := selectedreadrole.ComposeStandingIngressHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		actual := family145TransitionBytes
		if recipes {
			actual = recipeBytes
		}
		actual, err = selectedreadrole.BeforeAgentSnapshotHistory(root, actual, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var want, have any
		if json.Unmarshal(next, &want) != nil || json.Unmarshal(actual, &have) != nil || !reflect.DeepEqual(want, have) {
			t.Fatal("actual history is not the complete exact composition")
		}
		previous, err := selectedreadrole.EarlierStandingIngressHistory(root, actual, recipes)
		if err != nil || !bytes.Equal(previous, prior) {
			t.Fatal("history lost signed predecessor bytes:", err)
		}
		if _, err := selectedreadrole.EarlierStandingIngressHistory(root, prior, recipes); err == nil {
			t.Fatal("history inverse repeated on its predecessor")
		}
		var rows []map[string]any
		if err := json.Unmarshal(actual, &rows); err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []func([]map[string]any) []map[string]any{
			func(rows []map[string]any) []map[string]any { return rows[:len(rows)-1] },
			func(rows []map[string]any) []map[string]any { return append(rows, rows[0]) },
			func(rows []map[string]any) []map[string]any { rows[0]["Before"] = "foreign"; return rows },
		} {
			bad, err := json.Marshal(mutation(cloneRollbackHistory(t, rows)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := selectedreadrole.EarlierStandingIngressHistory(root, bad, recipes); err == nil {
				t.Fatal("foreign/deleted/duplicate historical record accepted")
			}
		}
	}
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

func TestFamily145StandingIngressControlWholeOwnerAdditionsKeepEveryEarlierByte(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, pin := range standingIngressOwnerAdditions {
		t.Run(filepath.Base(pin.file), func(t *testing.T) {
			current, err := os.ReadFile(filepath.Join(root, pin.file))
			if err != nil {
				t.Fatal(err)
			}
			previous, err := exec.Command("git", "-C", root, "show", selectedreadrole.StandingIngressPredecessor+":"+pin.file).Output()
			if err != nil {
				t.Fatal(err)
			}
			restored, err := earlierStandingIngressOwnerFile(root, pin.file, current)
			if err != nil || !bytes.Equal(restored, previous) {
				t.Fatal("whole owner did not restore its complete signed predecessor:", err)
			}
			for _, name := range pin.names {
				bad := bytes.Replace(current, []byte(name), []byte(name+"Foreign"), 1)
				if _, err := earlierStandingIngressOwnerFile(root, pin.file, bad); err == nil {
					t.Fatal("added owner declaration escaped its exact one-way pin:", name)
				}
			}
			if _, err := earlierStandingIngressOwnerFile(root, pin.file, append(current, '\n')); err == nil {
				t.Fatal("changed complete owner was accepted")
			}
		})
	}
}
