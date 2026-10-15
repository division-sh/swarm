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
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const deliveryRecoveryReadOwner = "internal/store/internal/backend/delivery/read_projections.go"
const deliveryRecoveryReadOwnerBefore = "28a43daef1ea1196242bd370653816ea61ea59460e6c0f2bbf96a05a81afab68"
const deliveryRecoveryReadOwnerAfter = "a2bf26a7d22fb377634f3503897e30206f504efc20a179aca3f84a2dca77fa36"

var deliveryRecoveryReadDeclarations = []string{
	"DeliveryRollbackStorage", "ReadDeliveryRollbackStorageTx", "ExactDeliveryDeadLetterStorage",
	"ReadExactDeliveryDeadLetterStorageTx", "ReadDeliveryAttemptFailureStorageTx",
	"DeliveryConformanceAttemptStorage", "ReadDeliveryConformanceAttemptsStorageTx",
	"CountMatchingOpenDeliveryAttemptStorageTx", "ReadDeliveryConformanceSelectionStorageTx",
	"CountDeliveryHandlerSelectionsStorageTx",
}

func earlierDeliveryRecoveryOwnerFile(root, file string, current []byte) ([]byte, error) {
	var err error
	current, err = earlierServedScenarioOwnerFile(root, file, current)
	if err != nil {
		return nil, err
	}
	if file != deliveryRecoveryReadOwner {
		return current, nil
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(current))
	if digest == deliveryRecoveryReadOwnerBefore {
		return current, nil
	}
	if digest != deliveryRecoveryReadOwnerAfter {
		return nil, fmt.Errorf("delivery recovery complete read owner changed")
	}
	for _, name := range deliveryRecoveryReadDeclarations {
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
	prior, err := exec.Command("git", "-C", root, "show", selectedreadrole.DeliveryRecoveryPredecessor+":"+file).Output()
	if err != nil || !bytes.Equal(restored, prior) || fmt.Sprintf("%x", sha256.Sum256(prior)) != deliveryRecoveryReadOwnerBefore {
		return nil, fmt.Errorf("delivery recovery read owner lost complete predecessor: %v", err)
	}
	return restored, nil
}

func TestFamily145DeliveryRecoveryWholeReadOwnerPreservesEveryEarlierByte(t *testing.T) {
	root := filepath.Clean("../../..")
	current, err := os.ReadFile(filepath.Join(root, deliveryRecoveryReadOwner))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := earlierDeliveryRecoveryOwnerFile(root, deliveryRecoveryReadOwner, current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := earlierDeliveryRecoveryOwnerFile(root, deliveryRecoveryReadOwner, prior); err != nil {
		t.Fatal("unchanged predecessor cannot continue into older inverse:", err)
	}
	for _, name := range deliveryRecoveryReadDeclarations {
		bad := bytes.Replace(current, []byte(name), []byte(name+"Foreign"), 1)
		if _, err := earlierDeliveryRecoveryOwnerFile(root, deliveryRecoveryReadOwner, bad); err == nil {
			t.Fatal("changed complete owner accepted:", name)
		}
	}
	if _, err := earlierDeliveryRecoveryOwnerFile(root, deliveryRecoveryReadOwner, append(current, '\n')); err == nil {
		t.Fatal("changed whole-file bytes accepted")
	}
}

func TestFamily145DeliveryRecoveryCompleteSourceAndSelectedOwners(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeliveryRecoveryPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 22 {
		t.Fatalf("delivery recovery declarations=%d want22 plus the exact carrier retirement", len(pins))
	}
	if err := selectedreadrole.ValidateDeliveryRecoveryCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		before, err := selectedreadrole.EarlierDeliveryRecoverySource(root, pin.File, pin.Function, pin.After)
		if err != nil || before != pin.Before {
			t.Fatalf("signed source inverse failed %s: %v", pin.Function, err)
		}
		for _, hostile := range []string{pin.Before, pin.After + "\n", strings.Replace(pin.After, "{", "{ foreignAuthority();", 1)} {
			if _, err := selectedreadrole.EarlierDeliveryRecoverySource(root, pin.File, pin.Function, hostile); err == nil {
				t.Fatalf("changed source accepted: %s", pin.Function)
			}
		}
		assertSelectedForkExecutionFiniteRewrite(t, pin)
		if err := family145DescriptorReadinessEscape([]byte("package proof\n" + pin.After)); err != nil {
			t.Fatalf("raw authority remains %s: %v", pin.Function, err)
		}
		assertDeliveryRecoveryRootMessages(t, pin)
	}
	assertDeliveryRecoveryCleanupAndClaims(t, pins)
}

func assertDeliveryRecoveryRootMessages(t *testing.T, pin selectedreadrole.EntityToolPin) {
	t.Helper()
	if !strings.HasPrefix(pin.Function, "Test") {
		return
	}
	want := deliveryRecoveryFailureMessages(t, pin.Before)
	if pin.Function == "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity" {
		want = append([]string{`Errorf:"remove node recovery poison mutation: %v"`}, want...)
	}
	if !reflect.DeepEqual(want, deliveryRecoveryFailureMessages(t, pin.After)) {
		t.Fatalf("original root assertions changed: %s", pin.Function)
	}
}

func deliveryRecoveryFailureMessages(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var messages []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := method.X.(*ast.Ident)
		if !ok || receiver.Name != "t" {
			return true
		}
		switch method.Sel.Name {
		case "Fatal", "Fatalf", "Error", "Errorf":
			if literal, ok := call.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
				messages = append(messages, method.Sel.Name+":"+literal.Value)
			}
		}
		return true
	})
	return messages
}

func assertDeliveryRecoveryCleanupAndClaims(t *testing.T, pins []selectedreadrole.EntityToolPin) {
	t.Helper()
	bodies := map[string]string{}
	for _, pin := range pins {
		bodies[pin.Function] = pin.After
	}
	for _, name := range []string{"expireNodeDeliveryClaim", "expireDeliveryClaimForConformance"} {
		if strings.Count(bodies[name], "storetest.ExpireExactDeliveryClaimFault(") != 1 || strings.Contains(bodies[name], "deliveryID string") {
			t.Fatalf("exact acquired claim owner was replaced: %s", name)
		}
	}
	if !strings.Contains(bodies["ageDeliveryClaimForConformance"], "storetest.AgeManagerClaimStartedAt(") {
		t.Fatal("canonical age owner was duplicated")
	}
	for _, name := range []string{"makeNodeDeliveryImmediatelyEligible", "makeDeliveryImmediatelyEligible"} {
		if strings.Count(bodies[name], "storetest.MakeDeliveryRecoveryRetryEligible(") != 1 {
			t.Fatalf("retry owner not shared: %s", name)
		}
	}
	install := bodies["installDeliveryDeadLetterFault"]
	if arm, write := strings.Index(install, "t.Cleanup(cleanup)"), strings.Index(install, "storetest.InstallDeliveryDiagnosticFault("); arm < 0 || write < arm || !strings.Contains(install, "context.WithoutCancel(ctx)") {
		t.Fatal("diagnostic cleanup is not armed before native installation")
	}
	root := bodies["TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity"]
	if arm, consumer := strings.Index(root, "t.Cleanup("), strings.Index(root, "newScopedTestEventBus("); arm < 0 || consumer < arm {
		t.Fatal("poison cleanup does not outlive joined consumers")
	}
	if strings.Contains(bodies["installNodeRecoveryPoisonMutation"], "t.Cleanup(") {
		t.Fatal("late poison cleanup can run ahead of consumer join")
	}
}

func TestFamily145DeliveryRecoveryAppendOnlyHistoryAndImmutableSiblings(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := selectedreadrole.PriorDeliveryRecoveryHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		actual := family145TransitionBytes
		if recipes {
			actual = recipeBytes
		}
		actual, err = selectedreadrole.BeforeServedScenarioHistory(root, actual, recipes)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := selectedreadrole.ComposeDeliveryRecoveryHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var before, after, want []map[string]any
		if json.Unmarshal(prior, &before) != nil || json.Unmarshal(actual, &after) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(after, want) {
			t.Fatal("history differs from exact complete composition")
		}
		assertDeliveryRecoveryHistoryOriginals(t, before, after, recipes)
		recovered, err := selectedreadrole.EarlierDeliveryRecoveryHistory(root, actual, recipes)
		if err != nil || !bytes.Equal(prior, recovered) {
			t.Fatalf("complete signed predecessor was not recovered: %v", err)
		}
		for _, cut := range []string{"lost", "order", "before", "after"} {
			var rows []map[string]any
			if err := json.Unmarshal(actual, &rows); err != nil {
				t.Fatal(err)
			}
			switch cut {
			case "lost":
				rows = rows[:len(rows)-1]
			case "order":
				rows[len(rows)-1], rows[len(rows)-2] = rows[len(rows)-2], rows[len(rows)-1]
			case "before":
				rows[len(rows)-1]["Before"] = "foreign"
			case "after":
				rows[len(rows)-1]["After"] = "foreign"
			}
			bad, err := json.Marshal(rows)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := selectedreadrole.EarlierDeliveryRecoveryHistory(root, bad, recipes); err == nil {
				t.Fatalf("changed history accepted: %s", cut)
			}
		}
	}
}

func assertDeliveryRecoveryHistoryOriginals(t *testing.T, before, after []map[string]any, recipes bool) {
	t.Helper()
	appended := 22
	if recipes {
		appended = 20
	}
	if len(after) != len(before)+appended {
		t.Fatalf("history appended%d want%d", len(after)-len(before), appended)
	}
	advanced, unchanged := 0, 0
	for index, row := range before {
		for key, value := range row {
			if !reflect.DeepEqual(value, after[index][key]) {
				t.Fatalf("original history row%d field%s was changed", index, key)
			}
		}
		if !recipes && !reflect.DeepEqual(row, after[index]) {
			t.Fatal("original transition was changed")
		}
		if recipes && (row["File"] == selectedreadrole.DeliveryRecoveryFile || row["File"] == selectedreadrole.DeliveryRecoveryStartupFile) {
			if reflect.DeepEqual(row, after[index]) {
				unchanged++
			} else {
				advanced++
			}
		}
	}
	if recipes && (advanced != 2 || unchanged != 3) {
		t.Fatalf("five existing endpoints changed outside2+3 boundary: %d/%d", advanced, unchanged)
	}
}

func TestFamily145DeliveryRecoveryManifestChecksActualSourceAndCarrierAfterWarmCache(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeliveryRecoveryPins(root)
	if err != nil {
		t.Fatal(err)
	}
	copy := deliveryRecoveryProbeCheckout(t, root)
	if err := selectedreadrole.ValidateDeliveryRecoveryCurrent(copy, pins); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{selectedreadrole.DeliveryRecoveryFile, selectedreadrole.DeliveryRecoveryStartupFile} {
		path := filepath.Join(copy, file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var function string
		for _, pin := range pins {
			if pin.File == file {
				function = pin.Function
				break
			}
		}
		for _, cut := range []string{"changed", "missing", "duplicate"} {
			next := selectedForkActualSourceMutation(t, raw, function, cut)
			if next == string(raw) {
				t.Fatal("mutation did not reach actual source")
			}
			if err := os.WriteFile(path, []byte(next), 0600); err != nil {
				t.Fatal(err)
			}
			if err := selectedreadrole.ValidateDeliveryRecoveryCurrent(copy, pins); err == nil {
				t.Fatalf("warm cache concealed actual%s/%s", file, cut)
			}
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	assertDeliveryRecoveryCarrierRefusesResurrection(t, copy, pins)
}

func assertDeliveryRecoveryCarrierRefusesResurrection(t *testing.T, copy string, pins []selectedreadrole.EntityToolPin) {
	t.Helper()
	path := filepath.Join(copy, selectedreadrole.DeliveryRecoveryFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(raw, []byte("selected any"), []byte("selected *sql.DB"), 1)
	if bytes.Equal(raw, changed) { // gofmt aligns the real field; mutate through AST offsets instead.
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, "carrier.go", raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			field, ok := n.(*ast.Field)
			if ok && len(field.Names) == 1 && field.Names[0].Name == "selected" {
				start, end := set.Position(field.Type.Pos()).Offset, set.Position(field.Type.End()).Offset
				changed = append(append(append([]byte{}, raw[:start]...), []byte("*sql.DB")...), raw[end:]...)
			}
			return true
		})
	}
	if bytes.Equal(raw, changed) {
		t.Fatal("carrier hostile control did not reach source")
	}
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateDeliveryRecoveryCurrent(copy, pins); err == nil {
		t.Fatal("raw carrier resurrection escaped actual-source authentication")
	}
}

func deliveryRecoveryProbeCheckout(t *testing.T, root string) string {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{selectedreadrole.DeliveryRecoveryPath, selectedreadrole.DeliveryRecoveryFile, selectedreadrole.DeliveryRecoveryStartupFile} {
		copyDeliveryRecoveryProofFile(t, root, copy, path)
	}
	return copy
}

func copyDeliveryRecoveryProofFile(t *testing.T, root, copy, path string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, path), raw, 0600); err != nil {
		t.Fatal(err)
	}
}
