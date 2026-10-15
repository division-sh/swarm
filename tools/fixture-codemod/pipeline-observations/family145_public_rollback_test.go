package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

//go:embed family145-public-rollback-preimages.json
var publicRollbackBytes []byte

func publicRollbackPins(t *testing.T) []publicrollback.Pin {
	t.Helper()
	pins, err := publicrollback.Decode(publicRollbackBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func restorePublicRollbackHistory(t *testing.T, rows []map[string]any, recipes bool) []map[string]any {
	pins, err := publicrollback.Decode(publicRollbackBytes)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		for _, pin := range pins {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			if recipes && row["After"] == pin.Before && row["Successor"] == pin.After {
				delete(row, "Successor")
			} else if !recipes && row["After"] == pin.After {
				row["After"] = pin.Before
			}
		}
	}
	return rows
}

func TestFamily145PublicRollbackPreservesHistoricalRecordsAndOrder(t *testing.T) {
	for _, path := range []string{"tools/fixture-codemod/pipeline-observations/recipes.json", "tools/fixture-codemod/pipeline-observations/family145-transitions.json"} {
		previous, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
		if err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil {
			t.Fatal(err)
		}
		var before, after []map[string]any
		if err := json.Unmarshal(previous, &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(current, &after); err != nil {
			t.Fatal(err)
		}
		after = restoreSourceForkHistory(t, after, strings.HasSuffix(path, "recipes.json"))
		restored := cloneRollbackHistory(t, after)
		restored = restorePublicRollbackHistory(t, restored, strings.HasSuffix(path, "recipes.json"))
		if !reflect.DeepEqual(before, restored) {
			t.Fatal("rollback changed historical records, metadata or order:", path)
		}
		for i, row := range before {
			if reflect.DeepEqual(row, after[i]) {
				continue
			}
			for _, key := range []string{"File", "Function", "Before", "Mechanical", "Removed", "Family"} {
				mutant := cloneRollbackHistory(t, after)
				mutant[i][key] = "unreviewedHistory"
				mutant = restorePublicRollbackHistory(t, mutant, strings.HasSuffix(path, "recipes.json"))
				if reflect.DeepEqual(before, mutant) {
					t.Fatal("history inverse erased changed metadata:", key)
				}
			}
		}
	}
}

func cloneRollbackHistory(t *testing.T, rows []map[string]any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var copy []map[string]any
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func TestFamily145PublicRollbackWholeRuleKeepsAllSevenCutsAndBothCallers(t *testing.T) {
	for index, pin := range publicRollbackPins(t) {
		raw, err := exec.Command("git", "show", publicrollback.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, raw, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("rollback predecessor is not complete signed source:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		body, err := selectedreadrole.EarlierServedScenarioSource(filepath.Clean("../../.."), pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))
		if err != nil || body != pin.After {
			t.Fatal("rollback current source escaped its exact whole rule:", pin.Function, err)
		}
		after, err := runphysical.Transform(publicrollback.Rules[index], pin.Before)
		if err != nil || after != pin.After {
			t.Fatal("rollback finite transform escaped its fixed cuts:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("rollback finite transform refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("rollback transform is not inert:", pin.Function, err)
		}
		for _, cut := range []string{"reader", "InspectSnapshot", "CountPhysicalRunIdentity", "CountTemplateInstances", "CountWorkflowRunFieldRows", "CountTemplatePrefixRoutingRules", "CountTemplateEventIdentity", "CountTemplateEventDeliveries", "CountTemplateEventReplayScopes", "telegram-chat", "runID", "eventID", "err != nil", "count != 0", "t.Fatalf", "query.label", "captureSelectedRuntimePersistence", "Install", "stage.install", "requestServedJSONRPC", "servedEventPublishAPIIdempotencyCount"} {
			if !strings.Contains(pin.After, cut) {
				continue
			}
			mutant := strings.Replace(pin.After, cut, "unreviewedRollbackCut", 1)
			if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
				t.Fatal("changed rollback owner/identity/assertion/cut accepted:", pin.Function, cut)
			}
		}
	}
}

func TestFamily145PublicRollbackNewOwnersHaveExactPhysicalPredicates(t *testing.T) {
	for _, owner := range [][3]string{
		{"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go", "CountTemplateEventReplayScopesTx", "func CountTemplateEventReplayScopesTx(ctx context.Context, tx *sql.Tx, event string) (int,error) { var count int;err:=tx.QueryRowContext(ctx,`SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$1`,event).Scan(&count);return count,err }"},
		{"internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "CountTemplateEventReplayScopesForTest", `func CountTemplateEventReplayScopesForTest(ctx context.Context, selected any, event string)(int,error){return readFanOutSemanticStorage(ctx,selected,[]string{event},func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplateEventReplayScopesTx(ctx,tx,event)})}`},
	} {
		raw, err := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || requestKeyDeclaration(t, raw, owner[1]) != requestKeyDeclaration(t, []byte("package proof\n"+owner[2]), owner[1]) {
			t.Fatal("rollback query/identity/native snapshot contract changed:", owner[1], err)
		}
	}
	path := "internal/store/internal/backend/pipelinepersistence/template_storage_observation.go"
	old, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
	current, readErr := os.ReadFile(filepath.Join("../../..", path))
	if err != nil || readErr != nil || requestKeyDeclaration(t, old, "CountTemplateSubscribedReplayScopeTx") != requestKeyDeclaration(t, current, "CountTemplateSubscribedReplayScopeTx") {
		t.Fatal("subscribed-only owner was silently broadened:", err, readErr)
	}
}
