package main

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/workflowstate"
)

//go:embed family145-workflow-state-preimages.json
var family145WorkflowStateBytes []byte

func workflowStatePins(t *testing.T) []workflowstate.Pin {
	t.Helper()
	pins, err := workflowstate.Decode(family145WorkflowStateBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145WorkflowStateCallerInverse(function, current string) string {
	current = family145ServedEventCountCallerInverse(function, current)
	pins, err := workflowstate.Decode(family145WorkflowStateBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return workflowstate.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func workflowStateOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", workflowstate.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func workflowStateCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	entry := servedObservationEarlierSource(t, path, function, requestKeyDeclaration(t, raw, function))
	return family145ServedEventCountPrevious(t, path, function, entry)
}

func TestFamily145WorkflowStateNativePredicatesRemainExact(t *testing.T) {
	for _, row := range []struct{ helper, owner, query string }{
		{"requireLifecycleFlowEntity", "internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go", "ReadWorkflowStateEntityStorageTx"},
		{"requirePendingInputStateCount", "internal/store/internal/backend/entityruntime/run_state_cardinality.go", "CountRunStateEntityStorageTx"},
	} {
		var before string
		for _, pin := range workflowStatePins(t) {
			if pin.Function == row.helper {
				before = workflowStateOriginalDeclaration(t, pin.File, pin.Function)
			}
		}
		oldSQL := channelAnchorSQL(t, before)
		newSQL := channelAnchorSQL(t, workflowStateCurrentDeclaration(t, row.owner, row.query))
		if len(oldSQL) != 1 || !reflect.DeepEqual(oldSQL, newSQL) {
			t.Fatal("physical state predicate changed:", row.helper, oldSQL, newSQL)
		}
	}
}
