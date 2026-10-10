package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusCoordinatorArgumentPreservesExactDependencyWiring(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row, caller recipe
	matched := 0
	for _, candidate := range rows {
		if candidate.Family != "native-bus-coordinator-raw-argument" {
			continue
		}
		matched++
		switch candidate.Function {
		case "newEventBusWorkflowCoordinator":
			row = candidate
		case "seedComponentFlowConstruction":
			caller = candidate
		default:
			t.Fatalf("unreviewed coordinator caller: %s", candidate.Function)
		}
	}
	if matched != 2 || row.Before == "" || caller.Before == "" {
		t.Fatal("coordinator and native component recipes must both exist exactly once")
	}
	want := strings.Replace(row.Before, "\tdb *sql.DB,\n", "", 1)
	if want == row.Before || want != row.After {
		t.Fatal("coordinator dependency graph or constructor behavior changed beyond unused argument removal")
	}
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	if actual != row.After {
		t.Fatal("actual coordinator no longer consumes the exact reviewed dependency graph")
	}
	for _, pair := range [][2]string{
		{"NewWorkflowPersistence(selected)", "NewWorkflowPersistence(foreign)"},
		{"RunLifecycle:            selected", "RunLifecycle:            foreign"},
		{"DeliveryStore:           selected", "DeliveryStore:           nil"},
		{"ReceiverExecution:       eventreceiver.NormalExecution()", "ReceiverExecution:       eventreceiver.ColdConstruction()"},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || mutant == want {
			t.Fatalf("weakened selected coordinator wiring admitted: %v", pair)
		}
	}
	if strings.Replace(caller.Before, "newEventBusWorkflowCoordinator(bus, nil, selected,", "newEventBusWorkflowCoordinator(bus, selected,", 1) != caller.After {
		t.Fatal("component source, stages, activation, acknowledgment or finalization changed")
	}
}
