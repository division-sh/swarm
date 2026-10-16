package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forkcompanion"
)

//go:embed family145-fork-companion-preimages.json
var family145ForkCompanionBytes []byte

func forkCompanionPins(t *testing.T) []forkcompanion.Pin {
	t.Helper()
	pins, err := forkcompanion.Decode(family145ForkCompanionBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ForkCompanionCallerInverse(function, current string) string {
	current = family145RequestKeyCallerInverse(function, current)
	current = family145ForkNoticeCallerInverse(function, current)
	pins, err := forkcompanion.Decode(family145ForkCompanionBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return forkcompanion.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckForkCompanionWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145RequestKeyCallerInverse(function, current)
	current = family145ForkNoticeCallerInverse(function, current)
	for _, pin := range forkCompanionPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("fork companion escaped complete owner/caller rule:", file, function)
			}
			return true
		}
	}
	return false
}

func requireForkCompanionPhysicalOwner(t *testing.T, pin forkcompanion.Pin) {
	t.Helper()
	const helper = `func readForkReceiverCompanions(t *testing.T, reader receiverProofStateReader, runID string, paths ...string) map[string][]string {
	t.Helper()
	out, err := storetest.ReadForkReceiverCompanionStorage(context.Background(), reader, runID, paths...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}`
	if family145ServedDebugNormalize(t, helper) != pin.After {
		t.Fatal("companion fixture restored independent SQL/decoding/owner construction")
	}
	raw, err := os.ReadFile("../../../internal/store/internal/backend/pipelinepersistence/fork_receiver_companion_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	owner := family145PublicA2CurrentDeclaration(t, raw, "ReadForkReceiverCompanionTableStorageTx")
	for _, cut := range []string{"`SELECT * FROM `+table+` WHERE run_id=$1`", "` AND instance_path=$2`", "args = append(args, paths[0])", "rows.Columns()", "json.Marshal(columns)", "rows.Scan(pointers...)", "values[i] = string(raw)", "json.Marshal(values)", "sort.Strings(out"} {
		if !strings.Contains(pin.Before, cut) || !strings.Contains(owner, cut) {
			t.Fatal("native companion owner lost original full physical predicate/columns/bytes/order:", cut)
		}
	}
	bridge, err := os.ReadFile("../../../internal/store/internal/runtimepersistence/test_fork_receiver_companion_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(bridge), `for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness"}`) ||
		!strings.Contains(string(bridge), "readServedDeliveryObservation(ctx, selected,") ||
		!strings.Contains(owner, `table != "flow_instances" && table != "flow_instance_runtime_readiness"`) {
		t.Fatal("companion fixed catalog or original per-table selected read boundary changed")
	}
}

func forkCompanionHostiles(t *testing.T, pin forkcompanion.Pin) {
	t.Helper()
	pins := forkCompanionPins(t)
	if forkcompanion.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || forkcompanion.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("companion inverse lost exact source or admitted foreign file")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"ReceiverStateReader", "reader", "ReadForkReceiverCompanionStorage", "runID", "paths...", "reflect.DeepEqual", "seed.RunID", "siblingSeed.RunID", "frontier", "fork.ForkRunID", "release", "t.Fatal", "sourceCompanions", "childCompanions"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignCompanionCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || forkcompanion.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("lost companion reader/identity/refusal/replay/lifetime cut accepted:", pin.Function, cut)
		}
	}
}
