package sourcefork

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"strings"
)

const Predecessor = "0396a9b746ae7a4c2d901e19be8858f4dfa3f553"
const HistoryPredecessor = "64eda6291965b8fb8d764b91b347c1c8185fa1a2"
const Path = "tools/fixture-codemod/pipeline-observations/family145-source-fork-cardinality-preimages.json"
const Digest = "46e028ffb16b29264fe757efb4bb67aa2e98cdda9236fb3398e589dfc96740e8"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("source fork cardinality manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 6 {
		return nil, fmt.Errorf("source fork cardinality whole-body inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{File: "internal/serveapp/main_runtime_test.go", Function: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Edits: []runphysical.Edit{
		splice(1, "forkRuns", "db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, initial.RunID)",
			"readServedSourceForkCardinality(reader, initial.RunID)", "t.Fatalf(\"count rejected join fork runs: %v\", err)", ""),
	}, Calls: 1},
	{File: "internal/serveapp/issue2277_audit_probe_test.go", Function: "TestAudit2277ForkCompletionLoss", Edits: []runphysical.Edit{
		splice(3, "forks", "rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, seed.RunID)", "readServedSourceForkCardinality(rt.ReceiverStateReader, seed.RunID)", "t.Fatal(err)", "var forks, completions int"),
		splice(3, "forks", "rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, seed.RunID)", "readServedSourceForkCardinality(rt.ReceiverStateReader, seed.RunID)", "t.Fatal(err)", ""),
	}, Calls: 2},
	{File: "internal/runtime/conformance/fan_out_resource_selected_fork_preissue_test.go", Function: "TestDeploymentSourceSelectedForkBeforeFirstRow", Edits: []runphysical.Edit{
		deployment(3, "childCount", ""),
	}, Calls: 1},
	{File: "internal/runtime/conformance/fan_out_resource_selected_fork_initial_race_test.go", Function: "TestDeploymentSourceSelectedForkConcurrentFirstMaterialization", Edits: []runphysical.Edit{
		deployment(3, "childCount", "var childCount, operationCount int"),
		deployment(3, "childCount", ""),
	}, Calls: 2},
	{File: "internal/runtime/conformance/fan_out_resource_selected_fork_test.go", Function: "proveSourceChangedPinPublicForkAndLostResponse", Edits: []runphysical.Edit{
		deployment(5, "childCount", ""),
	}, Calls: 1},
	{File: "internal/runtime/conformance/fan_out_resource_selected_fork_test.go", Function: "TestDeploymentSourceDynamicReceiverSelectedForkRefusesWithoutMutation", Edits: []runphysical.Edit{
		deployment(3, "children", ""),
	}, Calls: 1},
}

func deployment(depth int, destination, prefix string) runphysical.Edit {
	return splice(depth, destination, "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, sourceRunID)",
		"storetest.CountSelectedForkRunsForSource(f.ctx, f.selected, sourceRunID)", "t.Fatal(err)", prefix)
}

func splice(depth int, destination, query, read, failure, prefix string) runphysical.Edit {
	indent := strings.Repeat("\t", depth)
	before := "if err := " + query + ".Scan(&" + destination + "); err != nil {\n" + indent + "\t" + failure + "\n" + indent + "}"
	after := "{\n" + indent + "\tvar err error\n" + indent + "\t" + destination + ", err = " + read + "\n" + indent + "\tif err != nil {\n" + indent + "\t\t" + failure + "\n" + indent + "\t}\n" + indent + "}"
	if prefix != "" {
		before = prefix + "\n" + indent + before
		after = prefix + "\n" + indent + after
	}
	return runphysical.Edit{Before: before, After: after}
}
