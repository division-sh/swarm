package rundelivery

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "64eda6291965b8fb8d764b91b347c1c8185fa1a2"
const Path = "tools/fixture-codemod/pipeline-observations/family145-run-delivery-cardinality-preimages.json"
const Digest = "eabe5cadc2c89e898bc2af87cf5972e3acee641d954fd102dc22b37de9d55919"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("run delivery cardinality manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 4 {
		return nil, fmt.Errorf("run delivery cardinality complete declaration inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{"internal/serveapp/receiver_agent_collision_test.go", "requireServedReceiverAgentCollision", []runphysical.Edit{
		expression("failed", "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID)", "readServedRunDeadLetterCount(context.Background(), rt.ReceiverStateReader, runID)"),
	}, 1},
	{"internal/serveapp/fan_out_original_reporter_http_test.go", "waitIssue2394ReporterCursor", []runphysical.Edit{
		expression("dead", "rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID)", "readServedRunDeadLetterCount(ctx, rt.ReceiverStateReader, runID)"),
	}, 1},
	{"internal/serveapp/fan_out_original_reporter_http_test.go", "waitIssue2394ReporterDiagnosisWithin", []runphysical.Edit{
		expression("stillUnsettled", "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')`, runID)", "readServedRunNonterminalDeliveryCount(context.Background(), rt.ReceiverStateReader, runID)"),
	}, 1},
	{"internal/runtime/conformance/fan_out_resource_selected_fork_external_test.go", "TestSelectedDeploymentExternalEffectRecoveryBothStores", []runphysical.Edit{
		expression("failed", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, childID)", "storetest.CountRunDeadLetterDeliveries(f.ctx, f.selected, childID)"),
		expression("pending", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, childID)", "storetest.CountDeploymentPendingDeliveriesStorage(f.ctx, f.selected, childID)"),
		expression("deliveryCount", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, childID)", "storetest.CountRunDeliveryStorage(f.ctx, f.selected, childID)"),
	}, 3},
}

func expression(destination, query, read string) runphysical.Edit {
	return runphysical.Edit{Before: query + ".Scan(&" + destination + ")", After: "func() error { var err error; " + destination + ",err=" + read + "; return err }()"}
}
