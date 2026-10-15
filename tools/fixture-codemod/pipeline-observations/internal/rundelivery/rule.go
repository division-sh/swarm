package rundelivery

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "64eda6291965b8fb8d764b91b347c1c8185fa1a2"
const HistoryPredecessor = "88109a5c62c51c9787dbcad483dbd635e0f54f49"
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

func ValidateCurrentSource(pin Pin, raw []byte) error {
	if err := validatePinRule(pin); err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), pin.File, raw, parser.AllErrors)
	if err != nil {
		return err
	}
	count := 0
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == pin.Function {
			count++
			if requestkey.Node(fn) != pin.After {
				return fmt.Errorf("run delivery current complete body changed: %s", pin.Function)
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("run delivery current declaration count=%d want1: %s", count, pin.Function)
	}
	return nil
}

func validatePinRule(pin Pin) error {
	for _, rule := range Rules {
		if rule.File != pin.File || rule.Function != pin.Function {
			continue
		}
		after, err := runphysical.Transform(rule, pin.Before)
		if err != nil || after != pin.After || pin.Calls != rule.Calls || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return fmt.Errorf("run delivery complete pin/six substitutions changed: %s", pin.Function)
		}
		return nil
	}
	return fmt.Errorf("foreign run delivery pin: %s/%s", pin.File, pin.Function)
}

var Rules = []runphysical.Rule{
	{File: "internal/serveapp/receiver_agent_collision_test.go", Function: "requireServedReceiverAgentCollision", Edits: []runphysical.Edit{
		expression("failed", "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID)", "readServedRunDeadLetterCount(context.Background(), rt.ReceiverStateReader, runID)"),
	}, Calls: 1},
	{File: "internal/serveapp/fan_out_original_reporter_http_test.go", Function: "waitIssue2394ReporterCursor", Edits: []runphysical.Edit{
		expression("dead", "rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID)", "readServedRunDeadLetterCount(ctx, rt.ReceiverStateReader, runID)"),
	}, Calls: 1},
	{File: "internal/serveapp/fan_out_original_reporter_http_test.go", Function: "waitIssue2394ReporterDiagnosisWithin", Edits: []runphysical.Edit{
		expression("stillUnsettled", "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')`, runID)", "readServedRunNonterminalDeliveryCount(context.Background(), rt.ReceiverStateReader, runID)"),
	}, Calls: 1},
	{File: "internal/runtime/conformance/fan_out_resource_selected_fork_external_test.go", Function: "TestSelectedDeploymentExternalEffectRecoveryBothStores", Edits: []runphysical.Edit{
		expression("failed", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, childID)", "storetest.CountRunDeadLetterDeliveries(f.ctx, f.selected, childID)"),
		expression("pending", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, childID)", "storetest.CountDeploymentPendingDeliveriesStorage(f.ctx, f.selected, childID)"),
		expression("deliveryCount", "f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, childID)", "storetest.CountRunDeliveryStorage(f.ctx, f.selected, childID)"),
	}, Calls: 3},
}

func expression(destination, query, read string) runphysical.Edit {
	return runphysical.Edit{Before: query + ".Scan(&" + destination + ")", After: "func() error { var err error; " + destination + ",err=" + read + "; return err }()"}
}
