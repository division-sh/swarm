package publicrollback

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "cb71e60b75385615a90e82a0463f62f737d8d0e7"
const Path = "tools/fixture-codemod/pipeline-observations/family145-public-rollback-preimages.json"
const Digest = "0806a88943bacd967a72a39fb5c92f078443af44dd4d444ac62c3745a8deceb5"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("public rollback source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 3 {
		return nil, fmt.Errorf("public rollback whole-chain inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{"internal/serveapp/test_command_runtime_test.go", "requirePublicInputRollbackNoResidue", []runphysical.Edit{
		{"db *sql.DB", "reader receiverProofStateReader"},
		{"queries := []struct {\n\t\tlabel string\n\t\tsql   string\n\t\targs  []any\n\t}{{label: \"run\", sql: `SELECT COUNT(*) FROM runs WHERE run_id = ?`, args: []any{runID}}, {label: \"flow instance\", sql: `SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'`}, {label: \"entity\", sql: `SELECT COUNT(*) FROM entity_state WHERE run_id = ?`, args: []any{runID}}, {label: \"route\", sql: `SELECT COUNT(*) FROM routing_rules WHERE flow_instance LIKE 'telegram-chat/%'`}, {label: \"event\", sql: `SELECT COUNT(*) FROM events WHERE event_id = ?`, args: []any{eventID}}, {label: \"delivery\", sql: `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, args: []any{eventID}}, {label: \"replay scope\", sql: `SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id = ?`, args: []any{eventID}}}", "queries := []struct { label string; read func(context.Context)(int,error) }{\n{\"run\",func(ctx context.Context)(int,error){return storetest.CountPhysicalRunIdentity(ctx,reader,runID)}},\n{\"flow instance\",func(ctx context.Context)(int,error){return storetest.CountTemplateInstances(ctx,reader,\"telegram-chat\")}},\n{\"entity\",func(ctx context.Context)(int,error){return storetest.CountWorkflowRunFieldRows(ctx,reader,runID)}},\n{\"route\",func(ctx context.Context)(int,error){return storetest.CountTemplatePrefixRoutingRules(ctx,reader,\"telegram-chat\")}},\n{\"event\",func(ctx context.Context)(int,error){return storetest.CountTemplateEventIdentity(ctx,reader,eventID)}},\n{\"delivery\",func(ctx context.Context)(int,error){return storetest.CountTemplateEventDeliveries(ctx,reader,eventID)}},\n{\"replay scope\",func(ctx context.Context)(int,error){return storetest.CountTemplateEventReplayScopes(ctx,reader,eventID)}},\n}"},
		{"statement := query.sql\n\t\tif backend == servedparity.BackendExplicitPostgres && len(query.args) > 0 {\n\t\t\tstatement = strings.Replace(statement, \"?\", \"$1::uuid\", 1)\n\t\t}\n\t\tif err := db.QueryRowContext(context.Background(), statement, query.args...).Scan(&count); err != nil", "if reader==nil { t.Fatalf(\"%s count %s rollback rows: missing original selected reader\",backend,query.label) }\nerr:=reader.InspectSnapshot(context.Background(),func(ctx context.Context)error{\nvar err error; count,err=query.read(ctx);return err\n})\nif err!=nil"},
	}, 7},
	caller("TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores"),
	caller("TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores"),
}

func caller(function string) runphysical.Rule {
	return runphysical.Rule{File: "internal/serveapp/test_command_runtime_test.go", Function: function, Edits: []runphysical.Edit{
		{Before: "requirePublicInputRollbackNoResidue(t, db, backend, eventID, runID)", After: "requirePublicInputRollbackNoResidue(t, reader, backend, eventID, runID)"},
	}, Calls: 1}
}
