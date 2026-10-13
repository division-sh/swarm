package channelanchor

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

var Helpers = map[string]int{
	"waitChannelAnchorCard": 5, "waitChannelAnchorCardInState": 6,
	"waitChannelAnchorReceipt": 3, "waitChannelAnchorDecision": 3,
	"waitChannelCardMessageID": 4, "loadServedStandingOwnerByFlow": 4,
}

// Only these complete predecessor declarations have reviewed reader provenance.
var Readers = map[string]string{
	"TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney":      "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryBudgetNoticePublicJourney":                        "channelAnchorStorageObservation(t, h)",
	"TestChannelDraftTerminalRestartPublicJourney":                        "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryAbruptProcessDeathPublicJourney":                  "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryRecoveryNoticePublicJourney":                      "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryCompiledRenderFailureDoesNotStarveReceiptUpdates": "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliverySettlementDiagnosticBothStores":                   "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryHumanTemporalReceiptPublicJourney":                "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryUncertainCopyAuthorityPublicJourney":              "channelAnchorStorageObservation(t, h)",
	"TestChannelLearnedObjectRealAnchorProducersPublicJourney":            "channelAnchorStorageObservation(t, h)",
	"beginObjectInputCard":                                                "channelAnchorStorageObservation(t, h)",
	"createObjectUncertainCards":                                          "channelAnchorStorageObservation(t, h)",
	"TestChannelLearnedObjectPublicJourney":                               "channelAnchorStorageObservation(t, h)",
	"proveObjectChannelRecovery":                                          "channelAnchorStorageObservation(t, h)",
	"TestChannelLearnedObjectHumanTemporalReceiptPublicJourney":           "channelAnchorStorageObservation(t, h)",
	"runChannelOnboardingCrashBoundaryE2E":                                "channelAnchorStorageObservation(t, harness)",
	"TestChannelOneBotManyConversationsPublicJourney":                     "channelAnchorStorageObservation(t, h)",
	"TestChannelSourceLifecyclePublicJourney":                             "channelAnchorStorageObservation(t, h)",
	"runChannelDeliveryInboundDispositionE2E":                             "anchorReader",
	"TestStandingRestartMixedHealthyAndTerminalProcessParity":             "anchorReader",
}

type edit struct {
	start, end int
	value      string
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid channel-anchor declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
	}
	changes, err := callEdits(fset, fn)
	if err != nil {
		return "", 0, err
	}
	slices.SortFunc(changes, func(a, b edit) int { return b.start - a.start })
	after := before
	for _, change := range changes {
		after = after[:change.start] + change.value + after[change.end:]
	}
	if Helpers[fn.Name.Name] != 0 {
		after = strings.Replace(after, "db *sql.DB", "reader storetest.ChannelAnchorObservation", 1)
	}
	after, err = nativeBody(fn.Name.Name, after)
	if err != nil {
		return "", 0, err
	}
	after, err = requestkey.Normalize(after)
	return after, len(changes), err
}

func callEdits(fset *token.FileSet, fn *ast.FuncDecl) ([]edit, error) {
	var changes []edit
	var failure error
	start := fset.Position(fn.Pos()).Offset
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || Helpers[id.Name] == 0 {
			return true
		}
		value, err := transformedCall(fn, call, id)
		if err != nil {
			failure = err
			return false
		}
		changes = append(changes, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, value})
		return true
	})
	return changes, failure
}

func transformedCall(fn *ast.FuncDecl, call *ast.CallExpr, id *ast.Ident) (string, error) {
	if len(call.Args) != Helpers[id.Name] || call.Ellipsis.IsValid() || requestkey.Node(call.Args[0]) != "t" ||
		requestkey.Node(call.Args[1]) != "db" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified channel-anchor call: %s", fn.Name.Name)
	}
	reader := Readers[fn.Name.Name]
	if Helpers[fn.Name.Name] != 0 {
		reader = "reader"
	}
	if reader == "" {
		return "", fmt.Errorf("channel-anchor caller lacks exact reader provenance: %s", fn.Name.Name)
	}
	var args []string
	for _, arg := range call.Args {
		args = append(args, requestkey.Node(arg))
	}
	args[1] = reader
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}

func replaceExact(source, before, after string) (string, error) {
	if strings.Count(source, before) != 1 {
		return "", fmt.Errorf("channel-anchor source escaped exact rewrite cut: %s", before)
	}
	return strings.Replace(source, before, after, 1), nil
}

func nativeBody(function, source string) (string, error) {
	switch function {
	case "waitChannelAnchorCardInState":
		return cardBody(source)
	case "waitChannelAnchorReceipt":
		return replaceExact(source, "var raw string\n\t\terr := db.QueryRow(`SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r\n\t\t\tJOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id\n\t\t\tWHERE p.source_id=$1 AND r.state='sent' ORDER BY r.settled_at LIMIT 1`, cardID).Scan(&raw)",
			"raw, err := reader.ReadFirstSentSourceReceiptStorage(context.Background(), cardID)")
	case "waitChannelAnchorDecision":
		return replaceExact(source, "var status, verdict string\n\t\tif err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, cardID).Scan(&status, &verdict); err != nil {\n\t\t\tt.Fatal(err)\n\t\t}",
			"state, err := reader.ReadChannelAnchorDecisionStorage(context.Background(), cardID)\n\t\tif err != nil { t.Fatal(err) }\n\t\tstatus, verdict := state.Status, state.Verdict")
	case "loadServedStandingOwnerByFlow":
		return replaceExact(source, "query := `SELECT service_id, current_run_id, current_generation FROM standing_services WHERE flow_path = ?`\n\targs := []any{flowID}\n\tif backend == \"postgres\" {\n\t\tquery = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services WHERE flow_path = $1`\n\t}\n\tif err := db.QueryRowContext(context.Background(), query, args...).Scan(&serviceID, &runID, &generation); err != nil {",
			"owner, err := reader.ReadStandingFlowStorage(context.Background(), flowID)\n\tserviceID, runID, generation = owner.ServiceID, owner.RunID, owner.Generation\n\tif err != nil {")
	case "runChannelDeliveryInboundDispositionE2E":
		return replaceExact(source, "cardMessages[\"telegram-ingress\"] = waitChannelCardMessageID", "anchorReader := openChannelAnchorStorageAtLocation(t, backend, observerDSN)\ncardMessages[\"telegram-ingress\"] = waitChannelCardMessageID")
	case "TestStandingRestartMixedHealthyAndTerminalProcessParity":
		return replaceExact(source, "healthyService, healthyRun, healthyGeneration := loadServedStandingOwnerByFlow", "anchorReader := openChannelAnchorStorageAtLocation(t, backend, strings.TrimPrefix(selectedStore, \"postgres:\"))\nhealthyService, healthyRun, healthyGeneration := loadServedStandingOwnerByFlow")
	case "TestChannelDeliverySettlementDiagnosticBothStores":
		return replaceExact(source, "_, db, _ := startChannelAnchorJourney", "h, db, _ := startChannelAnchorJourney")
	}
	return source, nil
}

func cardBody(source string) (string, error) {
	var err error
	for _, change := range [][2]string{
		{"rows, err := db.Query(`SELECT card_id,CAST(anchor AS TEXT) FROM decision_cards WHERE run_id=$1 AND anchor_kind=$2 AND ($3='' OR status=$3)`, runID, string(kind), status)", "rows, err := reader.ReadChannelAnchorCardsStorage(context.Background(), runID, string(kind), status)"},
		{"\t\tdefer rows.Close()\n", ""},
		{"for rows.Next() {\n\t\t\tvar candidate, raw string\n\t\t\tif err := rows.Scan(&candidate, &raw); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}", "for _, row := range rows {\n\t\t\tcandidate, raw := row.CardID, row.AnchorJSON"},
		{"\t\terr = rows.Err()\n\t\trows.Close()\n\t\tif err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n", ""},
	} {
		source, err = replaceExact(source, change[0], change[1])
		if err != nil {
			return "", err
		}
	}
	return source, nil
}
