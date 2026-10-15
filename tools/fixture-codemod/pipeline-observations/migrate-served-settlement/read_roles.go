package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

const readRolePredecessor = "3a4a08d41"
const readRoleManifest = "tools/fixture-codemod/pipeline-observations/family145-selected-read-role-preimages.json"

var readRoleNames = map[string]bool{
	"readServedForkDeliveryEvidence": true, "readLifecycleNodeRecoveryDelivery": true,
	"logServedForkHandlerDiagnostics": true, "requireForkReceiverNoPublicationOrNotice": true,
	"readForkLoopActivation": true, "logLifecycleTemplateCompletionRows": true,
	"requireForkReceiverMutationOwners": true, "assertIssue2394ReporterEvents": true,
	"readLifecycleTemplateGate": true, "logServedForkWorkflowRows": true,
	"assertIssue2394TwoFullChunkHistory": true, "assertIssue2394OneSecondPayloads": true,
	"readForkHandlerAccumulators": true, "a2PortfolioAdmissionDomainCounts": true,
	"assertNoUnselectedTargetActivation": true, "requireLifecycleTemplateInstance": true,
	"readLifecycleStoredLoop": true, "lifecycleCompetingTimerDue": true,
	"requireOptionalForkReceiverHeaderProgress": true, "assertIssue2394ReporterClients": true,
}

type readRoleScalar struct {
	Port, Type string
	Fields     []string
}

var readRoleScalars = map[string]readRoleScalar{
	"SELECT COUNT(*) FROM mailbox WHERE source_event_id=$1":                                                                     {"CountSourceNoticeStorage", "int", []string{""}},
	"SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)":             {"CountSourceNonDiagnosticEventsStorage", "int", []string{""}},
	"SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2": {"CountSourceRunDeliveryStorage", "int", []string{""}},
	"SELECT COUNT(*) FROM events WHERE event_name NOT LIKE 'platform.%'":                                                        {"CountGlobalBusinessEventsStorage", "int", []string{""}},
	"SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='on_demand'":                                         {"CountRunUnselectedTemplateActivationsStorage", "int", []string{""}},
	"SELECT event_id, CAST(delivery_target_route AS TEXT), status FROM event_deliveries WHERE delivery_id=$1":                   {"ReadDeliveryRecoveryIdentityStorage", "storetest.DeliveryRecoveryIdentityStorage", []string{".EventID", ".Route", ".Status"}},
	"SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2":                                       {"ReadEntityAccumulatorTextStorage", "string", []string{""}},
	"SELECT CAST(accumulator AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2 AND flow_template=$3":                {"ReadTemplateEntityAccumulatorTextStorage", "string", []string{""}},
	"SELECT flow_template FROM flow_instances WHERE run_id=$1 AND instance_path=$2":                                             {"ReadInstanceTemplateStorage", "string", []string{""}},
	"SELECT fire_at FROM timers WHERE run_id = $1 AND entity_id = $2 AND task_type = 'workflow_timer'":                          {"ReadWorkflowTimerDueValueStorage", "any", []string{""}},
	"SELECT updated_at>$3 FROM flow_instances WHERE run_id=$1 AND instance_path=$2":                                             {"ReadInstanceClockAdvancedStorage", "bool", []string{""}},
}

type readRoleList struct {
	Port, Type string
	Fields     []string
}

var readRoleLists = map[string]readRoleList{
	"SELECT instance_path,flow_template,mode,CAST(config AS TEXT),status FROM flow_instances WHERE run_id=$1 ORDER BY instance_path":                           {"ReadServedForkWorkflowDiagnosticsStorage", "storetest.ServedForkWorkflowDiagnosticsStorage", []string{".Path", ".Template", ".Mode", ".Config", ".Status"}},
	"SELECT event_id,COALESCE(CAST(run_id AS TEXT),''),CAST(payload AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at,event_id": {"ReadServedForkRuntimeDiagnosticsStorage", "storetest.ServedForkRuntimeDiagnosticsStorage", []string{".EventID", ".RunID", ".Payload"}},
	"SELECT DISTINCT entity_id FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2":                                                                   {"ReadRunEventMutationOwnersStorage", "string", []string{""}},
	"SELECT delivery_id,event_id,route_identity,subscriber_type,subscriber_id,CAST(delivery_target_route AS TEXT),CAST(delivery_context AS TEXT),CAST(delivery_payload_projection AS TEXT),CAST(connect_execution_claim AS TEXT),CAST(receiver_materialization_plan AS TEXT) FROM event_deliveries WHERE run_id=$1 ORDER BY delivery_id": {"ReadServedDeliveryRouteEvidenceStorage", "storetest.ServedDeliveryRouteEvidenceStorage", []string{".ID", ".EventID", ".Identity", ".Kind", ".Recipient", ".Target", ".Context", ".Projection", ".Claim", ".Initialization"}},
	"SELECT claim_version, COALESCE(outcome,''), COALESCE(reason_code,'') FROM event_delivery_attempts WHERE delivery_id=$1 ORDER BY claim_version":                                                                                                                                                                                      {"ReadDeliveryRecoveryAttemptsStorage", "storetest.DeliveryRecoveryAttemptStorage", []string{".Version", ".Outcome", ".Reason"}},
	"SELECT ordinal,CAST(event_id AS TEXT),outcome_kind FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal":                                                                                                                                                                                                                          {"ReadOrderedRunFanOutOutcomesStorage", "storetest.OrderedFanOutOutcomeStorage", []string{".Ordinal", ".ID", ".Kind"}},
	"SELECT CAST(event_id AS TEXT),ordinal FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'":                                                                                                                                                                                                                           {"ReadCommittedRunFanOutOrdinalsStorage", "storetest.CommittedFanOutOrdinalStorage", []string{".ID", ".Ordinal"}},
	"SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key":                                                                                                                                                                                                      {"ReadFanOutObligationHistoryStorage", "storetest.FanOutObligationHistoryStorage", []string{".Key", ".Fact"}},
}

func migrateReadRoles(write bool) {
	previousChanges := changes
	changes = map[string]servedsettlement.Change{}
	for name := range readRoleNames {
		changes[name] = servedsettlement.Change{}
	}
	defer func() { changes = previousChanges }()
	root, err := os.Getwd()
	must(err)
	originals, err := collectSignedSources(root, readRolePredecessor)
	must(err)
	// The original source is type-checked before any syntactic query matching.
	originalPackages := load(root, originals)
	prepared := map[string][]byte{}
	authority := readRoleAuthority(originalPackages, root)
	for path, raw := range originals {
		prepared[path] = readRoleOwnerCandidate(path, raw, authority)
	}
	pkgs := load(root, prepared)
	blocked := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && !readRoleNames[fn.Name.Name] {
					blocked[fn.Name.Name] = true
				}
			}
		}
	}
	roles := discoverServedRoles(pkgs, blocked)
	if len(roles) != len(readRoleNames) {
		panic(fmt.Sprintf("selected read-role owner closure incomplete: %d/%d", len(roles), len(readRoleNames)))
	}
	uses := typedRoleCalls(root, pkgs, roles)
	if unresolved := untypedServedRoleReferences(prepared, roles, uses); len(unresolved) != 0 {
		panic(fmt.Sprintf("selected read-role excluded or indirect consumers: %v", unresolved))
	}
	outputs, _ := servedRoleCandidatesAt(prepared, roles, uses, nil, func(path string, pins []runphysical.Pin) ([]byte, error) {
		return destinationSource(path, prepared[path], pins)
	})
	for path, raw := range prepared {
		if string(raw) != string(originals[path]) && outputs[path] == nil {
			outputs[path] = raw
		}
	}
	pins := readRoleFinalPins(originals, outputs)
	validateCandidate(root, outputs)
	encoded, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	encoded = append(encoded, '\n')
	fmt.Printf("read-role declarations=%d closed-helpers=%d files=%d sha256=%x\n", len(pins), len(roles), len(outputs), sha256.Sum256(encoded))
	if write {
		for path, raw := range outputs {
			rows := []runphysical.Pin{}
			for _, pin := range pins {
				if pin.File == path {
					rows = append(rows, pin)
				}
			}
			_, err := destinationCandidate(path, rows)
			must(err)
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(readRoleManifest, encoded, 0644))
	}
}

func readRoleOwnerCandidate(path string, raw []byte, authority map[string]bool) []byte {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	must(err)
	var edits []servedsettlement.Splice
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (!readRoleNames[fn.Name.Name] && fn.Name.Name != "TestRunServeRuntimeDevScratchRunForkLifecycleSQLite") {
			continue
		}
		if !authority[readRoleBodyKey(fn)] {
			panic("selected read-role complete signed input changed: " + path + "/" + fn.Name.Name)
		}
		if after, ok := readRoleSpecialOwner(fn); ok {
			edits = append(edits, servedsettlement.Splice{Start: set.Position(fn.Pos()).Offset, End: set.Position(fn.End()).Offset, After: after})
			continue
		}
		edits = append(edits, readRoleDependencyEdits(set, fn)...)
		edits = append(edits, readRoleListEdits(path, set, fn, authority)...)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			after, ok := readRoleScalarCall(call)
			if ok {
				query := call.Fun.(*ast.SelectorExpr).X.(*ast.CallExpr)
				if !authority[fmt.Sprintf("%s:%d", path, set.Position(query.Pos()).Offset)] {
					panic("selected read-role query is not its original typed field")
				}
				edits = append(edits, servedsettlement.Splice{Start: set.Position(call.Pos()).Offset, End: set.Position(call.End()).Offset, After: after})
				return false
			}
			return true
		})
	}
	if len(edits) == 0 {
		return raw
	}
	out, err := imports.Process(path, apply(raw, edits), nil)
	must(err)
	return out
}

func readRoleListEdits(path string, set *token.FileSet, fn *ast.FuncDecl, authority map[string]bool) []servedsettlement.Splice {
	var edits []servedsettlement.Splice
	bindings := map[string]readRoleList{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) != 2 {
			return true
		}
		query, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok || len(query.Args) == 0 {
			return true
		}
		method, ok := query.Fun.(*ast.SelectorExpr)
		if !ok || requestkey.Node(method.X) != "rt.DB" || method.Sel.Name != "Query" {
			return true
		}
		literal, ok := query.Args[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		sql, err := strconv.Unquote(literal.Value)
		must(err)
		rule, ok := readRoleLists[sql]
		if !ok {
			return true
		}
		if !authority[fmt.Sprintf("%s:%d", path, set.Position(query.Pos()).Offset)] {
			panic("selected read-role row query is not its original typed field")
		}
		name, ok := assignment.Lhs[0].(*ast.Ident)
		if !ok || bindings[name.Name].Port != "" || query.Ellipsis.IsValid() {
			panic("ambiguous selected read-role row binding")
		}
		bindings[name.Name] = rule
		args := []string{"ctx", "rt.ReceiverStateReader"}
		for _, arg := range query.Args[1:] {
			args = append(args, requestkey.Node(arg))
		}
		resultType := "[]" + rule.Type
		if partialReadRole(rule.Port) {
			resultType = rule.Type
		}
		after := "readServedSelectedRole(context.Background(),rt.ReceiverStateReader,func(ctx context.Context)(" + resultType + ",error){return storetest." + rule.Port + "(" + strings.Join(args, ",") + ")})"
		edits = append(edits, servedsettlement.Splice{Start: set.Position(query.Pos()).Offset, End: set.Position(query.End()).Offset, After: after})
		return true
	})
	if len(bindings) == 0 {
		return nil
	}
	return append(edits, readRoleRowConsumerEdits(path, set, fn, authority, bindings)...)
}

func readRoleRowConsumerEdits(path string, set *token.FileSet, fn *ast.FuncDecl, authority map[string]bool, bindings map[string]readRoleList) []servedsettlement.Splice {
	var edits []servedsettlement.Splice
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if loop, ok := node.(*ast.ForStmt); ok {
			edits = append(edits, readRoleLoopEdits(path, set, loop, authority, bindings)...)
		}
		if removed, ok := readRoleRowTransport(node, bindings); ok {
			if !readRoleTransportAuthority(path, set, node, authority) {
				panic("selected read-role transport uses a shadowed row")
			}
			edits = append(edits, servedsettlement.Splice{Start: set.Position(node.Pos()).Offset, End: set.Position(node.End()).Offset, After: removed})
			return false
		}
		return true
	})
	return edits
}

func readRoleLoopEdits(path string, set *token.FileSet, loop *ast.ForStmt, authority map[string]bool, bindings map[string]readRoleList) []servedsettlement.Splice {
	call, ok := loop.Cond.(*ast.CallExpr)
	if !ok {
		return nil
	}
	method, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || method.Sel.Name != "Next" {
		return nil
	}
	rows := requestkey.Node(method.X)
	rule, ok := bindings[rows]
	if !ok {
		return nil
	}
	if !authority[fmt.Sprintf("%s:%d", path, set.Position(call.Pos()).Offset)] {
		panic("selected read-role iteration uses a shadowed row")
	}
	if loop.Init != nil || loop.Post != nil || len(call.Args) != 0 {
		panic("selected read-role iteration cut changed")
	}
	var edits []servedsettlement.Splice
	collection := rows
	if partialReadRole(rule.Port) {
		collection += ".Rows"
		preserved := readRolePartialScanGate(loop, bindings, rows)
		edits = append(edits, servedsettlement.Splice{Start: set.Position(loop.End()).Offset - 1, End: set.Position(loop.End()).Offset, After: "};" + preserved + ";"})
	}
	return append(edits, servedsettlement.Splice{Start: set.Position(loop.Pos()).Offset, End: set.Position(loop.Body.Pos()).Offset, After: "for _, selectedReadRow := range " + collection + " "})
}

func readRolePartialScanGate(loop *ast.ForStmt, bindings map[string]readRoleList, rows string) string {
	var preserved string
	for _, statement := range loop.Body.List {
		gate, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		init, ok := gate.Init.(*ast.AssignStmt)
		if !ok || len(init.Rhs) != 1 {
			continue
		}
		call, ok := init.Rhs[0].(*ast.CallExpr)
		if !ok || !selectedRowsMethodWithArguments(call, bindings, "Scan") {
			continue
		}
		if preserved != "" {
			panic("selected read-role partial diagnostic has multiple scan gates")
		}
		preserved = strings.Replace(requestkey.Node(gate), requestkey.Node(call), rows+".ScanError", 1)
	}
	if preserved == "" {
		panic("selected read-role partial diagnostic lost its scan gate")
	}
	return preserved
}

func readRoleTransportAuthority(path string, set *token.FileSet, node ast.Node, authority map[string]bool) bool {
	var call *ast.CallExpr
	switch statement := node.(type) {
	case *ast.DeferStmt:
		call = statement.Call
	case *ast.ExprStmt:
		call, _ = statement.X.(*ast.CallExpr)
	case *ast.IfStmt:
		assignment, _ := statement.Init.(*ast.AssignStmt)
		if assignment != nil && len(assignment.Rhs) == 1 {
			call, _ = assignment.Rhs[0].(*ast.CallExpr)
		}
	}
	return call != nil && authority[fmt.Sprintf("%s:%d", path, set.Position(call.Pos()).Offset)]
}

func readRoleAuthority(pkgs []*packages.Package, root string) map[string]bool {
	result := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || (!readRoleNames[fn.Name.Name] && fn.Name.Name != "TestRunServeRuntimeDevScratchRunForkLifecycleSQLite") {
					continue
				}
				readRoleFunctionAuthority(pkg, root, fn, result)
			}
		}
	}
	return result
}

func readRoleFunctionAuthority(pkg *packages.Package, root string, fn *ast.FuncDecl, result map[string]bool) {
	result[readRoleBodyKey(fn)] = true
	rows := map[types.Object]bool{}
	key := func(pos token.Pos) string {
		p := pkg.Fset.Position(pos)
		path, err := filepath.Rel(root, p.Filename)
		must(err)
		return fmt.Sprintf("%s:%d", filepath.ToSlash(path), p.Offset)
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && readRoleTypedDBCall(pkg, call) {
			result[key(call.Pos())] = true
		}
		if assignment, ok := node.(*ast.AssignStmt); ok {
			if object := readRoleRowObject(pkg, assignment); object != nil {
				rows[object] = true
			}
		}
		return true
	})
	// The second traversal sees all query bindings, independent of syntax order.
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := selector.X.(*ast.Ident)
		if ok && rows[pkg.TypesInfo.Uses[id]] {
			result[key(call.Pos())] = true
		}
		return true
	})
}

func readRoleBodyKey(fn *ast.FuncDecl) string {
	body, err := requestkey.Normalize(requestkey.Node(fn))
	must(err)
	return fmt.Sprintf("body/%s/%x", fn.Name.Name, sha256.Sum256([]byte(body)))
}

func readRoleRowObject(pkg *packages.Package, assignment *ast.AssignStmt) types.Object {
	if len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
		return nil
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok || !readRoleTypedDBCall(pkg, call) {
		return nil
	}
	id, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return nil
	}
	if object := pkg.TypesInfo.Defs[id]; object != nil {
		return object
	}
	return pkg.TypesInfo.Uses[id]
}

func readRoleTypedDBCall(pkg *packages.Package, call *ast.CallExpr) bool {
	method, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	field, ok := method.X.(*ast.SelectorExpr)
	return ok && field.Sel.Name == "DB" && isServedRoleType(pkg.TypesInfo.TypeOf(field.X)) && types.TypeString(pkg.TypesInfo.TypeOf(field), nil) == "*database/sql.DB"
}

func readRoleRowTransport(node ast.Node, bindings map[string]readRoleList) (string, bool) {
	if statement, ok := node.(*ast.DeferStmt); ok && selectedRowsMethod(statement.Call, bindings, "Close") {
		return "", true
	}
	if statement, ok := node.(*ast.ExprStmt); ok {
		if call, ok := statement.X.(*ast.CallExpr); ok && selectedRowsMethod(call, bindings, "Close") {
			return "", true
		}
	}
	statement, ok := node.(*ast.IfStmt)
	if !ok {
		return "", false
	}
	init, ok := statement.Init.(*ast.AssignStmt)
	if !ok || len(init.Rhs) != 1 {
		return "", false
	}
	call, ok := init.Rhs[0].(*ast.CallExpr)
	if !ok {
		return "", false
	}
	if selectedRowsMethod(call, bindings, "Err") {
		rule := bindings[requestkey.Node(call.Fun.(*ast.SelectorExpr).X)]
		if partialReadRole(rule.Port) {
			return strings.Replace(requestkey.Node(statement), requestkey.Node(call), requestkey.Node(call.Fun.(*ast.SelectorExpr).X)+".IterationError", 1), true
		}
		validateReadRoleFatalGate(statement)
		return "", true
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Scan" {
		return "", false
	}
	rule, ok := bindings[requestkey.Node(selector.X)]
	if !ok {
		return "", false
	}
	if len(call.Args) != len(rule.Fields) || call.Ellipsis.IsValid() {
		panic("selected read-role scan destinations changed")
	}
	// Transport-only fatal checks move to the fixed owner's complete read. No
	// business assertion, conditional, or error-recovery action may be removed.
	if !partialReadRole(rule.Port) {
		validateReadRoleFatalGate(statement)
	}
	var out string
	for i, arg := range call.Args {
		pointer, ok := arg.(*ast.UnaryExpr)
		if !ok || pointer.Op != token.AND {
			panic("selected read-role scan has a foreign destination")
		}
		out += requestkey.Node(pointer.X) + "=selectedReadRow" + rule.Fields[i] + ";"
	}
	return out, true
}

func partialReadRole(port string) bool {
	return port == "ReadServedForkWorkflowDiagnosticsStorage" || port == "ReadServedForkRuntimeDiagnosticsStorage"
}

func selectedRowsMethodWithArguments(call *ast.CallExpr, bindings map[string]readRoleList, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	_, ok = bindings[requestkey.Node(selector.X)]
	return ok
}

func readRoleDependencyEdits(set *token.FileSet, fn *ast.FuncDecl) []servedsettlement.Splice {
	var edits []servedsettlement.Splice
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || len(call.Args) < 2 {
			return true
		}
		name := ""
		if fn.Name.Name == "a2PortfolioAdmissionDomainCounts" && id.Name == "semanticNumericDomainCounts" {
			name = "readServedA2DomainCounts"
		}
		if fn.Name.Name == "assertIssue2394ReporterClients" && id.Name == "issue2394SurfaceSnapshot" {
			name = "readServedReporterSnapshot"
		}
		if name == "" {
			return true
		}
		if requestkey.Node(call.Args[1]) != "rt.DB" {
			panic("selected read-role dependency lost its exact raw field")
		}
		args := []string{requestkey.Node(call.Args[0]), "rt.ReceiverStateReader"}
		for _, arg := range call.Args[2:] {
			args = append(args, requestkey.Node(arg))
		}
		edits = append(edits, servedsettlement.Splice{Start: set.Position(call.Pos()).Offset, End: set.Position(call.End()).Offset, After: name + "(" + strings.Join(args, ",") + ")"})
		return false
	})
	return edits
}

func readRoleSpecialOwner(fn *ast.FuncDecl) (string, bool) {
	if fn.Name.Name == "TestRunServeRuntimeDevScratchRunForkLifecycleSQLite" {
		out := requestkey.Node(fn)
		for _, pair := range [][2]string{
			{"var deliveryReader servedRunDeliveryReader", "var reader receiverProofStateReader; var deliveryReader servedRunDeliveryReader"},
			{"deliveryReader = p.deps.DeliveryStore.SummarizeRun", "deliveryReader = p.deps.DeliveryStore.SummarizeRun;reader=requireReceiverProofStateReader(t,p.deps.EventStore)"},
			{"servedControlProofRuntime{DB: db}", "servedControlProofRuntime{ReceiverStateReader:reader}"},
		} {
			if strings.Count(out, pair[0]) != 1 {
				panic("selected dev-scratch inspection ingress changed")
			}
			out = strings.Replace(out, pair[0], pair[1], 1)
		}
		return out, true
	}
	if fn.Name.Name != "logLifecycleTemplateCompletionRows" {
		return "", false
	}
	return strings.Replace(requestkey.Node(fn.Type), "func(", "func "+fn.Name.Name+"(", 1) + ` {
t.Helper()
queries:=[]struct{label string;read func(context.Context,any,string)(storetest.ServedTemplateCompletionDiagnosticStorage,error)}{
{"events",storetest.ReadTemplateCompletionEventDiagnosticsStorage},
{"deliveries",storetest.ReadTemplateCompletionDeliveryDiagnosticsStorage},
{"receipts",storetest.ReadTemplateCompletionReceiptDiagnosticsStorage},
}
for _,q:=range queries{
observed,err:=readServedSelectedRole(context.Background(),rt.ReceiverStateReader,func(ctx context.Context)(storetest.ServedTemplateCompletionDiagnosticStorage,error){return q.read(ctx,rt.ReceiverStateReader,runID)})
if err!=nil{t.Logf("TEMPLATE_COMPLETION %s query error: %v",q.label,err);continue}
columns:=observed.Columns
if err:=observed.ColumnsError;err!=nil{t.Fatal(err)}
count:=0
for _,values:=range observed.Rows{
t.Logf("TEMPLATE_COMPLETION run=%s table=%s columns=%v row=%v",runID,q.label,columns,values)
count++
}
if err:=observed.ScanError;err!=nil{t.Fatal(err)}
if err:=observed.IterationError;err!=nil{t.Fatal(err)}
t.Logf("TEMPLATE_COMPLETION run=%s table=%s count=%d",runID,q.label,count)
}
}`, true
}

func validateReadRoleFatalGate(statement *ast.IfStmt) {
	if requestkey.Node(statement.Cond) != "err != nil" || statement.Else != nil || len(statement.Body.List) != 1 || requestkey.Node(statement.Body.List[0]) != "t.Fatal(err)" {
		panic("selected read-role scan/iteration handling is not the exact fatal transport gate")
	}
}

func selectedRowsMethod(call *ast.CallExpr, bindings map[string]readRoleList, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name || len(call.Args) != 0 {
		return false
	}
	_, ok = bindings[requestkey.Node(selector.X)]
	return ok
}

func readRoleScalarCall(call *ast.CallExpr) (string, bool) {
	scan, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || scan.Sel.Name != "Scan" {
		return "", false
	}
	query, ok := scan.X.(*ast.CallExpr)
	if !ok || len(query.Args) == 0 {
		return "", false
	}
	method, ok := query.Fun.(*ast.SelectorExpr)
	if !ok || (method.Sel.Name != "QueryRow" && method.Sel.Name != "QueryRowContext") || requestkey.Node(method.X) != "rt.DB" {
		return "", false
	}
	index, ctx := 0, "context.Background()"
	if method.Sel.Name == "QueryRowContext" {
		index, ctx = 1, requestkey.Node(query.Args[0])
	}
	if len(query.Args) <= index {
		return "", false
	}
	literal, ok := query.Args[index].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	sql, err := strconv.Unquote(literal.Value)
	must(err)
	rule, ok := readRoleScalars[sql]
	if !ok {
		return "", false
	}
	if len(rule.Fields) != len(call.Args) || call.Ellipsis.IsValid() || query.Ellipsis.IsValid() {
		panic("selected read-role scan column/evaluation inventory changed")
	}
	args := []string{"ctx", "rt.ReceiverStateReader"}
	for _, arg := range query.Args[index+1:] {
		args = append(args, requestkey.Node(arg))
	}
	after := "func()error{observed,err:=readServedSelectedRole(" + ctx + ",rt.ReceiverStateReader,func(ctx context.Context)(" + rule.Type + ",error){return storetest." + rule.Port + "(" + strings.Join(args, ",") + ")});"
	for i, arg := range call.Args {
		pointer, ok := arg.(*ast.UnaryExpr)
		if !ok || pointer.Op != token.AND {
			panic("selected read-role destination is not an exact address")
		}
		after += requestkey.Node(pointer.X) + "=observed" + rule.Fields[i] + ";"
	}
	return after + "return err}()", true
}

func readRoleFinalPins(originals, outputs map[string][]byte) []runphysical.Pin {
	var paths []string
	for path := range outputs {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	var pins []runphysical.Pin
	for _, path := range paths {
		before, err := parser.ParseFile(token.NewFileSet(), path, originals[path], parser.AllErrors)
		must(err)
		after, err := parser.ParseFile(token.NewFileSet(), path, outputs[path], parser.AllErrors)
		must(err)
		for _, decl := range before.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			next, err := uniqueDestination(after, fn.Name.Name)
			must(err)
			a, b := requestkey.Node(fn), requestkey.Node(next)
			if a != b {
				pins = append(pins, runphysical.Pin{File: path, Function: fn.Name.Name, Before: a, After: b, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(a))), Calls: 1})
			}
		}
	}
	return pins
}
