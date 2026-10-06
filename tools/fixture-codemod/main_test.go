package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestNotifyItemMetadataOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for helper, count := range map[string]int{"loadNotifyAllChildrenItemEvents": 6, "assertNotifyAllChildrenMetadata": 7, "dumpNotifyAllChildrenRuntimeState": 4} {
		for _, mutation := range []string{"none", "foreign", "shadow", "unknown-helper", "wrong-arity", "effectful-db", "already-migrated"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(conformancePackage, "conformance")
				if mutation == "foreign" {
					pkg = types.NewPackage("example/other", "other")
				}
				sqlpkg := types.NewPackage("database/sql", "sql")
				db := types.NewPointer(types.NewNamed(types.NewTypeName(0, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
				params := []*types.Var{}
				args := []ast.Expr{}
				for i := 0; i < count; i++ {
					typ := types.Type(types.Typ[types.Int])
					if i == 3 {
						typ = db
					}
					params = append(params, types.NewVar(0, pkg, "", typ))
					args = append(args, ast.NewIdent("arg"))
				}
				if mutation == "already-migrated" {
					params = append(params[:3], params[4:]...)
				}
				if mutation == "wrong-arity" {
					args = args[:len(args)-1]
				}
				if mutation == "effectful-db" {
					args[3] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
				}
				name := ast.NewIdent(helper)
				if mutation == "unknown-helper" {
					name.Name = "unknown"
				}
				obj := types.Object(types.NewFunc(0, pkg, name.Name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false)))
				if mutation == "shadow" {
					obj = types.NewVar(0, pkg, name.Name, db)
				}
				_, ok := matchNotifyObserverOwner(&types.Info{Uses: map[*ast.Ident]types.Object{name: obj}}, &ast.CallExpr{Fun: name, Args: args})
				if ok != (mutation == "none") {
					t.Fatalf("match=%v", ok)
				}
			})
		}
	}
}

func TestNotifyRuntimeOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for _, mutation := range []string{"none", "foreign", "shadow", "wrong-helper", "wrong-arity", "effectful-db", "already-migrated", "nonvariadic"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(conformancePackage, "conformance")
			if mutation == "foreign" {
				pkg = types.NewPackage("example/other", "other")
			}
			sqlpkg := types.NewPackage("database/sql", "sql")
			db := types.NewPointer(types.NewNamed(types.NewTypeName(0, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
			typesForArgs := []types.Type{types.Typ[types.Int], types.Typ[types.Int], db, types.Typ[types.Int], types.Typ[types.Int], types.NewSlice(types.Typ[types.Int])}
			args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("selected"), ast.NewIdent("db"), ast.NewIdent("source"), ast.NewIdent("now")}
			if mutation == "already-migrated" {
				typesForArgs = append(typesForArgs[:2], typesForArgs[3:]...)
			}
			if mutation == "wrong-arity" {
				args = args[:4]
			}
			if mutation == "effectful-db" {
				args[2] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
			}
			name := ast.NewIdent("newNotifyAllChildrenRuntime")
			if mutation == "wrong-helper" {
				name.Name = "anotherRuntime"
			}
			params := []*types.Var{}
			for _, typ := range typesForArgs {
				params = append(params, types.NewVar(0, pkg, "", typ))
			}
			obj := types.Object(types.NewFunc(0, pkg, name.Name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, mutation != "nonvariadic")))
			if mutation == "shadow" {
				obj = types.NewVar(0, pkg, name.Name, db)
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{name: obj}}
			_, ok := matchNotifyRuntimeOwner(info, &ast.CallExpr{Fun: name, Args: args})
			if ok != (mutation == "none") {
				t.Fatalf("match=%v", ok)
			}
		})
	}
}

func TestRuntimeNodeDeliveryCountRewriteRequiresExactOracleAndOriginalOwner(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-package", "wrong-helper", "dynamic-query", "extra-predicate", "changed-node-literal", "effectful-db", "effectful-want", "missing-owner", "wrong-owner", "wrong-arity", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(externalRuntimePackage, "runtime_test")
			sqlpkg := types.NewPackage("database/sql", "sql")
			db := types.NewPointer(types.NewNamed(types.NewTypeName(0, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
			ownerpkg := types.NewPackage("github.com/division-sh/swarm/internal/store/internal/runtimepersistence", "runtimepersistence")
			ownername := "PostgresStore"
			if mutation == "wrong-owner" {
				ownername = "lookalike"
			}
			owner := types.NewPointer(types.NewNamed(types.NewTypeName(0, ownerpkg, ownername, nil), types.NewStruct(nil, nil), nil))
			params := []*types.Var{
				types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "ctx", types.Typ[types.Int]),
				types.NewVar(0, pkg, "db", db), types.NewVar(0, pkg, "query", types.Typ[types.String]),
				types.NewVar(0, pkg, "want", types.Typ[types.Int]), types.NewVar(0, pkg, "args", types.NewSlice(types.NewInterfaceType(nil, nil).Complete())),
			}
			query := &ast.BasicLit{Kind: token.STRING, Value: "`SELECT COUNT(*) FROM event_deliveries\n WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`"}
			args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("ctx"), ast.NewIdent("db"), query, &ast.BasicLit{Kind: token.INT, Value: "1"}, ast.NewIdent("eventID"), ast.NewIdent("subscriberID")}
			helper := "assertRuntimeDBCount"
			switch mutation {
			case "wrong-package":
				pkg = types.NewPackage("example/other", "other")
			case "wrong-helper":
				helper = "unknown"
			case "dynamic-query":
				args[3] = ast.NewIdent("query")
			case "extra-predicate":
				query.Value = "`SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2 AND status='delivered'`"
			case "changed-node-literal":
				query.Value = "`SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'NODE' AND subscriber_id = $2`"
			case "effectful-db":
				args[2] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
			case "effectful-want":
				args[4] = &ast.CallExpr{Fun: ast.NewIdent("nextCount")}
			case "wrong-arity":
				args = args[:6]
			}
			name := &ast.Ident{Name: helper, NamePos: 10}
			scope := types.NewScope(nil, 1, 100, "caller")
			if mutation != "missing-owner" {
				scope.Insert(types.NewVar(1, pkg, "pg", owner))
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{name: types.NewFunc(0, pkg, helper, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, true))}, Scopes: map[ast.Node]*types.Scope{&ast.BlockStmt{}: scope}}
			if mutation == "shadow" {
				info.Uses[name] = types.NewVar(0, pkg, helper, db)
			}
			got, ok := matchRuntimeNodeDeliveryCount(info, &ast.CallExpr{Fun: name, Args: args})
			if ok != (mutation == "none") || ok && got != "pg" {
				t.Fatalf("match=%t owner=%s", ok, got)
			}
		})
	}
}

func TestChannelDispositionOwnerPropagationIsFiniteAndIdempotent(t *testing.T) {
	testChannelOwnerPropagation(t, []string{"waitObjectChannelDisposition", "proveObjectRejectedControl"})
}

func TestServedTransportProjectionRequiresExactResolvedFixture(t *testing.T) {
	testServedCapabilityProjection(t, []string{"issue2394SurfaceCLI", "issue2394ReporterRPC"})
}

func TestPendingInputStateCountProjectionRequiresOriginalFixture(t *testing.T) {
	testServedCapabilityProjection(t, []string{"requirePendingInputStateCount"})
}

func TestServedNodeLifecycleRemovalPreservesTypedTemporalControls(t *testing.T) {
	for _, helper := range []string{"requireServedEventPublishPreHandlerProof", "waitForServedEventPublishNodeDeliveryLifecycleForNode", "waitForServedEventPublishNodeDeliveryLifecycle"} {
		for _, mutation := range []string{"none", "wrong-temporal-type", "effectful-db", "raw-owner", "foreign", "wrong-arity", "already-migrated"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg, sqlpkg := types.NewPackage(serveappPackage, "serveapp"), types.NewPackage("database/sql", "sql")
				db := types.NewPointer(types.NewNamed(types.NewTypeName(0, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
				params := []*types.Var{types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "db", db), types.NewVar(0, pkg, "selected", types.NewInterfaceType(nil, nil).Complete())}
				args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("db"), ast.NewIdent("selected")}
				tail := []string{"backend", "runID", "eventID", "nodeID", "probe"}
				if helper == "requireServedEventPublishPreHandlerProof" {
					tail = []string{"backend", "proofs", "runID", "eventID", "nodeID"}
				}
				if helper == "waitForServedEventPublishNodeDeliveryLifecycle" {
					tail = []string{"backend", "runID", "eventID", "probe"}
				}
				for _, label := range tail {
					kind := types.Type(types.Typ[types.String])
					if label == "proofs" {
						kind = types.NewChan(types.RecvOnly, types.NewNamed(types.NewTypeName(0, pkg, "servedEventPublishPreHandlerProof", nil), types.NewStruct(nil, nil), nil))
					}
					if label == "probe" {
						ownerpkg := types.NewPackage("github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest", "lifecycletest")
						kind = types.NewPointer(types.NewNamed(types.NewTypeName(0, ownerpkg, "Probe", nil), types.NewStruct(nil, nil), nil))
					}
					if mutation == "wrong-temporal-type" && (label == "proofs" || label == "probe") {
						kind = types.Typ[types.Int]
					}
					params = append(params, types.NewVar(0, pkg, label, kind))
					args = append(args, ast.NewIdent(label))
				}
				switch mutation {
				case "effectful-db":
					args[1] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
				case "raw-owner":
					params[2] = types.NewVar(0, pkg, "selected", db)
				case "foreign":
					pkg = types.NewPackage("example/other", "other")
				case "wrong-arity":
					args = args[:len(args)-1]
				case "already-migrated":
					params = append(params[:1], params[2:]...)
				}
				fun := ast.NewIdent(helper)
				info := &types.Info{Uses: map[*ast.Ident]types.Object{fun: types.NewFunc(0, pkg, helper, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
				got, ok := matchServedEntityStateOwner(info, &ast.CallExpr{Fun: fun, Args: args})
				if ok != (mutation == "none") || ok && got != helper {
					t.Fatalf("match=%t helper=%s", ok, got)
				}
			})
		}
	}
}

func testServedCapabilityProjection(t *testing.T, helpers []string) {
	t.Helper()
	for _, helper := range helpers {
		for _, mutation := range []string{"none", "foreign", "lookalike", "already-endpoint", "effectful-owner", "effectful-test", "shadow"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(serveappPackage, "serveapp")
				name := "servedControlProofRuntime"
				if mutation == "lookalike" {
					name = "lookalike"
				}
				fixture := types.NewNamed(types.NewTypeName(0, pkg, name, nil), types.NewStruct(nil, nil), nil)
				owner, test := ast.Expr(ast.NewIdent("rt")), ast.Expr(ast.NewIdent("t"))
				if mutation == "effectful-owner" {
					owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
				}
				if mutation == "effectful-test" {
					test = &ast.CallExpr{Fun: ast.NewIdent("nextTest")}
				}
				if mutation == "foreign" {
					pkg = types.NewPackage("example/other", "other")
				}
				fun := ast.NewIdent(helper)
				ownerType := types.Type(fixture)
				if mutation == "already-endpoint" {
					ownerType = types.Typ[types.String]
				}
				info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{owner: {Type: ownerType}}, Uses: map[*ast.Ident]types.Object{fun: types.NewFunc(0, pkg, helper, types.NewSignatureType(nil, nil, nil, nil, nil, false))}}
				if mutation == "shadow" {
					info.Uses[fun] = types.NewVar(0, pkg, helper, types.Typ[types.Int])
				}
				_, got, ok := matchServedOwnerProjection(info, &ast.CallExpr{Fun: fun, Args: []ast.Expr{test, owner}})
				if ok != (mutation == "none") || ok && got != helper {
					t.Fatalf("match=%t helper=%s", ok, got)
				}
			})
		}
	}
}

func TestChannelAnchorOwnerPropagationRequiresExactHarness(t *testing.T) {
	testChannelOwnerPropagation(t, []string{"waitChannelAnchorCard"})
}

func TestChannelAnchorSettlementOwnerPropagationRequiresExactHarness(t *testing.T) {
	testChannelOwnerPropagation(t, []string{"waitChannelAnchorReceipt", "waitChannelAnchorDecision"})
}

func TestChannelObserverCountProjectionRequiresExactHarness(t *testing.T) {
	testChannelOwnerPropagation(t, []string{"waitNativeIntentCount", "waitChannelDeliverySendsSettled"})
}

func TestChannelDraftStateProjectionRequiresExactHarness(t *testing.T) {
	testChannelOwnerPropagation(t, []string{"selectedChannelDraftState"})
}

func TestMailboxCompletionReadProjectionRequiresExactFixture(t *testing.T) {
	testServedCapabilityProjection(t, []string{"gateCompletionRead"})
}

func TestLifecycleGateCommandProjectionRequiresExactFixture(t *testing.T) {
	testServedCapabilityProjection(t, []string{"lifecycleGateDecisionParams", "lifecycleDecisionParamsForCard", "waitLifecycleGateCard"})
}

func TestFanOutPublicReadProjectionRequiresExactFixture(t *testing.T) {
	testServedCapabilityProjection(t, []string{"awaitIssue2394SurfaceDiagnosis", "proveIssue2394SurfacePages", "proveIssue2394SurfaceEvents", "proveIssue2394SurfaceDiagnostics", "proveIssue2394SurfaceRefusals"})
}

func TestServedReceiptWaitOwnerRemovalIsFinite(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-helper", "wrong-package", "wrong-db", "wrong-owner", "wrong-outcome", "effectful-db", "wrong-arity", "already-migrated", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			db := types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("database/sql", "sql"), "DB", nil), types.NewStruct(nil, nil), nil))
			params := []*types.Var{types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "db", db), types.NewVar(0, pkg, "selected", types.NewInterfaceType(nil, nil).Complete())}
			for _, name := range []string{"backend", "eventID", "subscriberType", "subscriberID", "outcome"} {
				params = append(params, types.NewVar(0, pkg, name, types.Typ[types.String]))
			}
			params = append(params, types.NewVar(0, pkg, "want", types.Typ[types.Int]))
			name := "waitServedEventPublishReceiptOutcomeCount"
			args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("db"), ast.NewIdent("selected"), ast.NewIdent("backend"), ast.NewIdent("eventID"), ast.NewIdent("subscriberType"), ast.NewIdent("subscriberID"), ast.NewIdent("outcome"), ast.NewIdent("want")}
			switch mutation {
			case "wrong-helper":
				name = "other"
			case "wrong-package":
				pkg = types.NewPackage("example/other", "other")
			case "wrong-db":
				params[1] = types.NewVar(0, pkg, "db", types.Typ[types.Int])
			case "wrong-owner":
				params[2] = types.NewVar(0, pkg, "selected", db)
			case "wrong-outcome":
				params[7] = types.NewVar(0, pkg, "outcome", types.Typ[types.Int])
			case "effectful-db":
				args[1] = &ast.CallExpr{Fun: ast.NewIdent("nextDB")}
			case "wrong-arity":
				args = args[:8]
			case "already-migrated":
				params = append(params[:1], params[2:]...)
				args = append(args[:1], args[2:]...)
			}
			fun := ast.NewIdent(name)
			info := &types.Info{Uses: map[*ast.Ident]types.Object{fun: types.NewFunc(0, pkg, name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
			if mutation == "shadow" {
				info.Uses[fun] = types.NewVar(0, pkg, name, db)
			}
			got, ok := matchServedEntityStateOwner(info, &ast.CallExpr{Fun: fun, Args: args})
			if ok != (mutation == "none") || ok && got != name {
				t.Fatalf("receipt wait=%s/%t", got, ok)
			}
		})
	}
}

func TestSemanticNumericOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for _, mutation := range []string{"none", "foreign", "wrong-helper", "wrong-db", "wrong-owner", "wrong-endpoint", "effectful-db", "wrong-arity", "already-migrated", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			db := types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("database/sql", "sql"), "DB", nil), types.NewStruct(nil, nil), nil))
			params := []*types.Var{types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "endpoint", types.Typ[types.String]), types.NewVar(0, pkg, "db", db), types.NewVar(0, pkg, "selected", types.NewInterfaceType(nil, nil).Complete()), types.NewVar(0, pkg, "run", types.Typ[types.String])}
			name := "semanticNumericOutput"
			args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("endpoint"), ast.NewIdent("db"), ast.NewIdent("selected"), ast.NewIdent("run")}
			switch mutation {
			case "foreign":
				pkg = types.NewPackage("example/other", "other")
			case "wrong-helper":
				name = "other"
			case "wrong-db":
				params[2] = types.NewVar(0, pkg, "db", types.Typ[types.Int])
			case "wrong-owner":
				params[3] = types.NewVar(0, pkg, "selected", db)
			case "wrong-endpoint":
				params[1] = types.NewVar(0, pkg, "endpoint", types.Typ[types.Int])
			case "effectful-db":
				args[2] = &ast.CallExpr{Fun: ast.NewIdent("nextDB")}
			case "wrong-arity":
				args = args[:4]
			case "already-migrated":
				params = append(params[:2], params[3:]...)
				args = append(args[:2], args[3:]...)
			}
			fun := ast.NewIdent(name)
			info := &types.Info{Uses: map[*ast.Ident]types.Object{fun: types.NewFunc(0, pkg, name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
			if mutation == "shadow" {
				info.Uses[fun] = types.NewVar(0, pkg, name, db)
			}
			if got := matchSemanticNumericOwner(info, &ast.CallExpr{Fun: fun, Args: args}); got != (mutation == "none") {
				t.Fatalf("numeric owner matched=%t", got)
			}
		})
	}
}

func testChannelOwnerPropagation(t *testing.T, helpers []string) {
	t.Helper()
	for _, helper := range helpers {
		for _, mutation := range []string{"none", "wrong-package", "wrong-helper", "wrong-db", "effectful-db", "missing-harness", "wrong-harness", "dynamic-table", "unknown-table", "already-migrated", "shadow"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(serveappPackage, "serveapp")
				db := types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("database/sql", "sql"), "DB", nil), types.NewStruct(nil, nil), nil))
				harnessName := "channelOnboardingE2EHarness"
				if mutation == "wrong-harness" {
					harnessName = "lookalike"
				}
				harness := types.NewPointer(types.NewNamed(types.NewTypeName(0, pkg, harnessName, nil), types.NewStruct(nil, nil), nil))
				params := []*types.Var{types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "db", db), types.NewVar(0, pkg, "table", types.Typ[types.String]), types.NewVar(0, pkg, "occurrence", types.Typ[types.String]), types.NewVar(0, pkg, "allowed", types.NewSlice(types.Typ[types.String]))}
				if helper == "proveObjectRejectedControl" {
					params = append(params, types.NewVar(0, pkg, "extra", types.Typ[types.String]))
				}
				if helper == "waitChannelAnchorCard" {
					params = params[:4]
				}
				if helper == "waitChannelAnchorReceipt" || helper == "waitChannelAnchorDecision" || helper == "selectedChannelDraftState" {
					params = params[:3]
				}
				if helper == "waitNativeIntentCount" {
					params = params[:4]
				}
				if helper == "waitChannelDeliverySendsSettled" {
					params = params[:2]
				}
				if mutation == "wrong-db" {
					params[1] = types.NewVar(0, pkg, "db", types.Typ[types.Int])
				}
				if mutation == "already-migrated" {
					params[1] = types.NewVar(0, pkg, "h", harness)
				}
				if mutation == "wrong-package" {
					pkg = types.NewPackage("example/other", "other")
				}
				name := helper
				if mutation == "wrong-helper" {
					name = "other"
				}
				identifier := &ast.Ident{Name: name, NamePos: 10}
				args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("db"), &ast.BasicLit{Kind: token.STRING, Value: `"operator_channel_text_intents"`}, ast.NewIdent("occurrence"), ast.NewIdent("allowed")}
				if helper == "waitChannelAnchorCard" {
					args = args[:4]
				}
				if helper == "waitChannelAnchorReceipt" || helper == "waitChannelAnchorDecision" || helper == "selectedChannelDraftState" {
					args = args[:3]
				}
				if helper == "waitNativeIntentCount" {
					args = args[:4]
				}
				if helper == "waitChannelDeliverySendsSettled" {
					args = args[:2]
				}
				if mutation == "effectful-db" {
					args[1] = &ast.CallExpr{Fun: ast.NewIdent("nextDB")}
				}
				if mutation == "dynamic-table" && len(args) > 2 {
					args[2] = ast.NewIdent("table")
				}
				if mutation == "unknown-table" && len(args) > 2 {
					args[2] = &ast.BasicLit{Kind: token.STRING, Value: `"runs"`}
				}
				scope := types.NewScope(nil, 1, 100, "caller")
				if mutation != "missing-harness" {
					scope.Insert(types.NewVar(1, pkg, "h", harness))
				}
				info := &types.Info{Uses: map[*ast.Ident]types.Object{identifier: types.NewFunc(0, pkg, name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}, Scopes: map[ast.Node]*types.Scope{&ast.BlockStmt{}: scope}}
				if mutation == "shadow" {
					info.Uses[identifier] = types.NewVar(0, pkg, name, db)
				}
				owner, gotHelper, ok := matchChannelDispositionOwner(info, &ast.CallExpr{Fun: identifier, Args: args})
				want := mutation == "none" || helper != "waitObjectChannelDisposition" && (mutation == "dynamic-table" || mutation == "unknown-table")
				if ok != want || ok && (owner != "h" || gotHelper != helper) {
					t.Fatalf("matched=%t owner=%s helper=%s", ok, owner, gotHelper)
				}
			})
		}
	}
}

func TestServedEntityStateOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-package", "wrong-helper", "wrong-db-type", "missing-owner", "wrong-owner", "wrong-state-type", "effectful-db", "wrong-arity", "already-migrated", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			sqlpkg := types.NewPackage("database/sql", "sql")
			dbName := "DB"
			if mutation == "wrong-db-type" {
				dbName = "Tx"
			}
			db := types.NewPointer(types.NewNamed(types.NewTypeName(token.NoPos, sqlpkg, dbName, nil), types.NewStruct(nil, nil), nil))
			params := []*types.Var{types.NewVar(token.NoPos, pkg, "t", types.Typ[types.Int]), types.NewVar(token.NoPos, pkg, "db", db), types.NewVar(token.NoPos, pkg, "selected", types.NewInterfaceType(nil, nil).Complete())}
			for _, name := range []string{"backend", "runID", "entityID", "wantState"} {
				params = append(params, types.NewVar(token.NoPos, pkg, name, types.Typ[types.String]))
			}
			switch mutation {
			case "wrong-package":
				pkg = types.NewPackage("example/other", "other")
			case "missing-owner":
				params[2] = types.NewVar(token.NoPos, pkg, "backend", types.Typ[types.String])
			case "wrong-owner":
				params[2] = types.NewVar(token.NoPos, pkg, "selected", db)
			case "wrong-state-type":
				params[6] = types.NewVar(token.NoPos, pkg, "wantState", types.Typ[types.Int])
			case "already-migrated":
				params = append(params[:1], params[2:]...)
			}
			helper := "requireServedEventPublishEntityState"
			if mutation == "wrong-helper" {
				helper = "otherEntityState"
			}
			name := ast.NewIdent(helper)
			args := []ast.Expr{ast.NewIdent("t"), &ast.SelectorExpr{X: ast.NewIdent("rt"), Sel: ast.NewIdent("DB")}, ast.NewIdent("selected"), ast.NewIdent("backend"), ast.NewIdent("runID"), ast.NewIdent("entityID"), ast.NewIdent("wantState")}
			if mutation == "effectful-db" {
				args[1] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
			}
			if mutation == "wrong-arity" {
				args = args[:6]
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{name: types.NewFunc(token.NoPos, pkg, helper, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
			if mutation == "shadow" {
				info.Uses[name] = types.NewVar(token.NoPos, pkg, helper, db)
			}
			got, ok := matchServedEntityStateOwner(info, &ast.CallExpr{Fun: name, Args: args})
			if ok != (mutation == "none") || ok && got != helper {
				t.Fatalf("match=%t helper=%s", ok, got)
			}
		})
	}
}

func TestServedDatabaseOwnerProjectionIsFiniteAndIdempotent(t *testing.T) {
	for _, helper := range []string{"waitServedRunDeliveryQuiescence", "servedEventPublishDebugSummary", "requireServedEventPublishEntityState", "requireServedParitySettlementPostconditions", "requireServedControlAPIIdempotencyRows", "servedEventPublishAPIIdempotencyCount", "servedEventPublishDeliveryStatusCount", "waitServedEventPublishDeliveryStatusCount", "requireNoServedDeliveryStatusDuring", "servedEventNameCount", "requireServedEventNameCount", "issue2394SurfaceSnapshot", "readIssue2394StopSnapshot", "assertIssue2394StopUnchanged", "requireNoServedReceiptOutcomeDuring"} {
		for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "wrong-field", "bare-db", "effectful", "already-migrated", "shadow"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(serveappPackage, "serveapp")
				fixtureName := "servedControlProofRuntime"
				if mutation == "wrong-fixture" {
					fixtureName = "lookalike"
				}
				fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, fixtureName, nil), types.NewStruct(nil, nil), nil)
				owner := ast.Expr(ast.NewIdent("rt"))
				if mutation == "effectful" {
					owner = &ast.CallExpr{Fun: ast.NewIdent("next")}
				}
				db := ast.Expr(&ast.SelectorExpr{X: owner, Sel: ast.NewIdent("DB")})
				if mutation == "bare-db" {
					db = ast.NewIdent("db")
				}
				if mutation == "wrong-field" {
					db.(*ast.SelectorExpr).Sel = ast.NewIdent("Other")
				}
				index, replace, _ := servedDiagnosticHelperShape(helper)
				args := []ast.Expr{ast.NewIdent("t"), db, ast.NewIdent("backend")}
				params := []*types.Var{types.NewVar(token.NoPos, pkg, "t", types.Typ[types.Int]), types.NewVar(token.NoPos, pkg, "db", types.Typ[types.Int]), types.NewVar(token.NoPos, pkg, "backend", types.Typ[types.String])}
				if index == 2 {
					args = []ast.Expr{ast.NewIdent("t"), ast.NewIdent("endpoint"), db, ast.NewIdent("backend")}
					params = append(params, types.NewVar(token.NoPos, pkg, "backend", types.Typ[types.String]))
				}
				if mutation == "already-migrated" && !replace {
					params[index+1] = types.NewVar(token.NoPos, pkg, "selected", types.NewInterfaceType(nil, nil).Complete())
				}
				if mutation == "wrong-package" {
					pkg = types.NewPackage("example/other", "other")
				}
				name := ast.NewIdent(helper)
				info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}}, Uses: map[*ast.Ident]types.Object{name: types.NewFunc(token.NoPos, pkg, helper, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
				if mutation == "shadow" {
					info.Uses[name] = types.NewVar(token.NoPos, pkg, helper, types.Typ[types.Int])
				}
				_, _, gotIndex, gotReplace, ok := matchServedDatabaseOwner(info, &ast.CallExpr{Fun: name, Args: args})
				want := mutation == "none" || mutation == "already-migrated" && replace
				if ok != want || ok && (index != gotIndex || replace != gotReplace) {
					t.Fatalf("match=%t index=%d replace=%t", ok, gotIndex, gotReplace)
				}
			})
		}
	}
}

func TestServedDeliveryStatusOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-package", "wrong-helper", "raw-owner", "wrong-want", "effectful-db", "already-migrated", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg, sqlpkg := types.NewPackage(serveappPackage, "serveapp"), types.NewPackage("database/sql", "sql")
			db := types.NewPointer(types.NewNamed(types.NewTypeName(token.NoPos, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
			params := []*types.Var{types.NewVar(token.NoPos, pkg, "t", types.Typ[types.Int]), types.NewVar(token.NoPos, pkg, "db", db), types.NewVar(token.NoPos, pkg, "selected", types.NewInterfaceType(nil, nil).Complete())}
			args := []ast.Expr{ast.NewIdent("t"), ast.NewIdent("db"), ast.NewIdent("selected")}
			for _, label := range []string{"backend", "runID", "eventID", "subscriberType", "subscriberID", "status", "want"} {
				kind := types.String
				if label == "want" {
					kind = types.Int
				}
				params = append(params, types.NewVar(token.NoPos, pkg, label, types.Typ[kind]))
				args = append(args, ast.NewIdent(label))
			}
			helper := "waitServedEventPublishDeliveryStatusCountForRun"
			switch mutation {
			case "wrong-package":
				pkg = types.NewPackage("example/other", "other")
			case "wrong-helper":
				helper = "unknown"
			case "raw-owner":
				params[2] = types.NewVar(token.NoPos, pkg, "selected", db)
			case "wrong-want":
				params[9] = types.NewVar(token.NoPos, pkg, "want", types.Typ[types.String])
			case "effectful-db":
				args[1] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
			case "already-migrated":
				params = append(params[:1], params[2:]...)
			}
			name := ast.NewIdent(helper)
			info := &types.Info{Uses: map[*ast.Ident]types.Object{name: types.NewFunc(token.NoPos, pkg, helper, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))}}
			if mutation == "shadow" {
				info.Uses[name] = types.NewVar(token.NoPos, pkg, helper, db)
			}
			got, ok := matchServedEntityStateOwner(info, &ast.CallExpr{Fun: name, Args: args})
			if ok != (mutation == "none") || ok && got != helper {
				t.Fatalf("match=%t helper=%s", ok, got)
			}
		})
	}
}

func TestServedDiagnosticParameterPropagationIsFiniteAndIdempotent(t *testing.T) {
	source := []byte(`package serveapp
import "database/sql"
func requireServedEventPublishEntityState(t int, db *sql.DB, backend string) { servedEventPublishDebugSummary(t,db,backend,"run"); unknown(t,db) }
func servedEventPublishDebugSummary(t int, selected any, backend,run string){}
func untouched(db *sql.DB) { servedEventPublishDebugSummary(0,db,"","run") }
func requireServedStoppedPendingDelivery(t int, db *sql.DB, backend string) { servedEventPublishDebugSummary:=func(...any){};servedEventPublishDebugSummary(t,db,backend,"run") }
`)
	got, count, err := propagateServedDiagnosticOwner(source)
	if err != nil || count != 2 {
		t.Fatalf("propagation count=%d err=%v", count, err)
	}
	text := string(got)
	for _, want := range []string{"db *sql.DB, selected any", "servedEventPublishDebugSummary(t, selected, backend", "unknown(t, db)", "servedEventPublishDebugSummary(0, db", "servedEventPublishDebugSummary(t, db"} {
		if !strings.Contains(text, want) {
			t.Fatalf("lost finite rewrite boundary %q:\n%s", want, text)
		}
	}
	again, count, err := propagateServedDiagnosticOwner(got)
	if err != nil || count != 0 || string(again) != string(got) {
		t.Fatalf("repeated propagation changed source: %d %v", count, err)
	}
	if _, _, err := propagateServedDiagnosticOwner([]byte("package other")); err == nil {
		t.Fatal("wrong package accepted")
	}
}

func TestHistoryFieldOnlyExactUnfilteredCounts(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"SELECT COUNT(*) FROM events WHERE run_id=$1", "Events"},
		{"select count(*) FROM event_deliveries WHERE run_id=$1", "Deliveries"},
		{" SELECT\nCOUNT(*) FROM entity_state WHERE run_id=$1 ", "EntityState"},
		{"SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='pending'", ""},
		{"SELECT COUNT(*) FROM events WHERE run_id=$1; DELETE FROM events", ""},
		{"SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1", ""},
		{"SELECT COUNT(*) FROM events WHERE run_id=?", ""},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if got := historyField(tc.sql); got != tc.want {
				t.Fatalf("field=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestStoretestAliasRejectsDotAndBlankImports(t *testing.T) {
	for _, name := range []string{"", "fixture", ".", "_"} {
		imp := &ast.ImportSpec{Path: &ast.BasicLit{Kind: token.STRING, Value: `"` + storetestPackage + `"`}}
		if name != "" {
			imp.Name = ast.NewIdent(name)
		}
		got := storetestAlias(&ast.File{Imports: []*ast.ImportSpec{imp}})
		want := name
		if name == "" {
			want = "storetest"
		}
		if name == "." || name == "_" {
			want = ""
		}
		if got != want {
			t.Fatalf("alias=%q want=%q", got, want)
		}
	}
}

func TestMatchRefusesUntypedOrUnknownCalls(t *testing.T) {
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	call := &ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent("raw"), Sel: ast.NewIdent("Scan")}, Args: []ast.Expr{ast.NewIdent("destination")}}
	if _, _, _, _, ok := match(info, call); ok {
		t.Fatal("untyped raw call accepted")
	}
}

func TestPatternClassificationKeepsEndpointsAndPlumbingSeparate(t *testing.T) {
	for _, tc := range []struct{ member, want string }{
		{"call:database/sql.(*database/sql.Row).Scan", "cursor-plumbing"},
		{"call:database/sql.(*database/sql.Rows).Close", "cursor-plumbing"},
		{"call:database/sql.(*database/sql.DB).Close", "open-and-connection-lifetime"},
		{"call:database/sql.(*database/sql.DB).QueryRowContext", "sql-read-endpoints"},
		{"call:database/sql.(*database/sql.Tx).ExecContext", "sql-write-endpoints"},
		{"call:database/sql.(*database/sql.Tx).Commit", "transaction-protocol"},
		{"call:example.fixture", "other-raw-bearing-shared-helper-calls"},
	} {
		if got := patternClass(tc.member); got != tc.want {
			t.Fatalf("%s: %s want %s", tc.member, got, tc.want)
		}
	}
}

func TestMatchRequiresExactOwnerMethodDestinationAndPredicate(t *testing.T) {
	for _, mutation := range []string{"none", "predicate", "wrong-owner", "wrong-destination", "wrong-query-method", "dynamic-query"} {
		t.Run(mutation, func(t *testing.T) {
			sqlPackage := types.NewPackage("database/sql", "sql")
			methodType := func(name, method string) *types.Pointer {
				named := types.NewNamed(types.NewTypeName(token.NoPos, sqlPackage, name, nil), types.NewStruct(nil, nil), nil)
				ptr := types.NewPointer(named)
				named.AddMethod(types.NewFunc(token.NoPos, sqlPackage, method, types.NewSignatureType(types.NewVar(token.NoPos, sqlPackage, "", ptr), nil, nil, nil, nil, false)))
				return ptr
			}
			row := methodType("Row", "Scan")
			queryName := "QueryRowContext"
			if mutation == "wrong-query-method" {
				queryName = "QueryRow"
			}
			db := methodType("DB", queryName)
			fixturePackage := types.NewPackage(conformancePackage, "conformance")
			fixtureName := "deploymentResourceFixture"
			if mutation == "wrong-owner" {
				fixtureName = "unknownFixture"
			}
			fixture := types.NewNamed(types.NewTypeName(token.NoPos, fixturePackage, fixtureName, nil), types.NewStruct(nil, nil), nil)
			receiver, destination := ast.NewIdent("f"), ast.NewIdent("count")
			querySelector := &ast.SelectorExpr{X: &ast.SelectorExpr{X: receiver, Sel: ast.NewIdent("db")}, Sel: ast.NewIdent(queryName)}
			sql := "SELECT COUNT(*) FROM events WHERE run_id=$1"
			if mutation == "predicate" {
				sql += " AND event_name='selected'"
			}
			queryExpression := ast.Expr(&ast.BasicLit{Kind: token.STRING, Value: "`" + sql + "`"})
			if mutation == "dynamic-query" {
				queryExpression = ast.NewIdent("query")
			}
			query := &ast.CallExpr{Fun: querySelector, Args: []ast.Expr{ast.NewIdent("ctx"), queryExpression, ast.NewIdent("runID")}}
			scan := &ast.SelectorExpr{X: query, Sel: ast.NewIdent("Scan")}
			call := &ast.CallExpr{Fun: scan, Args: []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: destination}}}
			destinationType := types.Type(types.Typ[types.Int])
			if mutation == "wrong-destination" {
				destinationType = types.Typ[types.String]
			}
			info := &types.Info{
				Types: map[ast.Expr]types.TypeAndValue{
					receiver: {Type: types.NewPointer(fixture)}, destination: {Type: destinationType},
				},
				Selections: map[*ast.SelectorExpr]*types.Selection{
					scan:          types.NewMethodSet(row).Lookup(sqlPackage, "Scan"),
					querySelector: types.NewMethodSet(db).Lookup(sqlPackage, queryName),
				},
			}
			field, _, _, _, ok := match(info, call)
			if ok != (mutation == "none") || (ok && field != "Events") {
				t.Fatalf("match=%t field=%s", ok, field)
			}
		})
	}
}

func TestServedOwnerProjectionRequiresResolvedHelperAndPureOriginalFixture(t *testing.T) {
	for _, mutation := range []string{"none", "snapshot", "lifecycle-snapshot", "lifecycle-history", "lifecycle-stored-loop", "lifecycle-public-loop", "lifecycle-event-count", "lifecycle-template-gate", "lifecycle-flow-entity", "author-activity", "receiver-ingress-snapshot", "numeric-domain-counts", "fork-delivery-route", "fork-producer-evidence", "fork-notice", "portfolio-entity", "portfolio-events", "portfolio-event", "portfolio-emission", "receiver-public-readback", "portfolio-keyed-entity", "side-effects", "side-effect-test", "wrong-fixture", "wrong-package", "same-name-local", "unknown-helper"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			name := "servedControlProofRuntime"
			if mutation == "wrong-fixture" {
				name = "unknownFixture"
			}
			fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
			helper := "forkReceiverSelectedFixtureOwner"
			if mutation == "snapshot" {
				helper = "snapshotForkReceiverApplication"
			}
			switch mutation {
			case "lifecycle-snapshot":
				helper = "lifecycleStoredSnapshot"
			case "lifecycle-history":
				helper = "readLifecycleTransitionHistory"
			case "lifecycle-stored-loop":
				helper = "readLifecycleStoredLoop"
			case "lifecycle-public-loop":
				helper = "readLifecycleLoop"
			case "lifecycle-event-count":
				helper = "requireLifecycleEventCount"
			case "lifecycle-template-gate":
				helper = "readLifecycleTemplateGate"
			case "lifecycle-flow-entity":
				helper = "requireLifecycleFlowEntity"
			case "author-activity":
				helper = "servedControlProofAuthorActivityContext"
			case "receiver-ingress-snapshot":
				helper = "receiverIngressApplicationSnapshot"
			case "numeric-domain-counts":
				helper = "semanticNumericDomainCounts"
			case "fork-delivery-route":
				helper = "readServedForkDeliveryEvidence"
			case "fork-producer-evidence":
				helper = "readForkReceiverProducerEvidence"
			case "fork-notice":
				helper = "readForkReceiverNoticeDomain"
			case "portfolio-entity":
				helper = "requireA2PortfolioEntity"
			case "portfolio-events":
				helper = "requireA2PortfolioEvents"
			case "portfolio-event":
				helper = "requireA2PortfolioEvent"
			case "portfolio-emission":
				helper = "requireA2PortfolioEmission"
			case "receiver-public-readback":
				helper = "requireReceiverPublicReadback"
			case "portfolio-keyed-entity":
				helper = "requireA2PortfolioKeyedEntity"
			}
			if mutation == "unknown-helper" {
				helper = "rawStorageHelper"
			}
			if mutation == "wrong-package" {
				pkg = types.NewPackage("example/other", "other")
			}
			fun := ast.NewIdent(helper)
			owner := ast.Expr(ast.NewIdent("rt"))
			if mutation == "side-effects" {
				owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
			}
			test := ast.Expr(ast.NewIdent("t"))
			if mutation == "side-effect-test" {
				test = &ast.CallExpr{Fun: ast.NewIdent("nextTest")}
			}
			call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{test, owner}}
			info := &types.Info{
				Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
				Uses:  map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, pkg, helper, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
			}
			if mutation == "same-name-local" {
				info.Uses[fun] = types.NewVar(token.NoPos, pkg, helper, types.Typ[types.Int])
			}
			_, _, ok := matchServedOwnerProjection(info, call)
			if ok != (mutation == "none" || mutation == "snapshot" || mutation == "lifecycle-snapshot" || mutation == "lifecycle-history" || mutation == "lifecycle-stored-loop" || mutation == "lifecycle-public-loop" || mutation == "lifecycle-event-count" || mutation == "lifecycle-template-gate" || mutation == "lifecycle-flow-entity" || mutation == "author-activity" || mutation == "receiver-ingress-snapshot" || mutation == "numeric-domain-counts" || mutation == "fork-delivery-route" || mutation == "fork-producer-evidence" || mutation == "fork-notice" || mutation == "portfolio-entity" || mutation == "portfolio-events" || mutation == "portfolio-event" || mutation == "portfolio-emission" || mutation == "receiver-public-readback") {
				t.Fatalf("match=%t", ok)
			}
		})
	}
}

func TestSelectedForkPublicProjectionRequiresOriginalResolvedFixture(t *testing.T) {
	for _, helper := range []string{"requireSelectedForkDurablePublicReads", "requireSelectedForkDeclaredAgentReads"} {
		t.Run(helper, func(t *testing.T) {
			for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "side-effects", "shadow"} {
				t.Run(mutation, func(t *testing.T) {
					pkg := types.NewPackage(serveappPackage, "serveapp")
					name := "servedControlProofRuntime"
					if mutation == "wrong-fixture" {
						name = "otherFixture"
					}
					fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
					fnPkg := pkg
					if mutation == "wrong-package" {
						fnPkg = types.NewPackage("example/other", "other")
					}
					fun := ast.NewIdent(helper)
					owner := ast.Expr(ast.NewIdent("rt"))
					if mutation == "side-effects" {
						owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
					}
					call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{ast.NewIdent("t"), owner}}
					info := &types.Info{
						Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
						Uses:  map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, fun.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
					}
					if mutation == "shadow" {
						info.Uses[fun] = types.NewVar(token.NoPos, fnPkg, fun.Name, types.Typ[types.Int])
					}
					_, helper, ok := matchServedOwnerProjection(info, call)
					if ok != (mutation == "none") || ok && helper != fun.Name {
						t.Fatalf("match=%t helper=%s", ok, helper)
					}
				})
			}
		})
	}
}

func TestPipelineHandoffOwnerProjectionRequiresOriginalResolvedFixture(t *testing.T) {
	testServedCompletionOwnerProjection(t, []string{"waitForkReceiverSourceCompletion", "waitPublicationSiteCompletion"})
}

func TestMailboxEffectsOwnerProjectionRequiresOriginalResolvedFixture(t *testing.T) {
	testServedCompletionOwnerProjection(t, []string{"mailboxCompletionRunEffects"})
}

func testServedCompletionOwnerProjection(t *testing.T, helpers []string) {
	t.Helper()
	for _, helper := range helpers {
		for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "side-effects", "shadow", "already-migrated"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(serveappPackage, "serveapp")
				name := "servedControlProofRuntime"
				if mutation == "wrong-fixture" {
					name = "otherFixture"
				}
				fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
				fnPkg := pkg
				if mutation == "wrong-package" {
					fnPkg = types.NewPackage("example/other", "other")
				}
				fun := ast.NewIdent(helper)
				owner := ast.Expr(ast.NewIdent("rt"))
				if mutation == "side-effects" {
					owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
				}
				ownerType := types.Type(fixture)
				if mutation == "already-migrated" {
					ownerType = types.NewInterfaceType(nil, nil).Complete()
				}
				info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{owner: {Type: ownerType}}, Uses: map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, helper, types.NewSignatureType(nil, nil, nil, nil, nil, false))}}
				if mutation == "shadow" {
					info.Uses[fun] = types.NewVar(token.NoPos, pkg, helper, types.Typ[types.Int])
				}
				_, got, ok := matchServedOwnerProjection(info, &ast.CallExpr{Fun: fun, Args: []ast.Expr{ast.NewIdent("t"), owner, ast.NewIdent("runID")}})
				if ok != (mutation == "none") || ok && got != helper {
					t.Fatalf("match=%t helper=%s", ok, got)
				}
			})
		}
	}
}

func TestMailboxCompletionFaultProjectionRequiresOriginalResolvedFixture(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "side-effects", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			name := "servedControlProofRuntime"
			if mutation == "wrong-fixture" {
				name = "otherFixture"
			}
			fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
			fnPkg := pkg
			if mutation == "wrong-package" {
				fnPkg = types.NewPackage("example/other", "other")
			}
			fun := ast.NewIdent("installMailboxCompletionFaultWitness")
			owner := ast.Expr(ast.NewIdent("rt"))
			if mutation == "side-effects" {
				owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
			}
			call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{ast.NewIdent("t"), owner}}
			info := &types.Info{
				Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
				Uses:  map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, fun.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
			}
			if mutation == "shadow" {
				info.Uses[fun] = types.NewVar(token.NoPos, fnPkg, fun.Name, types.Typ[types.Int])
			}
			_, helper, ok := matchServedOwnerProjection(info, call)
			if ok != (mutation == "none") || ok && helper != fun.Name {
				t.Fatalf("match=%t helper=%s", ok, helper)
			}
		})
	}
}

func TestPortfolioJoinProjectionRequiresOriginalResolvedFixture(t *testing.T) {
	for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "side-effects", "shadow"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			name := "servedControlProofRuntime"
			if mutation == "wrong-fixture" {
				name = "otherFixture"
			}
			fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
			fnPkg := pkg
			if mutation == "wrong-package" {
				fnPkg = types.NewPackage("example/other", "other")
			}
			fun := ast.NewIdent("requireA2PortfolioJoin")
			owner := ast.Expr(ast.NewIdent("rt"))
			if mutation == "side-effects" {
				owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
			}
			call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{ast.NewIdent("t"), owner}}
			info := &types.Info{
				Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
				Uses:  map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, fun.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
			}
			if mutation == "shadow" {
				info.Uses[fun] = types.NewVar(token.NoPos, fnPkg, fun.Name, types.Typ[types.Int])
			}
			_, helper, ok := matchServedOwnerProjection(info, call)
			if ok != (mutation == "none") || ok && helper != fun.Name {
				t.Fatalf("match=%t helper=%s", ok, helper)
			}
		})
	}
}

func TestServedJoinPublicProjectionRequiresAuditedResolvedHelper(t *testing.T) {
	for _, helper := range []string{"a2ReadJoinPublicObligation", "a2RequireJoinPublicGraph", "a2ReadJoinPublicEvent", "a2WaitJoinPublicRun", "a2RequireJoinPublicClients", "a2JoinPublicCLI", "a2RequireJoinPublicTrace"} {
		t.Run(helper, func(t *testing.T) {
			for _, mutation := range []string{"none", "wrong-package", "wrong-fixture", "side-effects", "shadow"} {
				t.Run(mutation, func(t *testing.T) {
					pkg := types.NewPackage(serveappPackage, "serveapp")
					name := "servedControlProofRuntime"
					if mutation == "wrong-fixture" {
						name = "otherFixture"
					}
					fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
					fnPkg := pkg
					if mutation == "wrong-package" {
						fnPkg = types.NewPackage("example/other", "other")
					}
					fun := ast.NewIdent(helper)
					owner := ast.Expr(ast.NewIdent("rt"))
					if mutation == "side-effects" {
						owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
					}
					call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{ast.NewIdent("t"), owner}}
					info := &types.Info{
						Types: map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
						Uses:  map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, helper, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
					}
					if mutation == "shadow" {
						info.Uses[fun] = types.NewVar(token.NoPos, fnPkg, helper, types.Typ[types.Int])
					}
					_, gotHelper, ok := matchServedOwnerProjection(info, call)
					if ok != (mutation == "none") || ok && gotHelper != helper {
						t.Fatalf("match=%t helper=%s", ok, gotHelper)
					}
				})
			}
		})
	}
}

func TestMailboxSelectorProjectionRequiresExactLexicalTestingOwner(t *testing.T) {
	for _, mutation := range []string{"none", "missing-test", "wrong-test-type", "shadow-test", "side-effects", "wrong-fixture", "wrong-package", "shadow-helper", "extra-argument"} {
		t.Run(mutation, func(t *testing.T) {
			pkg := types.NewPackage(serveappPackage, "serveapp")
			name := "servedControlProofRuntime"
			if mutation == "wrong-fixture" {
				name = "otherFixture"
			}
			fixture := types.NewNamed(types.NewTypeName(token.NoPos, pkg, name, nil), types.NewStruct(nil, nil), nil)
			fnPkg := pkg
			if mutation == "wrong-package" {
				fnPkg = types.NewPackage("example/other", "other")
			}
			fun := &ast.Ident{Name: "selectedMailboxFixtureStore", NamePos: 30}
			owner := ast.Expr(ast.NewIdent("rt"))
			if mutation == "side-effects" {
				owner = &ast.CallExpr{Fun: ast.NewIdent("nextRuntime")}
			}
			call := &ast.CallExpr{Fun: fun, Args: []ast.Expr{owner}}
			if mutation == "extra-argument" {
				call.Args = append(call.Args, ast.NewIdent("unexpected"))
			}
			testingPackage := types.NewPackage("testing", "testing")
			testingT := types.NewPointer(types.NewNamed(types.NewTypeName(token.NoPos, testingPackage, "T", nil), types.NewStruct(nil, nil), nil))
			var testType types.Type = testingT
			if mutation == "wrong-test-type" {
				testType = types.Typ[types.Int]
			}
			outer := types.NewScope(nil, 1, 100, "function")
			if mutation != "missing-test" {
				outer.Insert(types.NewVar(5, nil, "t", testType))
			}
			inner := types.NewScope(outer, 20, 80, "nested block")
			if mutation == "shadow-test" {
				inner.Insert(types.NewVar(25, nil, "t", types.Typ[types.Int]))
			}
			info := &types.Info{
				Types:  map[ast.Expr]types.TypeAndValue{owner: {Type: fixture}},
				Uses:   map[*ast.Ident]types.Object{fun: types.NewFunc(token.NoPos, fnPkg, fun.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))},
				Scopes: map[ast.Node]*types.Scope{&ast.FuncType{}: outer, &ast.BlockStmt{}: inner},
			}
			if mutation == "shadow-helper" {
				info.Uses[fun] = types.NewVar(token.NoPos, fnPkg, fun.Name, types.Typ[types.Int])
			}
			_, helper, ok := matchServedOwnerProjection(info, call)
			if ok != (mutation == "none") || ok && helper != fun.Name {
				t.Fatalf("match=%t helper=%s", ok, helper)
			}
		})
	}
}
