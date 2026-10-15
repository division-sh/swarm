package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func nativeAPIPhysicalCardinalityRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{"TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency": true,
		"TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle":                           true,
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                        true,
		"TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency":                        true,
		"TestOperatorTestSetupRejectsContractInvalidEntities":                                     true,
		"testRuntimeNukeLayeredPostgresCapacity":                                                  true,
		"TestAdministrativeOperationCancellationReleasesLeaseAndCapacity":                         true,
		"TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency":                         true,
		"TestOperatorAgentSendDirectiveRunTargetErrors":                                           true,
		"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency":                        true,
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                                 true,
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency":        true,
		"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes":              true,
		"TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient":                          true,
		"TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed":                           true,
		"TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback":                true,
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":                 true,
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":              true,
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence":           true,
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                         true,
		"TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication": true,
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence":     true,
		"TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker":             true,
		"TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence":           true,
		"TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance":                     true,
		"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence":       true,
		"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":                  true,
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                             true,
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":                     true,
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":                  true,
		"TestOperatorEventReplaySubsetAndFailClosedCases":                                         true,
		"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                                true,
		"TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact":  true,
		"TestOperatorRunStartRejectsFlowScopedEventName":                                          true,
		"countAPIIdempotencyRows":                   true,
		"countAllRunRows":                           true,
		"countAllEventRows":                         true,
		"assertNoFlowScopedEventPublishPersistence": true,
		"assertNoRunStartPersistence":               true}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 39 {
		t.Fatalf("physical count recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}
func nativeAPIPhysicalCardinalityWorkload(t *testing.T, source string) string {
	t.Helper()
	if strings.HasPrefix(source, "func TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency(") {
		source = nativePublicRunControlSource(source)
	}
	source = strings.Replace(source, "\n\t\tdb    *sql.DB", "", 1)
	source = strings.Replace(source, "fixture{store: selected, db: storetest.DatabaseForTest(selected)}", "fixture{store: selected}", 1)
	source = strings.Replace(source, "_, db, _ := testutil.StartPostgres(t)\n\t\t\t\treturn fixture{store: storetest.AdmitPostgresRuntimeStore(t, db), db: db}", "selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)\n\t\t\t\treturn fixture{store: selected}", 1)
	source = strings.Replace(source, "\n\tdb := storetest.Database(sqliteStore)", "", 1)
	return nativeAPIEventConsumerWorkload(t, source)
}

func q10APIPhysicalCardinalityCurrentRecipe(t *testing.T, row recipe) recipe {
	t.Helper()
	if row.File == "internal/apiv1/operator_test_setup_test.go" && row.Function == "TestOperatorTestSetupRejectsContractInvalidEntities" {
		row.Successor = q10APISetupRefusalCurrentSource(t, row)
		return row
	}
	if row.Function != "TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency" || row.File != "internal/apiv1/operator_test_setup_test.go" {
		return q10PrivateTargetCurrentRecipe(t, row)
	}
	if row.Successor != "" || row.Family != "native-api-physical-cardinality-consumer" {
		t.Fatal("setup import historical identity changed")
	}
	current := row.After
	for _, edit := range []publicationSuccessorEdit{
		{"runID, entityID, \"waiting\", \"seeded\", true,", "runID, entityID, \"seeded\","},
		{"runID, entityID, \"ready\", \"changed\", false,", "runID, entityID, \"changed\","},
		{"assertTestSetupPersistence(t, db, bundleHash, runID, entityID, \"waiting\", \"seeded\", true)", "assertTestSetupPersistence(t, pg, bundleHash, runID, entityID, \"seeded\")"},
		{"if entityResult[\"alias\"] != \"subject\" || entityResult[\"entity_id\"] != entityID || entityResult[\"flow_instance\"] != \"operating\" || entityResult[\"entity_type\"] != \"product\" || entityResult[\"current_state\"] != \"waiting\" {", "if state, present := entityResult[\"current_state\"]; !present || state != nil || entityResult[\"alias\"] != \"subject\" || entityResult[\"entity_id\"] != entityID || entityResult[\"flow_instance\"] != \"operating\" || entityResult[\"entity_type\"] != \"product\" {"},
	} {
		want := 1
		if strings.Contains(edit.before, "\"waiting\", \"seeded\"") {
			want = 3
			if strings.HasPrefix(edit.before, "assertTestSetupPersistence") {
				want = 4
			}
		}
		if strings.Count(current, edit.before) != want {
			t.Fatalf("setup import historical cut changed: %q", edit.before)
		}
		current = strings.ReplaceAll(current, edit.before, edit.after)
	}
	guard := recipe{File: row.File, Function: row.Function, Before: current, After: current}
	for _, edit := range []publicationSuccessorEdit{
		{"!present || ", ""}, {"state != nil", "state == nil"},
		{"assertTestSetupPersistence(t, pg,", "assertTestSetupPersistence(t, foreignStore,"},
		{"pg, bundleHash, runID, entityID", "pg, bundleHash, foreignRun, entityID"},
		{"pg, bundleHash, runID, entityID", "pg, bundleHash, runID, foreignEntity"},
		{"count != 1", "count < 1"}, {"data[\"code\"] != IdempotencyConflictCode", "false"},
	} {
		mutant := strings.Replace(current, edit.before, edit.after, 1)
		if mutant == current {
			t.Fatalf("missing setup import hostile cut: %q", edit.before)
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
			t.Fatalf("setup import lost presence, owner, identity or replay assertion: %q", edit.before)
		}
	}
	if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.After), guard); err == nil || changed {
		t.Fatal("old lifecycle-bearing import accepted as current")
	}
	row.Successor = current
	return row
}

func q10APISetupRefusalCurrentSource(t *testing.T, row recipe) string {
	t.Helper()
	if row.Successor != "" || row.Family != "native-api-physical-cardinality-consumer" {
		t.Fatal("setup refusal historical identity changed")
	}
	current := row.After
	for _, edit := range []publicationSuccessorEdit{
		{"\t\tname      string\n\t\tedit      func(map[string]any)\n\t\twantField string", "\t\tname string\n\t\tconstructedRoot bool\n\t\tedit func(map[string]any)\n\t\twantField string\n\t\twantReason string"},
		{"name: \"undeclared current state\",", "name: \"undeclared current state\",\nconstructedRoot: true,"},
		{"wantField: \"entities[0].current_state\",", "wantField: \"entities[0].current_state\",\nwantReason: \"must be a declared state for the selected flow\","},
		{"name: \"undeclared gate\",", "name: \"undeclared gate\",\nconstructedRoot: true,"},
		{"wantField: \"entities[0].gates.missing_gate\",", "wantField: \"entities[0].gates.missing_gate\",\nwantReason: \"is not declared for the selected flow\","},
		{"name: \"non boolean gate\",", "name: \"non boolean gate\",\nconstructedRoot: true,"},
		{"wantField: \"entities[0].gates.review_ready\",", "wantField: \"entities[0].gates.review_ready\",\nwantReason: \"must be boolean\","},
		{"\n\t}\n\n\tfor _, tc := range cases {", q10APISetupImportRefusalCases + "\n\t}\n\n\tfor _, tc := range cases {"},
		{"\t\t\tentity := validTestSetupEntity(uuid.NewString(), \"waiting\", \"seeded\", true)", `			runID := uuid.NewString()
			entity := validTestSetupEntity(uuid.NewString(), "seeded")
			if tc.constructedRoot {
				entity["entity_id"], entity["flow_instance"], entity["entity_type"] = runID, "", "item"
				entity["current_state"], entity["fields"] = "new", map[string]any{}
			}
			baseline, err := testSetupEntityRequestFromParam(entity, 0, runID)
			if err != nil {
				t.Fatalf("invalid baseline input: %v", err)
			}
			if err := validateTestSetupEntityAgainstBundle(bundle, baseline, 0); err != nil {
				t.Fatalf("invalid baseline contract: %v", err)
			}`},
		{"testSetupBodyWithEntity(bundle.SourceArtifact.BundleHash(), uuid.NewString(),", "testSetupBodyWithEntity(bundle.SourceArtifact.BundleHash(), runID,"},
		{"\t\t\tif count := countAPIIdempotencyRows(t, pg); count != 0 {", `			if tc.wantReason != "" && details["reason"] != tc.wantReason {
				t.Fatalf("test.setup_entities invalid reason = %#v, want %q; details=%#v", details["reason"], tc.wantReason, details)
			}
			if count := countAPIIdempotencyRows(t, pg); count != 0 {`},
	} {
		if strings.Count(current, edit.before) != 1 {
			t.Fatalf("setup refusal historical cut changed: %q", edit.before)
		}
		current = strings.Replace(current, edit.before, edit.after, 1)
	}
	guard := recipe{File: row.File, Function: row.Function, Before: current, After: current}
	for _, edit := range []publicationSuccessorEdit{
		{"validateTestSetupEntityAgainstBundle(bundle, baseline, 0)", "validateTestSetupEntityAgainstBundle(bundle, baseline, 1)"},
		{"entity[\"entity_id\"], entity[\"flow_instance\"], entity[\"entity_type\"] = runID", "entity[\"entity_id\"], entity[\"flow_instance\"], entity[\"entity_type\"] = foreignRun"},
		{"details[\"reason\"] != tc.wantReason", "false"},
		{"details[\"field\"] != tc.wantField", "false"},
		{"count != 0", "count < 0"},
		{"entity[\"current_state\"] = nil", "entity[\"current_state\"] = \"\""},
		{"entity[\"gates\"] = nil", "entity[\"gates\"] = map[string]any{}"},
	} {
		mutant := strings.Replace(current, edit.before, edit.after, 1)
		if mutant == current {
			t.Fatalf("missing setup refusal control: %q", edit.before)
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
			t.Fatalf("setup refusal accepted wrong construction, missing validation or effects: %q", edit.before)
		}
	}
	if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.After), guard); err == nil || changed {
		t.Fatal("old lifecycle-bearing refusal setup accepted as current")
	}
	return current
}

const q10APISetupImportRefusalCases = `
		{
			name: "import declared current state",
			edit: func(entity map[string]any) { entity["current_state"] = "waiting" },
			wantField: "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import null current state",
			edit: func(entity map[string]any) { entity["current_state"] = nil },
			wantField: "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import empty current state",
			edit: func(entity map[string]any) { entity["current_state"] = "" },
			wantField: "entities[0].current_state",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import declared gate",
			edit: func(entity map[string]any) { entity["gates"] = map[string]any{"review_ready": true} },
			wantField: "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import null gates",
			edit: func(entity map[string]any) { entity["gates"] = nil },
			wantField: "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},
		{
			name: "import empty gates",
			edit: func(entity map[string]any) { entity["gates"] = map[string]any{} },
			wantField: "entities[0].gates",
			wantReason: "data-only imports cannot carry lifecycle inputs",
		},`

func TestNativeAPIPhysicalCardinalityRetainsWorkloadsAssertionsAndExactReadBindings(t *testing.T) {
	for _, row := range nativeAPIPhysicalCardinalityRecipes(t) {
		row = historicalMechanicalRecipe(t, q10APIPhysicalCardinalityCurrentRecipe(t, row))
		switch row.Function {
		case "countAPIIdempotencyRows", "countAllRunRows", "countAllEventRows":
			configuration := map[string][2]string{
				"countAPIIdempotencyRows": {"CountAPICommandReceipts", "count api_idempotency rows: %v"},
				"countAllRunRows":         {"CountPhysicalRuns", "count all run rows: %v"},
				"countAllEventRows":       {"CountPhysicalEvents", "count all event rows: %v"},
			}[row.Function]
			port, message := configuration[0], configuration[1]
			want := "func " + row.Function + "(t *testing.T,selected any) int {t.Helper();count,err:=storetest." + port + "(context.Background(),selected);if err!=nil{t.Fatalf(" + strconv.Quote(message) + ",err)};return count}"
			if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
				t.Fatalf("scalar physical read changed: %s", row.Function)
			}
		case "assertNoRunStartPersistence", "assertNoFlowScopedEventPublishPersistence":
			if nativeAPIEventHelperAssertions(t, row.Before) != nativeAPIEventHelperAssertions(t, row.After) {
				t.Fatalf("refusal assertion changed: %s", row.Function)
			}
			port := "storetest.ReadAPIFlowPublicationRefusalStorage(context.Background(), selected)"
			if row.Function == "assertNoRunStartPersistence" {
				port = "storetest.ReadAPIRunStartRefusalStorage(context.Background(), selected, runID)"
			}
			if !strings.Contains(formattedNativeReadNode(projectionShapeFunction(t, row.After)), port) {
				t.Fatalf("scope/read binding changed: %s", row.Function)
			}
		default:
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("source/API/receipt/workload changed: %s", row.Function)
			}
		}
	}
}
func TestNativeAPIPhysicalCardinalityRejectsLostAssertionsOrWrongOwner(t *testing.T) {
	for _, row := range nativeAPIPhysicalCardinalityRecipes(t) {
		if row.Function == "assertNoRunStartPersistence" || row.Function == "assertNoFlowScopedEventPublishPersistence" {
			broken := strings.Replace(row.After, " != ", " == ", 1)
			if broken == row.After || nativeAPIEventHelperAssertions(t, row.Before) == nativeAPIEventHelperAssertions(t, broken) {
				t.Fatalf("weakened refusal assertion admitted: %s", row.Function)
			}
		} else if strings.HasPrefix(row.Function, "Test") {
			broken := strings.Replace(row.After, "(t, pg)", "(t, wrongOwner)", 1)
			if broken != row.After && nativeAPIPhysicalCardinalityWorkload(t, row.Before) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("wrong physical owner admitted: %s", row.Function)
			}
		}
	}
}
func TestNativeAPIPhysicalCardinalityCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]map[string]int{}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := formattedNativeReadNode(call.Fun)
				switch name {
				case "countAPIIdempotencyRows", "countAllRunRows", "countAllEventRows", "assertNoFlowScopedEventPublishPersistence", "assertNoRunStartPersistence":
					if actual[fn.Name.Name] == nil {
						actual[fn.Name.Name] = map[string]int{}
					}
					actual[fn.Name.Name][name]++
					switch formattedNativeReadNode(call.Args[1]) {
					case "pg", "fixture.pg", "f.store", "sqliteStore", "selected":
					default:
						t.Fatalf("raw/foreign physical-count owner: %s", fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	expected := map[string]map[string]int{
		"TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock":                  {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows":         {"countAPIIdempotencyRows": 1, "countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteRejectsPrivateOrdinaryFlowEndpoint":                        {"countAPIIdempotencyRows": 1, "countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun":                        {"countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteRejectsCallerEntityIDForCreateEntityBeforePersistence":     {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency":                           {"countAPIIdempotencyRows": 1},
		"TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle":                           {"countAllRunRows": 1},
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                        {"countAllRunRows": 1},
		"TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency":                        {"countAPIIdempotencyRows": 3},
		"TestOperatorTestSetupRejectsContractInvalidEntities":                                     {"countAPIIdempotencyRows": 1},
		"testRuntimeNukeLayeredPostgresCapacity":                                                  {"countAPIIdempotencyRows": 1},
		"TestAdministrativeOperationCancellationReleasesLeaseAndCapacity":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorAgentSendDirectiveRunTargetErrors":                                           {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency":                        {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                                 {"countAPIIdempotencyRows": 2, "countAllRunRows": 1, "assertNoRunStartPersistence": 9},
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency":        {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes":              {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient":                          {"countAPIIdempotencyRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed":                           {"assertNoFlowScopedEventPublishPersistence": 2},
		"TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback":                {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":                 {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":              {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence":           {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication": {"countAPIIdempotencyRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence":     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker":             {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence":           {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance":                     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence":       {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":                  {"countAPIIdempotencyRows": 1},
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                             {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":                     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":                  {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplaySubsetAndFailClosedCases":                                         {"countAPIIdempotencyRows": 2},
		"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                                {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact":  {"assertNoRunStartPersistence": 2},
		"TestOperatorRunStartRejectsFlowScopedEventName":                                          {"assertNoRunStartPersistence": 1}}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("physical count callers differ: got=%v want=%v", actual, expected)
	}
}
