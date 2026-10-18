package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const proposedEffectOldOpening = "\tfor _, tc := range []struct {\n\t\tname string\n\t\topen func(*testing.T) (any, *sql.DB)\n\t}{\n\t\t{\n\t\t\tname: \"sqlite\",\n\t\t\topen: func(t *testing.T) (any, *sql.DB) {\n\t\t\t\tselected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())\n\t\t\t\treturn selected, storetest.DatabaseForTest(selected)\n\t\t\t},\n\t\t},\n\t\t{\n\t\t\tname: \"postgres\",\n\t\t\topen: func(t *testing.T) (any, *sql.DB) {\n\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\treturn storetest.AdmitPostgresRuntimeStore(t, db), db\n\t\t\t},\n\t\t},\n\t} {\n"
const proposedEffectNativeOpening = "\tfor _, tc := range []struct {\n\t\tname string\n\t\topen func(*testing.T) any\n\t}{\n\t\t{\n\t\t\tname: \"sqlite\",\n\t\t\topen: func(t *testing.T) any {\n\t\t\t\treturn storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())\n\t\t\t},\n\t\t},\n\t\t{\n\t\t\tname: \"postgres\",\n\t\t\topen: func(t *testing.T) any {\n\t\t\t\treturn storetest.StartPostgresRuntimeStore(t)\n\t\t\t},\n\t\t},\n\t} {\n"
const proposedEffectOldCount = "\tquery := `SELECT\n\t\t(SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'),\n\t\t(SELECT COUNT(*) FROM activity_attempts WHERE run_id = ? AND status = 'succeeded')`\n\targs := []any{runID, runID}\n\tif backend == \"postgres\" {\n\t\tquery = `SELECT\n\t\t\t(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'),\n\t\t\t(SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid AND status = 'succeeded')`\n\t\targs = []any{runID}\n\t}\n\tvar requests, attempts int\n\tif err := db.QueryRowContext(context.Background(), query, args...).Scan(&requests, &attempts); err != nil {\n\t\tt.Fatal(err)\n\t}\n"
const proposedEffectNativeCount = "\tcounts, err := storetest.ReadProposedEffectRunExecutionStorage(context.Background(), selected, runID)\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n"

func nativeProposedEffectPublicSource(source string) string {
	source = strings.Replace(source, proposedEffectOldOpening, proposedEffectNativeOpening, 1)
	source = strings.Replace(source, "persistence, db := tc.open(t)", "persistence := tc.open(t)", 1)
	source = strings.Replace(source, "newProposedEffectMailboxHandler(t, persistence, db, source, fact)", "newProposedEffectMailboxHandler(t, persistence, source, fact)", 1)
	source = strings.Replace(source, "\t\t\tinsertProposedEffectAPIRun(t, fixtureCtx, db, tc.name, runID, fact)\n", "\t\t\tstoretest.RequireRun(t, fixtureCtx, persistence.(storetest.RunFixtureStore), storetest.RunFixture{\n\t\t\t\tOrigin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: fact.BundleHash(), Artifact: bundle.SourceArtifact,\n\t\t\t})\n", 1)
	source = strings.Replace(source, "assertProposedEffectAPIExecutionRows(t, db, tc.name, runID)", "assertProposedEffectAPIExecutionRows(t, persistence, runID)", 1)
	source = strings.Replace(source, "\tdb *sql.DB,\n", "", 1)
	source = strings.Replace(source, "db *sql.DB, backend, runID string", "selected any, runID string", 1)
	source = strings.Replace(source, proposedEffectOldCount, proposedEffectNativeCount, 1)
	source = strings.Replace(source, "if requests != 1 || attempts != 1", "if counts.Requests != 1 || counts.SuccessfulAttempts != 1", 1)
	return strings.Replace(source, "\", requests, attempts)", "\", counts.Requests, counts.SuccessfulAttempts)", 1)
}

func TestNativeProposedEffectPublicFixtureRetainsAdmissionProviderReplayAndCounts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		root := row.Function == "TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores"
		if row.Family != "native-proposed-effect-public-fixture" && !root {
			continue
		}
		matched++
		before := nativeProposedEffectPublicSource(row.Before)
		if root {
			before = nativeMailboxCanonicalEventWorkload(t, row.Before)
		}
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		if root {
			got, afterErr = canonicalFunction(nativeMailboxCanonicalEventWorkload(t, row.After))
		}
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		endpoint := row.After
		if row.Function == "newProposedEffectMailboxHandler" {
			proveProposedEffectJournalSuccessor(t, row, actual, string(source))
			endpoint = row.Successor
		}
		written, writtenErr := canonicalFunction(endpoint)
		if err != nil || afterErr != nil || sourceErr != nil || writtenErr != nil || want != got || value != written {
			t.Fatalf("%s changed public effect workload beyond the exact native owner cuts", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore", "db.Query", "runlifecyclefixture.Require"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("proposed-effect public fixture regained raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"fact.BundleHash()", "bundle.SourceArtifact", "ReadProposedEffectRunExecutionStorage(context.Background(), selected, runID)", "counts.Requests != 1", "counts.SuccessfulAttempts != 1", "calls.Load()", "t.Fatal(", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedEffectCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if root && mutantErr == nil {
				changed, mutantErr = canonicalFunction(nativeMailboxCanonicalEventWorkload(t, mutant))
			}
			if mutantErr == nil && changed == want {
				t.Fatalf("effect source/identity/provider/assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 3 {
		t.Fatalf("public effect recipes=%d,want3", matched)
	}
	if strings.Contains(string(source), "func insertProposedEffectAPIRun(") {
		t.Fatal("retired reconstructed run fixture survived")
	}
}

const proposedEffectJournalInstallation = "\tinstallProposedEffectAPIJournal(t, persistence.(agentfixture.Store), bus, coordinator, fact)\n"

func validateProposedEffectJournalSuccessor(row recipe, actual, source string) error {
	// #2700/6251805014 and 6251798482 preserve both historical endpoints and
	// bind this one-call continuation to its original journal grant and cleanup.
	if row.Family != "native-proposed-effect-public-fixture" ||
		row.File != "internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go" ||
		row.Function != "newProposedEffectMailboxHandler" || row.Removed || row.Mechanical != nil {
		return fmt.Errorf("proposed-effect journal successor has a foreign recipe identity")
	}
	for _, pin := range []struct{ body, digest string }{
		{row.Before, "8debf88c7da65052e2f3315635a7133327cd7e07b40524ff4eb30e1a2d502fa5"},
		{row.After, "b6160a4b5946a508cbc73c00794b735f728f74165c8fb7f5a06097b13d2f6551"},
		{row.Successor, "4938577d8874c655a9056355389397d47af3a30a0df4c38785efb8487e301f46"},
		{source, "5909448c19121ecdcc5b500cb662fb79151f9dd5aa46b4fbde8f5f34ff21bfca"},
	} {
		if fmt.Sprintf("%x", sha256.Sum256([]byte(pin.body))) != pin.digest {
			return fmt.Errorf("proposed-effect journal successor changed a reviewed snapshot or installation source")
		}
	}
	if actual != row.Successor || strings.Count(actual, proposedEffectJournalInstallation) != 1 ||
		strings.Replace(actual, proposedEffectJournalInstallation, "", 1) != row.After {
		return fmt.Errorf("proposed-effect journal successor differs from its exact one-call continuation")
	}
	return nil
}

func proveProposedEffectJournalSuccessor(t *testing.T, row recipe, actual, source string) {
	t.Helper()
	if err := validateProposedEffectJournalSuccessor(row, actual, source); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := checkpointRecipes([]byte("package proof\n"+row.After), []recipe{row})
	if err != nil || len(checkpoint) != 1 {
		t.Fatalf("journal successor checkpoint: rows=%d err=%v", len(checkpoint), err)
	}
	updated, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.After), checkpoint[0])
	if err != nil || !changed || string(updated) != "package proof\n"+actual {
		t.Fatalf("journal successor did not append exactly its one installation: changed=%t err=%v", changed, err)
	}
	if _, changed, err := rewriteFunction(row.File, updated, checkpoint[0]); err != nil || changed {
		t.Fatalf("journal successor is not inert on repetition: changed=%t err=%v", changed, err)
	}
	without := strings.Replace(actual, proposedEffectJournalInstallation, "", 1)
	for name, body := range map[string]string{
		"missing_installation":   without,
		"duplicate_installation": strings.Replace(actual, proposedEffectJournalInstallation, strings.Repeat(proposedEffectJournalInstallation, 2), 1),
		"changed_store":          strings.Replace(actual, "persistence.(agentfixture.Store)", "other.(agentfixture.Store)", 1),
		"changed_bus":            strings.Replace(actual, "bus, coordinator, fact)", "otherBus, coordinator, fact)", 1),
		"changed_coordinator":    strings.Replace(actual, "bus, coordinator, fact)", "bus, otherCoordinator, fact)", 1),
		"changed_fact":           strings.Replace(actual, "bus, coordinator, fact)", "bus, coordinator, otherFact)", 1),
		"changed_helper":         strings.Replace(actual, "installProposedEffectAPIJournal(t,", "installOtherJournal(t,", 1),
		"changed_order":          strings.Replace(without, "\tcoordinator =", proposedEffectJournalInstallation+"\tcoordinator =", 1),
		"unrelated_body":         strings.Replace(actual, "return handler, bus", "return nil, bus", 1),
		"unrelated_assertion":    strings.Replace(actual, "persistence store does not implement MailboxAPIStore", "unreviewed mailbox assertion", 1),
		"foreign_function":       strings.Replace(actual, "func newProposedEffectMailboxHandler(", "func otherMailboxHandler(", 1),
	} {
		t.Run("journal_"+name, func(t *testing.T) {
			if body == actual {
				t.Fatal("control did not change the reviewed source")
			}
			candidate := row
			candidate.Successor = body
			current := strings.Replace(source, actual, body, 1)
			if validateProposedEffectJournalSuccessor(candidate, body, current) == nil ||
				validateProposedEffectJournalSuccessor(row, body, current) == nil {
				t.Fatal("changed journal installation/current-source pair accepted")
			}
		})
	}
	for name, edit := range map[string]func(*recipe){
		"foreign_family": func(row *recipe) { row.Family = "other-family" },
		"foreign_path":   func(row *recipe) { row.File = "internal/apiv1/other_test.go" },
		"foreign_name":   func(row *recipe) { row.Function = "otherMailboxHandler" },
		"before_rewrite": func(row *recipe) { row.Before = row.After },
		"after_rewrite":  func(row *recipe) { row.After = row.Successor },
	} {
		t.Run("journal_"+name, func(t *testing.T) {
			candidate := row
			edit(&candidate)
			if validateProposedEffectJournalSuccessor(candidate, actual, source) == nil {
				t.Fatal("changed journal recipe identity/history accepted")
			}
		})
	}
	for name, replacement := range map[string]string{
		"original_grant": strings.Replace(source, "grant.AdmitExecution(ctx)", "grant.AdmitExecution(context.Background())", 1),
		"helper_cleanup": strings.Replace(source, "bus.WaitForQuiescence(cleanup)", "bus.WaitForQuiescence(ctx)", 1),
		"source_current": source + "\n// Unreviewed source change.\n",
	} {
		t.Run("journal_"+name, func(t *testing.T) {
			if replacement == source || validateProposedEffectJournalSuccessor(row, actual, replacement) == nil {
				t.Fatal("changed original journal helper or source accepted")
			}
		})
	}
}

func TestNativeProposedEffectExecutionOwnerKeepsExactPhysicalPredicates(t *testing.T) {
	var row recipe
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range rows {
		if candidate.Function == "assertProposedEffectAPIExecutionRows" {
			row = candidate
		}
	}
	if row.Family != "native-proposed-effect-public-fixture" {
		t.Fatal("exact proposed effect counter recipe is missing")
	}
	old := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(row.Before, "SELECT\n", "SELECT \n"))
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_activity_result_readback.go", "ReadProposedEffectRunExecutionStorageForTest")
	fresh := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(owner, "SELECT\n", "SELECT \n"))
	normalize := func(query string) string {
		query = regexp.MustCompile(`\s+`).ReplaceAllString(query, "")
		query = strings.ReplaceAll(query, "run_id=$1::uuid", "CAST(run_idASTEXT)=$1")
		return strings.ReplaceAll(query, "run_id=?", "CAST(run_idASTEXT)=$1")
	}
	if len(old) != 2 || len(fresh) != 1 || normalize(old[0]) != normalize(fresh[0]) || normalize(old[1]) != normalize(fresh[0]) {
		t.Fatalf("physical run/status/event predicates changed: %v -> %v", old, fresh)
	}
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "validateSelectedForkStorageIdentity(runID)", "readServedDeliveryObservation(ctx, selected,", "Scan(&observed.Requests, &observed.SuccessfulAttempts)", "return ProposedEffectRunExecutionStorage{}, err"} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("native execution observation lost cut %q", cut)
		}
	}
}
