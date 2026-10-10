package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const publicNumericOldCounts = "\t\t\t\tout := map[string]int{}\n\t\t\t\tfor _, table := range []string{\"runs\", \"events\", \"event_deliveries\", \"entity_state\", \"api_idempotency\"} {\n\t\t\t\t\tvar n int\n\t\t\t\t\tif err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tout[table] = n\n\t\t\t\t}\n\t\t\t\treturn out\n"
const publicNumericNativeCounts = "\t\t\t\tobserved := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected)\n\t\t\t\treturn map[string]int{\"runs\": observed.Runs, \"events\": observed.Events, \"event_deliveries\": observed.Deliveries,\n\t\t\t\t\t\"entity_state\": observed.Entities, \"api_idempotency\": observed.Receipts}\n"
const publicNumericOldPayload = "\t\t\t\t\t\tvar payload string\n\t\t\t\t\t\tif err := db.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='scan.requested'`, ids.RunID).Scan(&payload); err != nil {\n\t\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t\t}\n"
const publicNumericNativePayload = "\t\t\t\t\t\teventID, err := storetest.ReadLatestNamedEventIdentityStorage(ctx, selected, \"scan.requested\", \"\")\n\t\t\t\t\t\tif err != nil {\n\t\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t\t}\n\t\t\t\t\t\tevidence := storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, ids.RunID, eventID)\n\t\t\t\t\t\tif !evidence.RecordFound || evidence.Record.RunID != ids.RunID || evidence.Record.EventName != \"scan.requested\" || evidence.Record.EventID != eventID {\n\t\t\t\t\t\t\tt.Fatalf(\"numeric operation lost exact stored event identity: %+v\", evidence.Record)\n\t\t\t\t\t\t}\n"

func nativePublicNumericFixtureSource(source string) string {
	source = strings.Replace(source, "\t\t\tvar db *sql.DB\n", "", 1)
	source = strings.Replace(source, "selected, db = s, storetest.DatabaseForTest(s)", "selected = s", 1)
	source = strings.Replace(source, "\t\t\t\t_, db, _ = testutil.StartPostgres(t)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, publicNumericOldCounts, publicNumericNativeCounts, 1)
	source = strings.Replace(source, publicNumericOldPayload, publicNumericNativePayload, 1)
	return strings.Replace(source, "canonicaljson.DecodePreservingNumberLexemes([]byte(payload), &decoded)", "canonicaljson.DecodePreservingNumberLexemes(evidence.Record.Payload, &decoded)", 1)
}

func TestNativePublicNumericReplayRetainsEveryCarrierAndDomainAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-public-numeric-replay-fixture" {
			continue
		}
		matched++
		want, err := canonicalFunction(nativePublicNumericFixtureSource(row.Before))
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != got {
			t.Fatal("numeric replay changed outside exact native fixture ownership")
		}
		for _, raw := range []string{"*sql.DB", "db.Query", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("numeric fixture retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"int(7), int64(7), float64(7)", "json.Number(\"7.0\")", "json.Number(\"7e0\")", "!reflect.DeepEqual(counts(), after)", "!evidence.RecordFound", "evidence.Record.RunID != ids.RunID", "evidence.Record.EventName != \"scan.requested\"", "evidence.Record.EventID != eventID", "DecodePreservingNumberLexemes(evidence.Record.Payload", "observed.Deliveries", "observed.Receipts", "t.Fatal(", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedNumericCut", 1)
			if mutant == row.After {
				t.Fatalf("numeric carrier/cardinality/assertion cut missing: %s", cut)
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("numeric proof mutation accepted: %s", cut)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("numeric native fixture recipes=%d,want1", matched)
	}
}
