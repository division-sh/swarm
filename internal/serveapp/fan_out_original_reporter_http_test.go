package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// These are the original reporter's 500 valid rows followed by its mixed four,
// not the separate six-row supported-consumer proof. Only surrounding fixture
// wiring is adapted for fully verified serve boot by issue2394ServedReporterSource.
func TestIssue2394ServedOriginalReporterFiveHundredBothStores(t *testing.T) {
	runIssue2394OriginalReporterHTTP(t, storetest.TransactionProbeOptions{}, 10*time.Second)
}

func TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores(t *testing.T) {
	runIssue2394OriginalReporterHTTP(t, storetest.TransactionProbeOptions{
		Delay: 300 * time.Millisecond, DelayScope: storetest.DelayAllCommits,
	}, 2*time.Minute)
}

func runIssue2394OriginalReporterHTTP(t *testing.T, transactionOptions storetest.TransactionProbeOptions, issuanceBudget time.Duration) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2394ServedReporterSource(t)
			opts, start := lifecycleRestartHarness(t, backend, root)
			opts.TestLLMRuntime = servedNoopLLMRuntime{}
			var selected any
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
			process, rt := start()
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("served output:\n%s", process.outputString())
				}
			})
			runID := uuid.NewString()
			const portfolio = "portfolio-numeric-valid"
			var started struct {
				RunID string `json:"run_id"`
			}
			issue2394ReporterRPC(t, rt, "run.start", map[string]any{
				"run_id": runID, "bundle_hash": rt.BundleHash, "event_name": "portfolio.opened",
				"payload": map[string]any{"portfolio_id": portfolio, "threshold": 75}, "idempotency_key": uuid.NewString(),
			}, &started)
			if started.RunID != runID {
				t.Fatalf("HTTP start changed run identity: %+v", started)
			}
			waitPublicationSiteCompletion(t, rt, runID)
			var metadataRaw []byte
			if err := rt.DB.QueryRow(`SELECT fields FROM entity_state WHERE run_id=$1 AND flow_instance='portfolio' ORDER BY updated_at DESC LIMIT 1`, runID).Scan(&metadataRaw); err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(metadataRaw, &metadata); err != nil || metadata["portfolio_id"] != portfolio {
				t.Fatalf("original portfolio metadata changed: %s err=%v", metadataRaw, err)
			}
			transactions := storetest.CollectTransactions(t, selected, transactionOptions)
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("failed served reporter transaction snapshot: %+v", transactions.Snapshot())
				}
			})
			wanted := make(map[string]map[string]any)
			issuanceStarted := time.Now()
			for batch := 0; batch < 20; batch++ {
				rows := make([]map[string]any, 0, 25)
				for row := 0; row < 25; row++ {
					ordinal := batch*25 + row
					payload := map[string]any{
						"account_id": fmt.Sprintf("numeric-%03d", ordinal), "eng_roles": ordinal,
						"gem_score": float64(ordinal) + 0.25, "external_id": uuid.NewString(),
					}
					rows = append(rows, payload)
					wanted[payload["account_id"].(string)] = payload
				}
				publishIssue2394ReporterBatch(t, rt, runID, portfolio, rows)
			}
			observed := waitIssue2394ReporterCursor(t, rt, runID, 500)
			// The SQL visibility observation can precede transaction-probe cleanup.
			// Wait for the real receipt, never substitute this poll's wall clock.
			var receipt storetest.TransactionSnapshot
			for deadline := time.Now().Add(5 * time.Minute); ; {
				receipt = transactions.Snapshot()
				if receipt.ByOperation[storetest.TransactionFanOutChunk].WriteCommits >= 20 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("missing acknowledged chunk receipt: %+v", receipt)
				}
				time.Sleep(10 * time.Millisecond)
			}
			claims, chunks := receipt.ByOperation[storetest.TransactionFanOutClaim], receipt.ByOperation[storetest.TransactionFanOutChunk]
			if claims.WriteCommits != 20 || chunks.WriteCommits != 20 || receipt.ByOperation[storetest.TransactionFanOutRelease].WriteCommits != 0 {
				t.Errorf("original 20 chunks require 20 claim + 20 atomic publication commits and no separate release: %+v", receipt)
			}
			if chunks.LastCommitAt.IsZero() || chunks.FirstCommitAt.Before(issuanceStarted) {
				t.Fatalf("commit acknowledgements missing or before first submission: %+v", chunks)
			}
			elapsed := chunks.LastCommitAt.Sub(issuanceStarted)
			t.Logf("HTTP original500 first batch submission -> final durable chunk acknowledgement: %s (target <=%s); cursor observed at %s; delay=%s scope=%s; transaction receipt=%+v",
				elapsed, issuanceBudget, observed.Sub(issuanceStarted), transactionOptions.Delay, transactionOptions.DelayScope, receipt)
			if elapsed > issuanceBudget {
				t.Errorf("original500 HTTP issuance exceeded unchanged %s target: %s", issuanceBudget, elapsed)
			}
			if transactionOptions.Delay > 0 && (chunks.DelayedCommits != 20 || chunks.InjectedDelay != 20*transactionOptions.Delay) {
				t.Errorf("original all-commit delay not applied to every chunk: %+v", chunks)
			}
			valid := waitIssue2394ReporterDiagnosis(t, rt, runID, 500)
			assertIssue2394ReporterSummary(t, valid.FanOut, runID, false)
			assertIssue2394ReporterHistory(t, rt.DB, runID)
			validEvents := assertIssue2394ReporterEvents(t, rt, runID, portfolio, wanted)
			for _, id := range []string{"numeric-000", "numeric-499"} {
				assertIssue2394ReporterEventGet(t, rt, validEvents[id])
			}

			mixed := []map[string]any{
				{"account_id": "mixed-before", "eng_roles": 1, "gem_score": 1.25, "external_id": uuid.NewString()},
				{"account_id": "mixed-empty-uuid", "eng_roles": 2, "gem_score": 2.25, "external_id": ""},
				{"account_id": "mixed-bad-number", "eng_roles": 3, "gem_score": "not-a-number", "external_id": uuid.NewString()},
				{"account_id": "mixed-after", "eng_roles": 4, "gem_score": 4.25, "external_id": uuid.NewString()},
			}
			publishIssue2394ReporterBatch(t, rt, runID, portfolio, mixed)
			waitIssue2394ReporterCursor(t, rt, runID, 504)
			final := waitIssue2394ReporterDiagnosis(t, rt, runID, 504)
			assertIssue2394ReporterSummary(t, final.FanOut, runID, true)
			assertIssue2394ReporterRejections(t, rt.DB, runID)
			wanted["mixed-before"], wanted["mixed-after"] = mixed[0], mixed[3]
			finalEvents := assertIssue2394ReporterEvents(t, rt, runID, portfolio, wanted)
			for id, before := range validEvents {
				if !reflect.DeepEqual(before, finalEvents[id]) {
					t.Fatalf("mixed batch changed original valid event %s", id)
				}
			}
			for _, id := range []string{"mixed-before", "mixed-after"} {
				assertIssue2394ReporterEventGet(t, rt, finalEvents[id])
			}
			assertIssue2394ReporterClients(t, rt, runID, final.FanOut)
		})
	}
}

// A longer transport timeout admits injected commit latency; it does not change
// the first-submission-to-durable-acknowledgement performance assertion above.
func issue2394ReporterRPC(t *testing.T, rt servedControlProofRuntime, method string, params map[string]any, out any) {
	t.Helper()
	response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, method, params, 30*time.Second)
	if response.Error != nil {
		t.Fatalf("%s refused: %+v", method, response.Error)
	}
	if err := json.Unmarshal(response.Result, out); err != nil {
		t.Fatalf("decode %s: %v; %s", method, err, response.Result)
	}
}

func publishIssue2394ReporterBatch(t *testing.T, rt servedControlProofRuntime, runID, portfolio string, rows []map[string]any) {
	t.Helper()
	var result servedEventPublishRPCResult
	issue2394ReporterRPC(t, rt, "event.publish", map[string]any{
		"run_id": runID, "event_name": "portfolio/portfolio.accounts.register.requested",
		"payload": map[string]any{"portfolio_id": portfolio, "account_ids": rows}, "idempotency_key": uuid.NewString(),
	}, &result)
	if result.RunID != runID || result.EventID == "" {
		t.Fatalf("HTTP batch admission lost exact run/event identity: %+v", result)
	}
}

func waitIssue2394ReporterCursor(t *testing.T, rt servedControlProofRuntime, runID string, expected int) time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for {
		var cardinality, cursor, owed, blocked, dead int
		if err := rt.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&cardinality, &cursor, &owed, &blocked); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, runID).Scan(&dead); err != nil {
			t.Fatal(err)
		}
		if dead != 0 || blocked != 0 || cursor > expected || cardinality > expected {
			t.Fatalf("original HTTP reporter cannot reach exact outcomes: cardinality=%d cursor=%d owed=%d blocked=%d dead=%d", cardinality, cursor, owed, blocked, dead)
		}
		if cardinality == expected && cursor == expected && owed == 0 {
			return time.Now()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("original HTTP reporter timed out: cardinality=%d cursor=%d owed=%d: %v", cardinality, cursor, owed, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitIssue2394ReporterDiagnosis(t *testing.T, rt servedControlProofRuntime, runID string, cursor int) cliapp.DiagnosticRunDiagnosisResult {
	t.Helper()
	var result cliapp.DiagnosticRunDiagnosisResult
	for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); {
		issue2394ReporterRPC(t, rt, "run.diagnose", map[string]any{"run_id": runID}, &result)
		f := result.FanOut
		if f.Cursor == cursor && f.Owed == 0 && f.Open == 0 && f.Blocked == 0 && f.Unsettled == 0 && f.BarrierArmed == 0 && f.BarrierPending == 0 &&
			result.TestQuiescence != nil && cliapp.BoolPointerValue(result.TestQuiescence.Ready) {
			return result
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("original HTTP reporter did not settle: %+v", result)
	return result
}

func assertIssue2394ReporterSummary(t *testing.T, f fanoutobligation.RunSummary, runID string, mixed bool) {
	t.Helper()
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
	intents, cardinality, committed, rejected := 20, 500, 500, 0
	if mixed {
		intents, cardinality, committed, rejected = 21, 504, 502, 2
	}
	if f.RunID != runID || f.Intents != intents || f.Cardinality != cardinality || f.Cursor != cardinality || f.Committed != committed || f.Settled != committed || f.SemanticRejected != rejected ||
		f.Unsettled != 0 || f.Owed != 0 || f.Open != 0 || f.Blocked != 0 || f.Canceled != 0 || f.BarrierArmed != 0 || f.BarrierPending != 0 {
		t.Fatalf("original reporter summary changed: %+v", f)
	}
	if !mixed {
		if f.SemanticRejectionSample != nil {
			t.Fatalf("valid rows acquired a rejection sample: %+v", f.SemanticRejectionSample)
		}
		return
	}
	sample := f.SemanticRejectionSample
	if sample == nil || sample.Ordinal != 1 || sample.Failure.Detail.Code != "emit_payload_contract_violation" {
		t.Fatalf("original mixed sample changed: %+v", sample)
	}
	assertIssue2394ReporterRejectionAttributes(t, sample.Failure, 1)
}

func assertIssue2394ReporterEvents(t *testing.T, rt servedControlProofRuntime, runID, portfolio string, wanted map[string]map[string]any) map[string]operatorread.OperatorEventFull {
	t.Helper()
	seen := make(map[string]operatorread.OperatorEventFull)
	ids, cursors := make(map[string]bool), make(map[string]bool)
	cursor := ""
	for {
		params := map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "portfolio/account.registered"}, "limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page operatorread.OperatorEventListResult
		issue2394ReporterRPC(t, rt, "event.list", params, &page)
		for _, event := range page.Events {
			account, _ := event.Payload["account_id"].(string)
			row, present := wanted[account]
			if !present || seen[account].EventID != "" || event.EventID == "" || ids[event.EventID] || event.RunID != runID || event.EventName != "portfolio/account.registered" {
				t.Fatalf("unexpected/duplicate/rejected reporter event: %+v", event)
			}
			expected := map[string]any{"account_id": account, "portfolio_id": portfolio, "eng_roles": float64(row["eng_roles"].(int)), "gem_score": row["gem_score"], "external_id": row["external_id"], "eligible": true}
			if !reflect.DeepEqual(event.Payload, expected) || len(event.Deliveries) != 0 || event.NoDelivery == nil {
				t.Fatalf("original payload/no-delivery evidence changed for %s: %+v want=%v", account, event, expected)
			}
			seen[account], ids[event.EventID] = event, true
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Events) == 0 || cursors[page.NextCursor] {
			t.Fatalf("event pagination made no progress: %+v", page)
		}
		cursors[page.NextCursor], cursor = true, page.NextCursor
	}
	if len(seen) != len(wanted) {
		t.Fatalf("original reporter emitted %d unique events, want %d", len(seen), len(wanted))
	}
	rows, err := rt.DB.Query(`SELECT CAST(event_id AS TEXT),ordinal FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ordinals := make(map[string]int)
	for account, event := range seen {
		ordinal := wanted[account]["eng_roles"].(int) % 25
		if account == "mixed-before" {
			ordinal = 0
		} else if account == "mixed-after" {
			ordinal = 3
		}
		ordinals[event.EventID] = ordinal
	}
	outcomes := make(map[string]bool)
	for rows.Next() {
		var id string
		var ordinal int
		if err := rows.Scan(&id, &ordinal); err != nil {
			t.Fatal(err)
		}
		want, exists := ordinals[id]
		if !exists || outcomes[id] || ordinal != want {
			t.Fatalf("committed outcome lost exact event/ordinal: id=%s ordinal=%d want=%d exists=%t", id, ordinal, want, exists)
		}
		outcomes[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != len(wanted) {
		t.Fatalf("durable unique committed outcomes=%d want=%d", len(outcomes), len(wanted))
	}
	return seen
}

func assertIssue2394ReporterEventGet(t *testing.T, rt servedControlProofRuntime, want operatorread.OperatorEventFull) {
	t.Helper()
	var got operatorread.OperatorEventFull
	issue2394ReporterRPC(t, rt, "event.get", map[string]any{"event_id": want.EventID}, &got)
	if got.EventID != want.EventID || got.RunID != want.RunID || !reflect.DeepEqual(got.Payload, want.Payload) || len(got.Deliveries) != 0 || got.NoDelivery == nil {
		t.Fatalf("original reporter event.get changed payload/no-delivery: %+v want=%+v", got, want)
	}
}

func assertIssue2394ReporterHistory(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	rows, err := db.Query(`SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	observed := make(map[string][]int)
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			t.Fatal(err)
		}
		var fact struct {
			Kind        string `json:"fact_kind"`
			Cursor      int    `json:"cursor"`
			Cardinality int    `json:"cardinality"`
		}
		if err := json.Unmarshal(raw, &fact); err != nil {
			t.Fatal(err)
		}
		if fact.Kind != "intent" {
			continue
		}
		if fact.Cardinality != 25 {
			t.Fatalf("original reporter intent %s cardinality=%d, want25", key, fact.Cardinality)
		}
		observed[key] = append(observed[key], fact.Cursor)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 20 {
		t.Fatalf("original reporter history intents=%d, want20", len(observed))
	}
	for key, cursors := range observed {
		if !reflect.DeepEqual(cursors, []int{0, 25}) {
			t.Fatalf("intent %s cursor history=%v, want one25-item commit after creation", key, cursors)
		}
	}
}

func assertIssue2394ReporterRejections(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	rows, err := db.Query(`SELECT ordinal,CAST(failure AS TEXT),CAST(event_id AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='semantic_rejected' ORDER BY ordinal`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := make(map[int]bool)
	for rows.Next() {
		var ordinal int
		var raw string
		var eventID sql.NullString
		if err := rows.Scan(&ordinal, &raw, &eventID); err != nil {
			t.Fatal(err)
		}
		if (ordinal != 1 && ordinal != 2) || seen[ordinal] || eventID.Valid {
			t.Fatalf("changed rejection ordinal/event: ordinal=%d event=%+v", ordinal, eventID)
		}
		failure, err := runtimefailures.UnmarshalEnvelope([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		assertIssue2394ReporterRejectionAttributes(t, failure, ordinal)
		seen[ordinal] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("durable original rejection ordinals=%v, want1,2", seen)
	}
}

func assertIssue2394ReporterRejectionAttributes(t *testing.T, failure runtimefailures.Envelope, ordinal int) {
	t.Helper()
	want := map[string]string{"event": "portfolio/account.registered", "kind": "schema_mismatch", "path": "$.external_id", "constraint": "format", "expected": "uuid", "actual": ""}
	if ordinal == 2 {
		want["path"], want["constraint"], want["expected"], want["actual"] = "$.gem_score", "type", "number", "string"
	}
	if failure.Detail.Code != "emit_payload_contract_violation" {
		t.Fatalf("rejection ordinal%d changed code: %+v", ordinal, failure)
	}
	for key, value := range want {
		actual, present := failure.Detail.Attributes[key].(string)
		if !present || actual != value {
			t.Fatalf("rejection ordinal%d %s=%#v, want present %q", ordinal, key, failure.Detail.Attributes[key], value)
		}
	}
}

func assertIssue2394ReporterClients(t *testing.T, rt servedControlProofRuntime, runID string, summary fanoutobligation.RunSummary) {
	t.Helper()
	before := issue2394SurfaceSnapshot(t, rt.DB, runID)
	var page fanoutobligation.ListPage
	query := fanoutobligation.ListQuery{RunID: runID, Limit: 100}
	issue2394ReporterRPC(t, rt, "run.fan_out.list", map[string]any{"run_id": runID, "limit": 100}, &page)
	if err := page.Validate(query); err != nil {
		t.Fatal(err)
	}
	if len(page.Intents) != 21 || page.NextCursor != "" {
		t.Fatalf("original HTTP list lost intents: %+v", page)
	}
	for _, intent := range page.Intents {
		if intent.Status != fanoutobligation.StatusClosed || intent.Cursor != intent.Cardinality || intent.Owed != 0 || intent.Runtime.Availability != "available" {
			t.Fatalf("original HTTP list lost settled/live evidence: %+v", intent)
		}
	}
	keys := strings.Fields(issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", runID, "--limit", "100", "--quiet"))
	if len(keys) != len(page.Intents) {
		t.Fatalf("CLI original reporter list has %d keys, want21", len(keys))
	}
	for i, key := range keys {
		if key != page.Intents[i].Key.String() {
			t.Fatalf("CLI original reporter key[%d]=%q want=%q", i, key, page.Intents[i].Key.String())
		}
	}
	var jsonStatus cliapp.DiagnosticRunDiagnosisResult
	output := issue2394SurfaceCLI(t, rt, "run", "status", runID, "--json")
	if err := json.Unmarshal([]byte(output), &jsonStatus); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(jsonStatus.FanOut, summary) || strings.Contains(output, `"rejected":`) {
		t.Fatalf("original API/CLI JSON evidence changed: %s", output)
	}
	human := issue2394SurfaceCLI(t, rt, "run", "status", runID)
	for _, want := range []string{"502 committed items", "2 semantic rejections", "$.external_id", `actual=""`, "emit_payload_contract_violation"} {
		if !strings.Contains(human, want) {
			t.Fatalf("original human status missing %q: %s", want, human)
		}
	}
	if after := issue2394SurfaceSnapshot(t, rt.DB, runID); !reflect.DeepEqual(before, after) {
		t.Fatal("original reporter supported read surfaces changed durable evidence")
	}
}
