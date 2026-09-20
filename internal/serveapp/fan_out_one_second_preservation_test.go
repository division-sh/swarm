package serveapp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// M31 preservation only: two full successive chunks under real commit delay,
// not a third original500 workload or a one-second-latency throughput target.
func TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			if runIssue2394DelayedHTTPProcess(t, backend) {
				return
			}
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
			runID, portfolio := uuid.NewString(), "one-second-preservation"
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
			transactions := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{
				Delay: time.Second, DelayScope: storetest.DelayAllCommits,
			})
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("failed one-second preservation receipt: %+v", transactions.Snapshot())
				}
			})
			rows := make([]map[string]any, 64)
			for ordinal := range rows {
				rows[ordinal] = map[string]any{
					"account_id": fmt.Sprintf("preserve-%02d", ordinal), "eng_roles": ordinal,
					"gem_score": float64(ordinal) + 0.25, "external_id": uuid.NewString(),
				}
			}
			publishIssue2394ReporterBatch(t, rt, runID, portfolio, rows)
			// Read committed headers without changing admission, clocks, leases or
			// serving. The second real delayed turn leaves the first prefix visible.
			seenPrefix := false
			for deadline := time.Now().Add(5 * time.Minute); ; {
				var count, cursor, budget int
				if err := rt.DB.QueryRow(`SELECT COUNT(*),COALESCE(MAX(cursor),0),COALESCE(MIN(next_chunk_size),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&count, &cursor, &budget); err != nil {
					t.Fatal(err)
				}
				if count > 1 || (count == 1 && (budget != 32 || (cursor != 0 && cursor != 32 && cursor != 64))) {
					t.Fatalf("slow successful serving changed chunk geometry: count=%d cursor=%d budget=%d", count, cursor, budget)
				}
				if cursor == 32 {
					seenPrefix = true
				}
				if cursor == 64 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("bounded preservation did not finish: cursor=%d receipt=%+v", cursor, transactions.Snapshot())
				}
				time.Sleep(25 * time.Millisecond)
			}
			if !seenPrefix {
				t.Fatal("did not observe the committed 32-item prefix with restored budget32")
			}
			diagnosis := waitIssue2394ReporterDiagnosis(t, rt, runID, 64)
			f := diagnosis.FanOut
			if err := f.Validate(); err != nil {
				t.Fatal(err)
			}
			if f.RunID != runID || f.Intents != 1 || f.Cardinality != 64 || f.Cursor != 64 || f.Committed != 64 || f.Settled != 64 ||
				f.SemanticRejected != 0 || f.SemanticRejectionSample != nil || f.Canceled != 0 || f.Owed != 0 || f.Open != 0 || f.Blocked != 0 || f.Unsettled != 0 || f.BarrierArmed != 0 || f.BarrierPending != 0 {
				t.Fatalf("one-second delayed exact settlement changed: %+v", f)
			}
			assertIssue2394TwoFullChunkHistory(t, rt, runID)
			assertIssue2394OneSecondPayloads(t, rt, runID, portfolio, rows)
			before := issue2394SurfaceSnapshot(t, rt.DB, runID)
			query := fanoutobligation.ListQuery{RunID: runID, Limit: 100}
			var page fanoutobligation.ListPage
			issue2394ReporterRPC(t, rt, "run.fan_out.list", map[string]any{"run_id": runID, "limit": 100}, &page)
			if err := page.Validate(query); err != nil {
				t.Fatal(err)
			}
			if len(page.Intents) != 1 || page.NextCursor != "" {
				t.Fatalf("one-second preservation lost exact intent: %+v", page)
			}
			intent := page.Intents[0]
			if intent.Status != fanoutobligation.StatusClosed || intent.Cardinality != 64 || intent.Cursor != 64 || intent.NextChunkSize != 32 || intent.Owed != 0 || intent.Retry != nil || intent.ClaimOwner != "" || intent.LeaseExpiresAt != nil || intent.Runtime.Availability != "available" {
				t.Fatalf("one-second preservation lost released cap32 readback: %+v", intent)
			}
			var cliPage fanoutobligation.ListPage
			if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "fan-out", "list", runID, "--json")), &cliPage); err != nil {
				t.Fatal(err)
			}
			if err := cliPage.Validate(query); err != nil {
				t.Fatal(err)
			}
			if len(cliPage.Intents) != 1 || cliPage.Intents[0].Key != intent.Key || cliPage.Intents[0].Cursor != 64 || cliPage.Intents[0].NextChunkSize != 32 || cliPage.Intents[0].Status != fanoutobligation.StatusClosed {
				t.Fatalf("CLI lost exact successive-chunk result: %+v", cliPage)
			}
			var cliStatus cliapp.DiagnosticRunDiagnosisResult
			if err := json.Unmarshal([]byte(issue2394SurfaceCLI(t, rt, "run", "status", runID, "--json")), &cliStatus); err != nil || !reflect.DeepEqual(cliStatus.FanOut, f) {
				t.Fatalf("CLI changed settled summary: %+v err=%v", cliStatus.FanOut, err)
			}
			if after := issue2394SurfaceSnapshot(t, rt.DB, runID); !reflect.DeepEqual(before, after) {
				t.Fatal("supported readback changed durable preservation evidence")
			}
			receipt := transactions.Snapshot()
			for operation, want := range map[storetest.TransactionOperation]uint64{
				storetest.TransactionFanOutClaim: 2, storetest.TransactionFanOutChunk: 2, storetest.TransactionFanOutProducer: 1,
				storetest.TransactionFanOutRelease: 0, storetest.TransactionFanOutRetry: 0, storetest.TransactionFanOutBlock: 0,
			} {
				if got := receipt.ByOperation[operation].WriteCommits; got != want {
					t.Fatalf("%s write commits=%d, want%d: %+v", operation, got, want, receipt)
				}
			}
			for operation, counts := range receipt.ByOperation {
				if counts.Failed != 0 || counts.CommitFailures != 0 || counts.CleanupFailures != 0 || counts.DelayedCommits != counts.ReadCommits+counts.WriteCommits || counts.InjectedDelay != time.Duration(counts.DelayedCommits)*time.Second {
					t.Fatalf("%s did not preserve successful all-commit one-second delay: %+v", operation, counts)
				}
			}
			t.Logf("M31 one-second all-commit preservation: history=[0 32 64], budget32 after each success, exact64 payloads/settlements and HTTP/CLI readback; no throughput target; receipt=%+v", receipt)
		})
	}
}

func assertIssue2394TwoFullChunkHistory(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	history := make(map[string][]int)
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
		if fact.Kind == "intent" {
			if fact.Cardinality != 64 {
				t.Fatalf("one-second preservation changed history cardinality: %+v", fact)
			}
			history[key] = append(history[key], fact.Cursor)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history contains %d intents, want1", len(history))
	}
	for key, cursors := range history {
		if !reflect.DeepEqual(cursors, []int{0, 32, 64}) {
			t.Fatalf("intent %s history=%v, want exact [0 32 64]", key, cursors)
		}
	}
}

func assertIssue2394OneSecondPayloads(t *testing.T, rt servedControlProofRuntime, runID, portfolio string, wanted []map[string]any) {
	t.Helper()
	var page operatorread.OperatorEventListResult
	issue2394ReporterRPC(t, rt, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "portfolio/account.registered"}, "limit": 100}, &page)
	if len(page.Events) != 64 || page.NextCursor != "" {
		t.Fatalf("delayed publication lost exact64 events: %+v", page)
	}
	byID := make(map[string]operatorread.OperatorEventFull)
	for _, event := range page.Events {
		if event.EventID == "" || byID[event.EventID].EventID != "" {
			t.Fatalf("duplicate/empty delayed event identity: %+v", event)
		}
		byID[event.EventID] = event
	}
	rows, err := rt.DB.Query(`SELECT ordinal,CAST(event_id AS TEXT),outcome_kind FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ordinal int
		var id, kind string
		if err := rows.Scan(&ordinal, &id, &kind); err != nil {
			t.Fatal(err)
		}
		if ordinal != count || ordinal >= len(wanted) || kind != "committed" {
			t.Fatalf("delayed outcome changed ordinal/kind: ordinal=%d count=%d kind=%s", ordinal, count, kind)
		}
		event, ok := byID[id]
		row := wanted[ordinal]
		expected := map[string]any{"account_id": row["account_id"], "portfolio_id": portfolio, "eng_roles": float64(ordinal), "gem_score": row["gem_score"], "external_id": row["external_id"], "eligible": true}
		if !ok || event.RunID != runID || event.EventName != "portfolio/account.registered" || !reflect.DeepEqual(event.Payload, expected) || len(event.Deliveries) != 0 || event.NoDelivery == nil {
			t.Fatalf("ordinal%d exact delayed payload/settlement changed: %+v want=%v", ordinal, event, expected)
		}
		delete(byID, id)
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 64 || len(byID) != 0 {
		t.Fatalf("delayed outcomes=%d unmatched events=%d, want64/0", count, len(byID))
	}
}
