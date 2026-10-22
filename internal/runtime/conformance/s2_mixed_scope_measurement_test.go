package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

// Local measurement only: this root is not part of the S2 landing branch.
func TestS2SingleScopeMixedTrafficMeasurementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"batch_only", "live_only", "mixed"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				measureS2SingleScope(t, backend, mode)
			})
		}
	}
}

func measureS2SingleScope(t *testing.T, backend, mode string) {
	p := newSupplementalServingProbe(0)
	f := newSupplementalServingFixture(t, backend, p)
	ctx, runID := f.startRun(t, 0)
	rt := f.runtimes[0]
	batchCount, liveWorkers := 20, 4
	const rowsPerBatch, livePerWorker = 25, 25
	if mode == "batch_only" {
		liveWorkers = 0
	}
	if mode == "live_only" {
		batchCount = 0
	}
	transactions := storetest.CollectTransactions(t, f.selected, storetest.TransactionProbeOptions{})
	defer func() { t.Logf("S2_RAW backend=%s mode=%s transactions=%+v", backend, mode, transactions.Snapshot()) }()
	start := make(chan struct{})
	var workers sync.WaitGroup
	var submitted, liveWithPendingBatch atomic.Int64
	liveDurations := make([]time.Duration, liveWorkers*livePerWorker)
	batchDurations := make([]time.Duration, batchCount)
	if batchCount != 0 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for batch := 0; batch < batchCount; batch++ {
				began := time.Now()
				submitSupplementalIntent(t, f, runID, batch, rowsPerBatch)
				batchDurations[batch] = time.Since(began)
				submitted.Add(1)
			}
		}()
	}
	for worker := 0; worker < liveWorkers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for item := 0; item < livePerWorker; item++ {
				began := time.Now()
				publishNotifyAllChildrenEventAsync(t, ctx, rt, f.sources[0], runID,
					"portfolio.membership.seeded", map[string]any{
						"portfolio_id": runID,
						"account_ids": []map[string]any{{
							"account_id": fmt.Sprintf("live-%d-%d", worker, item),
							"eng_roles":  1, "gem_score": 1.0,
						}},
					})
				liveDurations[worker*livePerWorker+item] = time.Since(began)
				if int64(p.snapshot().Finished) < submitted.Load() {
					liveWithPendingBatch.Add(1)
				}
			}
		}()
	}
	started := time.Now()
	close(start)
	workers.Wait()
	producersElapsed := time.Since(started)
	if t.Failed() {
		return
	}
	for batch := 0; batch < batchCount; batch++ {
		waitSupplementalReceipt(t, p, rowsPerBatch)
	}
	waitNotifyAllChildrenRuntime(t, rt, runID)
	elapsed := time.Since(started)
	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transactions.StopAndJoin(joinCtx); err != nil {
		t.Fatal(err)
	}
	snapshot := transactions.Snapshot()
	if batchCount != 0 {
		assertSupplementalEffects(t, f, runID, 0, batchCount, rowsPerBatch)
	}
	var liveEvents int
	eventName := f.sources[0].ResolveFlowEventReference(".", "portfolio.membership.seeded")
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, runID, eventName).Scan(&liveEvents); err != nil {
		t.Fatal(err)
	}
	if liveEvents != liveWorkers*livePerWorker {
		t.Fatalf("live events=%d, want %d", liveEvents, liveWorkers*livePerWorker)
	}
	assertNotifyAllChildrenRunEventCount(t, ctx, f.db, runID)
	pipeline, err := f.selected.PipelineObligations().SummarizeRun(ctx, runID)
	if err != nil || pipeline.Replayable != 0 || pipeline.Deferred != 0 || pipeline.TerminalNonSuccess != 0 {
		t.Fatalf("unfinished or unsuccessful input: summary=%+v err=%v", pipeline, err)
	}
	stats := p.snapshot()
	if stats.Err != nil || stats.Finished != batchCount || stats.Loaded != 0 || stats.PendingCommits != 0 {
		t.Fatalf("incomplete batch work: %+v", stats)
	}
	report := map[string]any{
		"backend": backend, "mode": mode, "run_id": runID,
		"scope":         "one current physical run, one static portfolio instance, existing compiled flow",
		"batch_intents": batchCount, "batch_rows": batchCount * rowsPerBatch,
		"live_events": liveEvents, "live_workers": liveWorkers,
		"live_acks_while_batch_pending": liveWithPendingBatch.Load(),
		"producer_seconds":              producersElapsed.Seconds(), "drained_seconds": elapsed.Seconds(),
		"live_publish_ack_ms":  s2MeasurementDurations(liveDurations),
		"batch_publish_ack_ms": s2MeasurementDurations(batchDurations),
		"batch_stats":          stats, "pipeline": pipeline, "transactions": snapshot,
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("S2_MEASUREMENT %s", raw)
}

func s2MeasurementDurations(values []time.Duration) map[string]float64 {
	if len(values) == 0 {
		return nil
	}
	values = slices.Sorted(slices.Values(values))
	percentile := func(percent int) float64 {
		index := (len(values)*percent+99)/100 - 1
		return float64(values[index]) / float64(time.Millisecond)
	}
	return map[string]float64{
		"p50": percentile(50), "p95": percentile(95), "p99": percentile(99),
		"max": float64(values[len(values)-1]) / float64(time.Millisecond),
	}
}
