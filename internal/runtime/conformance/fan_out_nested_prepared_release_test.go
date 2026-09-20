package conformance

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Inject one acknowledged precommit refusal only after real evaluation and
// planning. All claim/retry mutations and the later commit use the granted owner.
func TestIssue2394NestedPreparedReleaseBeforeRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			r := &nestedPreparedRejection{
				failure: runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "nested_test_precommit_unavailable", "test", "commit", nil),
				reached: make(chan nestedServingReceipt, 1), resume: make(chan struct{}),
			}
			t.Cleanup(r.open)
			proveNestedCapacityOneHandoff(t, backend, []string{"sibling-c", "sibling-a", "sibling-b"}, true, 5*time.Second, r)
		})
	}
}

type nestedPreparedRejection struct {
	failure  error
	rejected atomic.Bool
	returned atomic.Bool
	reports  atomic.Int32
	matched  atomic.Int32
	resume   chan struct{}
	once     sync.Once
	reached  chan nestedServingReceipt
	planIDs  []string
	receipt  nestedServingReceipt
}

func (r *nestedPreparedRejection) open() { r.once.Do(func() { close(r.resume) }) }

func (r *nestedPreparedRejection) beforeTurn(ctx context.Context) {
	if r.returned.Load() {
		select {
		case <-r.resume:
		case <-ctx.Done():
		}
	}
}

func (r *nestedPreparedRejection) rejectPrepared(command pipeline.FanOutChunkCommand) error {
	if !r.rejected.CompareAndSwap(false, true) {
		return nil
	}
	for _, outcome := range command.Outcomes {
		if outcome.Publication != nil {
			r.planIDs = append(r.planIDs, outcome.Publication.DurablePublicationEventID())
		}
	}
	return r.failure
}

// Cleanup errors joined with the injected refusal remain failures, not an
// expected error merely because errors.Is finds the injected member.
func (r *nestedPreparedRejection) onlyExpected(err error) bool {
	if err == r.failure {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		members := joined.Unwrap()
		if len(members) == 0 {
			return false
		}
		for _, member := range members {
			if !r.onlyExpected(member) {
				return false
			}
		}
		return true
	}
	return false
}

func (r *nestedPreparedRejection) reportExpected(err error) bool {
	if !r.onlyExpected(err) {
		return false
	}
	r.reports.Add(1)
	return true
}

func (r *nestedPreparedRejection) afterTurn(receipt nestedServingReceipt) {
	if r.onlyExpected(receipt.Err) && r.returned.CompareAndSwap(false, true) {
		r.reached <- receipt
	}
}

func (r *nestedPreparedRejection) assertReleasedBeforeRetry(t *testing.T, ctx context.Context, db *sql.DB, p *nestedServingProbe, runID string, cardinality int) {
	t.Helper()
	// This cleanup must precede the runtime's exact registration join.
	t.Cleanup(r.open)
	select {
	case r.receipt = <-r.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("real prepared refusal did not return with exact plan/claim cleanup")
	}
	p.publications.mu.Lock()
	live, acquired, released, returned, lifeErr := len(p.publications.live), p.publications.acquired, p.publications.released, p.publications.returned, p.publications.err
	p.publications.mu.Unlock()
	if live != 0 || acquired != cardinality || released != cardinality || returned != 0 || lifeErr != nil || len(r.planIDs) != cardinality {
		t.Fatalf("prepared refusal disposal: live=%d acquired=%d released=%d returned=%d plans=%v err=%v", live, acquired, released, returned, r.planIDs, lifeErr)
	}
	key := r.receipt.Key
	var cursor, claims, outcomes int
	if err := db.QueryRowContext(ctx, `SELECT cursor,CASE WHEN claim_owner IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&cursor, &claims); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, runID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if cursor != 0 || claims != 0 || outcomes != 0 || !r.receipt.CommittedAt.IsZero() || r.receipt.Publications != 0 {
		t.Fatalf("refusal mutated exact intent: cursor=%d claims=%d outcomes=%d receipt=%+v", cursor, claims, outcomes, r.receipt)
	}
	seen := map[string]bool{}
	for _, id := range r.planIDs {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, runID, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if id == "" || seen[id] || count != 0 {
			t.Fatalf("rejected plan identity=%q duplicate=%v durable events=%d", id, seen[id], count)
		}
		seen[id] = true
	}
	r.open()
}

func (r *nestedPreparedRejection) matchesReceipt(receipt nestedServingReceipt) bool {
	if !r.onlyExpected(receipt.Err) || receipt.Key != r.receipt.Key || !receipt.ReturnedAt.Equal(r.receipt.ReturnedAt) {
		return false
	}
	return r.matched.Add(1) == 1
}

func (r *nestedPreparedRejection) assertFinal(t *testing.T, p *nestedServingProbe, publications int) {
	t.Helper()
	p.publications.mu.Lock()
	defer p.publications.mu.Unlock()
	life := &p.publications
	if r.reports.Load() != 1 || r.matched.Load() != 1 || life.acquired != publications+len(r.planIDs) || life.released != len(r.planIDs) || life.returned != publications {
		t.Fatalf("nested precommit refusal exact final census: reports=%d receipts=%d acquired=%d released=%d returned=%d", r.reports.Load(), r.matched.Load(), life.acquired, life.released, life.returned)
	}
	t.Logf("M29 actual prepared refusal: %d exact plans released with no durable event/outcome/cursor mutation; legal recovery returned %d actual publications with all nested recipient effects", len(r.planIDs), publications)
}
