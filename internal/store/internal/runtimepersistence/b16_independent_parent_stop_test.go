package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
)

// Separate pools and registries force the parent through PostgreSQL advisory
// exclusion, rather than letting a local group-token lookup satisfy the proof.
func TestB16IndependentPostgresParentStopRetainedGroup(t *testing.T) {
	raw, db, connector := newP16RaceStore(t, "postgres")
	fixture, base, group, members, owner, command := prepareP16PublicationGroup(t, raw, db, "postgres", 34)
	ctx, cancel := context.WithTimeout(base, 10*time.Second)
	defer cancel()
	otherDB, err := sql.Open("postgres", connector.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	other := newPostgresStoreWithBackend(mustPostgresBackend(otherDB))
	other.acceptCurrentSchemaForTest()
	registerTestAuthorActivityCatalogForContext(t, other, ctx)
	primary := raw.(*PostgresStore)
	if primary.backend == other.backend || primary.pipelinePostgresOwner.PostgresPipelineClaimsForTest() == other.pipelinePostgresOwner.PostgresPipelineClaimsForTest() {
		t.Fatal("independent parent accidentally shares backend or claim registry")
	}
	before := readStopCommitEvidence(t, db, fixture.runID)
	if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
		t.Fatal(err)
	}
	if err := group.ValidateCommitted(ctx, b16Claims(members)); err != nil {
		t.Fatal(err)
	}
	first, err := group.Settle(ctx, members[:1])
	requireB18Acknowledged(t, first, err, members[:1])
	assertDirectiveReceipt(t, db, members[0].Claim.EventID(), "processed", nil)
	held := readB16Snapshot(t, db)
	stop := func() error {
		_, err := other.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: fixture.runID})
		return err
	}
	err = stop()
	failure, typed := failures.EnvelopeFromError(err)
	if !errors.Is(err, pipelineobligation.ErrBusy) || !typed || failure.Detail.Code != "pipeline_parent_claim_busy" || failure.Detail.Attributes["stage"] != "pipeline_claim" || failure.Detail.Attributes["claim_owner"] != "external" || failure.Detail.Attributes["event_id"] != members[1].Claim.EventID() {
		t.Fatalf("independent stop did not refuse exact advisory-held member: err=%v failure=%+v", err, failure)
	}
	requireB16Snapshot(t, db, held)
	// ValidateCommitted requires the complete immutable membership and current
	// claims, so it cannot validate a suffix after the prefix was consumed.
	// The real suffix acknowledgement proves that refusal preserved its claim.
	last, err := group.Settle(ctx, members[1:])
	requireB18Acknowledged(t, last, err, members[1:])
	if err := group.Close(ctx); err != nil {
		t.Fatal(err)
	}
	prefix := readB16Snapshot(t, db)
	receipts := readB16PostgresMemberReceipts(t, ctx, db, members)
	// This is a new invocation after an acknowledged handoff, not a busy retry
	// loop or a claim that stop preempts live foreground publication authority.
	if err := stop(); err != nil {
		t.Fatalf("independent stop after finite group handoff: %v", err)
	}
	assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 32, 32)
	after := readStopCommitEvidence(t, db, fixture.runID)
	if after.status != "cancelled" || after.control != "stopped" || after.canceled != 1 || after.pending != 0 || after.events != before.events+2 || after.revisions != before.revisions+4 {
		t.Fatalf("publication, two acknowledged segments and stop accounting: before=%+v after=%+v", before, after)
	}
	var cardinality, cursor int
	var status string
	if err := db.QueryRowContext(ctx, `SELECT cardinality,cursor,status FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&cardinality, &cursor, &status); err != nil || cardinality != 34 || cursor != 32 || status != "canceled" {
		t.Fatalf("compact canceled suffix: cardinality=%d cursor=%d status=%s err=%v", cardinality, cursor, status, err)
	}
	final := readB16Snapshot(t, db)
	for _, table := range []string{"events", "fan_out_outcomes"} {
		if !reflect.DeepEqual(prefix[table], final[table]) {
			t.Fatalf("independent stop rewrote immutable prefix table %s", table)
		}
	}
	if got := readB16PostgresMemberReceipts(t, ctx, db, members); !reflect.DeepEqual(receipts, got) {
		t.Fatalf("independent stop rewrote acknowledged receipts: before=%v after=%v", receipts, got)
	}
	for _, member := range members {
		assertDirectiveReceipt(t, db, member.Claim.EventID(), "processed", nil)
	}
	if _, err := owner.LoadFanOutEvaluation(ctx, command.Claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
		t.Fatalf("stopped suffix retained evaluation authority: %v", err)
	}
	if _, err := group.Settle(ctx, members); err == nil {
		t.Fatal("closed retained group recovered settlement authority")
	}
	if err := stop(); !errors.Is(err, runcontrol.ErrAlreadyTerminal) {
		t.Fatalf("duplicate independent stop: %v", err)
	}
	requireB16Snapshot(t, db, final)
}

func readB16PostgresMemberReceipts(t *testing.T, ctx context.Context, db *sql.DB, members []pipelineobligation.PublicationSettlementMember) []string {
	t.Helper()
	rows := make([]string, len(members))
	for i, member := range members {
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY subscriber_type,subscriber_id),'[]'::jsonb)::text FROM event_receipts r WHERE event_id=$1`, member.Claim.EventID()).Scan(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	return rows
}
