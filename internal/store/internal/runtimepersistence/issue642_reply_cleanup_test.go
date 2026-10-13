package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestIssue642ReplyOriginEvidencePreventsAncestorPurgeBothStores(t *testing.T) {
	cells := []struct {
		name string
		run  func(*testing.T, *issue642ReplyCleanupFixture, string)
	}{
		{"source_only_refused", issue642ReplyCleanupOriginalRefusal},
		{"source_and_middle_refused_with_grandchild_preserved", issue642ReplyCleanupOriginalRefusal},
		{"enclosing_cross_run_corruption_defense_refused", issue642ReplyCleanupEnclosingRefusal},
		{"runless_preserved_original_evidence_refused", issue642ReplyCleanupRunlessRefusal},
		{"dry_run_no_mutation", issue642ReplyCleanupDryRun},
		{"explicit_complete_set_cleanup", issue642ReplyCleanupCompleteSet},
		{"cancelled_cleanup_rollback", issue642ReplyCleanupCancelled},
		{"late_sql_cleanup_rollback", issue642ReplyCleanupLateSQL},
	}
	for _, entrance := range []string{"postgres/pooled", "postgres/retained", "sqlite/retained"} {
		t.Run(entrance, func(t *testing.T) {
			for _, cell := range cells {
				t.Run(cell.name, func(t *testing.T) {
					f := newIssue642ReplyCleanupFixture(t, strings.HasPrefix(entrance, "postgres"))
					f.entrance = entrance
					cell.run(t, f, cell.name)
				})
			}
		})
	}
}

func TestIssue642ReplyPausedDiscardDependencyRefusalBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		backend := "sqlite"
		if postgres {
			backend = "postgres"
		}
		t.Run(backend, func(t *testing.T) {
			for _, cell := range []string{"original_evidence", "enclosing_cross_run_corruption_defense"} {
				t.Run(cell, func(t *testing.T) {
					f := newIssue642ReplyCleanupFixture(t, postgres)
					runID := f.original
					var preserved replycontext.Record
					if cell == "original_evidence" {
						f.seedChain(t)
						preserved = f.childReply
					} else {
						runID = f.middle
						preserved = f.seedEnclosingCorruptionDefense(t)
					}
					before := f.snapshot(t)
					f.requireNoAncestry(t, before)
					state := issue642ReplyCleanupRow(t, before, "runs", "run_id", runID)
					if state == nil || issue642ReplyCleanupText(t, state, "status") != "paused" {
						t.Fatal("discard proof did not start at an existing paused native run")
					}
					err := f.store.DiscardMaterializedSelectedContractExecutionFork(f.ctx, runID)
					f.requireReplyRefusal(t, err, before)
					f.requireRecord(t, preserved)
				})
			}
		})
	}
}

func TestIssue642ReplyOwnedContextDiscardBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		backend := "sqlite"
		if postgres {
			backend = "postgres"
		}
		t.Run(backend, func(t *testing.T) {
			for _, retained := range []bool{false, true} {
				cell := "whole_run_owned_contexts"
				if retained {
					cell = "retained_owned_context_tombstones"
				}
				t.Run(cell, func(t *testing.T) {
					f := newIssue642ReplyCleanupFixture(t, postgres)
					f.seedChain(t)
					runID := f.middle
					var executionID string
					if retained {
						runID, executionID = f.prepareRetainedDiscard(t)
					}
					f.seedOwnedNestedReplies(t, runID)
					owned := f.inventory(t, runID)
					wantContexts := 3 // One context-only row plus the two fresh owned rows.
					if retained {
						wantContexts = 2
					}
					if len(owned) != wantContexts {
						t.Fatalf("owned discard inventory=%d, want %d", len(owned), wantContexts)
					}
					before := f.snapshot(t)
					if err := f.store.DiscardMaterializedSelectedContractExecutionFork(f.ctx, runID); err != nil {
						t.Fatalf("discard exact owned reply contexts: %v", err)
					}
					if remaining := f.inventory(t, runID); len(remaining) != 0 {
						t.Fatalf("discard retained owned reply contexts: %+v", remaining)
					}
					after := f.snapshot(t)
					f.requireOutsideRepliesUnchanged(t, before, after, runID)
					for _, original := range owned {
						if original.SourceRunID == runID {
							if issue642ReplyCleanupRow(t, after, "events", "event_id", original.SourceRequestEventID) != nil {
								t.Fatalf("discard retained deleted run's original request %s", original.SourceRequestEventID)
							}
							continue
						}
						prior := issue642ReplyCleanupRow(t, before, "events", "event_id", original.SourceRequestEventID)
						current := issue642ReplyCleanupRow(t, after, "events", "event_id", original.SourceRequestEventID)
						if prior == nil || !reflect.DeepEqual(prior, current) {
							t.Fatalf("discard changed external original request %s", original.SourceRequestEventID)
						}
					}
					if retained {
						issue642ReplyRequireRetainedTombstones(t, before, after, runID, executionID, owned)
						if err := f.store.DiscardMaterializedSelectedContractExecutionFork(f.ctx, runID); err == nil {
							t.Fatal("repeat retained discard admitted an already terminal run")
						}
					} else {
						if issue642ReplyCleanupRow(t, after, "runs", "run_id", runID) != nil {
							t.Fatal("unretained owned-context discard kept the paused run")
						}
						if err := f.store.DiscardMaterializedSelectedContractExecutionFork(f.ctx, runID); err != nil {
							t.Fatalf("repeat absent-run discard: %v", err)
						}
					}
					f.requireUnchanged(t, after)
				})
			}
		})
	}
}

type issue642ReplyCleanupFixture struct {
	store      selectedForkDiscardStore
	db         *sql.DB
	postgres   bool
	ctx        context.Context
	entrance   string
	process    startupownership.ProcessCapability
	original   string
	middle     string
	child      string
	preserved  string
	events     map[string]string
	childReply replycontext.Record
	control    replycontext.Record
	runless    replycontext.Record
}

func newIssue642ReplyCleanupFixture(t *testing.T, postgres bool) *issue642ReplyCleanupFixture {
	t.Helper()
	backend := "sqlite"
	if postgres {
		backend = "postgres"
	}
	store, db, _ := selectedForkDiscardTestStore(t, backend)
	if postgres {
		registerTestAuthorActivityCatalog(t, store.(*PostgresStore))
	}
	f := &issue642ReplyCleanupFixture{
		store: store, db: db, postgres: postgres, ctx: testAuthorActivityContext(),
		original: uuid.NewString(), middle: uuid.NewString(), child: uuid.NewString(), preserved: uuid.NewString(),
		events: make(map[string]string),
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, runID := range []string{f.original, f.middle, f.child, f.preserved} {
		requirePausedRunForTest(t, f.ctx, store, runID, now)
		f.events[runID] = uuid.NewString()
		event := eventtest.PersistedProjectionForProducer(
			f.events[runID], "provider.requested", eventtest.Producer(events.EventProducerPlatform, "test"), "",
			json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, now,
		)
		if err := commitSemanticEventFixture(f.ctx, store, event); err != nil {
			t.Fatalf("seed canonical original request: %v", err)
		}
	}
	return f
}

func (f *issue642ReplyCleanupFixture) replies(t *testing.T) replyContextStoreTestSurface {
	t.Helper()
	owner, ok := f.store.(replyContextStoreTestSurface)
	if !ok {
		t.Fatalf("native cleanup store lacks reply owner: %T", f.store)
	}
	return owner
}

func (f *issue642ReplyCleanupFixture) insertPhysical(t *testing.T, record replycontext.Record) {
	t.Helper()
	// A physical constraint fixture is not fork projection or publication admission.
	err := runUnrevisionedEventFixtureTransactionForTest(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		inserted, err := replycontextstore.InsertConstraintFixtureForTest(ctx, tx, f.postgres, record)
		if err != nil {
			return err
		}
		if !inserted {
			return errors.New("reply cleanup constraint fixture was not newly inserted")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed reply physical dependency: %v", err)
	}
	f.requireRecord(t, record)
}

func (f *issue642ReplyCleanupFixture) seedChain(t *testing.T) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	original := replyContextFreezeTestRecord(f.events[f.original], f.original, "cleanup-original", now)
	if err := f.replies(t).CreateReplyContext(f.ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{f.middle, f.child} {
		projected := replyContextFreezeTestRecord(uuid.NewString(), runID, "cleanup-context-only", now)
		projected.SourceRunID, projected.SourceRequestEventID = f.original, f.events[f.original]
		f.insertPhysical(t, projected)
		if _, found, err := ReadCanonicalEventRecordForTest(f.ctx, f.store, projected.RequestEventID); err != nil || found {
			t.Fatalf("retention fixture acquired a copied logical request: found=%v err=%v", found, err)
		}
		if runID == f.child {
			f.childReply = projected
		}
	}
	inner := replyContextFreezeTestRecord(uuid.NewString(), f.child, "cleanup-inner", now)
	inner.SourceRunID, inner.SourceRequestEventID = f.original, f.events[f.original]
	inner.RequesterFlowID = f.childReply.ProviderFlowID
	inner.ID = replycontext.DeterministicID(inner.RequestEventID, inner.RequesterFlowID, inner.RequestOutputPin, inner.ReplyInputPin, inner.ProviderFlowID, inner.Origin)
	if err := inner.BindEnclosing(f.childReply); err != nil {
		t.Fatal(err)
	}
	f.insertPhysical(t, inner)
	f.control = replyContextFreezeTestRecord(f.events[f.preserved], f.preserved, "cleanup-preserved", now)
	if err := f.replies(t).CreateReplyContext(f.ctx, f.control); err != nil {
		t.Fatal(err)
	}
	runlessEvent := uuid.NewString()
	seedReplyRunlessPhysicalEvent(t, f.ctx, f.replies(t), runlessEvent)
	f.runless = replyContextFreezeTestRecord(uuid.NewString(), "", "cleanup-runless", now)
	f.runless.SourceRequestEventID = runlessEvent
	f.insertPhysical(t, f.runless)
}

func (f *issue642ReplyCleanupFixture) seedEnclosingCorruptionDefense(t *testing.T) replycontext.Record {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	parent := replyContextFreezeTestRecord(f.events[f.middle], f.middle, "cleanup-enclosing", now)
	if err := f.replies(t).CreateReplyContext(f.ctx, parent); err != nil {
		t.Fatal(err)
	}
	child := replyContextFreezeTestRecord(f.events[f.child], f.child, "cleanup-hostile-enclosing", now)
	child.RequesterFlowID = parent.ProviderFlowID
	child.ID = replycontext.DeterministicID(child.RequestEventID, child.RequesterFlowID, child.RequestOutputPin, child.ReplyInputPin, child.ProviderFlowID, child.Origin)
	child.EnclosingReplyContextID = parent.ID
	if err := child.ValidateEnclosing(parent); err == nil {
		t.Fatal("cross-run enclosing fixture unexpectedly became lawful publication")
	}
	// The original pair belongs to C, so only the hostile B enclosing reference
	// can forbid B cleanup. This exercises corruption defense, not lawful routing.
	f.insertPhysical(t, child)
	return child
}

func (f *issue642ReplyCleanupFixture) inventory(t *testing.T, runID string) []replycontext.Record {
	t.Helper()
	records, err := ReadReplyContextRecordsForTest(f.ctx, f.store, runID)
	if err != nil {
		t.Fatalf("read exact owned reply inventory: %v", err)
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if record.RunID != runID || seen[record.ID] {
			t.Fatalf("reply inventory contains foreign or duplicate row: %+v", record)
		}
		seen[record.ID] = true
	}
	return records
}

func (f *issue642ReplyCleanupFixture) seedOwnedNestedReplies(t *testing.T, runID string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	requestID := f.commitOwnedReplyEvent(t, runID, "provider.requested", now)
	outer := replyContextFreezeTestRecord(requestID, runID, "discard-owned-outer", now)
	if err := f.replies(t).CreateReplyContext(f.ctx, outer); err != nil {
		t.Fatalf("create owned outer reply: %v", err)
	}
	innerRequest := f.commitOwnedReplyEvent(t, runID, "provider.requested", now)
	inner := replyContextFreezeTestRecord(innerRequest, runID, "discard-owned-inner", now)
	inner.RequesterFlowID, inner.ProviderFlowID = outer.ProviderFlowID, "inner-provider"
	inner.ID = replycontext.DeterministicID(inner.RequestEventID, inner.RequesterFlowID, inner.RequestOutputPin, inner.ReplyInputPin, inner.ProviderFlowID, inner.Origin)
	if err := inner.BindEnclosing(outer); err != nil {
		t.Fatal(err)
	}
	if err := f.replies(t).CreateReplyContext(f.ctx, inner); err != nil {
		t.Fatalf("create owned nested reply: %v", err)
	}
	answer := f.commitOwnedReplyEvent(t, runID, "provider.replied", now)
	terminal, outcome, err := f.replies(t).ClaimReplyContext(f.ctx, inner.ID, answer)
	if err != nil || outcome != replycontext.ClaimAccepted || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID != answer || terminal.EnclosingReplyContextID != outer.ID {
		t.Fatalf("settle owned inner reply: record=%+v outcome=%s err=%v", terminal, outcome, err)
	}
	if terminal.TerminalAt == nil || !terminal.UpdatedAt.Equal(*terminal.TerminalAt) {
		t.Fatalf("owned terminal claim lost its exact settlement clocks: %+v", terminal)
	}
	if f.postgres {
		// The scalar claim result retains Go precision; PostgreSQL stores microseconds.
		expectedTerminalAt := runlifecycle.CanonicalTimestamp(*terminal.TerminalAt)
		terminal.TerminalAt = &expectedTerminalAt
		terminal.UpdatedAt = runlifecycle.CanonicalTimestamp(terminal.UpdatedAt)
	}
	f.requireRecord(t, outer)
	f.requireRecord(t, terminal)
}

func (f *issue642ReplyCleanupFixture) commitOwnedReplyEvent(t *testing.T, runID, name string, at time.Time) string {
	t.Helper()
	id := uuid.NewString()
	event := eventtest.PersistedProjectionForProducer(id, events.EventType(name), eventtest.Producer(events.EventProducerPlatform, "test"), "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
	if err := commitSemanticEventFixture(f.ctx, f.store, event); err != nil {
		t.Fatalf("commit owned reply evidence: %v", err)
	}
	return id
}

func (f *issue642ReplyCleanupFixture) prepareRetainedDiscard(t *testing.T) (string, string) {
	t.Helper()
	fixture := newSelectedProviderCompletionFixture(t, f.store, f.db, !f.postgres)
	issued, err := f.store.IssueRunForkSelectedContractRuntimeExecution(f.ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := f.store.ClaimRunForkSelectedContractRuntimeExecution(f.ctx, issued, "reply-owned-discard", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	admitSelectedProviderFixture(t, f.ctx, fixture, issued, authority)
	authority.Target = selectedProviderTarget(fixture)
	completionCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(f.ctx, authority), newCompletionControllerForTest(f.store)),
		"selected:reply-owned-discard",
	)
	completionCtx = selectedProviderClaimContext(t, completionCtx, fixture, authority)
	completionCtx = managedSelectedExecutionStoreTestContext(t, completionCtx, authority)
	completionCtx = withManagedCompletionTestSurface(t, completionCtx, authority, "openai_compatible")
	handle, err := beginManagedCompletionForTest(t, completionCtx, "openai_compatible", []byte("reply-owned-discard"))
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkLaunched(completionCtx); err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkResponseObserved(completionCtx, map[string]any{"response_fingerprint": "reply-owned-discard"}); err != nil {
		t.Fatal(err)
	}
	settleSelectedCompletionForTest(t, completionCtx, handle, authority.Target, time.Now().UTC())
	if err := f.store.QuiesceRunForkSelectedContractRuntimeExecution(f.ctx, authority); err != nil {
		t.Fatal(err)
	}
	if acknowledged, err := f.store.CloseRunForkSelectedContractRuntimeExecution(f.ctx, authority.ID); !acknowledged || err != nil {
		t.Fatalf("close acknowledged retained completion: acknowledged=%v err=%v", acknowledged, err)
	}
	return fixture.forkRun, issued.ExecutionID
}

func (f *issue642ReplyCleanupFixture) requireOutsideRepliesUnchanged(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, deletedRun string) {
	t.Helper()
	for _, record := range []replycontext.Record{f.childReply, f.control, f.runless} {
		f.requireRecord(t, record)
	}
	preservedRows := func(snapshot map[string]SelectedForkStorageTableSnapshot) []map[string]SelectedForkSnapshotCell {
		rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["reply_contexts"])
		if err != nil {
			t.Fatal(err)
		}
		var kept []map[string]SelectedForkSnapshotCell
		for _, row := range rows {
			if issue642ReplyCleanupText(t, row, "run_id") != deletedRun {
				kept = append(kept, row)
			}
		}
		return kept
	}
	if !reflect.DeepEqual(preservedRows(before), preservedRows(after)) {
		t.Fatal("owned-run discard changed a source, sibling or runless reply row")
	}
	for _, runID := range []string{f.original, f.middle, f.child, f.preserved} {
		if runID == deletedRun {
			continue
		}
		for _, identity := range []struct{ table, key, id string }{
			{"runs", "run_id", runID}, {"events", "event_id", f.events[runID]},
		} {
			prior := issue642ReplyCleanupRow(t, before, identity.table, identity.key, identity.id)
			current := issue642ReplyCleanupRow(t, after, identity.table, identity.key, identity.id)
			if prior == nil || !reflect.DeepEqual(prior, current) {
				t.Fatalf("owned discard changed preserved %s/%s", identity.table, identity.id)
			}
		}
	}
	priorRunlessEvent := issue642ReplyCleanupRow(t, before, "events", "event_id", f.runless.SourceRequestEventID)
	retainedRunlessEvent := issue642ReplyCleanupRow(t, after, "events", "event_id", f.runless.SourceRequestEventID)
	if priorRunlessEvent == nil || !reflect.DeepEqual(priorRunlessEvent, retainedRunlessEvent) {
		t.Fatal("owned discard changed the lawful runless original event")
	}
}

func (f *issue642ReplyCleanupFixture) request(t *testing.T, runIDs ...string) destructivereset.CleanupRequest {
	t.Helper()
	if f.process == nil {
		var err error
		f.process, err = f.store.(startupownership.Store).AcquireProcessCapability(f.ctx, testStartupAcquireRequest("issue642-reply-cleanup"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.process.Release(context.Background()); err != nil {
				t.Errorf("release reply cleanup process: %v", err)
			}
		})
	}
	return admitRetainedResetCleanupProofWithContext(t, f.ctx, f.process, f.store.(destructivereset.QuiescenceStore), runIDs[0], false, runIDs[1:]...)
}

func (f *issue642ReplyCleanupFixture) apply(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	if f.entrance == "postgres/pooled" {
		return f.store.(*PostgresStore).ApplyDestructiveResetCleanup(ctx, req)
	}
	return f.process.ApplyDestructiveResetCleanup(ctx, req, nil)
}

func (f *issue642ReplyCleanupFixture) snapshot(t *testing.T) map[string]SelectedForkStorageTableSnapshot {
	t.Helper()
	witness, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	return witness
}

func (f *issue642ReplyCleanupFixture) requireUnchanged(t *testing.T, before map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	after := f.snapshot(t)
	if reflect.DeepEqual(before, after) {
		return
	}
	for table, rows := range before {
		if !reflect.DeepEqual(rows, after[table]) {
			t.Fatalf("reply cleanup changed %s: before=%+v after=%+v", table, rows, after[table])
		}
	}
	t.Fatal("reply cleanup changed the physical table inventory")
}

func (f *issue642ReplyCleanupFixture) requireNoAncestry(t *testing.T, snapshot map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	for _, runID := range []string{f.original, f.middle, f.child} {
		row := issue642ReplyCleanupRow(t, snapshot, "runs", "run_id", runID)
		if row == nil || issue642ReplyCleanupText(t, row, "forked_from_run_id") != "" {
			t.Fatal("run ancestry could mask the exact reply dependency guard")
		}
	}
}

func (f *issue642ReplyCleanupFixture) requireRecord(t *testing.T, expected replycontext.Record) {
	t.Helper()
	actual, err := f.replies(t).LoadReplyContext(f.ctx, expected.ID)
	if err != nil || !reflect.DeepEqual(actual, expected.Normalized()) {
		t.Fatalf("retained reply row changed: actual=%+v expected=%+v err=%v", actual, expected, err)
	}
}

func (f *issue642ReplyCleanupFixture) requireReplyRefusal(t *testing.T, err error, before map[string]SelectedForkStorageTableSnapshot) {
	t.Helper()
	if !errors.Is(err, destructivereset.ErrInvalidRequest) || !strings.Contains(err.Error(), "cannot delete reply evidence while context") {
		t.Fatalf("cleanup did not reach the reply dependency refusal: %v", err)
	}
	f.requireUnchanged(t, before)
}

func issue642ReplyCleanupOriginalRefusal(t *testing.T, f *issue642ReplyCleanupFixture, cell string) {
	f.seedChain(t)
	runIDs := []string{f.original}
	if cell == "source_and_middle_refused_with_grandchild_preserved" {
		runIDs = append(runIDs, f.middle)
	}
	req := f.request(t, runIDs...)
	before := f.snapshot(t)
	f.requireNoAncestry(t, before)
	_, err := f.apply(f.ctx, req)
	f.requireReplyRefusal(t, err, before)
	f.requireRecord(t, f.childReply)
}

func issue642ReplyCleanupEnclosingRefusal(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	child := f.seedEnclosingCorruptionDefense(t)
	req := f.request(t, f.middle)
	before := f.snapshot(t)
	f.requireNoAncestry(t, before)
	_, err := f.apply(f.ctx, req)
	f.requireReplyRefusal(t, err, before)
	f.requireRecord(t, child)
}

func issue642ReplyCleanupRunlessRefusal(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	child := replyContextFreezeTestRecord(uuid.NewString(), "", "cleanup-runless-original", time.Now().UTC().Truncate(time.Microsecond))
	child.SourceRunID, child.SourceRequestEventID = f.original, f.events[f.original]
	// Runtime runless creation remains refused; only the existing physical
	// constraint fixture supplies this preserved NULL-run dependency control.
	f.insertPhysical(t, child)
	req := f.request(t, f.original)
	before := f.snapshot(t)
	f.requireNoAncestry(t, before)
	_, err := f.apply(f.ctx, req)
	f.requireReplyRefusal(t, err, before)
	f.requireRecord(t, child)
}

func issue642ReplyCleanupDryRun(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	f.seedChain(t)
	req := f.request(t, f.original)
	req.Result.DryRun = true
	before := f.snapshot(t)
	result, err := f.apply(f.ctx, req)
	if err != nil || !result.DryRun || !reflect.DeepEqual(result.RunIDs, []string{f.original}) {
		t.Fatalf("dry-run cleanup: result=%+v err=%v", result, err)
	}
	var contexts, originalEvents int64
	for _, table := range result.Tables {
		if table.DeletedRows != 0 {
			t.Fatalf("dry-run reported physical deletion: %+v", table)
		}
		switch table.Table {
		case "reply_contexts":
			contexts = table.MatchedRows
		case "events":
			originalEvents = table.MatchedRows
		}
	}
	if contexts != 1 || originalEvents != 1 {
		t.Fatalf("dry-run widened its owned-row predicates: contexts=%d events=%d", contexts, originalEvents)
	}
	f.requireUnchanged(t, before)
}

func issue642ReplyCleanupCompleteSet(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	f.seedChain(t)
	req := f.request(t, f.original, f.middle, f.child)
	before := f.snapshot(t)
	result, err := f.apply(f.ctx, req)
	if err != nil || result.DryRun || len(result.RunIDs) != 3 {
		t.Fatalf("explicit complete-set cleanup: result=%+v err=%v", result, err)
	}
	allowed := map[string]bool{f.original: true, f.middle: true, f.child: true}
	for _, runID := range result.RunIDs {
		if !allowed[runID] {
			t.Fatalf("cleanup silently expanded or duplicated its run set: %q", runID)
		}
		delete(allowed, runID)
	}
	after := f.snapshot(t)
	for _, runID := range []string{f.original, f.middle, f.child} {
		if issue642ReplyCleanupRow(t, after, "runs", "run_id", runID) != nil || issue642ReplyCleanupRow(t, after, "events", "event_id", f.events[runID]) != nil {
			t.Fatalf("complete-set cleanup retained source evidence for %s", runID)
		}
	}
	var deletedContexts int64
	for _, table := range result.Tables {
		if table.Table == "reply_contexts" {
			deletedContexts = table.DeletedRows
		}
	}
	if deletedContexts != 4 {
		t.Fatalf("complete-set cleanup deleted %d contexts, want the four owned rows only", deletedContexts)
	}
	for _, control := range []replycontext.Record{f.control, f.runless} {
		f.requireRecord(t, control)
		for _, identity := range []struct{ table, key, id string }{
			{"reply_contexts", "reply_context_id", control.ID},
			{"events", "event_id", control.SourceRequestEventID},
		} {
			oldRow := issue642ReplyCleanupRow(t, before, identity.table, identity.key, identity.id)
			newRow := issue642ReplyCleanupRow(t, after, identity.table, identity.key, identity.id)
			if oldRow == nil || !reflect.DeepEqual(oldRow, newRow) {
				t.Fatalf("complete-set cleanup changed preserved %s/%s", identity.table, identity.id)
			}
		}
	}
	preservedBefore := issue642ReplyCleanupRow(t, before, "runs", "run_id", f.preserved)
	preservedAfter := issue642ReplyCleanupRow(t, after, "runs", "run_id", f.preserved)
	if preservedBefore == nil || !reflect.DeepEqual(preservedBefore, preservedAfter) {
		t.Fatal("complete-set cleanup changed the unrelated preserved run")
	}
}

func issue642ReplyCleanupCancelled(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	f.seedChain(t)
	req := f.request(t, f.original, f.middle, f.child)
	before := f.snapshot(t)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	_, err := f.apply(ctx, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled native cleanup: %v", err)
	}
	// This proves entry cancellation, not cancellation after a SQL mutation.
	f.requireUnchanged(t, before)
}

func issue642ReplyCleanupLateSQL(t *testing.T, f *issue642ReplyCleanupFixture, _ string) {
	f.seedChain(t)
	req := f.request(t, f.original, f.middle, f.child)
	// The existing event-delete trigger fires after group-4 context deletion,
	// so this is a late SQL rollback rather than an early dependency refusal.
	installSelectedForkDiscardFailure(t, f.ctx, f.db, !f.postgres)
	before := f.snapshot(t)
	_, err := f.apply(f.ctx, req)
	if err == nil || !strings.Contains(err.Error(), "injected selected discard failure") {
		t.Fatalf("cleanup did not reach the late event-delete fault: %v", err)
	}
	f.requireUnchanged(t, before)
	f.requireRecord(t, f.childReply)
	if f.entrance != "postgres/pooled" {
		operation, err := f.process.ReadResetOperation(f.ctx, req.OperationID)
		if err != nil || operation.Phase != destructivereset.PhaseQuiesced || operation.Cleanup != nil {
			t.Fatalf("late SQL failure advanced the retained cleanup receipt: %+v %v", operation, err)
		}
	}
}

func issue642ReplyRequireRetainedTombstones(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, runID, executionID string, owned []replycontext.Record) {
	t.Helper()
	priorRun := issue642ReplyCleanupRow(t, before, "runs", "run_id", runID)
	retainedRun := issue642ReplyCleanupRow(t, after, "runs", "run_id", runID)
	if priorRun == nil || retainedRun == nil || issue642ReplyCleanupText(t, priorRun, "status") != "paused" || issue642ReplyCleanupText(t, retainedRun, "status") != "cancelled" {
		t.Fatal("retained owned-context discard did not keep the exact cancelled run tombstone")
	}
	priorExecution := issue642ReplyCleanupRow(t, before, "run_fork_selected_contract_runtime_executions", "execution_id", executionID)
	retainedExecution := issue642ReplyCleanupRow(t, after, "run_fork_selected_contract_runtime_executions", "execution_id", executionID)
	if priorExecution == nil || issue642ReplyCleanupText(t, priorExecution, "state") != "closed" || !reflect.DeepEqual(priorExecution, retainedExecution) {
		t.Fatal("retained owned-context discard changed the acknowledged closed executor evidence")
	}
	priorHead := issue642ReplyCleanupRow(t, before, "run_fork_revision_heads", "run_id", runID)
	currentHead := issue642ReplyCleanupRow(t, after, "run_fork_revision_heads", "run_id", runID)
	if priorHead == nil || currentHead == nil {
		t.Fatal("retained reply discard lost its historical revision head")
	}
	priorRevision := issue642ReplyCleanupInteger(t, priorHead, "last_revision")
	currentRevision := issue642ReplyCleanupInteger(t, currentHead, "last_revision")
	if priorRevision < 1 || currentRevision != priorRevision+1 {
		t.Fatalf("retained reply discard revisions=%d -> %d, want one atomic successor", priorRevision, currentRevision)
	}
	latest := make(map[string]int64, len(owned))
	for _, record := range owned {
		latest[record.ID] = 0
	}
	priorFacts, err := DecodeSelectedForkSnapshotRowsForTest(before["run_fork_fact_revisions"])
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range priorFacts {
		if issue642ReplyCleanupText(t, row, "run_id") != runID || issue642ReplyCleanupText(t, row, "family") != string(runforkrevision.FamilyReplyContexts) {
			continue
		}
		id := issue642ReplyCleanupText(t, row, "fact_key")
		previous, expected := latest[id]
		if !expected {
			t.Fatalf("retained discard fixture contains an unowned historical reply %s", id)
		}
		revision := issue642ReplyCleanupInteger(t, row, "revision")
		if revision > previous {
			if !issue642ReplyCleanupBoolean(t, row, "present") {
				t.Fatalf("owned reply %s was already tombstoned before discard", id)
			}
			latest[id] = revision
		}
	}
	for id, revision := range latest {
		if revision < 1 || revision > priorRevision {
			t.Fatalf("owned reply %s had no admitted historical fact before discard", id)
		}
	}
	facts, err := DecodeSelectedForkSnapshotRowsForTest(after["run_fork_fact_revisions"])
	if err != nil {
		t.Fatal(err)
	}
	tombstones := make(map[string]int, len(owned))
	for _, row := range facts {
		if issue642ReplyCleanupText(t, row, "run_id") != runID || issue642ReplyCleanupText(t, row, "family") != string(runforkrevision.FamilyReplyContexts) || issue642ReplyCleanupInteger(t, row, "revision") != currentRevision {
			continue
		}
		id := issue642ReplyCleanupText(t, row, "fact_key")
		if _, expected := latest[id]; !expected || issue642ReplyCleanupBoolean(t, row, "present") {
			t.Fatalf("retained cleanup broadened reply history or omitted a tombstone for %s", id)
		}
		tombstones[id]++
	}
	for id := range latest {
		if tombstones[id] != 1 {
			t.Fatalf("owned reply %s has %d tombstones at the discard revision, want one", id, tombstones[id])
		}
	}
}

func issue642ReplyCleanupInteger(t *testing.T, row map[string]SelectedForkSnapshotCell, column string) int64 {
	t.Helper()
	cell, exists := row[column]
	if !exists || cell.Type != "int64" {
		t.Fatalf("native cleanup row has no integer column %s", column)
	}
	var value int64
	if err := json.Unmarshal(cell.Value, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func issue642ReplyCleanupBoolean(t *testing.T, row map[string]SelectedForkSnapshotCell, column string) bool {
	t.Helper()
	cell, exists := row[column]
	if !exists {
		t.Fatalf("native cleanup row omitted boolean column %s", column)
	}
	if cell.Type == "int64" {
		value := issue642ReplyCleanupInteger(t, row, column)
		if value != 0 && value != 1 {
			t.Fatalf("native SQLite boolean %s is not zero or one", column)
		}
		return value == 1
	}
	if cell.Type != "bool" {
		t.Fatalf("native cleanup boolean %s has type %s", column, cell.Type)
	}
	var value bool
	if err := json.Unmarshal(cell.Value, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func issue642ReplyCleanupRow(t *testing.T, snapshot map[string]SelectedForkStorageTableSnapshot, table, key, id string) map[string]SelectedForkSnapshotCell {
	t.Helper()
	evidence, exists := snapshot[table]
	if !exists {
		t.Fatalf("native cleanup witness omitted table %s", table)
	}
	rows, err := DecodeSelectedForkSnapshotRowsForTest(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var match map[string]SelectedForkSnapshotCell
	for _, row := range rows {
		if issue642ReplyCleanupText(t, row, key) == id {
			if match != nil {
				t.Fatalf("native witness duplicates %s/%s", table, id)
			}
			match = row
		}
	}
	return match
}

func issue642ReplyCleanupText(t *testing.T, row map[string]SelectedForkSnapshotCell, column string) string {
	t.Helper()
	cell, exists := row[column]
	if !exists {
		t.Fatalf("native cleanup row omitted column %s", column)
	}
	value, err := cell.Text()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
