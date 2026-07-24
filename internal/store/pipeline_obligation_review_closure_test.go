package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestExecutableEventCommitRequiresCurrentPublicationClaimOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(pipelineObligationParityStore)
			owner := selected.PipelineObligations()
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			t.Run("missing", func(t *testing.T) {
				event := reviewClosureEvent(runID, time.Now().UTC())
				assertExecutableCommitRejected(t, ctx, fixture, selected, event, runtimepipelineobligation.Claim{})
			})

			t.Run("released", func(t *testing.T) {
				event := reviewClosureEvent(runID, time.Now().UTC().Add(time.Microsecond))
				claim, err := owner.ClaimPublication(ctx, event.ID())
				if err != nil {
					t.Fatalf("ClaimPublication: %v", err)
				}
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("Release: %v", err)
				}
				assertExecutableCommitRejected(t, ctx, fixture, selected, event, claim)
			})

			t.Run("foreign", func(t *testing.T) {
				event := reviewClosureEvent(runID, time.Now().UTC().Add(2*time.Microsecond))
				claim, err := owner.ClaimPublication(ctx, uuid.NewString())
				if err != nil {
					t.Fatalf("ClaimPublication: %v", err)
				}
				defer func() { _ = owner.Release(context.WithoutCancel(ctx), claim) }()
				assertExecutableCommitRejected(t, ctx, fixture, selected, event, claim)
			})

			t.Run("wrong_purpose", func(t *testing.T) {
				sourceID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(3*time.Microsecond))
				work, err := owner.ClaimEvent(ctx, sourceID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("ClaimEvent: %v", err)
				}
				defer func() { _ = owner.Release(context.WithoutCancel(ctx), work.Claim) }()
				event := reviewClosureEvent(runID, time.Now().UTC().Add(4*time.Microsecond))
				assertExecutableCommitRejected(t, ctx, fixture, selected, event, work.Claim)
			})

			t.Run("current_exact_claim", func(t *testing.T) {
				event := reviewClosureEvent(runID, time.Now().UTC().Add(5*time.Microsecond))
				claim, err := owner.ClaimPublication(ctx, event.ID())
				if err != nil {
					t.Fatalf("ClaimPublication: %v", err)
				}
				if err := commitReviewClosureEvent(ctx, selected, event, claim); err != nil {
					t.Fatalf("commit with current publication claim: %v", err)
				}
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("release committed publication claim: %v", err)
				}
				if !reviewClosureEventExists(t, ctx, fixture, event.ID()) {
					t.Fatalf("event %s was not committed", event.ID())
				}
			})
		})
	}
}

func TestPipelineScanBoundsExaminationAndContinuesPastBusyCandidatesOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(pipelineObligationParityStore)
			owner := selected.PipelineObligations()
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			base := time.Now().UTC().Add(-time.Hour)

			const heldCount = 5
			held := make([]runtimepipelineobligation.Claim, 0, heldCount)
			heldIDs := make([]string, 0, heldCount)
			for i := 0; i < heldCount; i++ {
				eventID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(time.Duration(i)*time.Microsecond))
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("claim blocker %d: %v", i, err)
				}
				held = append(held, work.Claim)
				heldIDs = append(heldIDs, eventID)
			}
			laterID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(heldCount*time.Microsecond))

			scan, err := owner.OpenScan(ctx, runtimepipelineobligation.RunScanRequest(runID))
			if err != nil {
				t.Fatalf("OpenScan: %v", err)
			}
			totalExamined := 0
			var later runtimepipelineobligation.ClaimedWork
			exhausted := false
			for pass := 0; pass < 10 && !exhausted; pass++ {
				batch, err := owner.ClaimBatch(ctx, scan, 2)
				if err != nil {
					t.Fatalf("ClaimBatch pass %d: %v", pass, err)
				}
				if batch.Examined > 2 {
					t.Fatalf("ClaimBatch pass %d examined %d candidates, want <= 2", pass, batch.Examined)
				}
				totalExamined += batch.Examined
				for _, work := range batch.Work {
					if work.Event.ID() != laterID {
						t.Fatalf("claimed busy-prefix event %s, want only %s", work.Event.ID(), laterID)
					}
					later = work
				}
				exhausted = batch.Exhausted
			}
			if !exhausted || later.Event.ID() != laterID || totalExamined != heldCount+1 {
				t.Fatalf("scan result: exhausted=%v later=%s examined=%d, want true %s %d", exhausted, later.Event.ID(), totalExamined, laterID, heldCount+1)
			}
			if err := owner.CloseScan(ctx, scan); err != nil {
				t.Fatalf("CloseScan: %v", err)
			}
			if err := owner.Release(ctx, later.Claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("claim survived cursor close: %v", err)
			}
			for _, claim := range held {
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("release blocker: %v", err)
				}
			}
			fresh, err := owner.OpenScan(ctx, runtimepipelineobligation.RunScanRequest(runID))
			if err != nil {
				t.Fatalf("OpenScan after exhaustion: %v", err)
			}
			restarted, err := owner.ClaimBatch(ctx, fresh, 1)
			if err != nil || len(restarted.Work) != 1 || restarted.Work[0].Event.ID() != heldIDs[0] {
				t.Fatalf("fresh scan after exhaustion = %#v err=%v, want oldest event %s", restarted, err, heldIDs[0])
			}
			if err := owner.CloseScan(ctx, fresh); err != nil {
				t.Fatalf("CloseScan after reset proof: %v", err)
			}
		})
	}
}

func TestPipelineScanCancellationAndAbandonmentReleaseClaimsOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(pipelineObligationParityStore)
			owner := selected.PipelineObligations()
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))

			for _, mode := range []string{"cancel", "abandon"} {
				t.Run(mode, func(t *testing.T) {
					scan, err := owner.OpenScan(ctx, runtimepipelineobligation.RunScanRequest(runID))
					if err != nil {
						t.Fatalf("OpenScan: %v", err)
					}
					batch, err := owner.ClaimBatch(ctx, scan, 1)
					if err != nil || len(batch.Work) != 1 || batch.Work[0].Event.ID() != eventID {
						t.Fatalf("ClaimBatch: work=%d err=%v", len(batch.Work), err)
					}
					switch mode {
					case "cancel":
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						if _, err := owner.ClaimBatch(cancelled, scan, 1); !errors.Is(err, context.Canceled) {
							t.Fatalf("cancelled ClaimBatch error = %v, want context canceled", err)
						}
						if err := owner.CloseScan(ctx, scan); !errors.Is(err, runtimepipelineobligation.ErrStaleScan) {
							t.Fatalf("cancelled scan remained open: %v", err)
						}
					case "abandon":
						if err := owner.CloseScan(ctx, scan); err != nil {
							t.Fatalf("CloseScan: %v", err)
						}
					}
					reclaimed, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
					if err != nil {
						t.Fatalf("reclaim after %s: %v", mode, err)
					}
					if err := owner.Release(ctx, reclaimed.Claim); err != nil {
						t.Fatalf("release reclaimed work: %v", err)
					}
				})
			}
		})
	}
}

func TestCorruptPipelineScopeClassificationIsTypedForMissingAndInvalidFacts(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{name: "missing", err: runtimepipelineobligation.ErrMissingScope, code: "committed_pipeline_scope_missing"},
		{name: "invalid", err: runtimepipelineobligation.ErrInvalidScope, code: "committed_pipeline_scope_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			disposition, ok := corruptPipelineScopeDisposition(uuid.NewString(), test.err)
			if !ok || disposition.Kind() != runtimepipelineobligation.DispositionQuarantined ||
				disposition.ReasonCode() != test.code {
				t.Fatalf("classification = %#v, %v", disposition, ok)
			}
		})
	}
}

func TestClaimEventReturnsTypedCorruptDecisionWorkForAcknowledgedAndUnacknowledgedOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, acknowledged := range []bool{false, true} {
			name := "unacknowledged"
			if acknowledged {
				name = "acknowledged"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				fixture := backend.open(t)
				selected := fixture.store.(pipelineObligationParityStore)
				owner := selected.PipelineObligations()
				ctx := testAuthorActivityContext()
				runID := uuid.NewString()
				seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				at := time.Now().UTC().Add(-time.Minute)
				eventID := commitPipelineParityEvent(t, ctx, selected, runID, at)
				insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, at)
				if acknowledged {
					current, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
					if err != nil {
						t.Fatalf("ClaimEvent before acknowledgement: %v", err)
					}
					if err := owner.MarkDecisionProcessed(ctx, current.Claim); err != nil {
						t.Fatalf("MarkDecisionProcessed: %v", err)
					}
					if err := owner.Release(ctx, current.Claim); err != nil {
						t.Fatalf("Release acknowledged claim: %v", err)
					}
				}
				if err := deletePipelineScope(ctx, fixture, eventID); err != nil {
					t.Fatalf("delete committed scope corruption fixture: %v", err)
				}

				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
				if err != nil {
					t.Fatalf("ClaimEvent corrupt decision work: %v", err)
				}
				disposition, preclassified := work.PreDispatchDisposition()
				if !preclassified ||
					disposition.Kind() != runtimepipelineobligation.DispositionQuarantined ||
					disposition.ReasonCode() != "committed_pipeline_scope_missing" {
					t.Fatalf("corrupt decision classification = %#v classified=%v", disposition, preclassified)
				}
				if work.Acknowledged != acknowledged {
					t.Fatalf("acknowledged = %v, want %v", work.Acknowledged, acknowledged)
				}
				if err := owner.Release(ctx, work.Claim); err != nil {
					t.Fatalf("Release corrupt decision claim: %v", err)
				}
			})
		}
	}
}

func TestPipelineScanRechecksDecisionEligibilityAfterEachMutationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(pipelineObligationParityStore)
			owner := selected.PipelineObligations()
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			at := time.Now().UTC().Add(-time.Minute)
			firstID := commitPipelineParityEvent(t, ctx, selected, runID, at)
			secondID := commitPipelineParityEvent(t, ctx, selected, runID, at.Add(time.Microsecond))
			insertProducerIdentityDecisionObligation(t, fixture, ctx, firstID, runID, at)
			insertProducerIdentityDecisionObligation(t, fixture, ctx, secondID, runID, at.Add(time.Microsecond))

			scan, err := owner.OpenScan(ctx, runtimepipelineobligation.GlobalScanRequest())
			if err != nil {
				t.Fatalf("OpenScan: %v", err)
			}
			defer func() { _ = owner.CloseScan(context.WithoutCancel(ctx), scan) }()
			first, err := owner.ClaimBatch(ctx, scan, 10)
			if err != nil || len(first.Work) != 1 || first.Work[0].Event.ID() != firstID {
				t.Fatalf("first ClaimBatch: %#v err=%v", first, err)
			}
			if err := owner.Settle(ctx, first.Work[0].Claim, runtimepipelineobligation.Quarantined("first_poison", nil)); err != nil {
				t.Fatalf("settle first: %v", err)
			}
			second, err := owner.ClaimBatch(ctx, scan, 10-first.Examined)
			if err != nil || len(second.Work) != 1 || second.Work[0].Event.ID() != secondID {
				t.Fatalf("second ClaimBatch: %#v err=%v", second, err)
			}
			if err := owner.Settle(ctx, second.Work[0].Claim, runtimepipelineobligation.Acknowledged("second_processed")); err != nil {
				t.Fatalf("settle second: %v", err)
			}
		})
	}
}

func TestInactiveProcessedDecisionRouteClosesAtParentTerminalizationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(pipelineObligationParityStore)
			owner := selected.PipelineObligations()
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
			insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, time.Now().UTC().Add(-time.Minute))

			work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
			if err != nil {
				t.Fatalf("claim decision route: %v", err)
			}
			if err := owner.MarkDecisionProcessed(ctx, work.Claim); err != nil {
				t.Fatalf("mark decision processed: %v", err)
			}
			if err := terminalizeReviewClosureRun(ctx, fixture, owner, runID); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
				t.Fatalf("terminalization with active decision claim error = %v, want ErrBusy", err)
			}
			if err := owner.Release(ctx, work.Claim); err != nil {
				t.Fatalf("release decision claim: %v", err)
			}

			setPipelineRunStatus(t, ctx, fixture, runID, "failed", "")
			if err := terminalizeReviewClosureRun(ctx, fixture, owner, runID); err != nil {
				t.Fatalf("terminalize inactive processed decision route: %v", err)
			}
			if status := readDecisionRouteStatus(t, ctx, fixture, eventID); status != "superseded" {
				t.Fatalf("decision route status = %q, want superseded", status)
			}
			count, outcome, _ := readExactPipelineReceipt(t, ctx, fixture, eventID)
			if count != 1 || outcome != "success" {
				t.Fatalf("processed receipt = count:%d outcome:%q, want preserved success", count, outcome)
			}
			if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				t.Fatalf("inactive decision route claim error = %v, want ErrIneligible", err)
			}
			presence, err := owner.GlobalWorkPresence(ctx)
			if err != nil {
				t.Fatalf("GlobalWorkPresence: %v", err)
			}
			if presence.Any() {
				t.Fatalf("inactive processed route remains globally visible: %#v", presence)
			}
			if err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("late_success")); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("late decision-route settlement error = %v, want ErrStaleClaim", err)
			}
		})
	}
}

func TestPostgresParentTerminalizationLinearizesClaimRegistration(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	selected := fixture.store.(*PostgresStore)
	owner := selected.PipelineObligations()
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))

	registry := selected.postgresPipelineClaims()
	parentChecked := make(chan struct{})
	releaseParent := make(chan struct{})
	claimAtRegistry := make(chan struct{})
	var parentOnce, claimOnce sync.Once
	registry.testAfterParentClaimScan = func() {
		parentOnce.Do(func() { close(parentChecked) })
		<-releaseParent
	}
	registry.testBeforeClaimRegistryLock = func() {
		claimOnce.Do(func() { close(claimAtRegistry) })
	}
	t.Cleanup(func() {
		registry.mu.Lock()
		registry.testAfterParentClaimScan = nil
		registry.testBeforeClaimRegistryLock = nil
		registry.mu.Unlock()
	})

	parentErr := make(chan error, 1)
	go func() {
		parentErr <- terminalizeReviewClosureRun(ctx, fixture, owner, runID)
	}()
	<-parentChecked

	claimErr := make(chan error, 1)
	go func() {
		_, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
		claimErr <- err
	}()
	<-claimAtRegistry
	close(releaseParent)

	if err := <-parentErr; err != nil {
		t.Fatalf("parent terminalization: %v", err)
	}
	if err := <-claimErr; !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
		t.Fatalf("claim registered across parent fence: %v, want ErrIneligible", err)
	}
	_, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
	if outcome != "dead_letter" || reason != "run_stopped" {
		t.Fatalf("parent terminal receipt outcome=%q reason=%q", outcome, reason)
	}
}

func TestPostgresPipelineClaimLeaseExcludesIndependentStoreUntilRelease(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	primary := newTestPostgresStore(t, db)
	fixture := authorActivityReceiptFixture{
		store:   primary,
		db:      db,
		dialect: runtimeauthoractivity.DialectPostgres,
	}
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	eventID := commitPipelineParityEvent(t, ctx, primary, runID, time.Now().UTC().Add(-time.Minute))
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	secondaryDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open independent PostgreSQL pool: %v", err)
	}
	t.Cleanup(func() { _ = secondaryDB.Close() })
	if err := secondaryDB.PingContext(ctx); err != nil {
		t.Fatalf("ping independent PostgreSQL pool: %v", err)
	}
	secondary := &PostgresStore{DB: secondaryDB}
	secondary.schemaAdmission.markCurrent()

	primaryWork, err := primary.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("primary claim: %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("primary pool capacity while claim lease is current = %d, want 3", got)
	}
	if _, err := secondary.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrBusy) {
		t.Fatalf("independent store claim while primary lease is current = %v, want ErrBusy", err)
	}
	if err := primary.PipelineObligations().Release(ctx, primaryWork.Claim); err != nil {
		t.Fatalf("release primary claim: %v", err)
	}
	if got := db.Stats().MaxOpenConnections; got != 2 {
		t.Fatalf("primary pool capacity after claim release = %d, want 2", got)
	}
	secondaryWork, err := secondary.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatalf("independent store claim after release: %v", err)
	}
	if err := secondary.PipelineObligations().Release(ctx, secondaryWork.Claim); err != nil {
		t.Fatalf("release independent claim: %v", err)
	}
}

func TestPostgresPipelineClaimConnectionWaitIsCancellableOutsideRegistryLock(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := testAuthorActivityContext()
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("hold only PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() { _ = held.Close() })

	registry := selected.postgresPipelineClaims()
	baselineWait := db.Stats().WaitCount
	claimCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	claimErr := make(chan error, 1)
	go func() {
		_, err := selected.PipelineObligations().ClaimPublication(claimCtx, uuid.NewString())
		claimErr <- err
	}()

	waitDeadline := time.NewTimer(5 * time.Second)
	defer waitDeadline.Stop()
	waitTick := time.NewTicker(time.Millisecond)
	defer waitTick.Stop()
	for db.Stats().WaitCount == baselineWait {
		select {
		case <-waitDeadline.C:
			t.Fatal("claim did not wait for the exhausted PostgreSQL pool")
		case <-waitTick.C:
		}
	}

	registryAvailable := make(chan struct{})
	go func() {
		registry.mu.Lock()
		registry.mu.Unlock()
		close(registryAvailable)
	}()
	select {
	case <-registryAvailable:
	case <-time.After(time.Second):
		t.Fatal("claim held the process registry while waiting for a PostgreSQL connection")
	}

	cancel()
	select {
	case err := <-claimErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled claim error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled claim did not return")
	}
	registry.mu.Lock()
	claimCount := len(registry.claims)
	registry.mu.Unlock()
	if claimCount != 0 {
		t.Fatalf("cancelled pre-claim connection wait registered %d claims, want 0", claimCount)
	}
}

func reviewClosureEvent(runID string, at time.Time) events.Event {
	return eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("test.event"), "runtime", "", []byte(`{"ok":true}`),
		0, runID, "", events.EventEnvelope{}, at.UTC(),
	)
}

func assertExecutableCommitRejected(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	selected pipelineObligationParityStore,
	event events.Event,
	claim runtimepipelineobligation.Claim,
) {
	t.Helper()
	if err := commitReviewClosureEvent(ctx, selected, event, claim); err == nil {
		t.Fatalf("event commit %s succeeded without its current publication claim", event.ID())
	}
	if reviewClosureEventExists(t, ctx, fixture, event.ID()) {
		t.Fatalf("rejected event %s remained visible", event.ID())
	}
}

func commitReviewClosureEvent(
	ctx context.Context,
	selected pipelineObligationParityStore,
	event events.Event,
	claim runtimepipelineobligation.Claim,
) error {
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	ctx, release, err := semanticEventFixtureContext(ctx, selected, event)
	if err != nil {
		return err
	}
	defer release()
	commit := func(txctx context.Context, tx *sql.Tx, store eventCommitTxStore) error {
		outcome, err := store.appendAdmittedEventTxOutcome(txctx, tx, admitted)
		if err != nil {
			return err
		}
		if outcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("append outcome = %v", outcome)
		}
		return (sqlPublishCommitter{tx: tx, store: store}).commitInitialSideEffects(txctx, runtimebus.CommitPublishRequest{
			Event: admitted, ReplayScope: runtimepipelineobligation.ScopeDirect, PipelineClaim: claim,
		}, true)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return commit(txctx, tx, store)
		})
	case *SQLiteRuntimeStore:
		return store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			return commit(txctx, tx, store)
		})
	default:
		return fmt.Errorf("unsupported review closure store %T", selected)
	}
}

func reviewClosureEventExists(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string) bool {
	t.Helper()
	query := `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = ?)`
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		query = `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = $1::uuid)`
	}
	var exists bool
	if err := fixture.db.QueryRowContext(ctx, query, eventID).Scan(&exists); err != nil {
		t.Fatalf("check event visibility: %v", err)
	}
	return exists
}

func terminalizeReviewClosureRun(
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	owner runtimepipelineobligation.Store,
	runID string,
) error {
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if _, err := owner.TerminalizeRun(txctx, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
