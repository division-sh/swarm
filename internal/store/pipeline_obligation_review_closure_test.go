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

func TestPipelineClaimNextTraversesPastBusyCandidatePageOnSQLiteAndPostgres(t *testing.T) {
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

			held := make([]runtimepipelineobligation.Claim, 0, pipelineCandidatePageSize)
			for i := 0; i < pipelineCandidatePageSize; i++ {
				eventID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(time.Duration(i)*time.Microsecond))
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("claim blocker %d: %v", i, err)
				}
				held = append(held, work.Claim)
			}
			laterID := commitPipelineParityEvent(t, ctx, selected, runID, base.Add(pipelineCandidatePageSize*time.Microsecond))

			work, ok, err := owner.ClaimNext(ctx, runtimepipelineobligation.RunRecoveryQuery(runID))
			if err != nil || !ok || work.Event.ID() != laterID {
				t.Fatalf("ClaimNext after busy page: event=%s ok=%v err=%v, want %s", work.Event.ID(), ok, err, laterID)
			}
			if err := owner.Release(ctx, work.Claim); err != nil {
				t.Fatalf("release later claim: %v", err)
			}
			for _, claim := range held {
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("release blocker: %v", err)
				}
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
