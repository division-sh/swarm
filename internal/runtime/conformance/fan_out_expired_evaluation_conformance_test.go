package conformance

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type m35PublicationReceipt struct {
	eventID string
	err     error
}

type m35PublicationLogger struct {
	conformanceRuntimeLoggerHook
	receipts chan m35PublicationReceipt
}

func (l *m35PublicationLogger) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlated map[string]string, detail any, failure *failures.Envelope, durationUS int) error {
	err := l.conformanceRuntimeLoggerHook.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlated, detail, failure, durationUS)
	if component == "eventbus" && action == "published" && (eventType == "portfolio/portfolio.opened" || eventType == "portfolio/portfolio.accounts.register.requested") {
		l.receipts <- m35PublicationReceipt{eventID: eventID, err: err}
	}
	return err
}

func (l *m35PublicationLogger) wait(t *testing.T, ctx context.Context, eventIDs ...string) {
	t.Helper()
	pending := make(map[string]bool, len(eventIDs))
	for _, id := range eventIDs {
		pending[id] = true
	}
	for len(pending) > 0 {
		select {
		case receipt := <-l.receipts:
			if !pending[receipt.eventID] || receipt.err != nil {
				t.Fatalf("M35 ingress publication diagnostic: event=%s pending=%v err=%v", receipt.eventID, pending, receipt.err)
			}
			delete(pending, receipt.eventID)
		case <-ctx.Done():
			t.Fatalf("M35 ingress publication diagnostics did not persist: pending=%v err=%v", pending, ctx.Err())
		}
	}
}

// This executor decorates only the exact owner supplied by shared serving. It
// neither chooses candidates nor acquires claims outside the production caller.
type expiredEvaluationExecutor struct {
	*pipeline.PipelineCoordinator
	first   atomic.Bool
	loaded  chan fanoutobligation.Claim
	resume  chan struct{}
	settled chan expiredEvaluationReceipt
	recover chan struct{}
	fresh   chan fanoutobligation.Claim
}

type expiredEvaluationReceipt struct {
	owner  *expiredEvaluationOwner
	result pipeline.FanOutTurnResult
	err    error
}

func (e *expiredEvaluationExecutor) ServeFanOutCandidate(ctx context.Context, selected pipeline.FanOutObligationOwner, candidate fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	first := e.first.CompareAndSwap(false, true)
	owner := &expiredEvaluationOwner{FanOutObligationOwner: selected, executor: e, first: first}
	result, err := e.PipelineCoordinator.ServeFanOutCandidate(ctx, owner, candidate)
	if first {
		e.settled <- expiredEvaluationReceipt{owner: owner, result: result, err: err}
		// Keep the actual permit until assertions finish; the production selector
		// cannot dispatch a successor for this candidate while it is in flight.
		select {
		case <-e.recover:
		case <-ctx.Done():
		}
	}
	return result, err
}

type expiredEvaluationOwner struct {
	pipeline.FanOutObligationOwner
	executor          *expiredEvaluationExecutor
	first             bool
	claim             fanoutobligation.Claim
	command           pipeline.FanOutChunkCommand
	commitErr         error
	released          []fanoutobligation.Claim
	releaseErr        error
	loadedAt, groupAt time.Time
}

func (o *expiredEvaluationOwner) ClaimFanOutIntent(ctx context.Context, req pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if o.first {
		req.Lease = 500 * time.Millisecond
	}
	intent, claim, found, err := o.FanOutObligationOwner.ClaimFanOutIntent(ctx, req)
	if found && err == nil {
		o.claim = claim
		if !o.first {
			select {
			case o.executor.fresh <- claim:
			default:
			}
		}
	}
	return intent, claim, found, err
}

func (o *expiredEvaluationOwner) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	input, err := o.FanOutObligationOwner.LoadFanOutEvaluation(ctx, claim)
	if err == nil {
		o.loadedAt = time.Now()
	}
	return input, err
}

func (o *expiredEvaluationOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	group, err := o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	if err == nil && group != nil {
		o.groupAt = time.Now()
	}
	if err == nil && group != nil && o.first {
		// The real group is admitted before expiry, after immutable input load.
		// Pause before evaluation so expiry must still fence the evaluated commit,
		// rather than merely rejecting a new group before any evaluation occurs.
		o.executor.loaded <- claim
		select {
		case <-o.executor.resume:
		case <-ctx.Done():
			return group, ctx.Err()
		}
	}
	return group, err
}

func (o *expiredEvaluationOwner) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	o.command = command
	committed, err := o.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	o.commitErr = err
	return committed, err
}

func (o *expiredEvaluationOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	o.released = append(o.released, claim)
	o.releaseErr = o.FanOutObligationOwner.ReleaseFanOutClaim(ctx, claim)
	return o.releaseErr
}

func TestFanOutProductionCallerM35ExpiredEvaluationOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected notifyAllChildrenStore
			var db *sql.DB
			if backend == "postgres" {
				_, database, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = database
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			} else {
				sqlite := storetest.StartSQLiteRuntimeStore(t)
				selected, db = sqlite, storetest.DatabaseForTest(sqlite)
			}
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericReporterSink: true})
			gate := &expiredEvaluationExecutor{
				loaded: make(chan fanoutobligation.Claim, 1), resume: make(chan struct{}),
				settled: make(chan expiredEvaluationReceipt, 1), recover: make(chan struct{}), fresh: make(chan fanoutobligation.Claim, 1),
			}
			defer func() {
				select {
				case <-gate.resume:
				default:
					close(gate.resume)
				}
				select {
				case <-gate.recover:
				default:
					close(gate.recover)
				}
			}()
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{
				fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
					gate.PipelineCoordinator = pc
					return gate
				},
			})
			runID := uuid.NewString()
			logger := &m35PublicationLogger{
				conformanceRuntimeLoggerHook: conformanceRuntimeLoggerHook{logger: runtimepkg.NewRuntimeLogger(selected.(runtimepkg.RuntimeLogPersistence), runtime.posture, runtimepkg.NewRuntimePayloadAdmitter(nil, source, runtime.sourceArtifactFact))},
				receipts:                     make(chan m35PublicationReceipt, 2),
			}
			runtime.bus.SetLoggerHook(logger)
			ctx, cancel := context.WithTimeout(correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), runtime.sourceArtifactFact), runID), 15*time.Second)
			defer cancel()
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "fan-out-m35", runtime.sourceArtifactFact)); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			openedID := publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "m35", "threshold": 75})
			triggerID := publishNotifyAllChildrenEventAsync(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "m35", "account_ids": []map[string]any{{"account_id": "expired-then-fresh", "eng_roles": 7, "gem_score": 7.25}},
			})
			var claim fanoutobligation.Claim
			select {
			case claim = <-gate.loaded:
			case receipt := <-gate.settled:
				t.Fatalf("first production turn failed before evaluation gate: %v", receipt.err)
			case <-ctx.Done():
				t.Fatal("production caller did not reach evaluation gate")
			}
			assertExpiredEvaluationState(t, ctx, db, claim, true)
			// Acknowledged ingress dispatch can still be writing its publication
			// diagnostic after admitting fan-out work. Join those exact writes
			// before measuring whether the expired evaluator changes ANY event.
			logger.wait(t, ctx, openedID, triggerID)
			var eventsBefore int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&eventsBefore); err != nil {
				t.Fatal(err)
			}
			if delay := time.Until(claim.LeaseUntil.Add(20 * time.Millisecond)); delay > 0 {
				time.Sleep(delay)
			}
			assertExpiredEvaluationState(t, ctx, db, claim, true)
			close(gate.resume)
			var receipt expiredEvaluationReceipt
			select {
			case receipt = <-gate.settled:
			case <-ctx.Done():
				t.Fatal("expired production turn did not settle")
			}
			if !errors.Is(receipt.err, fanoutobligation.ErrStaleClaim) || !errors.Is(receipt.owner.commitErr, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("expired production commit: commit=%v turn=%v", receipt.owner.commitErr, receipt.err)
			}
			command := receipt.owner.command
			if receipt.owner.loadedAt.IsZero() || receipt.owner.groupAt.Before(receipt.owner.loadedAt) || !receipt.owner.groupAt.Before(claim.LeaseUntil) {
				t.Fatalf("M35 requires real input/group admission before expiry: loaded=%s group=%s lease=%s", receipt.owner.loadedAt, receipt.owner.groupAt, claim.LeaseUntil)
			}
			if command.Claim != claim || !command.Now.Before(claim.LeaseUntil) || len(command.Outcomes) != 1 || command.Outcomes[0].Publication == nil {
				t.Fatalf("proof requires real evaluated publication with original pre-expiry turn time: %+v", command)
			}
			if receipt.result.Refill || len(receipt.owner.released) != 1 || receipt.owner.released[0] != claim || receipt.owner.releaseErr != nil {
				t.Fatalf("exact expired cleanup: result=%+v claims=%+v err=%v", receipt.result, receipt.owner.released, receipt.owner.releaseErr)
			}
			assertExpiredEvaluationState(t, ctx, db, claim, false)
			var eventsAfter int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&eventsAfter); err != nil || eventsAfter != eventsBefore {
				t.Fatalf("expired evaluator changed durable events: before=%d after=%d err=%v", eventsBefore, eventsAfter, err)
			}
			select {
			case successor := <-gate.fresh:
				t.Fatalf("successor existed before expired cleanup proof: %+v", successor)
			default:
			}
			close(gate.recover)
			var fresh fanoutobligation.Claim
			select {
			case fresh = <-gate.fresh:
			case <-ctx.Done():
				t.Fatal("unchanged production recovery did not reclaim")
			}
			if fresh.Key != claim.Key || fresh.Generation <= claim.Generation || !fresh.LeaseUntil.After(time.Now()) {
				t.Fatalf("recovery did not acquire fresh exact authority: old=%+v new=%+v", claim, fresh)
			}
			waitNotifyAllChildrenRuntimeWithin(t, runtime, runID, 10*time.Second)
			summary, err := selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Cursor != 1 || summary.Committed != 1 || summary.SemanticRejected != 0 || summary.Owed != 0 {
				t.Fatalf("fresh production recovery: summary=%+v err=%v", summary, err)
			}
			registrations := loadNotifyAllChildrenNumericRegistrations(t, ctx, selected, db, runID)
			if len(registrations) != 1 || registrations["expired-then-fresh"].EngRoles != 7 {
				t.Fatalf("fresh production publication: %+v", registrations)
			}
			t.Logf("M35: generation %d expired without successor; real publication rejected, exact cleanup retained cursor/outcomes/events; generation %d recovered", claim.Generation, fresh.Generation)
		})
	}
}

func assertExpiredEvaluationState(t *testing.T, ctx context.Context, db *sql.DB, claim fanoutobligation.Claim, held bool) {
	t.Helper()
	var cursor, outcomes, intents, retryWaiting int
	var generation uint64
	var owner, status string
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, claim.Key.RunID).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("M35 exact single intent: count=%d err=%v", intents, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT cursor,status,COALESCE(claim_owner,''),claim_generation,CASE WHEN retry_ready_at IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1`, claim.Key.RunID).Scan(&cursor, &status, &owner, &generation, &retryWaiting); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, claim.Key.RunID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	wantOwner := ""
	if held {
		wantOwner = claim.Owner
	}
	if cursor != 0 || outcomes != 0 || status != "open" || generation != claim.Generation || owner != wantOwner || retryWaiting != 0 {
		t.Fatalf("expired evaluation mutated or replaced intent: cursor=%d outcomes=%d status=%s owner=%q generation=%d retry=%d, claim=%+v held=%t", cursor, outcomes, status, owner, generation, retryWaiting, claim, held)
	}
}
