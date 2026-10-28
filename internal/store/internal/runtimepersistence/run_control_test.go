package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func requireLiveRunControlSelection(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	owner := selectedAdmissionLifecycleWriter(fixture.store).(interface {
		runtimerunlifecycle.LiveRunReader
		CreateRun(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
		CreateRunTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
		ClassifyLiveRunCreationFailure(context.Context, runtimerunlifecycle.LiveRunScope, string, error) error
		PauseRunControlOutcome(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error)
		ContinueRunControlOutcome(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error)
		StopRunControlOutcome(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error)
		SelectLiveRunTx(context.Context, *mutationprotocol.Attempt, string, time.Time) (string, error)
		InsertRunForkRunTx(context.Context, *mutationprotocol.Attempt, string, string, runfork.RunForkPoint, time.Time, runtimecorrelation.SourceArtifactFact, sourceartifact.RootAdmission) error
		RunDispatchBlocked(context.Context, string) (bool, error)
		RunDispatchParked(context.Context, string) (bool, error)
		RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error)
	})
	reader := fixture.store.(runtimerunlifecycle.LiveRunReader)
	landingReader := fixture.store.(runtimerunlifecycle.LandingRunReader)
	scope := runtimerunlifecycle.LiveRunScope{FlowPath: "."}
	source, _ := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	root, present, rootErr := runtimecorrelation.RootAdmissionFromContext(ctx)
	if rootErr != nil || !present {
		t.Fatalf("live run control fixture requires its captured source admission: %v", rootErr)
	}
	requireLanding := func(runID string, state runtimerunlifecycle.State) {
		t.Helper()
		landing, found, err := landingReader.LoadLandingRun(ctx, scope)
		if err != nil || !found || landing.RunID != runID || landing.State != state || !landing.Source.Matches(source) {
			t.Fatalf("landing selection=%+v found=%v err=%v; want %s/%s", landing, found, err, runID, state)
		}
	}
	first, second := uuid.NewString(), uuid.NewString()
	request := runtimerunlifecycle.CreateRequest{RunID: first, Source: source, RootAdmission: root, Origin: runtimerunlifecycle.DeploymentRunOrigin(), StartedAt: time.Now().UTC(), LiveScope: scope}
	for _, invalid := range []string{"../source", "one//two", `one\two`, " first"} {
		invalidRequest := request
		invalidRequest.LiveScope.FlowPath = invalid
		if _, err := owner.CreateRun(ctx, invalidRequest); err == nil {
			t.Fatalf("noncanonical live flow admitted: %q", invalid)
		}
	}
	if _, err := owner.CreateRun(ctx, request); err != nil {
		t.Fatalf("create live run: %v", err)
	}
	requireLanding(first, runtimerunlifecycle.StateLive)
	if observed, err := reader.LoadRunLiveScope(ctx, first); err != nil || observed != scope {
		t.Fatalf("live scope does not retain the admitted source flow: %+v %v", observed, err)
	}
	if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(first)); err != nil || disposition != runtimerunlifecycle.CandidateRetainedLive {
		t.Fatalf("settling live work requested run terminality: %s %v", disposition, err)
	}
	if disposition, err := owner.CreateRun(ctx, request); err != nil || disposition != runtimerunlifecycle.MutationExactNoop {
		t.Fatalf("exact live creation replay: %s/%v", disposition, err)
	}
	request.RunID = second
	var conflict *runtimerunlifecycle.LiveRunConflictError
	failed := runSelectedFixtureMutation(ctx, fixture.store, "publication live conflict", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		_, err := owner.CreateRunTx(ctx, attempt, request)
		return err
	})
	if failed == nil {
		t.Fatal("transactional creation ignored the live sibling")
	}
	cleanup := errors.New("independent publication cleanup")
	classified := owner.ClassifyLiveRunCreationFailure(ctx, scope, second, errors.Join(failed, cleanup))
	if !errors.As(classified, &conflict) || conflict.LiveRunID != first || !errors.Is(classified, failed) || !errors.Is(classified, cleanup) {
		t.Fatalf("settled publication lost its conflict or independent errors: %v", classified)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if got := owner.ClassifyLiveRunCreationFailure(cancelled, scope, second, failed); got != failed {
		t.Fatalf("cancelled publication attempted conflict readback: %v", got)
	}
	if _, err := owner.CreateRun(ctx, request); !errors.As(err, &conflict) || conflict.LiveRunID != first {
		t.Fatalf("competing live creation did not name the current run: %v", err)
	}
	if _, err := owner.PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: first}); err != nil {
		t.Fatal(err)
	}
	requireLanding(first, runtimerunlifecycle.StatePaused)
	if _, err := owner.CreateRun(ctx, request); err != nil {
		t.Fatal(err)
	}
	requireLanding(second, runtimerunlifecycle.StateLive)
	if _, err := owner.ContinueRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: first}); !errors.As(err, &conflict) || conflict.LiveRunID != second {
		t.Fatalf("continue ignored its live sibling: %v", err)
	}
	if _, err := owner.PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: second}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.ContinueRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: first}); err != nil {
		t.Fatal(err)
	}
	requireLanding(first, runtimerunlifecycle.StateLive)
	rollback := errors.New("rollback live switch")
	err := runSelectedFixtureMutation(ctx, fixture.store, "live switch rollback", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		if previous, err := owner.SelectLiveRunTx(ctx, attempt, second, time.Now().UTC()); err != nil || previous != first {
			return errors.Join(err, errors.New("live switch lost outgoing selection"))
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	requireLanding(first, runtimerunlifecycle.StateLive)
	if live, found, err := reader.LoadLiveRun(ctx, scope); err != nil || !found || live.RunID != first {
		t.Fatalf("rolled-back switch changed selection: %+v/%v/%v", live, found, err)
	}
	for runID, want := range map[string]bool{first: false, second: true} {
		if blocked, err := owner.RunDispatchBlocked(ctx, runID); err != nil || blocked != want {
			t.Fatalf("rolled-back switch changed dispatch gate %s: blocked=%v, want %v; err=%v", runID, blocked, want, err)
		}
		if parked, err := owner.RunDispatchParked(ctx, runID); err != nil || parked != want {
			t.Fatalf("rolled-back switch changed claim gate %s: parked=%v, want %v; err=%v", runID, parked, want, err)
		}
	}
	err = runSelectedFixtureMutation(ctx, fixture.store, "live switch", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		_, err := owner.SelectLiveRunTx(ctx, attempt, second, time.Now().UTC())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if live, found, err := reader.LoadLiveRun(ctx, scope); err != nil || !found || live.RunID != second {
		t.Fatalf("committed switch lost selection: %+v/%v/%v", live, found, err)
	}
	requireLanding(second, runtimerunlifecycle.StateLive)
	for runID, want := range map[string]bool{first: true, second: false} {
		if blocked, err := owner.RunDispatchBlocked(ctx, runID); err != nil || blocked != want {
			t.Fatalf("committed switch dispatch gate %s: blocked=%v, want %v; err=%v", runID, blocked, want, err)
		}
		if parked, err := owner.RunDispatchParked(ctx, runID); err != nil || parked != want {
			t.Fatalf("committed switch claim gate %s: parked=%v, want %v; err=%v", runID, parked, want, err)
		}
	}
	if _, err := owner.StopRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: second}); !errors.Is(err, runtimeruncontrol.ErrNotPaused) {
		t.Fatalf("live stop did not require pause: %v", err)
	}
	third := uuid.NewString()
	err = runSelectedFixtureMutation(ctx, fixture.store, "fork older paused sibling", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		if err := owner.InsertRunForkRunTx(ctx, attempt, third, first,
			runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision, Revision: 1}, time.Now().UTC(), source, root); err != nil {
			return err
		}
		previous, err := owner.SelectLiveRunTx(ctx, attempt, third, time.Now().UTC())
		if err != nil || previous != second {
			return errors.Join(err, errors.New("fork of older sibling did not pause the current live sibling"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if live, found, err := reader.LoadLiveRun(ctx, scope); err != nil || !found || live.RunID != third {
		t.Fatalf("fork lost its inherited flow selection: %+v found=%v err=%v", live, found, err)
	}
	requireLanding(third, runtimerunlifecycle.StateLive)
	for runID, want := range map[string]string{first: "paused", second: "paused", third: "live"} {
		state, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
		if err != nil || state.Status != want {
			t.Fatalf("fork switch changed sibling %s: %+v want=%s err=%v", runID, state, want, err)
		}
	}
	if err := runSelectedFixtureMutation(ctx, fixture.store, "replay live selection", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		previous, err := owner.SelectLiveRunTx(ctx, attempt, third, time.Now().UTC())
		if err != nil || previous != "" {
			return errors.Join(err, errors.New("replayed switch paused its own live run"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStore_RunControlTransitionsAndStopAbandonsPendingWork(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	event := seedPostgresSemanticEventRecordFixture(
		t, ctx, db, eventID, runID, events.EventType("custom.stop"),
		events.EventProducerPlatform, "test", "", "", time.Now().UTC(),
	)
	for _, route := range []events.DeliveryRoute{
		testAgentDeliveryRoute(t, runID, "agent-pending", "fixture/agent-pending"),
		testEntitylessNodeDeliveryRoute("node-pending"),
	} {
		if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
			t.Fatalf("seed pending %s delivery: %v", route.Recipient.Code(), err)
		}
	}

	if outcome, err := pg.PauseRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
		t.Fatalf("PauseRunControl: outcome=%+v err=%v", outcome, err)
	}
	if outcome, err := pg.ContinueRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
		t.Fatalf("ContinueRunControl: outcome=%+v err=%v", outcome, err)
	}
	state, err := pg.StopRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test", Now: time.Now().UTC()})
	if err != nil {
		t.Fatalf("StopRunControl: %v", err)
	}
	if !state.Acknowledged || state.State.Status != "cancelled" || state.State.ControlStatus != "stopped" || state.State.AbandonedDeliveries != 2 {
		t.Fatalf("stop state = %+v, want cancelled/stopped/2", state)
	}

	var deliveryStatus, reasonCode string
	nodePending := mustPersistenceRootNode("node-pending").Key()
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_id = 'agent-pending'
	`, eventID).Scan(&deliveryStatus, &reasonCode); err != nil {
		t.Fatalf("load stopped delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" {
		t.Fatalf("stopped delivery = %s/%s, want dead_letter/run_stopped", deliveryStatus, reasonCode)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
	`, eventID, nodePending).Scan(&deliveryStatus, &reasonCode); err != nil {
		t.Fatalf("load stopped node delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || reasonCode != "run_stopped" {
		t.Fatalf("stopped node delivery = %s/%s, want dead_letter/run_stopped", deliveryStatus, reasonCode)
	}
	var nodeReceiptOutcome, nodeReceiptReason string
	if err := db.QueryRowContext(ctx, `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = $2
	`, eventID, nodePending).Scan(&nodeReceiptOutcome, &nodeReceiptReason); err != nil {
		t.Fatalf("load stopped node outcome: %v", err)
	}
	if nodeReceiptOutcome != "terminalized" || nodeReceiptReason != "run_stopped" {
		t.Fatalf("stopped node outcome = %s/%s, want terminalized/run_stopped", nodeReceiptOutcome, nodeReceiptReason)
	}
	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
		  AND outcome = 'dead_letter'
	`, eventID).Scan(&receiptCount); err != nil {
		t.Fatalf("count stopped pipeline receipts: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("stopped pipeline receipts = %d, want 1", receiptCount)
	}

	if outcome, err := pg.StopRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: runID}); !errors.Is(err, runtimeruncontrol.ErrAlreadyTerminal) || outcome.Acknowledged {
		t.Fatalf("repeat StopRunControl outcome=%+v err=%v, want unacknowledged ErrAlreadyTerminal", outcome, err)
	}
}

func TestPostgresStore_RunControlContinueRequiresOperatorPauseOwner(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	ctx = originalStoreFixtureContext(t, ctx, pg)
	runID := uuid.NewString()
	requirePausedRunForTest(t, ctx, pg, runID, time.Now().UTC())
	if outcome, err := pg.ContinueRunControlOutcome(ctx, runtimeruncontrol.TransitionRequest{RunID: runID}); !errors.Is(err, runtimeruncontrol.ErrNotPaused) || outcome.Acknowledged {
		t.Fatalf("ContinueRunControl without operator pause owner outcome=%+v err=%v, want unacknowledged ErrNotPaused", outcome, err)
	}
}
