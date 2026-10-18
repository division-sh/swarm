package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Native owner cuts retain the original command across a durable successor.
// PostgreSQL also interrupts its exact original session while the local
// possession observer is held outside serialization and proof deadlines.
func TestSystemNodeMutationRejectsPredecessorAfterProcessCutoverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []struct {
			name string
			mode mutationprotocol.Evidence
		}{
			{"engine", 0},
			{"story", mutationprotocol.Story},
			{"authority_fence", mutationprotocol.AuthorityFence},
			{"revision_only", mutationprotocol.RevisionOnly},
		} {
			entrances := []string{"pooled"}
			if backend == "postgres" && operation.mode != 0 {
				entrances = append(entrances, "retained")
			}
			for _, entrance := range entrances {
				cuts := []string{"current", "successor", "retired_grant", "missing", "foreign_store"}
				if backend == "postgres" && entrance == "pooled" {
					cuts = append(cuts, "native_loss")
				}
				for _, cut := range cuts {
					t.Run(backend+"/"+operation.name+"/"+entrance+"/"+cut, func(t *testing.T) {
						proveSystemNodeProcessCutover(t, backend, operation.mode, entrance, cut)
					})
				}
			}
		}
		t.Run(backend+"/nested_and_ended_native_context", func(t *testing.T) {
			proveSystemNodeNativeContextConfinement(t, backend)
		})
		t.Run(backend+"/original_session_proof_confinement", func(t *testing.T) {
			proveOriginalSessionProofConfinement(t, backend)
		})
	}
}

func proveOriginalSessionProofConfinement(t *testing.T, backend string) {
	t.Helper()
	f, _ := constructWorkflowMutationFixture(t, backend, "original-session-proof", time.Now().UTC().Add(-time.Minute))
	if err := f.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	original, err := startupownership.CaptureMutationAuthority(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var missing *startupownership.OriginalSessionInterruptionForTest
	if err := missing.Interrupt(t.Context(), original); err == nil {
		t.Fatal("nil native interruption admitted")
	}
	if _, err := startupownership.PrepareOriginalSessionInterruptionForTest(t.Context(), startupownership.MutationAuthority{}); err == nil {
		t.Fatal("missing original authority issued a native interruption")
	}
	if _, err := startupownership.HoldOriginalPossessionObservationForTest(startupownership.MutationAuthority{}); err == nil {
		t.Fatal("missing original authority held a possession observer")
	}
	interruption, err := startupownership.PrepareOriginalSessionInterruptionForTest(t.Context(), original)
	if backend == "sqlite" {
		if err == nil || interruption != nil {
			t.Fatal("SQLite issued a PostgreSQL session interruption")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startupownership.PrepareOriginalSessionInterruptionForTest(t.Context(), original); err == nil {
		t.Fatal("one original session issued a second interruption handle")
	}
	foreign, _ := constructWorkflowMutationFixture(t, backend, "foreign-original-session-proof", time.Now().UTC().Add(-time.Minute))
	foreignOriginal, err := startupownership.CaptureMutationAuthority(foreign.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := interruption.Interrupt(t.Context(), foreignOriginal); err == nil {
		t.Fatal("foreign original owner consumed a native interruption")
	}
	if err := foreignOriginal.Validate(); err != nil {
		t.Fatalf("foreign refusal changed the foreign owner: %v", err)
	}
	hold, err := startupownership.HoldOriginalPossessionObservationForTest(original)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	if _, err := startupownership.HoldOriginalPossessionObservationForTest(original); err == nil {
		t.Fatal("duplicate original possession hold admitted")
	}
	select {
	case <-hold.Entered():
	case <-time.After(4 * time.Second):
		t.Fatal("original observer did not enter its cancellable hold")
	}
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	// Ordinary cleanup must cancel and join an armed hold without its release
	// being necessary for opMu or the session's proof deadline to proceed.
	if err := process.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	hold.Close()
	successor, err := f.store.(startupownership.Store).AcquireProcessCapability(t.Context(), testStartupAcquireRequest("ended-handle-successor"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = successor.Release(context.Background()) })
	if err := interruption.Interrupt(t.Context(), original); err == nil {
		t.Fatal("ended original handle interrupted a replacement session")
	}
	if err := successor.ProveCurrent(t.Context()); err != nil {
		t.Fatalf("ended-handle refusal changed its replacement owner: %v", err)
	}
}

func proveSystemNodeProcessCutover(t *testing.T, backend string, mode mutationprotocol.Evidence, entrance, cut string) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	flowID := "system-node-process-fence"
	f, record := constructWorkflowMutationFixture(t, backend, flowID, at)
	selected := f.store.(stateOnlyAcquisitionStore)
	node := mustPersistenceNode(flowID, "predecessor-handler")
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: flowID, FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID,
		}),
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "handler.requested", "fixture", "", []byte(`{}`), 0,
		correlation.RunIDFromContext(f.ctx),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, record.EntityID), record.Identity.Route.InstancePath), at)
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, selected, event, route)
	if err != nil {
		t.Fatalf("claim predecessor system-node work: %+v err=%v", claimed, err)
	}
	record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
	command := runtimepipeline.WorkflowEngineMutationCommand{State: record, DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
		Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
		RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
	}}
	if err := command.Validate(); err != nil {
		t.Fatalf("validate the genuine predecessor command before the cut: %v", err)
	}
	if mode == 0 && cut == "current" {
		if _, err := HoldWorkflowMutationCommitForTest(f.ctx, f.store, claimed.Claim); err == nil {
			t.Fatal("missing original claim context armed a workflow observation")
		}
		claimContext := runtimedelivery.WithClaim(f.ctx, claimed.Claim)
		if _, err := HoldWorkflowMutationCommitForTest(claimContext, f.store, runtimedelivery.Claim{}); err == nil {
			t.Fatal("missing claim armed a workflow observation")
		}
		foreign, _ := constructWorkflowMutationFixture(t, backend, "foreign-observation-owner", at)
		if _, err := HoldWorkflowMutationCommitForTest(claimContext, foreign.store, claimed.Claim); err == nil {
			t.Fatal("foreign selected owner armed an original workflow observation")
		}
		observation, err := HoldWorkflowMutationCommitForTest(claimContext, f.store, claimed.Claim)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := observation.SuccessorWaiting(t.Context()); err == nil {
			t.Fatal("unentered observation inspected native work")
		}
		observation.Close()
		if _, err := observation.SuccessorWaiting(t.Context()); err == nil {
			t.Fatal("ended observation inspected native work")
		}
		if _, err := HoldWorkflowMutationCommitForTest(claimContext, f.store, claimed.Claim); err == nil {
			t.Fatal("consumed original owner observation was rebound")
		}
		// Closing the optional observer must not intercept the actual command.
		f.ctx = claimContext
	}
	before, found, err := f.workflows.Load(f.ctx, record.Identity)
	if err != nil || !found {
		t.Fatalf("load predecessor state: found=%v err=%v", found, err)
	}
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	old, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	next := old
	ctx, err := startupownership.FreezeMutationAuthority(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var observationHold *startupownership.PossessionObservationHoldForTest
	if cut == "native_loss" {
		if err := f.manager.Shutdown(); err != nil {
			t.Fatalf("join unrelated fixture projections before the native cut: %v", err)
		}
		original, err := startupownership.CaptureMutationAuthority(ctx)
		if err != nil {
			t.Fatal(err)
		}
		interruption, err := startupownership.PrepareOriginalSessionInterruptionForTest(ctx, original)
		if err != nil {
			t.Fatal(err)
		}
		observationHold, err = startupownership.HoldOriginalPossessionObservationForTest(original)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(observationHold.Close)
		select {
		case <-observationHold.Entered():
		case <-time.After(4 * time.Second):
			t.Fatal("original observer did not reach its pre-admission hold")
		}
		if err := interruption.Interrupt(context.Background(), original); err != nil {
			t.Fatalf("interrupt exact original session without local disposal: %v", err)
		}
		if err := interruption.Interrupt(context.Background(), original); err == nil {
			t.Fatal("one-shot session interruption was reused")
		}
		successor, err := f.store.(startupownership.Store).AcquireProcessCapability(context.Background(), testStartupAcquireRequest("native-loss-successor"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = successor.Release(context.Background()) })
		next, err = successor.Evidence()
		if err != nil || next.AuthorityGeneration != old.AuthorityGeneration+1 {
			t.Fatalf("actual successor did not acknowledge the next generation: %+v err=%v", next, err)
		}
		if _, err := process.Evidence(); err != nil {
			t.Fatalf("original was locally fenced before its pooled mutation: %v", err)
		}
	}
	if cut == "successor" || cut == "retired_grant" {
		if err := f.manager.Shutdown(); err != nil {
			t.Fatalf("join fixture projections before relinquishing its process: %v", err)
		}
	}
	switch cut {
	case "successor":
		if err := process.Release(f.ctx); err != nil {
			t.Fatalf("release predecessor possession: %v", err)
		}
		successor, err := f.store.(startupownership.Store).AcquireProcessCapability(context.Background(), testStartupAcquireRequest("system-node-successor"))
		if err != nil {
			t.Fatalf("acquire successor: %v", err)
		}
		t.Cleanup(func() { _ = successor.Release(context.Background()) })
		next, err = successor.Evidence()
		if err != nil || next.AuthorityGeneration != old.AuthorityGeneration+1 {
			t.Fatalf("prove durable process cutover: before=%+v after=%+v err=%v", old, next, err)
		}
	case "retired_grant":
		if err := f.grant.Retire(f.ctx); err != nil {
			t.Fatalf("retire only the original runtime grant: %v", err)
		}
	case "missing":
		ctx = context.Background()
	case "foreign_store":
		foreign, _ := constructWorkflowMutationFixture(t, backend, flowID, at)
		ctx, err = startupownership.WithRuntimeMutationAuthority(context.Background(), foreign.grant)
		if err != nil {
			t.Fatal(err)
		}
	}
	beforeFacts, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, event.RunID(), event.ID())
	if err != nil {
		t.Fatal(err)
	}
	activity := f.store.(authoractivity.Reader)
	beforeActivity, err := activity.HeadAuthorActivity(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var acknowledged, entered bool
	if mode == 0 {
		committed, commitErr := f.store.(runtimepipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
		acknowledged, err = committed.Committed, commitErr
	} else {
		write := func(context.Context, *mutationprotocol.Attempt) (struct{}, error) {
			entered = true
			return struct{}{}, nil
		}
		var result mutationprotocol.Result[struct{}]
		switch selected := f.store.(type) {
		case *PostgresStore:
			if entrance == "retained" {
				conn, err := f.db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				session, err := selected.backend.NewSessionAuthority(conn)
				if err != nil {
					_ = conn.Close()
					t.Fatal(err)
				}
				defer session.Release()
				result = mutationprotocol.RunRetainedPostgres(ctx, session, mode, mutationprotocol.Ordinary, nil, nil, write)
			} else {
				result = mutationprotocol.RunPostgres(ctx, selected.backend, mode, mutationprotocol.Ordinary, nil, nil, write)
			}
		case *SQLiteRuntimeStore:
			result = mutationprotocol.RunSQLite(ctx, selected.backend, "process admission diagnostic", mode, mutationprotocol.Ordinary, nil, nil, write)
		default:
			t.Fatalf("unknown selected fixture owner %T", f.store)
		}
		acknowledged, err = result.Acknowledged(), result.Err()
	}
	after, found, readErr := f.workflows.Load(f.ctx, record.Identity)
	afterFacts, factsErr := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, event.RunID(), event.ID())
	afterActivity, activityErr := activity.HeadAuthorActivity(f.ctx)
	if factsErr != nil || activityErr != nil {
		t.Fatalf("read exact post-command effects: facts=%v activity=%v", factsErr, activityErr)
	}
	t.Logf("process_generation=%d->%d acknowledged=%v domain_entered=%v refusal=%v state_conserved=%v found=%v read_err=%v",
		old.AuthorityGeneration, next.AuthorityGeneration, acknowledged, entered, err, reflect.DeepEqual(before, after), found, readErr)
	if cut == "current" {
		if err != nil || !acknowledged || readErr != nil || !found || (mode != 0 && !entered) || (mode == 0 && reflect.DeepEqual(before, after)) {
			t.Fatalf("current original owner must admit the valid command: %v", err)
		}
		return
	}
	if err == nil || acknowledged || entered || readErr != nil || !found || !reflect.DeepEqual(before, after) ||
		!reflect.DeepEqual(beforeFacts, afterFacts) || beforeActivity != afterActivity {
		t.Fatalf("predecessor system-node mutation must refuse without changing state after successor cutover")
	}
	if observationHold != nil {
		if _, err := process.Evidence(); err != nil {
			t.Fatalf("pooled refusal used a locally terminal predecessor: %v", err)
		}
		observationHold.Close()
		select {
		case <-process.Done():
		case <-time.After(4 * time.Second):
			t.Fatal("ordinary possession observation did not resume after hold release")
		}
	}
}

func proveSystemNodeNativeContextConfinement(t *testing.T, backend string) {
	t.Helper()
	f, _ := constructWorkflowMutationFixture(t, backend, "native-context-fence", time.Now().UTC().Add(-time.Minute))
	var native func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)
	switch selected := f.store.(type) {
	case *PostgresStore:
		native = selected.backend.RunAdmittedTransactionOutcome
	case *SQLiteRuntimeStore:
		native = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
			return selected.backend.RunAdmittedTransactionOutcome(ctx, "native context confinement", fn)
		}
	default:
		t.Fatalf("unsupported fixture %T", f.store)
	}
	var retained context.Context
	called := false
	ack, err := native(f.ctx, func(ctx context.Context, _ *sql.Tx) error {
		retained = ctx
		nestedACK, nestedErr := native(ctx, func(context.Context, *sql.Tx) error {
			called = true
			return nil
		})
		if nestedErr == nil || nestedACK || called {
			t.Fatalf("nested native mutation entered its domain: ack=%v err=%v", nestedACK, nestedErr)
		}
		return nil
	})
	if err != nil || !ack || retained == nil {
		t.Fatalf("outer original-owner native mutation: ack=%v err=%v", ack, err)
	}
	ack, err = native(retained, func(context.Context, *sql.Tx) error {
		called = true
		return nil
	})
	if err == nil || ack || called {
		t.Fatalf("ended transaction context admitted a new native write: ack=%v err=%v", ack, err)
	}
	command, cancelCommand := context.WithCancel(f.ctx)
	cancelCommand()
	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	carried := startupownership.CarryMutationAuthority(work, command)
	original, err := startupownership.CaptureMutationAuthority(f.ctx)
	accepted, acceptedErr := startupownership.CaptureMutationAuthority(carried)
	if err != nil || acceptedErr != nil || accepted.ProcessAuthority() != original.ProcessAuthority() || carried.Err() != nil {
		t.Fatalf("accepted work lost original custody or borrowed caller cancellation: original=%v accepted=%v", err, acceptedErr)
	}
	foreign, _ := constructWorkflowMutationFixture(t, backend, "foreign-accepted-owner", time.Now().UTC().Add(-time.Minute))
	if _, err := startupownership.CaptureMutationAuthority(startupownership.CarryMutationAuthority(foreign.ctx, f.ctx)); err == nil {
		t.Fatal("accepted work replaced a foreign original owner")
	}
	if startupownership.HasMutationAuthority(startupownership.CarryMutationAuthority(work, context.Background())) {
		t.Fatal("missing command custody was replaced by a current owner")
	}
}
