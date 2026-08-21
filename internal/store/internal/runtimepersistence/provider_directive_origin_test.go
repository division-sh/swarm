package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/google/uuid"
)

type providerDirectiveTestStore interface {
	completionSettlementTestStore
	runtimeagentcontrol.DirectiveOperationStore
	ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
}

func TestProviderDirectiveOriginCurrentSuccessParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, operation := admitProviderDirectiveOrigin(t, fixture, store, "current-success")
		beforeDeliveries := providerDirectiveDeliveryCount(t, fixture)
		ctx := providerDirectiveContext(t, fixture, origin, "current-success")
		handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "current-success")
		requirePersistedProviderDirectiveOrigin(t, fixture, handle.Attempt().AttemptID, origin)

		settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "provider-head-next")
		result, err := handle.SettleCompletion(ctx, settlement)
		if err != nil {
			t.Fatalf("settle current directive completion: %v", err)
		}
		if !result.Committed || result.Disposition != runtimeeffects.CompletionSettlementCurrent || result.OriginSettled {
			t.Fatalf("current directive settlement=%+v", result)
		}
		requireDirectiveOperationState(t, store, operation.OperationID, runtimeagentcontrol.DirectiveOperationExecuting, "")
		executed, err := store.RecordDirectiveExecuted(testAuthorActivityContext(), operation.OperationID, origin.ExecutionOwnerID, directiveOperationResponseForTest(operation), time.Now().UTC())
		if err != nil || executed.State != runtimeagentcontrol.DirectiveOperationExecuted {
			t.Fatalf("record current directive result: state=%q err=%v", executed.State, err)
		}
		finalized, err := store.FinalizeDirectiveSuccess(testAuthorActivityContext(), operation.OperationID, time.Now().UTC(), time.Hour)
		if err != nil || finalized.State != runtimeagentcontrol.DirectiveOperationSucceeded {
			t.Fatalf("finalize current directive: state=%q err=%v", finalized.State, err)
		}
		requireProviderDirectiveDeliveryCount(t, fixture, beforeDeliveries)
	})
}

func TestProviderDirectiveOriginRejectsMissingAmbiguousAndForeignParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, _ := admitProviderDirectiveOrigin(t, fixture, store, "closed-origin")
		foreign := origin
		foreign.ExecutionOwnerID = uuid.NewString()
		for _, candidate := range []struct {
			name string
			ctx  func(context.Context) context.Context
		}{
			{name: "missing", ctx: func(ctx context.Context) context.Context { return ctx }},
			{name: "ambiguous", ctx: func(ctx context.Context) context.Context {
				return runtimedelivery.WithClaim(runtimeeffects.WithDirectiveCompletionOrigin(ctx, origin), fixture.origin)
			}},
			{name: "foreign_execution_owner", ctx: func(ctx context.Context) context.Context {
				return runtimeeffects.WithDirectiveCompletionOrigin(ctx, foreign)
			}},
		} {
			t.Run(candidate.name, func(t *testing.T) {
				ctx := candidate.ctx(providerDirectiveBaseContext(t, fixture, "closed-origin-"+candidate.name))
				if _, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(candidate.name), nil); err == nil {
					t.Fatalf("%s directive origin authorized provider completion", candidate.name)
				}
				requireProviderAttemptCount(t, fixture, 0)
			})
		}
	})
}

func TestProviderDirectiveOriginSupersessionParity(t *testing.T) {
	targets := []struct {
		name  string
		phase runtimemanager.AgentLifecyclePhase
	}{
		{name: "restart", phase: runtimemanager.AgentLifecycleRunning},
		{name: "reconfigure", phase: runtimemanager.AgentLifecycleRunning},
		{name: "teardown", phase: runtimemanager.AgentLifecycleTerminated},
		{name: "fail", phase: runtimemanager.AgentLifecycleFailed},
	}
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, target := range targets {
			for _, outcome := range []string{"success", "error"} {
				t.Run(target.name+"/"+outcome, func(t *testing.T) {
					fixture := freshProviderDrainFixture(t, base.sqlite)
					store := requireProviderDirectiveStore(t, fixture)
					origin, operation := admitProviderDirectiveOrigin(t, fixture, store, target.name+"-"+outcome)
					beforeDeliveries := providerDirectiveDeliveryCount(t, fixture)
					ctx := providerDirectiveContext(t, fixture, origin, target.name+"-"+outcome)
					handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", target.name+"-"+outcome)
					transition := supersedeProviderDrainFixtureWithKind(t, fixture, target.name, target.phase)
					if transition.ProviderDrainCount != 1 {
						t.Fatalf("directive supersession transition=%+v, want one drain", transition)
					}

					settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "anthropic_api", "provider-head-current", "forbidden-head")
					wantCode := "provider_attempt_drained_before_directive_completion"
					if outcome == "error" {
						failureErr := runtimefailures.New(runtimefailures.ClassConnectorFailure, "directive_provider_failed", "provider-directive-test", "complete", nil)
						failure, ok := runtimefailures.EnvelopeFromError(failureErr)
						if !ok {
							t.Fatalf("construct provider failure: %v", failureErr)
						}
						settlement.Settlement.State = runtimeeffects.StateTerminalFailure
						settlement.Settlement.Failure = &failure
						settlement.AgentTurn.Failure = &failure
						settlement.ProviderHead = nil
						wantCode = "directive_provider_failed"
					}
					result, err := handle.SettleCompletion(ctx, settlement)
					if err != nil {
						t.Fatalf("settle superseded directive completion: %v", err)
					}
					if !result.Committed || !result.OriginSettled || result.Disposition != runtimeeffects.CompletionSettlementDrained || !result.Origin.Directive.Same(origin) {
						t.Fatalf("superseded directive settlement=%+v", result)
					}
					requireDirectiveOperationState(t, store, operation.OperationID, runtimeagentcontrol.DirectiveOperationIndeterminate, wantCode)
					requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
					requireProviderDirectiveDeliveryCount(t, fixture, beforeDeliveries)
				})
			}
		}
	})
}

func TestProviderDirectiveOriginPrelaunchAbandonmentParity(t *testing.T) {
	forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
		store := requireProviderDirectiveStore(t, fixture)
		origin, operation := admitProviderDirectiveOrigin(t, fixture, store, "prelaunch")
		beforeDeliveries := providerDirectiveDeliveryCount(t, fixture)
		ctx := providerDirectiveContext(t, fixture, origin, "prelaunch")
		handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("prelaunch"), nil)
		if err != nil {
			t.Fatalf("authorize directive completion: %v", err)
		}
		result := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
		if result.ProviderDrainCount != 0 || result.Phase != runtimemanager.AgentLifecycleTerminated {
			t.Fatalf("prelaunch directive transition=%+v", result)
		}
		requireDirectiveOperationState(t, store, operation.OperationID, runtimeagentcontrol.DirectiveOperationFailed, "provider_attempt_superseded_before_launch")
		if err := handle.MarkLaunched(ctx); err == nil {
			t.Fatal("abandoned directive provider attempt launched")
		}
		requireProviderDirectiveDeliveryCount(t, fixture, beforeDeliveries)
	})
}

func TestProviderDirectiveOriginRecoveryParity(t *testing.T) {
	cases := []struct {
		name        string
		terminalRun bool
	}{
		{name: "captured_drain"},
		{name: "terminal_run_drain", terminalRun: true},
	}
	forEachProviderDrainStore(t, func(t *testing.T, base completionSettlementFixture) {
		for _, candidate := range cases {
			t.Run(candidate.name, func(t *testing.T) {
				fixture := freshProviderDrainFixture(t, base.sqlite)
				store := requireProviderDirectiveStore(t, fixture)
				origin, operation := admitProviderDirectiveOrigin(t, fixture, store, candidate.name)
				beforeDeliveries := providerDirectiveDeliveryCount(t, fixture)
				ctx := providerDirectiveContext(t, fixture, origin, candidate.name)
				handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", candidate.name)
				transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleTerminated)
				if candidate.terminalRun {
					result, err := store.ApplyActiveRunQuiescence(testAuthorActivityContext(), runtimerunquiescence.Request{
						OperationName: "provider_directive_terminal_run_recovery", RequestedAt: time.Now().UTC(),
						RunIDs: []string{fixture.authority.Target.RunID}, ReasonCode: runtimerunquiescence.ServeAbandonReasonCode,
						ControlledBy: "provider-directive-test", DeliveryNote: "terminal provider directive recovery proof",
					})
					if err != nil || len(result.Runs) != 1 || !result.Runs[0].Changed {
						t.Fatalf("terminalize directive run: result=%+v err=%v", result, err)
					}
				}
				now := time.Now().UTC()
				summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now))
				if err != nil || summary.OutcomeUncertain != 1 {
					t.Fatalf("recover directive drain: summary=%+v err=%v", summary, err)
				}
				requireDirectiveOperationState(t, store, operation.OperationID, runtimeagentcontrol.DirectiveOperationIndeterminate, "effect_recovery_outcome_unconfirmed")
				requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
				requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "settled")
				requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleTerminated, transition.Generation)
				again, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second)))
				if err != nil || again != (runtimeeffects.RecoverySummary{}) {
					t.Fatalf("repeat directive recovery: summary=%+v err=%v", again, err)
				}
				requireProviderDirectiveDeliveryCount(t, fixture, beforeDeliveries)
			})
		}
	})
}

func admitProviderDirectiveOrigin(t *testing.T, fixture completionSettlementFixture, store providerDirectiveTestStore, label string) (runtimeagentcontrol.DirectiveExecutionOrigin, runtimeagentcontrol.DirectiveOperation) {
	t.Helper()
	now := time.Now().UTC()
	operationID, eventID := uuid.NewString(), uuid.NewString()
	request := runtimeagentcontrol.SendDirectiveRequest{
		AgentID: fixture.authority.Normal.Identity.AgentID(), FlowInstance: fixture.authority.Normal.Identity.FlowInstance(),
		Directive: "continue " + label, RunID: fixture.authority.Target.RunID,
		Source: runtimeagentcontrol.DirectiveSourceV1RPC, OperatorID: "provider-directive-test",
	}
	event, err := runtimeagentcontrol.NewDirectiveEvent(request, runtimeagentcontrol.RunTargetResolution{
		RunID: fixture.authority.Target.RunID, Mode: runtimeagentcontrol.RunResolutionSpecified,
	}, operationID, eventID, now, executionposture.Live)
	if err != nil {
		t.Fatalf("construct directive event: %v", err)
	}
	admittedEvent, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit directive event: %v", err)
	}
	reservation, err := store.ReserveDirectiveOperation(testAuthorActivityContext(), runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID: operationID, Method: runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID: "provider-directive-test", IdempotencyKey: "provider-directive-" + label + "-" + operationID,
			RequestHash: "provider-directive-hash-" + operationID, AgentIdentity: fixture.authority.Normal.Identity,
			Directive: request.Directive, RequestedRunID: fixture.authority.Target.RunID, ResolvedRunID: fixture.authority.Target.RunID,
			RunIDResolution: runtimeagentcontrol.RunResolutionSpecified, Source: request.Source, OperatorID: request.OperatorID,
			DirectiveEventID: eventID, State: runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: admittedEvent, Now: now,
	})
	if err != nil {
		t.Fatalf("reserve directive operation: %v", err)
	}
	ownerID := uuid.NewString()
	admission, err := store.AdmitDirectiveExecution(testAuthorActivityContext(), runtimeagentcontrol.DirectiveExecutionAdmissionRequest{
		OperationID: operationID, OwnerID: ownerID, Now: now.Add(time.Millisecond), Lease: time.Minute,
		ExecutionPosture: executionposture.Live,
	})
	if err != nil {
		t.Fatalf("admit directive execution: %v", err)
	}
	origin, err := runtimeagentcontrol.NewDirectiveExecutionOrigin(admission.Operation)
	if err != nil {
		t.Fatalf("construct directive provider origin: %v", err)
	}
	return origin, reservation.Operation
}

func providerDirectiveContext(t *testing.T, fixture completionSettlementFixture, origin runtimeagentcontrol.DirectiveExecutionOrigin, operation string) context.Context {
	t.Helper()
	return runtimeeffects.WithDirectiveCompletionOrigin(providerDirectiveBaseContext(t, fixture, operation), origin)
}

func providerDirectiveBaseContext(t *testing.T, fixture completionSettlementFixture, operation string) context.Context {
	t.Helper()
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), fixture.authority.ExecutionMode)
	controller := runtimeeffects.NewCompletionController(fixture.store, fixture.store, fixture.store, nil).WithExecutionPosture(executionposture.Live)
	ctx = runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, fixture.authority), controller)
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "provider-directive:"+operation)
	return withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
}

func requireProviderDirectiveStore(t *testing.T, fixture completionSettlementFixture) providerDirectiveTestStore {
	t.Helper()
	store, ok := fixture.store.(providerDirectiveTestStore)
	if !ok {
		t.Fatalf("completion store %T does not expose directive provider authority", fixture.store)
	}
	return store
}

func requirePersistedProviderDirectiveOrigin(t *testing.T, fixture completionSettlementFixture, attemptID string, origin runtimeagentcontrol.DirectiveExecutionOrigin) {
	t.Helper()
	query := `SELECT origin_kind,origin_directive_operation_id,origin_directive_owner_id,origin_delivery_id,origin_claim_token FROM runtime_external_effect_attempts WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT origin_kind,origin_directive_operation_id::text,origin_directive_owner_id,origin_delivery_id::text,origin_claim_token::text FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
	}
	var kind, operationID, ownerID string
	var deliveryID, claimToken sql.NullString
	if err := fixture.db.QueryRow(query, attemptID).Scan(&kind, &operationID, &ownerID, &deliveryID, &claimToken); err != nil {
		t.Fatalf("load persisted directive origin: %v", err)
	}
	if kind != string(runtimeeffects.CompletionOriginDirective) || operationID != origin.OperationID || ownerID != origin.ExecutionOwnerID || deliveryID.Valid || claimToken.Valid {
		t.Fatalf("persisted directive origin=%q/%q/%q delivery=%v claim=%v", kind, operationID, ownerID, deliveryID, claimToken)
	}
}

func requireDirectiveOperationState(t *testing.T, store providerDirectiveTestStore, operationID string, want runtimeagentcontrol.DirectiveOperationState, wantCode string) {
	t.Helper()
	op, found, err := store.LoadDirectiveOperation(testAuthorActivityContext(), operationID)
	if err != nil || !found || op.State != want {
		t.Fatalf("directive operation=%+v found=%v err=%v, want %s", op, found, err, want)
	}
	gotCode := ""
	if op.Failure != nil {
		gotCode = op.Failure.Detail.Code
	}
	if gotCode != wantCode {
		t.Fatalf("directive failure code=%q, want %q", gotCode, wantCode)
	}
}

func providerDirectiveDeliveryCount(t *testing.T, fixture completionSettlementFixture) int {
	t.Helper()
	var count int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM event_deliveries`).Scan(&count); err != nil {
		t.Fatalf("count directive-origin deliveries: %v", err)
	}
	return count
}

func requireProviderDirectiveDeliveryCount(t *testing.T, fixture completionSettlementFixture, want int) {
	t.Helper()
	if got := providerDirectiveDeliveryCount(t, fixture); got != want {
		t.Fatalf("directive provider path delivery count=%d, want unchanged %d", got, want)
	}
}
