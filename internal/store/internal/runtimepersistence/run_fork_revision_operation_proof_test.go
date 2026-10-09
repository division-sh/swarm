package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type publicationRevisionProofStore interface {
	CommitPublication(context.Context, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
}

func TestRunForkRevisionTargetFailurePublicationIsCompleteOnBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 8, 26, 2, 30, 0, 0, time.UTC)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, backend.name == "postgres", runID)

			event := eventtest.ExistingRunRootIngress(
				uuid.NewString(), "revision.target_failure", "gateway", "", []byte(`{"target":"missing"}`), 0,
				runID, events.EventEnvelope{}, now,
			)
			admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatalf("admit target-failure publication: %v", err)
			}
			ctx, releaseCatalog, err := semanticEventFixtureContext(ctx, fixture.store, admitted.Event())
			if err != nil {
				t.Fatal(err)
			}
			defer releaseCatalog()
			scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
			if !ok {
				t.Fatal("target-failure publication has no author scope")
			}
			pipeline := pipelineObligationOwnerForFixture(fixture.store)
			claim, err := pipeline.ClaimPublication(ctx, admitted.ID())
			if err != nil {
				t.Fatalf("claim target-failure publication: %v", err)
			}
			defer func() {
				if err := pipeline.Release(context.WithoutCancel(ctx), claim); err != nil {
					t.Errorf("release target-failure publication claim: %v", err)
				}
			}()
			ledger, err := events.NewConnectEvaluationLedger(nil)
			if err != nil {
				t.Fatalf("construct target-failure evaluation ledger: %v", err)
			}
			settlement, err := events.NewNoDeliverySettlement(
				events.EventWriteNormalPublication,
				events.NoDeliveryDeclaredConsumerNoPlan,
				ledger,
			)
			if err != nil {
				t.Fatalf("construct target-failure settlement: %v", err)
			}
			failure := runtimefailures.Normalize(
				runtimefailures.NewTarget("target_unreachable_terminated", "eventbus", "resolve_delivery_target", map[string]any{"event_id": event.ID()}),
				"eventbus", "resolve_delivery_target",
			)
			disposition := runtimepipelineobligation.DeadLetter(failure.Detail.Code, &failure)
			command := runtimebus.PublicationCommand{
				Commit: runtimebus.CommitPublishRequest{
					Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeSubscribed,
					PipelineClaim: claim, Disposition: &disposition,
					DeadLetter: &runtimedeadletters.Record{
						OriginalEventID: event.ID(), OriginalEvent: string(event.Type()), OriginalPayload: event.Payload(),
						EntityID: event.EntityID(), FlowInstance: "runtime", Failure: failure, ChainDepth: event.ChainDepth(),
						HandlerNode: "pin_routing", Timestamp: now.Format(time.RFC3339Nano),
					},
				},
				AuthorScope: scope, HasAuthorScope: true,
				AuthorDescriptor: runtimeauthoractivity.EventDescriptor{
					EventType: string(event.Type()), Disposition: runtimeauthoractivity.StoryDifferent,
				},
				HasAuthorDescriptor: true,
			}
			store, ok := fixture.store.(publicationRevisionProofStore)
			if !ok {
				t.Fatalf("selected store %T has no publication commit owner", fixture.store)
			}
			committed, err := store.CommitPublication(ctx, command)
			if err != nil {
				t.Fatalf("commit target-failure publication: %v", err)
			}
			if committed.AppendOutcome != runtimebus.EventAppendInserted {
				t.Fatalf("target-failure append outcome = %v, want inserted", committed.AppendOutcome)
			}
			requireCompleteRunForkRevision(t, ctx, fixture, runID)
		})
	}
}

func TestRunForkRevisionDirectDeliveryTerminalizationIsCompleteOnBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.PersistedProjection(
				uuid.NewString(), events.EventType("revision.direct_terminalization"), "gateway", "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{}, now,
			)
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("revision-terminal")),
				Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
					FlowID: "revision-terminal", FlowInstance: "revision-terminal/one",
				}),
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit terminalization fixture: %v", err)
			}
			terminalized, err := fixture.store.TerminalizeRun(ctx, runID, "revision_terminalized")
			if err != nil {
				t.Fatalf("TerminalizeRun: %v", err)
			}
			if len(terminalized) != 1 || terminalized[0].Current.Status != runtimedelivery.StatusDeadLetter {
				t.Fatalf("terminalizations = %#v, want one dead-lettered delivery", terminalized)
			}
			requireCompleteRunForkRevision(t, ctx, fixture, runID)
		})
	}
}

func requireCompleteRunForkRevision(t testing.TB, ctx context.Context, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin run-fork revision validation: %v", err)
	}
	defer tx.Rollback()
	switch fixture.store.(type) {
	case *PostgresStore:
		err = privaterunforkrevision.ValidateCompletePostgres(ctx, tx, runID)
	case *SQLiteRuntimeStore:
		err = privaterunforkrevision.ValidateCompleteSQLite(ctx, tx, runID)
	default:
		t.Fatalf("unsupported selected store %T", fixture.store)
	}
	if err != nil {
		t.Fatalf("validate complete run-fork revision for %s: %v", runID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run-fork revision validation: %v", err)
	}
}

func publishCompleteRunForkRevisionBaseline(t testing.TB, ctx context.Context, db *sql.DB, postgres bool, runID string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin run-fork revision baseline: %v", err)
	}
	defer tx.Rollback()
	effects, err := privaterunforkrevision.ForRun(runID, privaterunforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatal(err)
	}
	if postgres {
		_, err = privaterunforkrevision.FinalizePostgres(ctx, tx, effects)
	} else {
		_, err = privaterunforkrevision.FinalizeSQLite(ctx, tx, effects)
	}
	if err != nil {
		t.Fatalf("finalize run-fork revision baseline: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run-fork revision baseline: %v", err)
	}
}
