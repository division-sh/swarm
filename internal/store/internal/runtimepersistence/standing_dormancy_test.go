package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestDormantStandingQuiescenceDoesNotReplayOnRestorationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reason := range []runtimerunlifecycle.StandingBindingBlockReason{
			runtimerunlifecycle.StandingBindingCredentialsAbsent,
			runtimerunlifecycle.StandingBindingRecoveryRequired,
		} {
			t.Run(backend+"/"+string(reason), func(t *testing.T) {
				f := openStandingDispositionParityFixture(t, backend)
				f.workflow, _ = newGenericScheduleAwareWorkflowTestCoordinator(t, f.selected)
				ctx := testAuthorActivityRuntimeContext()
				candidate := f.candidate("credential-loss")
				created, err := f.workflow.ReconcileStandingService(ctx, candidate)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.workflow.PublishStandingService(ctx, created.ServiceID, created.RunID, created.Generation); err != nil {
					t.Fatal(err)
				}
				workCtx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(candidate.Source.BundleHash()), created.RunID)
				eventID, unsettledID, sessionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				identity := mustTestAgentIdentityForRun(created.RunID, "standing-agent", "standing/ingress")
				fields := testAgentIdentityStorageFields(t, identity)
				route := testAgentDeliveryRoute(t, created.RunID, "standing-agent", "standing/ingress")
				event := eventtest.PersistedProjection(eventID, events.EventType("standing.work"), "test", "", json.RawMessage(`{}`), 0,
					created.RunID, "", events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticEventFixtureWithRoutes(workCtx, f.selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				if err := commitSemanticEventFixture(workCtx, f.selected, eventtest.PersistedProjection(unsettledID, events.EventType("standing.unsettled"), "test", "", json.RawMessage(`{}`), 0,
					created.RunID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
					t.Fatal(err)
				}
				seedTestAgentRow(t, ctx, f.db, backend == "postgres", identity, "active")
				marks := []string{"?", "?", "?", "?", "?", "?", "?", "?", "?"}
				if backend == "postgres" {
					marks = []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9"}
				}
				if _, err := f.db.ExecContext(ctx, `INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance, memory_enabled, conversation, runtime_state, status
			) VALUES (`+strings.Join(marks, ",")+`, TRUE, '[]', '{}', 'active')`,
					sessionID, created.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(workCtx, f.selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				if result, err := f.selected.BindAgentSession(workCtx, claimed.Claim, sessionID); err != nil || !result.Acknowledged {
					t.Fatalf("bind pending session: %#v, %v", result, err)
				}
				timers := f.selected.(genericScheduleLifecycleConsumerStore)
				generic, workflow := seedGenericScheduleTimerFamilies(t, timers, f.db, workCtx)
				candidate.BindingEnabled = false
				candidate.BindingBlockReason = reason
				wantKind, wantReason := runtimerunlifecycle.StandingRestartCredentialDormant, "ingress_credentials_absent"
				if reason == runtimerunlifecycle.StandingBindingRecoveryRequired {
					wantKind, wantReason = runtimerunlifecycle.StandingRestartRecoveryRequired, "ingress_authority_stale"
				}
				parked, err := f.workflow.ReconcileStandingService(ctx, candidate)
				if err != nil || parked.RestartDisposition.Kind != wantKind || !parked.DeliveryContinuationRequired {
					t.Fatalf("credential quiescence = %#v, %v", parked, err)
				}
				assertGenericScheduleTimerFamilyCancellation(t, timers, f.db, workCtx, generic, workflow, parked.TimerCancellations)
				assertHistory := func() {
					t.Helper()
					var deliveryStatus, deliveryReason, sessionStatus, sessionReason, pipelineOutcome, pipelineReason string
					if err := f.db.QueryRowContext(ctx, `SELECT status, reason_code FROM event_deliveries WHERE event_id = `+marks[0]+` AND subscriber_type = 'agent'`, eventID).Scan(&deliveryStatus, &deliveryReason); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRowContext(ctx, `SELECT status, termination_reason FROM agent_sessions WHERE session_id = `+marks[0], sessionID).Scan(&sessionStatus, &sessionReason); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = `+marks[0]+` AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
						t.Fatal(err)
					}
					if deliveryStatus != "dead_letter" || deliveryReason != wantReason || sessionStatus != "terminated" || sessionReason != "cancelled" || pipelineOutcome != "dead_letter" || pipelineReason != wantReason {
						t.Fatalf("retained quiescence history = delivery %s/%s session %s/%s pipeline %s/%s", deliveryStatus, deliveryReason, sessionStatus, sessionReason, pipelineOutcome, pipelineReason)
					}
				}
				assertHistory()
				assertStandingDisposition(t, ctx, f, created.RunID, wantKind)
				candidate.BindingEnabled = true
				candidate.BindingBlockReason = ""
				for i := 0; i < 2; i++ {
					restored, err := f.workflow.ReconcileStandingService(ctx, candidate)
					if err != nil || !restored.RestartDisposition.Executable() || restored.RunID != created.RunID || restored.Generation != created.Generation {
						t.Fatalf("restore exact N = %#v, %v", restored, err)
					}
					assertHistory()
					assertGenericScheduleTimerFamilyCancellation(t, timers, f.db, workCtx, generic, workflow, parked.TimerCancellations)
				}
			})
		}
	}
}

func TestDormantStandingRestorationPreservesSuspendedOverrideBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := f.candidate("credential-loss-suspended")
			created, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "operator", Reason: "maintenance"}); err != nil {
				t.Fatal(err)
			}
			candidate.BindingEnabled = false
			candidate.BindingBlockReason = runtimerunlifecycle.StandingBindingCredentialsAbsent
			parked, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil || parked.RestartDisposition.Kind != runtimerunlifecycle.StandingRestartCredentialDormant || parked.RestartDisposition.OperatorOverride != "suspended" {
				t.Fatalf("suspended dormancy = %#v, %v", parked, err)
			}
			candidate.BindingEnabled = true
			candidate.BindingBlockReason = ""
			restored, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil || restored.RestartDisposition.Kind != runtimerunlifecycle.StandingRestartSuspended || restored.RunID != created.RunID || restored.Generation != created.Generation || restored.RestartDisposition.OperatorOverride != "suspended" {
				t.Fatalf("suspended restoration = %#v, %v", restored, err)
			}
			statuses, err := f.workflow.ListStandingServiceStatuses(ctx)
			if err != nil || len(statuses) != 1 || statuses[0].OverrideActor != "operator" || statuses[0].OverrideReason != "maintenance" {
				t.Fatalf("operator history = %#v, %v", statuses, err)
			}
		})
	}
}

func TestDormantStandingReconciliationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityRuntimeContext()
			var db *sql.DB
			var coordinator *runtimepipeline.PipelineCoordinator
			if backend == "sqlite" {
				selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = selected.backend.ConstructionHandle()
				coordinator = newSQLiteWorkflowTestCoordinator(t, db, selected)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = opened
				selected := admitTestPostgresStore(t, opened)
				coordinator = newPostgresWorkflowTestCoordinator(t, db, selected)
			}
			artifact := storeTestSourceArtifact("credential-dormancy-" + backend)
			seedStoreTestPersistedArtifact(t, db, artifact)
			candidate := runtimepipeline.StandingServiceCandidate{
				ServiceID: runtimeflowidentity.StandingServiceID("telegram-ingress"), FlowPath: "telegram-ingress",

				Source: mustStoreTestSourceArtifactFact(artifact.BundleHash()), BindingEnabled: false,
				BindingBlockReason: runtimerunlifecycle.StandingBindingCredentialsAbsent,
			}
			if _, err := coordinator.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate}); err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"standing_services", "standing_service_generations", "standing_service_journal", "runs"} {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("fresh dormant %s count=%d err=%v", table, count, err)
				}
			}
			candidate.BindingEnabled = true
			candidate.BindingBlockReason = ""
			created, err := coordinator.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
			if err != nil || len(created) != 1 || !created[0].RestartDisposition.Executable() {
				t.Fatalf("enabled creation = %#v err=%v", created, err)
			}
			candidate.BindingEnabled = false
			candidate.BindingBlockReason = runtimerunlifecycle.StandingBindingCredentialsAbsent
			parked, err := coordinator.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
			if err != nil || len(parked) != 1 || parked[0].RestartDisposition.Kind != runtimerunlifecycle.StandingRestartCredentialDormant {
				t.Fatalf("retained dormant = %#v err=%v", parked, err)
			}
			if parked[0].RunID != created[0].RunID || parked[0].Generation != 1 || !parked[0].RestartDisposition.DeclarationPresent {
				t.Fatalf("dormancy rewrote ownership: %#v", parked)
			}
			for _, verb := range []string{"resume", "reset"} {
				operation := runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"}
				var err error
				if verb == "resume" {
					_, err = coordinator.ResumeStandingService(ctx, operation)
				} else {
					_, err = coordinator.ResetStandingService(ctx, operation)
				}
				if err == nil {
					t.Fatalf("%s bypassed dormant binding", verb)
				}
			}
			candidate.BindingEnabled = true
			candidate.BindingBlockReason = ""
			for i := 0; i < 2; i++ {
				restored, err := coordinator.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
				if err != nil || len(restored) != 1 || !restored[0].RestartDisposition.Executable() || restored[0].RunID != created[0].RunID || restored[0].Generation != 1 {
					t.Fatalf("restoration %d = %#v err=%v", i, restored, err)
				}
			}
			// Complete-set omission is still declaration removal, not credential dormancy.
			removed, err := coordinator.ReconcileStandingServiceSet(ctx, nil)
			if err != nil || len(removed) != 1 || removed[0].RestartDisposition.Kind != runtimerunlifecycle.StandingRestartOrphaned {
				t.Fatalf("removed declaration = %#v err=%v", removed, err)
			}
		})
	}
}

func TestRecoveryRequiredStandingReconciliationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := f.candidate("learned-authority-stale")
			created, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "operator", Reason: "maintenance"}); err != nil {
				t.Fatal(err)
			}
			candidate.BindingEnabled, candidate.BindingBlockReason = false, runtimerunlifecycle.StandingBindingRecoveryRequired
			for i := 0; i < 2; i++ {
				blocked, err := f.workflow.ReconcileStandingService(ctx, candidate)
				if err != nil || blocked.RestartDisposition.Kind != runtimerunlifecycle.StandingRestartRecoveryRequired || blocked.RunID != created.RunID || blocked.Generation != created.Generation || blocked.RestartDisposition.OperatorOverride != "suspended" {
					t.Fatalf("same-generation recovery-required state = %#v, %v", blocked, err)
				}
			}
			assertStandingDisposition(t, ctx, f, created.RunID, runtimerunlifecycle.StandingRestartRecoveryRequired)
			for _, operation := range []func(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error){f.workflow.ResumeStandingService, f.workflow.ResetStandingService} {
				if _, err := operation(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "operator"}); err == nil {
					t.Fatal("standing control bypassed stale learned authority")
				}
			}
			candidate.BindingEnabled, candidate.BindingBlockReason = true, ""
			restored, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil || restored.RestartDisposition.Kind != runtimerunlifecycle.StandingRestartSuspended || restored.RunID != created.RunID || restored.Generation != created.Generation {
				t.Fatalf("restoration lost same N or suspension = %#v, %v", restored, err)
			}
		})
	}
}
