package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestLifecycleDiagnosticForkLifetime(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, scenario := range []string{"delayed_close", "causal_delayed_close", "causal_discard", "discard_pending", "discard_projected", "discard_rollback", "discard_race", "historical_provenance_conflict", "malformed_provenance", "ack_event_conflict", "ack_without_event"} {
				t.Run(scenario, func(t *testing.T) {
					selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
					fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
					// This proof materializes its actor through the real lifecycle owner,
					// rather than the completion-only fixture's synthetic agent row.
					if _, err := db.Exec(`DELETE FROM agents WHERE run_id=$1`, fixture.forkRun); err != nil {
						t.Fatal(err)
					}
					ctx := testAuthorActivityContext()
					identity := mustTestAgentIdentityForRun(fixture.forkRun, "diagnostic-fork-worker", "global")
					transition := diagnosticTestTransition(t, identity, runtimemanager.LifecycleDiagnosticOrigin{}, executionmode.Live)
					plan, err := identity.Plan()
					if err != nil {
						t.Fatal(err)
					}
					revision, err := runtimemanager.AgentConfigPlanRevision(transition.Agent.Config, plan)
					if err != nil {
						t.Fatal(err)
					}
					hash := fixture.request.DeclarationPlan.BundleHash
					fixture.request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(hash, []agenttopology.DesiredAgent{{
						Identity: plan, ConfigRevision: revision, Source: agenttopology.SourceCoordinate{BundleHash: hash},
					}})
					if err != nil {
						t.Fatal(err)
					}
					fixture.request.Preparation.DeclarationPlanFingerprint = fixture.request.DeclarationPlan.Revision
					fixture.request.Preparation.Actors = []runfork.SelectedForkPreparedActor{{Plan: plan, ConfigurationRevision: revision, Backend: selection.BackendAnthropic, Mode: executionmode.Live}}
					fixture.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
					fixture.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
					fixture.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
					issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
					if err != nil {
						t.Fatal(err)
					}
					authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "diagnostic-proof", time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					binding, err := selected.(interface {
						RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
					}).RequireRunForkSelectedContractBinding(ctx, fixture.forkRun)
					if err != nil {
						t.Fatal(err)
					}
					process, err := fixture.process.Evidence()
					if err != nil {
						t.Fatal(err)
					}
					grant, err := fixture.process.IssueSelectedForkGenerationGrant(ctx, startupownership.SelectedForkGrantRequest{
						RuntimeInstanceID: process.RuntimeInstanceID, BundleHash: hash,
						Binding: startupownership.SelectedForkGrantBinding{
							BindingID: binding.BindingID, ForkRunID: fixture.forkRun, ExecutionID: issued.ExecutionID,
							ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration, ExecutionOwner: authority.ExecutionOwner,
							AdmissionFingerprint: issued.AdmissionFingerprint, ContainerPlanFingerprint: issued.ContainerPlanFingerprint,
							ActorCensusFingerprint: issued.ActorCensusFingerprint, EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
							DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint, PreparationFingerprint: issued.PreparationFingerprint,
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := grant.Retire(context.Background()); err != nil {
							t.Error(err)
						}
					})
					if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
						t.Fatal(err)
					}
					if _, err := grant.AdmitExecution(ctx); err != nil {
						t.Fatal(err)
					}
					projector := selected.(diagnosticProjectionTestStore)
					origin := runtimemanager.LifecycleDiagnosticOrigin{
						Owner: runtimemanager.LifecycleDiagnosticSelectedFork, Causality: runtimemanager.LifecycleDiagnosticObservation,
						SelectedFork: authority.SelectedFork, SourceRunID: fixture.sourceRun, ForkEventID: fixture.eventID,
					}
					if strings.HasPrefix(scenario, "causal_") {
						parent := uuid.NewString()
						lineage, err := events.NewSelectedForkLineage(fixture.forkRun, fixture.sourceRun, fixture.eventID, runfork.RunForkSelectedContractExecutionOwner, "", "live")
						if err != nil {
							t.Fatal(err)
						}
						now := time.Now().UTC()
						event := eventtest.SelectedForkReplay(parent, events.EventType("selected.test"), eventtest.Producer(events.EventProducerPlatform, runfork.RunForkSelectedContractExecutionOwner), "", []byte(`{}`), 0, lineage, events.EventEnvelope{}, now)
						if err := commitSelectedForkEventFixture(ctx, selected.(selectedForkEventFixtureStore), event, runfork.RunForkSelectedContractExecutionLineage{
							ForkRunID: fixture.forkRun, SourceRunID: fixture.sourceRun, SourceEventID: fixture.eventID, ForkEventID: parent, EventName: "selected.test", SelectionAuthority: runfork.RunForkSelectedContractExecutionOwner, CreatedAt: now,
						}); err != nil {
							t.Fatal(err)
						}
						origin.Causality, origin.ParentEventID = runtimemanager.LifecycleDiagnosticAcceptedEvent, parent
					}
					transition.DiagnosticOrigin, transition.ConfigRevision = origin, revision
					transition.Topology, err = agenttopology.SelectedDeclarationAdmission(fixture.forkRun, fixture.request.DeclarationPlan)
					if err != nil {
						t.Fatal(err)
					}
					transition.Agent.Topology = transition.Topology
					if _, err := grant.CommitAgentLifecycleTransition(ctx, transition); err != nil {
						t.Fatal(err)
					}
					item := diagnosticTestOperation(t, projector, transition.OperationID)
					if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
						t.Fatal(err)
					}
					if err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
						t.Fatal(err)
					}
					if scenario == "malformed_provenance" {
						proveMalformedDiagnosticProvenance(t, db, projector, item)
						return
					}
					if scenario == "historical_provenance_conflict" {
						if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET actor_census_fingerprint='foreign' WHERE execution_id=$1`, authority.ID); err != nil {
							t.Fatal(err)
						}
						if _, err := projectDiagnosticTestBatch(ctx, projector, 100); err == nil {
							t.Fatal("projected foreign historical execution")
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 0, 1)
						return
					}
					if scenario == "delayed_close" || scenario == "causal_delayed_close" || scenario == "discard_projected" || strings.HasPrefix(scenario, "ack_") {
						if n, err := projectDiagnosticTestBatch(context.Background(), projector, 100); err != nil || n != 1 {
							t.Fatalf("delayed projection n=%d err=%v", n, err)
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
						if scenario == "ack_event_conflict" {
							if _, err := db.Exec(`UPDATE events SET payload='{}',payload_bytes=$2 WHERE event_id=$1`, diagnosticEventID(item.OutboxID), []byte(`{}`)); err != nil {
								t.Fatal(err)
							}
						}
						if scenario == "ack_without_event" {
							if _, err := db.Exec(`DELETE FROM events WHERE event_id=$1`, diagnosticEventID(item.OutboxID)); err != nil {
								t.Fatal(err)
							}
						}
						var validationErr error
						if sqlite {
							s := selected.(*SQLiteRuntimeStore)
							validationErr = s.backend.RunTransaction(ctx, "validate diagnostic proof", func(ctx context.Context, tx *sql.Tx) error {
								_, err := s.eventSQLiteOwner.LifecycleObservationIDsTx(ctx, tx, fixture.forkRun)
								return err
							})
						} else {
							s := selected.(*PostgresStore)
							validationErr = s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
								if err := s.eventPostgresOwner.RequirePresentTx(ctx, tx, fixture.forkRun); err != nil {
									return err
								}
								_, err := s.eventPostgresOwner.LifecycleObservationIDsTx(ctx, tx, fixture.forkRun)
								return err
							})
						}
						if strings.HasPrefix(scenario, "ack_") {
							if validationErr == nil {
								t.Fatal("forged acknowledged observation accepted")
							}
							return
						}
						if validationErr != nil {
							t.Fatal(validationErr)
						}
						if scenario == "delayed_close" || scenario == "causal_delayed_close" {
							return
						}
					}
					if scenario == "discard_rollback" {
						if _, err := projectDiagnosticTestBatch(ctx, projector, 100); err != nil {
							t.Fatal(err)
						}
						remove := installDiagnosticFailureTrigger(t, db, sqlite, "events", "DELETE", "")
						defer remove()
						if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err == nil {
							t.Fatal("injected discard succeeded")
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
						return
					}
					if scenario == "discard_race" {
						sink := selected.(interface {
							SetEventPayloadAdmitter(runtimebus.PayloadAdmitter)
						})
						entered, release := make(chan struct{}), make(chan struct{})
						var once sync.Once
						sink.SetEventPayloadAdmitter(func(ctx context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
							if event.Type() == events.EventTypePlatformRuntimeLog {
								once.Do(func() { close(entered); <-release })
							}
							return storeTestPayloadAdmitter(ctx, event, flowID)
						})
						projected, discarded := make(chan error, 1), make(chan error, 1)
						go func() { _, err := projectDiagnosticTestBatch(ctx, projector, 100); projected <- err }()
						select {
						case <-entered:
						case <-time.After(5 * time.Second):
							close(release)
							t.Fatal("projector did not acquire run before event admission")
						}
						go func() { discarded <- selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun) }()
						close(release)
						for index, done := range []chan error{projected, discarded} {
							select {
							case err := <-done:
								if err != nil {
									var conflict *pq.Error
									if index != 1 || sqlite || !errors.As(err, &conflict) || conflict.Code != "40001" {
										t.Fatal(err)
									}
									assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
									// Serializable discard may lose to the committed projection.
									// The explicit caller retry must remove the intact aggregate.
									if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
										t.Fatal(err)
									}
								}
							case <-time.After(10 * time.Second):
								t.Fatal("projector/discard did not join")
							}
						}
						sink.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
					} else {
						if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
							t.Fatal(err)
						}
					}
					var rows int
					if err := db.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1`, fixture.forkRun).Scan(&rows); err != nil || rows != 0 {
						t.Fatalf("discard retained queue=%d err=%v", rows, err)
					}
					if n, err := projectDiagnosticTestBatch(ctx, projector, 100); err != nil || n != 0 {
						t.Fatalf("discard resurrection n=%d err=%v", n, err)
					}
					assertDiagnosticCounts(t, db, item.OutboxID, 0, 0)
				})
			}
		})
	}
}

func proveMalformedDiagnosticProvenance(t *testing.T, db *sql.DB, projector diagnosticProjectionTestStore, item diaglog.LifecycleDiagnostic) {
	t.Helper()
	var raw []byte
	if err := db.QueryRow(`SELECT payload FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, item.OutboxID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var original runtimemanager.AgentLifecycleTransitionResult
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	write := func(result runtimemanager.AgentLifecycleTransitionResult) {
		t.Helper()
		payload, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		provenance, err := json.Marshal(result.DiagnosticProvenance)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE agent_lifecycle_operations SET result=$2 WHERE operation_id=$1`, item.OperationID, string(payload)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE agent_lifecycle_diagnostic_outbox SET payload=$2,provenance=$3,execution_mode=$4 WHERE outbox_id=$1`, item.OutboxID, string(payload), string(provenance), string(result.DiagnosticProvenance.EventMode)); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"source_run", "fork_run", "execution", "generation", "binding", "actor_census", "admission", "container_plan", "effective_config", "artifact", "actor_mode", "event_mode", "owner_missing", "causal_without_parent", "observation_with_parent", "missing_parent"} {
		t.Run(field, func(t *testing.T) {
			changed := original
			p := &changed.DiagnosticProvenance
			switch field {
			case "source_run":
				p.Origin.SourceRunID = uuid.NewString()
			case "fork_run":
				p.Origin.SelectedFork.ForkRunID = uuid.NewString()
			case "execution":
				p.Origin.SelectedFork.ExecutionID = uuid.NewString()
			case "generation":
				p.Origin.SelectedFork.Generation++
			case "binding":
				p.BindingID = uuid.NewString()
			case "actor_census":
				p.Origin.SelectedFork.ActorCensusFingerprint = "foreign"
			case "admission":
				p.Origin.SelectedFork.AdmissionFingerprint = "foreign"
			case "container_plan":
				p.Origin.SelectedFork.ContainerPlanFingerprint = "foreign"
			case "effective_config":
				p.Origin.SelectedFork.EffectiveConfigFingerprint = "foreign"
			case "artifact":
				changed.ProcessBinding.BundleHash = "sha256:foreign"
			case "actor_mode":
				p.ActorMode = executionmode.Mock
			case "event_mode":
				p.EventMode = executionmode.Mock
			case "owner_missing":
				p.Origin.Owner = ""
			case "causal_without_parent":
				p.Origin.Causality = runtimemanager.LifecycleDiagnosticAcceptedEvent
			case "observation_with_parent":
				p.Origin.ParentEventID = uuid.NewString()
			case "missing_parent":
				p.Origin.Causality, p.Origin.ParentEventID = runtimemanager.LifecycleDiagnosticAcceptedEvent, uuid.NewString()
			}
			// Matching copied payload/provenance is insufficient: the durable
			// lifecycle/fork owners must still reject altered historical authority.
			write(changed)
			defer write(original)
			if n, err := projectDiagnosticTestBatch(context.Background(), projector, 100); err == nil || n != 0 {
				t.Fatalf("forged %s projected n=%d error=%v", field, n, err)
			}
			assertDiagnosticCounts(t, db, item.OutboxID, 0, 1)
		})
	}
	if n, err := projectDiagnosticTestBatch(context.Background(), projector, 100); err != nil || n != 1 {
		t.Fatalf("restored authority failed n=%d error=%v", n, err)
	}
	assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
}
