package runforkexecution

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type selectedOperationActivationProbe struct {
	SelectedContractForkLifecycle
	before   func(context.Context)
	fail     error
	discards *int
}

func (p selectedOperationActivationProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, runID string) error {
	*p.discards++
	return p.SelectedContractForkLifecycle.DiscardMaterializedSelectedContractExecutionFork(ctx, runID)
}

func (p selectedOperationActivationProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.before(ctx)
	if p.fail != nil {
		return runfork.RunForkActivation{}, p.fail
	}
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

type selectedOperationPublicationProbe struct {
	SelectedContractReplayPersistence
	after func(context.Context)
}

type selectedOperationCandidateSink struct {
	reserved, submitted, cancelled int
	fail                           error
	submitErr                      error
	afterReserve                   func()
	beforeSubmit                   func()
}

func (s *selectedOperationCandidateSink) ReserveCompletionCandidate(ctx context.Context) (runlifecycle.CandidateAdmission, error) {
	s.reserved++
	if s.fail != nil {
		return nil, s.fail
	}
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return nil, errors.New("candidate handoff lost occurrence")
	}
	lease, err := owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if s.afterReserve != nil {
		s.afterReserve()
		s.afterReserve = nil
	}
	return &selectedOperationCandidateAdmission{sink: s, lease: lease}, nil
}

type selectedOperationCandidateAdmission struct {
	sink  *selectedOperationCandidateSink
	lease *worklifetime.Lease
}

func (a *selectedOperationCandidateAdmission) Submit(runlifecycle.Candidate) error {
	if a.sink.beforeSubmit != nil {
		a.sink.beforeSubmit()
		a.sink.beforeSubmit = nil
	}
	a.sink.submitted++
	return errors.Join(a.lease.Done(), a.sink.submitErr)
}

func (a *selectedOperationCandidateAdmission) Cancel() error {
	a.sink.cancelled++
	return a.lease.Done()
}

func (p selectedOperationPublicationProbe) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	result, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
	if err == nil {
		p.after(ctx)
	}
	return result, err
}

func TestSelectedContractOperationReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, point := range []string{"before_execution", "after_event_commit", "before_activation", "activation_failure", "client_cancellation", "registered_sink", "retiring_sink", "sink_refusal", "submission_failure", "delayed_submission", "process_shutdown"} {
			t.Run(backend+"/"+point, func(t *testing.T) {
				var db *sql.DB
				var selected any
				var owner SelectedContractExecutionOwner
				var capabilityStore startupownership.Store
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, db, capabilityStore = s, storetest.Database(s), s
					owner = selectedContractSQLiteExecutionOwnerForTest(t, s)
				} else {
					_, db, _ = testutil.StartPostgres(t)
					s := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, capabilityStore = s, s
					owner = selectedContractExecutionOwnerForTest(t, s)
				}
				ctx, cancel := context.WithCancel(runForkTestContext(t))
				defer cancel()
				root := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: root, SourceRoot: filepath.Join(root, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(root)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				sourceID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				seedSelectedOperationSource(t, ctx, backend, db, selected, loaded, sourceID, eventID, entityID)
				process, _ := worklifetime.ProcessFromContext(ctx)
				retireRuntime := func() {
					t.Helper()
					if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if point == "before_execution" {
					retireRuntime()
				}
				var bound *worklifetime.SelectedForkOccurrence
				failure := errors.New("injected activation failure")
				var sink, successor *selectedOperationCandidateSink
				var discards int
				submitting, releaseSubmission := make(chan struct{}), make(chan struct{})
				activation := selectedOperationActivationProbe{SelectedContractForkLifecycle: owner.ports.fork, discards: &discards, before: func(ctx context.Context) {
					if point == "process_shutdown" {
						t.Fatal("process shutdown reached activation")
					}
					occurrence, ok := worklifetime.OccurrenceFromContext(ctx)
					bound, _ = occurrence.(*worklifetime.SelectedForkOccurrence)
					if !ok || bound == nil || process.ActiveCount() == 0 {
						t.Fatal("activation lost selected process ownership")
					}
					if point != "before_execution" && point != "after_event_commit" {
						retireRuntime()
					}
					if point == "client_cancellation" {
						cancel()
						if err := ctx.Err(); err != nil {
							t.Fatalf("committed work inherited client cancellation: %v", err)
						}
					}
					if point == "registered_sink" || point == "retiring_sink" || point == "sink_refusal" || point == "submission_failure" || point == "delayed_submission" {
						sink = &selectedOperationCandidateSink{}
						registrar := selected.(runlifecycle.CandidateRegistrar)
						scope := runlifecycle.CandidateScope{BundleHash: loaded.SourceArtifactFact.BundleHash()}
						registration, err := registrar.RegisterCompletionCandidateSink(ctx, scope, sink)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(registration.Release)
						if point == "sink_refusal" {
							sink.fail = failure
						}
						if point == "submission_failure" {
							sink.submitErr = failure
						}
						if point == "delayed_submission" {
							sink.beforeSubmit = func() { close(submitting); <-releaseSubmission }
						}
						if point == "retiring_sink" {
							sink.afterReserve = func() {
								registration.Release()
								successor = &selectedOperationCandidateSink{fail: errors.New("unexpected successor admission")}
								registration, err := registrar.RegisterCompletionCandidateSink(ctx, scope, successor)
								if err != nil {
									t.Fatal(err)
								}
								t.Cleanup(registration.Release)
							}
						}
					}
				}}
				if point == "activation_failure" {
					activation.fail = failure
				}
				owner.ports.fork = activation
				if point == "after_event_commit" {
					owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(context.Context) { retireRuntime() }}
				}
				if point == "process_shutdown" {
					retireRuntime()
					owner.ports.replay = selectedOperationPublicationProbe{SelectedContractReplayPersistence: owner.ports.replay, after: func(ctx context.Context) {
						occurrence, _ := worklifetime.OccurrenceFromContext(ctx)
						bound, _ = occurrence.(*worklifetime.SelectedForkOccurrence)
						process.Retire()
						<-ctx.Done()
					}}
				}
				request := SelectedContractExecutionRequest{
					SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
					ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
					AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: selectedContractTestProcessCapability(t, ctx, capabilityStore)},
				}
				var result SelectedContractExecutionResult
				if point == "delayed_submission" {
					driver, driverErr := process.Begin(ctx)
					if driverErr != nil {
						t.Fatal(driverErr)
					}
					type executionOutcome struct {
						result SelectedContractExecutionResult
						err    error
					}
					finished := make(chan executionOutcome, 1)
					go func() {
						result, err := ExecuteSelectedContractRunFork(ctx, request)
						finished <- executionOutcome{result, errors.Join(err, driver.Done())}
					}()
					select {
					case <-submitting:
					case outcome := <-finished:
						t.Fatalf("execution returned before candidate submission: %v", outcome.err)
					}
					var status, executionState string
					readErr := db.QueryRow(`SELECT r.status, e.state FROM runs r JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=r.run_id WHERE r.forked_from_run_id=$1`, sourceID).Scan(&status, &executionState)
					active := process.ActiveCount()
					close(releaseSubmission)
					outcome := <-finished
					result, err = outcome.result, outcome.err
					if readErr != nil || status != "running" || executionState != "quiesced" || active < 3 {
						t.Fatalf("handoff lost committed state/operation ownership: %s/%s active=%d err=%v", status, executionState, active, readErr)
					}
				} else {
					result, err = ExecuteSelectedContractRunFork(ctx, request)
				}
				if point == "activation_failure" || point == "sink_refusal" || point == "process_shutdown" {
					if (point != "process_shutdown" && !errors.Is(err, failure)) || err == nil {
						t.Fatalf("activation failure: %v", err)
					}
					var sourceState, forkState, executionState string
					if err := db.QueryRow(`SELECT s.status, f.status, e.state FROM runs s JOIN runs f ON f.forked_from_run_id=s.run_id JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=f.run_id WHERE s.run_id=$1`, sourceID).Scan(&sourceState, &forkState, &executionState); err != nil || sourceState != "running" || forkState != "cancelled" || executionState != "closed" {
						t.Fatalf("failure disposition source/fork/execution = %s/%s/%s: %v", sourceState, forkState, executionState, err)
					}
					for _, table := range []string{"events", "event_deliveries", "entity_state", "entity_mutations"} {
						var remaining int
						if err := db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE run_id=$1`, result.Materialization.ForkRunID).Scan(&remaining); err != nil || remaining != 0 {
							t.Fatalf("failure left %d %s rows: %v", remaining, table, err)
						}
					}
				} else {
					if point == "submission_failure" {
						if !errors.Is(err, failure) || !result.Activation.Activated || discards != 0 {
							t.Fatalf("post-commit failure lost activation or attempted discard: activated=%v discards=%d err=%v", result.Activation.Activated, discards, err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if result.ExecutedEventCount != 1 || result.Activation.ForkRunStatus != runfork.RunForkActivatedStatus {
						t.Fatalf("selected execution count=%d activation=%s: %v", result.ExecutedEventCount, result.Activation.ForkRunStatus, err)
					}
					var status, executionStatus string
					if err := db.QueryRow(`SELECT r.status, e.state FROM runs r JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=r.run_id WHERE r.run_id=$1`, result.Materialization.ForkRunID).Scan(&status, &executionStatus); err != nil || status != "running" || executionStatus != "closed" {
						t.Fatalf("durable activation/execution = %s/%s: %v", status, executionStatus, err)
					}
					var lineage int
					if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id=$1 AND fork_run_id=$2`, sourceID, result.Materialization.ForkRunID).Scan(&lineage); err != nil || lineage != 1 {
						t.Fatalf("exact execution lineage = %d: %v", lineage, err)
					}
				}
				if sink != nil {
					if sink.reserved == 0 {
						t.Fatal("registered candidate sink was not exercised")
					}
					if point != "sink_refusal" && (sink.submitted != sink.reserved || sink.cancelled != 0) {
						t.Fatalf("candidate settlement reserved/submitted/cancelled = %d/%d/%d", sink.reserved, sink.submitted, sink.cancelled)
					}
				}
				if successor != nil && (successor.reserved != 0 || successor.submitted != 0 || successor.cancelled != 0) {
					t.Fatalf("old handoff mutated successor: %+v", successor)
				}
				if bound == nil {
					t.Fatal("activation was not reached")
				}
				if err == nil {
					owner.ports.contexts.mu.Lock()
					retained := false
					for _, entry := range owner.ports.contexts.entries {
						retained = retained || (entry.binding.ForkRunID == result.Materialization.ForkRunID && entry.retained != nil)
					}
					owner.ports.contexts.mu.Unlock()
					if !retained || process.ActiveCount() == 0 {
						t.Fatal("successful active fork lost its process-owned control handoff")
					}
				}
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := bound.Begin(context.Background()); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("operation did not release selected execution: %v", err)
				}
				if process.ActiveCount() != 0 {
					t.Fatalf("operation leaked %d process leases", process.ActiveCount())
				}
			})
		}
	}
}

func seedSelectedOperationSource(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected any, loaded LoadedSelectedContractSource, runID, eventID, entityID string) {
	t.Helper()
	at := time.Unix(1700002200, 0).UTC()
	fixture := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: loaded.SourceArtifactFact, Artifact: selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash()), StartedAt: at.Add(-time.Minute)}
	if backend == "sqlite" {
		s := selected.(*store.SQLiteRuntimeStore)
		if _, err := s.EnsureSourceArtifact(ctx, fixture.Artifact); err != nil {
			t.Fatal(err)
		}
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
	} else {
		s := selected.(*store.PostgresStore)
		if _, err := s.EnsureSourceArtifact(ctx, fixture.Artifact); err != nil {
			t.Fatal(err)
		}
		runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "item.received", "source-runtime", "", []byte(`{}`), 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1"), eventtest.ConcreteTemplateRoutingSource("flow_a", "flow-a/1", entityID), at, executionmode.Mock)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("source-only-node")}, pipelineobligation.ScopeSubscribed)
	for _, query := range []string{
		`INSERT INTO entity_mutations (run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at) VALUES ($1,$2,'lifecycle_state','','null','"pending"',$3,'platform','selected-execution-test','seed',$4)`,
		`INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,name,current_state,gates,fields,accumulator,revision,entered_state_at,created_at,updated_at) SELECT $1,$2,'flow-a/1','test_entity','Selected Execution Entity','pending','{}','{}','{}',1,$4,$4,$4 WHERE $3<>''`,
	} {
		if _, err := db.ExecContext(ctx, query, runID, entityID, eventID, at.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if backend == "sqlite" {
		_, err = runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...)
	} else {
		_, err = runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
