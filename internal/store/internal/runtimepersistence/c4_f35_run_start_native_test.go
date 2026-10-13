package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type c4StartPlanner interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

func c4AdmitCreatingInput(t *testing.T, event events.Event, bundleHash string) events.AdmittedEvent {
	t.Helper()
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: bundleHash, FlowID: ".", EventKey: string(event.Type()),
		SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000", SchemaClass: events.PayloadSchemaSchemaLess,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	return admitted
}

func c4LoadStartProjection(t *testing.T, ctx context.Context, selected any, runID string) (int64, runforkrevision.StartProjection, error) {
	t.Helper()
	var revision int64
	var projection runforkrevision.StartProjection
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		revision, projection, err = runforkrevision.LoadStartProjection(ctx, tx, runID)
		return err
	})
	return revision, projection, err
}

func c4RequireRevisionCount(t *testing.T, ctx context.Context, selected any, runID string, want int64) {
	t.Helper()
	var count int64
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		return err
	})
	if err != nil || count != want {
		t.Fatalf("run %s revision count=%d want=%d err=%v", runID, count, want, err)
	}
}

func c4RequireExclusiveStart(t *testing.T, ctx context.Context, selected any, runID, bundleHash, firstTurn string) runfork.RunForkPlan {
	t.Helper()
	revision, projection, err := c4LoadStartProjection(t, ctx, selected, runID)
	if err != nil || revision != 1 || projection.SourceBundleHash != bundleHash || projection.FirstTurnEventID != firstTurn {
		t.Fatalf("immutable creation projection: revision=%d projection=%+v err=%v", revision, projection, err)
	}
	plan, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true})
	if err != nil || plan.ForkPoint != (runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}) ||
		plan.EventCountAtFork != 0 || plan.PendingWorkCount != 0 || len(plan.PendingWork) != 0 {
		t.Fatalf("exclusive creation cut contains ingress/work: plan=%+v err=%v", plan, err)
	}
	ids, known := plan.HistoricalEventIDs(1)
	if !known || len(ids) != 0 || len(plan.HistoricalInputCoordinates()) != 0 || len(plan.HistoricalJoinCoordinates()) != 0 {
		t.Fatal("creation cut admitted publication history")
	}
	if firstTurn == "" {
		if plan.StartFirstTurn != nil {
			t.Fatal("eventless creation invented first-turn input")
		}
	} else {
		input, present, err := plan.OriginalStartFirstTurn()
		if err != nil || !present {
			t.Fatalf("creating input lost: present=%t err=%v", present, err)
		}
		event, present := input.Event()
		if !present || event.ID() != firstTurn || event.RunID() != runID || input.Coordinates().Schema.BundleHash != bundleHash {
			t.Fatal("first turn lost its exact creating-input/schema ownership")
		}
	}
	for _, fact := range projection.Facts {
		switch fact.Family {
		case runforkrevision.FamilyEvents, runforkrevision.FamilyEventDeliveries, runforkrevision.FamilyEventReceipts, runforkrevision.FamilyDeadLetters, runforkrevision.FamilyReplyContexts:
			t.Fatalf("these creation controls must not include ingress effects: %+v", fact)
		}
	}
	return plan
}

// The physical COMMIT fault is the existing deployment owner control, not a
// process-death test. The new oracle observes the original start, not a census.
func TestC4F35DeploymentStartNativeCommitAccountingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			name := "rollback"
			if committed {
				name = "lost_ack"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				selected, _, connector := newP16RaceStore(t, backend)
				ctx, command := c4DeploymentStartCommand(t, selected, false)
				owner := selected.(bus.DeploymentRunCreationCommitOwner)
				runID := command.RunCreation.RunID
				fault := errors.New("C4 deployment physical commit boundary")
				var commits atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					commits.Add(1)
					if committed {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				})
				result, err := owner.CommitDeploymentRunCreation(ctx, command)
				if !errors.Is(err, fault) || result.Acknowledged || len(result.Activations) != 0 || commits.Load() != 1 {
					t.Fatalf("uncertain creation exposed authority: %+v commits=%d err=%v", result, commits.Load(), err)
				}
				if !committed {
					c4RequireRevisionCount(t, ctx, selected, runID, 0)
					if _, _, err := c4LoadStartProjection(t, ctx, selected, runID); !errors.Is(err, sql.ErrNoRows) {
						t.Fatalf("rolled-back creation retained a start: %v", err)
					}
					rows, _, err := selected.(runOriginReadStore).ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 100})
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range rows {
						if row.RunID == runID {
							t.Fatal("rolled-back creation retained its run")
						}
					}
					if _, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, AtStart: true}); err == nil {
						t.Fatal("rolled-back creation remained plannable")
					}
				} else {
					c4RequireExclusiveStart(t, ctx, selected, runID, command.RunCreation.BundleHash, "")
					c4RequireRevisionCount(t, ctx, selected, runID, 1)
				}
				result, err = owner.CommitDeploymentRunCreation(ctx, command)
				wantActivations := 1
				if committed {
					wantActivations = 0
				}
				if err != nil || !result.Acknowledged || result.Replay != committed || len(result.Activations) != wantActivations {
					t.Fatalf("creation reconciliation: %+v err=%v", result, err)
				}
				before := c4RequireExclusiveStart(t, ctx, selected, runID, command.RunCreation.BundleHash, "")
				c4RequireRevisionCount(t, ctx, selected, runID, 1)
				requireRunOriginHeader(t, ctx, selected.(runOriginReadStore), runID, runlifecycle.DeploymentRunOrigin(), 0)
				_, projection, err := c4LoadStartProjection(t, ctx, selected, runID)
				if err != nil || len(before.Entities) != len(command.Root.Plan.ConstructionPlans()) || len(before.FanOutObligations) != 1 {
					t.Fatalf("creation construction/feed omitted: entities=%d feeds=%d err=%v", len(before.Entities), len(before.FanOutObligations), err)
				}
				for i := 0; i < 2; i++ {
					replay, err := owner.CommitDeploymentRunCreation(ctx, command)
					if err != nil || !replay.Acknowledged || !replay.Replay || len(replay.Activations) != 0 {
						t.Fatalf("permanent creation replay: %+v err=%v", replay, err)
					}
					after := c4RequireExclusiveStart(t, ctx, selected, runID, command.RunCreation.BundleHash, "")
					c4RequireRevisionCount(t, ctx, selected, runID, 1)
					_, got, err := c4LoadStartProjection(t, ctx, selected, runID)
					if err != nil || !reflect.DeepEqual(after, before) || !reflect.DeepEqual(got, projection) {
						t.Fatalf("retry changed immutable start: %v", err)
					}
				}
			})
		}
	}
}

// This is the canonical event-cut fork writer, not selected start-cut execution
// and not a rollback/crash receipt for the fork owner.
func TestC4F35EventForkStartNativeRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityContextForBundle(runLifecycleCandidateParityBundleHash)
			runID := uuid.NewString()
			initial := eventtest.RunCreatingRootIngress(uuid.NewString(), "c4.fork.source", "ingress", "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{}, time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
			admitted := c4AdmitCreatingInput(t, initial, runLifecycleCandidateParityBundleHash)
			processed := pipelineobligation.Acknowledged("pipeline_persisted")
			if _, err := commitAdmittedSemanticEventFixtureOutcomeWithDisposition(ctx, fixture.store, admitted, nil, pipelineobligation.ScopeDirect, &processed); err != nil {
				t.Fatal(err)
			}
			c4RequireExclusiveStart(t, ctx, fixture.store, runID, runLifecycleCandidateParityBundleHash, initial.ID())
			selected := fixture.store.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: initial.ID()}
			created, err := selected.MaterializeRunFork(ctx, request)
			if err != nil || created.ForkRunID == "" || created.ForkPoint.Kind != runfork.RunForkPointEvent || created.ForkPoint.EventID != initial.ID() ||
				created.ForkPoint.Revision != 1 || created.ForkRunStatus != runfork.RunForkMaterializedStatus || !created.ExecutionReady || created.MaterializedEntityCount != 0 {
				t.Fatalf("canonical event-cut fork creation: %+v err=%v", created, err)
			}
			origin, err := created.ForkPoint.MaterializationRunOrigin(runID)
			if err != nil {
				t.Fatal(err)
			}
			requireRunOriginHeader(t, ctx, fixture.store.(runOriginReadStore), created.ForkRunID, origin, 0)
			before := c4RequireExclusiveStart(t, ctx, fixture.store, created.ForkRunID, runLifecycleCandidateParityBundleHash, "")
			for i := 0; i < 2; i++ {
				c4RequireRevisionCount(t, ctx, fixture.store, created.ForkRunID, 1)
				replayed, err := selected.MaterializeRunFork(ctx, request)
				if err != nil || replayed.ForkRunID != created.ForkRunID || replayed.ForkPoint != created.ForkPoint {
					t.Fatalf("fork retry minted another creation: %+v err=%v", replayed, err)
				}
				after := c4RequireExclusiveStart(t, ctx, fixture.store, created.ForkRunID, runLifecycleCandidateParityBundleHash, "")
				if !reflect.DeepEqual(after, before) {
					t.Fatal("fork retry changed child original start")
				}
			}
			c4RequireRevisionCount(t, ctx, fixture.store, created.ForkRunID, 1)
			sourceOrigin, err := runlifecycle.EventRunOrigin(initial.ID(), string(initial.Type()))
			if err != nil {
				t.Fatal(err)
			}
			requireRunOriginHeader(t, ctx, fixture.store.(runOriginReadStore), runID, sourceOrigin, 1)
		})
	}
}

func TestC4F35StandingStartNativeCommitAccountingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			name := "rollback"
			if committed {
				name = "lost_ack"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				selected, db, connector := newP16RaceStore(t, backend)
				artifact := storeTestSourceArtifact("c4-standing-" + backend)
				bundleHash := artifact.BundleHash()
				setup := testAuthorActivityContextForBundle(bundleHash)
				fact := sourceartifactfixture.RequireArtifact(t, setup, selected.(sourceartifactfixture.Writer), artifact)
				ctx := testAuthorActivityRuntimeContext()
				workflow := newWorkflowTestCoordinator(t, db, pipeline.NewWorkflowPersistence(selected.(workflowTestSelectedStore)), selected.(workflowTestSelectedStore))
				flowPath := "c4-standing/" + backend
				candidate := pipeline.StandingServiceCandidate{BindingEnabled: true,
					ServiceID: flowidentity.StandingServiceID(flowPath), FlowPath: flowPath, Source: fact,
				}
				fault := errors.New("C4 standing physical commit boundary")
				var commits atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					commits.Add(1)
					if committed {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				})
				uncertain, err := workflow.ReconcileStandingService(ctx, candidate)
				if !errors.Is(err, fault) || uncertain.RunID != "" || commits.Load() != 1 {
					t.Fatalf("uncertain standing creation exposed a run: %+v commits=%d err=%v", uncertain, commits.Load(), err)
				}
				observed, found, err := workflow.LoadReconciledStandingService(ctx, candidate)
				if err != nil || found != committed {
					t.Fatalf("standing commit readback: found=%t want=%t err=%v", found, committed, err)
				}
				reader := selected.(runOriginReadStore)
				rows, _, err := reader.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{BundleHash: bundleHash, Limit: 100})
				want := 0
				if committed {
					want = 1
				}
				if err != nil || len(rows) != want {
					t.Fatalf("atomic standing run cardinality=%d want=%d err=%v", len(rows), want, err)
				}
				if committed {
					c4RequireExclusiveStart(t, ctx, selected, observed.RunID, bundleHash, "")
					c4RequireRevisionCount(t, ctx, selected, observed.RunID, 1)
				}
				retried, err := workflow.ReconcileStandingService(ctx, candidate)
				if err != nil || retried.RunID == "" || retried.Generation != 1 || (committed && retried.RunID != observed.RunID) {
					t.Fatalf("standing creation retry: %+v err=%v", retried, err)
				}
				origin, err := runlifecycle.StandingGenerationRunOrigin(candidate.ServiceID, 1)
				if err != nil {
					t.Fatal(err)
				}
				requireRunOriginHeader(t, ctx, reader, retried.RunID, origin, 0)
				before := c4RequireExclusiveStart(t, ctx, selected, retried.RunID, bundleHash, "")
				replayed, err := workflow.ReconcileStandingService(ctx, candidate)
				if err != nil || replayed.RunID != retried.RunID || replayed.Generation != 1 {
					t.Fatalf("standing retry duplicated generation: %+v err=%v", replayed, err)
				}
				c4RequireRevisionCount(t, ctx, selected, retried.RunID, 1)
				after := c4RequireExclusiveStart(t, ctx, selected, retried.RunID, bundleHash, "")
				if !reflect.DeepEqual(after, before) {
					t.Fatal("standing retry changed original start")
				}
			})
		}
	}
}

func c4DeploymentStartCommand(t *testing.T, selected any, withTimer bool) (context.Context, bus.DeploymentRunCreationCommand) {
	t.Helper()
	files := map[string]string{
		"schema.yaml": "name: c4-start\npins:\n  outputs:\n    - records.ready\n",
		"events.yaml": "records.ready:\n  body: text\n",
	}
	if withTimer {
		files["schema.yaml"] += "stages:\n  waiting:\n    timers:\n      - {id: waiting.timeout, after: 1h, emit: timer.elapsed}\n"
		files["events.yaml"] += "timer.elapsed:\n"
	} else {
		files["nested/schema.yaml"] = "name: nested\n"
	}
	f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, files, nil, ownStoreTestAgentManager, nil)
	data := selected.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	catalog, err := contracts.BuildDurableDataCatalog(f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.EnsureSourceArtifactWithData(f.ctx, f.bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
		Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"body\":\"recorded\"}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	ctx := correlation.WithRunID(f.ctx, runID)
	at := time.Now().UTC().Truncate(time.Microsecond)
	source := semanticview.Wrap(f.bundle)
	plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, OccurredAt: at,
		Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, "", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := bus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
			Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        bus.FlowInstanceActivationCommand{Plan: plan},
	}
	for _, construction := range plan.ConstructionPlans() {
		command.Root.RouteTopology = append(command.Root.RouteTopology, bus.FlowInstanceRouteRecordSet{
			Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: construction.Identity.Route()},
		})
	}
	return ctx, command
}
