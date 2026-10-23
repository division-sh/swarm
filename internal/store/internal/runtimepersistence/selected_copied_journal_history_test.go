package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type selectedCopiedInputRows struct {
	driver.Rows
	fault string
}

type selectedCopiedExecutionObserver struct {
	runforkexecution.SelectedContractRuntimeExecutionLifecycle
	request runfork.SelectedContractRuntimeExecutionIssueRequest
}

func (o *selectedCopiedExecutionObserver) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, request runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	issued, err := o.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, request)
	if err == nil {
		o.request = request
	}
	return issued, err
}

func (r *selectedCopiedInputRows) Next(values []driver.Value) error {
	if r.fault == "missing_lineage" {
		return io.EOF
	}
	if err := r.Rows.Next(values); err != nil {
		return err
	}
	switch r.fault {
	case "foreign_source":
		values[1] = uuid.NewString()
	case "foreign_child":
		values[0] = uuid.NewString()
	case "foreign_event":
		values[3] = uuid.NewString()
	case "changed_authority":
		values[5] = "not-the-committed-publication-owner"
	case "changed_time":
		values[6] = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return nil
}

// Reach: a real recorded-result copy belongs to the whole closed child cohort,
// including a successor, without inventing a producing generation for its NULL.
func proveClosedCopiedSelectedJournalCohort(t *testing.T, backend string) {
	t.Helper()
	t.Run("copied_closed_cohort", func(t *testing.T) {
		control := &activityGrantHistoryConnector{}
		opened := openPermanentCohortNativeStore(t, backend, control)
		ctx := testAuthorActivityContext()
		process, err := agentfixture.ProcessCapability(t, ctx, opened.store.(agentfixture.Store))
		if err != nil {
			t.Fatal(err)
		}
		reusePermanentCohortProcess(t, opened.store, process)
		observer := &selectedCopiedExecutionObserver{SelectedContractRuntimeExecutionLifecycle: opened.store.(runforkexecution.SelectedContractRuntimeExecutionLifecycle)}
		gate := selectedStoreActivationGateForTest(t, ctx, opened.store, observer)
		source := selectedActivityProducerSourceWithLoops(t, false, false)
		bundle, _ := semanticview.Bundle(source)
		fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
		if err != nil {
			t.Fatal(err)
		}
		opened.store.(interface {
			SetEventPayloadAdmitter(runtimebus.PayloadAdmitter)
		}).SetEventPayloadAdmitter(runtimepkg.NewRuntimePayloadAdmitter(nil, source, fact))
		if _, err := opened.store.(selectedSourceArtifactStore).EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
			t.Fatal(err)
		}
		coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
		current, present, err := process.CurrentSourceSet(ctx)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := agenttopology.NewSourceSetPlan(append(current.Sources, coordinate), current.Agents)
		if err != nil {
			t.Fatal(err)
		}
		if present {
			_, err = process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan})
		} else {
			_, err = process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan})
		}
		if err != nil {
			t.Fatal(err)
		}
		grant, err := agentfixture.AdmitGeneration(t, ctx, opened.store.(agentfixture.Store), plan, coordinate)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		admitted, err := grant.AdmitExecution(ctx)
		if err != nil {
			t.Fatal(err)
		}
		journal, err := startupownership.CaptureActivityJournal(ctx, grant, admitted)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := grant.Retire(context.Background()); err != nil {
				t.Error(err)
			}
		})
		runID, rootID := uuid.NewString(), uuid.NewString()
		ctx = seedSelectedActivitySourceRun(t, opened, runID, source)
		at := time.Now().UTC().Truncate(time.Microsecond)
		root := eventtest.ExistingRunRootIngress(rootID, "activity.seeded", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
		if err := commitSemanticPipelineProcessedEventFixture(ctx, opened.store, root); err != nil {
			t.Fatal(err)
		}
		instance := constructSelectedActivityProducerFixture(t, ctx, opened.store.(agentFixtureFlowStore), source, root, "flow-a", at)
		node := mustPersistenceNode("flow-a", "writer")
		routing, err := pinrouting.AdmitNodeExecutionRoutingSource(source, node, "flow-a", events.RouteIdentity{FlowID: "flow-a", FlowInstance: instance.InstancePath, EntityID: instance.EntityID})
		if err != nil {
			t.Fatal(err)
		}
		var parents []events.Event
		originals := make(map[string]pipeline.ActivityAttemptRecord)
		for index, value := range []string{"first-recorded-result", "second-recorded-result"} {
			if index != 0 {
				rootID = uuid.NewString()
				at = at.Add(time.Microsecond)
				root = eventtest.ExistingRunRootIngress(rootID, "activity.seeded", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
				if err := commitSemanticPipelineProcessedEventFixture(ctx, opened.store, root); err != nil {
					t.Fatal(err)
				}
			}
			identity := activityidentity.Fact{RunID: runID, SourceEventID: rootID, EntityID: instance.EntityID, Owner: activityidentity.MustNodeOwner(node),
				ExecutionFlowID: "flow-a", HandlerEventKey: "review.accepted", ActivityID: "commit", Tool: "provider.write", Attempt: 1}
			results, err := runtimecontracts.ActivityResultEventsForSite(runtimecontracts.ActivitySite{Node: node, HandlerEventKey: identity.HandlerEventKey,
				Kind: runtimecontracts.ActivitySiteHandler, RuleIndex: -1, Spec: runtimecontracts.ActivitySpec{ID: identity.ActivityID, Tool: identity.Tool}})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]any{"activity_id": identity.ActivityID, "tool": identity.Tool, "input": map[string]any{"value": value},
				"success_event": results.SuccessEvent, "failure_event": results.FailureEvent, "entity_id": instance.EntityID, "node_id": identity.Owner.Key(),
				"flow_id": "flow-a", "flow_instance": instance.InstancePath, "handler_event_key": identity.HandlerEventKey, "source_run_id": runID, "source_event_id": rootID,
				"source_task_id": "", "parent_event_id": identity.ParentEventID, "chain_depth": 0})
			if err != nil {
				t.Fatal(err)
			}
			parent := eventtest.ChildForProducerWithRoutingSource(activityidentity.RequestEventID(identity), "platform.activity_requested", eventtest.Producer(events.EventProducerPlatform, "workflow"), "", payload, 1,
				events.EventLineage{RunID: runID, ParentEventID: rootID, ExecutionMode: executionmode.Live}, events.EnvelopeForSourceRoute(events.EventEnvelope{}, routing.Route()), routing, at)
			binding, err := runtimepkg.NewRuntimePayloadAdmitter(nil, source, fact)(ctx, parent, events.EventPayloadProducer(parent, "flow-a"))
			if err != nil {
				t.Fatal(err)
			}
			parent, err = events.ApplyPayloadAdmission(parent, binding)
			if err != nil {
				t.Fatal(err)
			}
			admittedInput, err := events.AdmitForPublish(parent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, opened.store, admittedInput, nil, pipelineobligation.ScopeDirect); err != nil {
				t.Fatal(err)
			}
			original, inserted, err := journal.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{RequestEventID: parent.ID(), RunID: runID, SourceEventID: rootID,
				EntityID: instance.EntityID, FlowInstance: instance.InstancePath, NodeID: identity.Owner.Key(), HandlerEventKey: identity.HandlerEventKey,
				ActivityID: identity.ActivityID, Tool: identity.Tool, Attempt: 1, SuccessEvent: results.SuccessEvent, FailureEvent: results.FailureEvent,
				InputHash: "copied-history-input", ExecutionMode: executionmode.Live, StartedAt: at})
			if err != nil || !inserted {
				t.Fatalf("start original captured journal: inserted=%t err=%v", inserted, err)
			}
			original.Status, original.ResultEventID, original.ResultEventType = pipeline.ActivityAttemptStatusSucceeded, activityidentity.ResultEventID(identity, results.SuccessEvent), results.SuccessEvent
			original.ResultPayload = map[string]any{"activity_id": identity.ActivityID, "result": map[string]any{"value": value}}
			original, inserted, err = journal.CompleteActivityAttempt(ctx, original)
			if err != nil || !inserted {
				t.Fatalf("complete original captured journal: committed=%t err=%v", inserted, err)
			}
			parents = append(parents, parent)
			originals[parent.ID()] = original
		}
		parent := parents[len(parents)-1]
		if err := grant.Retire(ctx); err != nil {
			t.Fatal(err)
		}
		carriage := originalCarriageForRun(t, opened.store, parent.RunID())
		lifecycle := opened.store.(runforkexecution.SelectedContractForkLifecycle)
		f := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{
			store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: ctx, runID: parent.RunID(), eventID: parent.ID(),
		}, source: source}
		staged, activation := stageForkContentionFixtureAt(t, f, true, parent.ID(), true)
		planAtCut, err := lifecycle.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: parent.RunID(), At: parent.ID(), ResolvedPoint: &staged.ForkPoint})
		if err != nil {
			t.Fatal(err)
		}
		for _, origin := range planAtCut.ActivityRequestOrigins() {
			carriage, err = carriage.WithActivityOrigin(origin)
			if err != nil {
				t.Fatal(err)
			}
		}
		var attached effects.Authority
		var publicationErr error
		publicationAttempted := false
		call := func(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
			attached, _ = effects.AuthorityFromContext(ctx)
			activated, err := lifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
			if activated.Activated {
				publicationAttempted = true
				for _, input := range parents {
					publicationErr = errors.Join(publicationErr, commitCopiedCohortInput(t, ctx, opened.store, attached, input, source, fact, carriage))
				}
			}
			return activated, errors.Join(err, publicationErr)
		}
		if activated, err := gate(activation, staged.ForkPoint, call); err != nil || !activated.Activated {
			t.Fatalf("activate copied journal through its original owner: %+v err=%v", activated, err)
		}
		if !publicationAttempted || publicationErr != nil {
			t.Fatalf("original child input publication: attempted=%t err=%v", publicationAttempted, publicationErr)
		}
		inputID := activityidentity.ForkLineageEventID(staged.ForkRunID, parent.ID())
		if _, found, err := opened.store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, inputID); err != nil || !found {
			t.Fatalf("native selected publication missing after activation: input=%s found=%t err=%v", inputID, found, err)
		}
		selected := opened.store.(selectedCompletionAuthorityStore)
		if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, attached); err != nil {
			t.Fatal(err)
		}
		if closed, err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, attached.ID); err != nil || !closed {
			t.Fatalf("close original copied execution: closed=%t err=%v", closed, err)
		}
		issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, observer.request)
		if err != nil || issued.Generation != 2 {
			t.Fatalf("issue copied-cohort successor from original preparation: %+v err=%v", issued, err)
		}
		successor, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "copied-cohort-successor", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, successor); err != nil {
			t.Fatal(err)
		}
		if closed, err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil || !closed {
			t.Fatalf("close original copied-cohort successor: closed=%t err=%v", closed, err)
		}
		before := historicalContextDatabaseRows(t, opened.db, backend == "postgres")
		for _, fault := range []string{"complete", "missing_lineage", "foreign_source", "foreign_child", "foreign_event", "changed_authority", "changed_time", "copy_payload", "copy_clock", "last_lineage", "last_copy_payload"} {
			t.Run(fault, func(t *testing.T) {
				control.copiedFault = fault
				control.copiedLineageReads = 0
				defer func() { control.copiedFault = "" }()
				var got runforkpersistence.ClosedSelectedInstalledJournalCohort
				err := runUnrevisionedEventFixtureTransactionForTest(ctx, opened.store, func(ctx context.Context, tx *sql.Tx) error {
					var err error
					got, err = runforkpersistence.ReadClosedSelectedInstalledJournalCohortTx(ctx, tx, staged.ForkRunID, backend == "sqlite", carriage)
					return err
				})
				if fault != "complete" {
					if err == nil || !reflect.DeepEqual(got, runforkpersistence.ClosedSelectedInstalledJournalCohort{}) {
						t.Fatalf("inconsistent copied lineage returned membership: %+v err=%v", got, err)
					}
					if (fault == "last_lineage" || fault == "last_copy_payload") && control.copiedLineageReads != len(parents) {
						t.Fatalf("late copied-record refusal stopped before the second lineage: reads=%d want=%d err=%v", control.copiedLineageReads, len(parents), err)
					}
					return
				}
				if err != nil || len(got.Generations) < 2 || len(got.CopiedJournals) != len(parents) {
					t.Fatalf("copied journal lacks its full closed successor cohort: %+v source=%s expected_input=%s err=%v", got, parent.ID(), activityidentity.ForkLineageEventID(staged.ForkRunID, parent.ID()), err)
				}
				seen := make(map[string]bool)
				for _, copy := range got.CopiedJournals {
					original, found := originals[copy.Origin.Request.ID()]
					if !found || seen[copy.Record.RequestEventID] || !copy.Record.ExecutionBinding.IsZero() || copy.Record.RunID != staged.ForkRunID || copy.Record.Status != original.Status || !reflect.DeepEqual(copy.Record.ResultPayload, original.ResultPayload) || !reflect.DeepEqual(copy.Record.Failure, original.Failure) || copy.InputEventID != activityidentity.ForkLineageEventID(staged.ForkRunID, copy.Origin.Request.ID()) || copy.Origin.Validate() != nil {
						t.Fatalf("copied canonical evidence or original lineage changed: %+v original=%+v", copy, original)
					}
					seen[copy.Record.RequestEventID] = true
				}
			})
		}
		historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, opened.db, backend == "postgres"))
	})
}

func commitCopiedCohortInput(t *testing.T, ctx context.Context, store any, authority effects.Authority, parent events.Event, source semanticview.Source, fact correlation.SourceArtifactFact, original semanticview.OriginalLoopCarriage) (finalErr error) {
	t.Helper()
	replay := store.(runforkexecution.SelectedContractReplayPersistence)
	inputs, err := replay.LoadRunForkSelectedContractSourceEvents(ctx, parent.RunID(), authority.SelectedFork.ForkRunID, []string{parent.ID()}, original, originalRootAdmissionForRun(t, store, parent.RunID()))
	if err != nil {
		return err
	}
	if len(inputs) != 1 {
		return errors.New("copied journal requires one original selected input")
	}
	input, err := runfork.ProjectSelectedContractSourceEvent(parent.RunID(), authority.SelectedFork.ForkRunID, inputs[0])
	if err != nil {
		return err
	}
	lineage, err := events.NewSelectedForkLineage(authority.SelectedFork.ForkRunID, parent.RunID(), parent.ID(), runfork.RunForkSelectedContractExecutionOwner, "", executionmode.Live)
	if err != nil {
		return err
	}
	event := eventtest.SelectedForkReplayWithRoutingSource(activityidentity.ForkLineageEventID(lineage.DestinationRunID(), parent.ID()), parent.Type(),
		eventtest.Producer(events.EventProducerPlatform, lineage.AuthorityStamp()), input.Payload, lineage,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, input.RoutingSource.Route()), input.RoutingSource, time.Now().UTC().Truncate(time.Microsecond))
	binding, err := runtimepkg.NewRuntimePayloadAdmitter(nil, source, fact)(ctx, event, events.EventPayloadProducer(event, input.RoutingSource.Route().FlowID))
	if err != nil {
		return err
	}
	event, err = events.ApplyPayloadAdmission(event, binding)
	if err != nil {
		return err
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	delivery, err := deliverylifecycle.NewSelectedExecutionAuthority(fact, authority.ID, lineage.DestinationRunID(), authority.SelectedFork.Generation)
	if err != nil {
		return err
	}
	owner := pipelineObligationOwnerForFixture(store)
	claim, err := owner.ClaimPublication(ctx, event.ID())
	if err != nil {
		return err
	}
	defer func() { finalErr = errors.Join(finalErr, owner.Release(context.WithoutCancel(ctx), claim)) }()
	committed, err := replay.CommitSelectedForkEvent(ctx, runtimebus.CommitSelectedForkEventRequest{
		Commit: runtimebus.CommitPublishRequest{Event: admitted, RouteSettlement: testRouteSettlement(event, nil), ReplayScope: pipelineobligation.ScopeDirect, PipelineClaim: claim, DeliveryAuthority: delivery},
		Lineage: runfork.RunForkSelectedContractExecutionLineage{ForkRunID: lineage.DestinationRunID(), SourceRunID: parent.RunID(), SourceEventID: parent.ID(),
			ForkEventID: event.ID(), EventName: string(event.Type()), SelectionAuthority: lineage.AuthorityStamp(), CreatedAt: event.CreatedAt()},
	})
	if err := errors.Join(err, committed.Validate()); err != nil {
		return err
	}
	if _, found, err := store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, event.ID()); err != nil || !found {
		return errors.Join(errors.New("native copied input missing immediately after commit"), err)
	}
	t.Logf("native copied input committed: event=%s child=%s source=%s", event.ID(), event.RunID(), parent.ID())
	return nil
}
