package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedActivityCrashCheckpoint struct {
	InputID, ResultID  string
	ResultPayload      json.RawMessage
	Selection          effects.SelectedContractForkAuthority
	ExecutionOwner     string
	LeaseExpiresAt     time.Time
	FenceGeneration    uint64
	ExecutionMode      executionmode.Mode
	ProviderCalls      int64
	ConsumedGeneration attemptgeneration.Generation
}

type selectedActivityInputSettlementHold struct {
	pipelineobligation.Store
	sourceRequest string
}

func (p selectedActivityInputSettlementHold) Settle(ctx context.Context, claim pipelineobligation.Claim, disposition pipelineobligation.Disposition) (pipelineobligation.SettlementOutcome, error) {
	authority, selected := effects.AuthorityFromContext(ctx)
	if selected && authority.Kind == effects.AuthoritySelectedContractFork && claim.EventID() == activityidentity.ForkLineageEventID(authority.SelectedFork.ForkRunID, p.sourceRequest) {
		<-ctx.Done()
		return pipelineobligation.SettlementOutcome{}, ctx.Err()
	}
	return p.Store.Settle(ctx, claim, disposition)
}

type selectedActivityResultConsumerCrash struct {
	pipeline.WorkflowPersistenceOwner
	test  *testing.T
	after func(context.Context, events.Event, attemptgeneration.Generation)
}

func (p selectedActivityResultConsumerCrash) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	committed, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if !committed.Committed || err != nil || command.DeliverySuccess == nil {
		return committed, err
	}
	event, found := correlation.InboundEventFromContext(ctx)
	if !found || event.RunID() != command.DeliverySuccess.Claim.RunID() {
		p.test.Fatalf("acknowledged result consumer lost its exact inbound frame: found=%t", found)
	}
	if event.Type() == "flow_a/connector.succeeded" {
		var buckets map[string]map[string]any
		if err := json.Unmarshal(command.State.Accumulator, &buckets); err != nil {
			p.test.Fatal(err)
		}
		loops, err := loopruntime.List(buckets)
		if err != nil || len(loops) != 1 || loops[0].Attempt != 2 {
			p.test.Fatalf("result consumer did not advance its owned loop: %+v error=%v", loops, err)
		}
		p.after(ctx, event, loops[0].Generation())
	}
	return committed, err
}

func (c selectedActivityCrashCheckpoint) previousAuthority() effects.Authority {
	return effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: c.Selection.ExecutionID, SelectedFork: c.Selection,
		ExecutionOwner: c.ExecutionOwner, LeaseExpiresAt: c.LeaseExpiresAt, FenceGeneration: c.FenceGeneration, ExecutionMode: c.ExecutionMode}
}

type selectedActivitySuccessorClaim struct {
	SelectedContractRuntimeExecutionLifecycle
	claimed chan effects.Authority
}

func (p selectedActivitySuccessorClaim) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, execution runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (effects.Authority, error) {
	authority, err := p.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, execution, owner, lease)
	if err == nil {
		p.claimed <- authority
	}
	return authority, err
}

// Embed the original native facade so the probe does not remove any publication,
// activation, readback or authority interfaces used by the actual EventBus.
type selectedActivityCrashSQLitePublication struct {
	*store.SQLiteRuntimeStore
	after func(context.Context, events.Event)
}

func (s selectedActivityCrashSQLitePublication) CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error) {
	result, err := s.SQLiteRuntimeStore.CommitPublication(ctx, command)
	if result.Acknowledged && err == nil {
		s.after(ctx, command.Commit.Event.Event())
	}
	return result, err
}

type selectedActivityCrashPostgresPublication struct {
	*store.PostgresStore
	after func(context.Context, events.Event)
}

func (s selectedActivityCrashPostgresPublication) CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error) {
	result, err := s.PostgresStore.CommitPublication(ctx, command)
	if result.Acknowledged && err == nil {
		s.after(ctx, command.Commit.Event.Event())
	}
	return result, err
}

func runSelectedActivityResultCrashHelper(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected any, owner SelectedContractExecutionOwner, capability startupownership.ProcessCapability) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":"live-read-result"}`))
	}))
	defer server.Close()
	selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
	advanced := os.Getenv("SWARM_SELECTED_CRASH_CUT") == "retained_activity_read_result_consumed"
	readOnly := advanced || os.Getenv("SWARM_SELECTED_CRASH_CUT") == "retained_activity_read_result_committed"
	effectClass, forkPolicy, wantCalls := contracts.ActivityEffectClassNonIdempotentWrite, contracts.ActivityForkReuseRecordedResult, int64(0)
	if readOnly {
		effectClass, forkPolicy, wantCalls = contracts.ActivityEffectClassReadOnly, contracts.ActivityForkReexecuteRead, 1
	}
	loaded := selectedContractActivityDeclaredSourceWithResultConsumer(t, server.URL, effectClass, selection, advanced)
	fixture := activityForkFixture{db: activityForkFixtureDB{DB: db, sqlite: backend == "sqlite"}, owner: owner}
	if native, ok := selected.(*store.SQLiteRuntimeStore); ok {
		fixture.sqlite = native
	}
	sourceRun, initiatingID, entityID := uuid.NewString(), uuid.NewString(), flowidentity.EntityID("flow_a")
	at := time.Now().UTC().Truncate(time.Microsecond)
	activation, err := loopruntime.New(sourceRun, entityID, "flow_a", "revision", "revision_id", uuid.NewString(), "review", 3, at.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	node := mustRunForkNode("flow_a", "test-node")
	fact := activityidentity.Fact{RunID: sourceRun, SourceEventID: initiatingID, EntityID: entityID, Owner: activityidentity.MustNodeOwner(node),
		ExecutionFlowID: "flow_a", HandlerEventKey: "review.requested", ActivityID: "connector", Tool: "provider.connector", Attempt: 1, RevisionID: activation.RevisionID}
	requestID := activityidentity.RequestEventID(fact)
	source := eventtest.StaticFlowRoutingSource("flow_a", "flow_a", entityID)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a", EntityID: entityID})}
	fixture.seedSource(t, loaded, sourceRun, entityID, requestID, at, route, source)
	seedSelectedContractActivityLoop(t, fixture.db, sourceRun, entityID, requestID, activation, at)
	seedSelectedContractActivityRequest(t, fixture.db, sourceRun, requestID, selectedContractActivityRequestPayload{
		ActivityID: "connector", Tool: "provider.connector", Input: map[string]any{"value": "x"}, EffectClass: string(effectClass),
		SuccessEvent: "flow_a/connector.succeeded", FailureEvent: "flow_a/connector.failed", RetryMaxAttempts: 1, ForkPolicy: string(forkPolicy),
		EntityID: entityID, FlowInstance: "flow_a", NodeID: fact.Owner.Key(), FlowID: "flow_a", HandlerEventKey: "review.requested", SourceEventID: initiatingID,
		SourceRunID: sourceRun, Attempt: 1, Generation: activation.Generation(), LoopStage: "review"})
	fixture.capture(t, sourceRun)
	if !readOnly {
		seedSelectedContractActivityAttempt(t, fixture.db, fact, activation.Generation(), "succeeded", "flow_a/connector.succeeded", "", "", at)
	}
	t.Logf("ACTIVITY_CRASH_SOURCE_READY source=%s request=%s", sourceRun, requestID)
	after := func(ctx context.Context, event events.Event, consumed attemptgeneration.Generation) {
		t.Logf("ACTIVITY_CRASH_PUBLICATION_ACK event=%s type=%s run=%s", event.ID(), event.Type(), event.RunID())
		if event.Type() != "flow_a/connector.succeeded" {
			return
		}
		authority, present := effects.AuthorityFromContext(ctx)
		if !present || !authority.Valid() || authority.SelectedFork.ForkRunID != event.RunID() || calls.Load() != wantCalls {
			t.Fatalf("result commit lost exact selected authority or changed provider execution: %+v calls=%d want=%d", authority, calls.Load(), wantCalls)
		}
		inputID := activityidentity.ForkLineageEventID(event.RunID(), requestID)
		receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, selected, inputID)
		if err != nil || receipt.Found {
			t.Fatalf("crash cut missed the still-pending original input: %+v error=%v", receipt, err)
		}
		input := storetest.LoadCanonicalEventRecord(t, ctx, selected, inputID)
		if input.RunID() != event.RunID() || input.Type() != "platform.activity_requested" {
			t.Fatal("result committed before its exact selected input publication")
		}
		pipe := os.NewFile(3, "selected-activity-checkpoint")
		checkpoint := selectedForkCrashCheckpoint{SourceRun: sourceRun, ForkRun: event.RunID(), Activity: &selectedActivityCrashCheckpoint{
			InputID: inputID, ResultID: event.ID(), ResultPayload: event.Payload(), Selection: authority.SelectedFork,
			ExecutionOwner: authority.ExecutionOwner, LeaseExpiresAt: authority.LeaseExpiresAt, FenceGeneration: authority.FenceGeneration, ExecutionMode: authority.ExecutionMode,
			ProviderCalls: calls.Load(), ConsumedGeneration: consumed}}
		if err := json.NewEncoder(pipe).Encode(checkpoint); err != nil {
			t.Fatal(err)
		}
		_ = pipe.Close()
		var b [1]byte
		_, err = os.Stdin.Read(b[:])
		t.Fatalf("acknowledged activity publication resumed without SIGKILL: %v", err)
	}
	if advanced {
		owner.ports.pipelineObligations = selectedActivityInputSettlementHold{owner.ports.pipelineObligations, requestID}
		owner.ports.workflow = pipeline.NewWorkflowPersistence(selectedActivityResultConsumerCrash{selected.(pipeline.WorkflowPersistenceOwner), t, after})
	} else {
		publication := func(ctx context.Context, event events.Event) { after(ctx, event, attemptgeneration.Generation{}) }
		switch native := selected.(type) {
		case *store.SQLiteRuntimeStore:
			owner.ports.events = selectedActivityCrashSQLitePublication{native, publication}
		case *store.PostgresStore:
			owner.ports.events = selectedActivityCrashPostgresPublication{native, publication}
		default:
			t.Fatalf("activity crash requires original native selected store, got %T", selected)
		}
	}
	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: sourceRun, At: requestID, AllowSourceFreeze: true, Owner: owner,
		SourceLoader: &fakeSelectedContractSourceLoader{loaded: loaded}, ContractSelection: selection,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability},
		ForkOperation: &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "retained-crash", IdempotencyKey: "fixed-cut", TransportHash: "retained-crash-transport",
			SourceRunID: sourceRun, ForkEventID: requestID, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ACTIVITY_CRASH_ACTIVATION_ACK child=%s activated=%t", result.Materialization.ForkRunID, result.Activation.Activated)
	<-ctx.Done()
	t.Fatal("selected activity execution stopped before its physical result checkpoint")
}

func TestIssue642SelectedActivityResultSIGKILLReactivationBothStores(t *testing.T) {
	proveSelectedActivityResultCrashRecovery(t, "retained_activity_result_committed")
}

func TestIssue642SelectedReadOnlyActivityResultSIGKILLReactivationBothStores(t *testing.T) {
	proveSelectedActivityResultCrashRecovery(t, "retained_activity_read_result_committed")
}

func TestIssue642SelectedReadOnlyConsumedResultSIGKILLAccountingBothStores(t *testing.T) {
	proveSelectedActivityResultCrashRecovery(t, "retained_activity_read_result_consumed")
}

func proveSelectedActivityResultCrashRecovery(t *testing.T, cut string) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var construct func() SelectedContractExecutionOwner
			var reopen func() startupownership.Store
			var dsn string
			if backend == "sqlite" {
				s, again := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
				selected, dsn = s, s.Path()
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				dsn = testutil.StartPostgresDSN(t)
				s, again := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
				selected = s
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
			if checkpoint.Activity == nil || !checkpoint.Activity.previousAuthority().Valid() || checkpoint.Activity.Selection.ForkRunID != checkpoint.ForkRun {
				t.Fatalf("crash cut lacks exact selected result/executor evidence: %+v", checkpoint)
			}
			inputID, resultID := checkpoint.Activity.InputID, checkpoint.Activity.ResultID
			read := context.Background()
			input := storetest.LoadCanonicalEventRecord(t, read, selected, inputID)
			result := storetest.LoadCanonicalEventRecord(t, read, selected, resultID)
			lineage, present := input.SelectedForkLineage()
			if !present || lineage.SourceRunID() != checkpoint.SourceRun || input.RunID() != checkpoint.ForkRun || result.RunID() != checkpoint.ForkRun ||
				result.Type() != "flow_a/connector.succeeded" || !reflect.DeepEqual(result.Payload(), checkpoint.Activity.ResultPayload) {
				t.Fatal("SIGKILL did not retain the exact input/result publications")
			}
			receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(read, selected, inputID)
			if err != nil || receipt.Found {
				t.Fatalf("SIGKILL did not preserve unsettled input accounting: %+v %v", receipt, err)
			}
			advanced := cut == "retained_activity_read_result_consumed"
			readOnly := advanced || cut == "retained_activity_read_result_committed"
			if advanced && (!checkpoint.Activity.ConsumedGeneration.Valid() || checkpoint.Activity.ConsumedGeneration.Attempt != 2) {
				t.Fatalf("cut lacks exact consumed-result loop advancement: %+v", checkpoint.Activity)
			}
			if advanced {
				requireSelectedActivityConsumedState(t, read, selected, checkpoint, resultID)
			}
			wantAttempts, wantCalls := 1, int64(0)
			if readOnly {
				wantAttempts, wantCalls = 0, 1
			}
			attempts, err := storetest.ReadActivityAttemptStorage(read, selected, checkpoint.ForkRun)
			if err != nil || len(attempts) != wantAttempts || checkpoint.Activity.ProviderCalls != wantCalls {
				t.Fatalf("crash lost the exact copied terminal journal: %+v %v", attempts, err)
			}
			var journal pipeline.ActivityAttemptRecord
			if !readOnly {
				var found bool
				journal, found, err = selected.(pipeline.ActivityAttemptJournal).LoadActivityAttempt(read, attempts[0].RequestEventID)
				if err != nil || !found || journal.Status != pipeline.ActivityAttemptStatusSucceeded || journal.ResultEventID != resultID {
					t.Fatalf("crash result differs from its exact native journal: %+v %v", journal, err)
				}
			}
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			original, found, err := operations.LoadForkOperation(read, "retained-crash", "fixed-cut", "retained-crash-transport")
			if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.Result == nil || original.ForkRunID != checkpoint.ForkRun {
				t.Fatalf("crash lost exact activation acknowledgment: %+v %v", original, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(read, selected, checkpoint.SourceRun)
			if err != nil {
				t.Fatal(err)
			}
			selected = reopen()
			operations = selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			ctx := runForkTestContext(t)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			owner := construct()
			process, _ := worklifetime.ProcessFromContext(ctx)
			if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			baseline := process.ActiveCount()
			claims := make(chan effects.Authority, 4)
			owner.ports.runtimeExecution = selectedActivitySuccessorClaim{owner.ports.runtimeExecution, claims}
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
			recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), SelectedForkRecoveryEnvironment{
				SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability}})
			if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
				t.Fatalf("actual selected successor failed: %+v %v", recovered, err)
			}
			wait, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			var remaining int
			for {
				receipt, err = storetest.ReadLatestPlatformPipelineReceiptStorage(wait, selected, inputID)
				if err != nil {
					t.Fatalf("read successor input settlement: last=%+v %v", receipt, err)
				}
				if receipt.Found {
					if receipt.Outcome != "success" || (len(receipt.Failure) > 0 && string(receipt.Failure) != "null") {
						t.Fatalf("successor did not preserve committed result accounting: %+v", receipt)
					}
					remaining, err = storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, checkpoint.ForkRun)
					if err != nil {
						t.Fatalf("read successor pipeline completion: %v", err)
					}
					if remaining == 0 {
						break
					}
				}
				select {
				case <-wait.Done():
					t.Fatalf("successor left exact input or output handoff unsettled: receipt=%+v incomplete=%d cause=%v", receipt, remaining, context.Cause(wait))
				case <-time.After(10 * time.Millisecond):
				}
			}
			for name, want := range map[string]int{"platform.activity_requested": 1, "flow_a/connector.succeeded": 1, "flow_a/connector.failed": 0} {
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.ForkRun, name); err != nil || count != want {
					t.Fatalf("recovery output %s=%d want=%d error=%v", name, count, want, err)
				}
			}
			if actual := storetest.LoadCanonicalEventRecord(t, wait, selected, resultID); !reflect.DeepEqual(result, actual) {
				t.Fatal("successor changed the acknowledged physical result")
			}
			if advanced {
				requireSelectedActivityConsumedState(t, wait, selected, checkpoint, resultID)
			}
			var current effects.Authority
			select {
			case current = <-claims:
			default:
				t.Fatal("recovery never claimed a current native selected executor")
			}
			if len(claims) != 0 || !current.Valid() || current.SelectedFork.ExecutionID == checkpoint.Activity.Selection.ExecutionID || current.SelectedFork.Generation <= checkpoint.Activity.Selection.Generation {
				t.Fatalf("recovery did not issue a fresh selected executor: old=%+v current=%+v", checkpoint.Activity.previousAuthority(), current)
			}
			if err := owner.ports.runtimeExecution.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, current, time.Minute); err != nil {
				t.Fatalf("fresh successor lacks native mutation authority: %v", err)
			}
			if err := owner.ports.runtimeExecution.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, checkpoint.Activity.previousAuthority(), time.Minute); err == nil {
				t.Fatal("dead selected executor retained mutation authority")
			}
			retire, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			if err := owner.RetireSelectedContexts(retire); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("selected successor did not join explicit retirement: work=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
			}
			if pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, checkpoint.ForkRun); err != nil || pending != 0 {
				t.Fatalf("successor retained incomplete pipeline work: pending=%d error=%v", pending, err)
			}
			ack, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(original, ack) {
				t.Fatalf("successor changed original activation acknowledgment: %+v %v", ack, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("child successor changed source business state: %v", err)
			}
			selected = reopen()
			durable, err := storetest.ReadLatestPlatformPipelineReceiptStorage(wait, selected, inputID)
			if err != nil || !reflect.DeepEqual(receipt, durable) {
				t.Fatalf("successor input settlement did not survive native reopen: %+v %v", durable, err)
			}
			if actual := storetest.LoadCanonicalEventRecord(t, wait, selected, resultID); !reflect.DeepEqual(result, actual) {
				t.Fatal("durable successor settlement changed the acknowledged physical output")
			}
			if advanced {
				requireSelectedActivityConsumedState(t, wait, selected, checkpoint, resultID)
			}
			if !readOnly {
				retained, found, err := selected.(pipeline.ActivityAttemptJournal).LoadActivityAttempt(wait, journal.RequestEventID)
				if err != nil || !found || !reflect.DeepEqual(journal, retained) {
					t.Fatalf("successor replay modified copied journal bytes or clocks: %+v %v", retained, err)
				}
			} else if attempts, err := storetest.ReadActivityAttemptStorage(wait, selected, checkpoint.ForkRun); err != nil || len(attempts) != 0 {
				t.Fatalf("read-only successor fabricated a journal: %+v %v", attempts, err)
			}
			t.Logf("ISSUE642_ACTIVITY_SIGKILL_REACTIVATED backend=%s input=%s result=%s old_generation=%d new_generation=%d settlement=%s leases=%d", backend,
				inputID, resultID, checkpoint.Activity.Selection.Generation, current.SelectedFork.Generation, receipt.Outcome, process.ActiveCount())
		})
	}
}

func requireSelectedActivityConsumedState(t *testing.T, ctx context.Context, selected any, checkpoint selectedForkCrashCheckpoint, resultID string) {
	t.Helper()
	header, found, err := selected.(pipeline.WorkflowPersistenceOwner).LoadWorkflowInstance(ctx,
		flowidentity.RunScopedFlowInstance{RunID: checkpoint.ForkRun, Route: flowidentity.StoredRoute("flow_a", "flow_a", "flow_a")})
	if err != nil || !found {
		t.Fatalf("native consumed-result receiver absent: found=%t error=%v", found, err)
	}
	raw, err := json.Marshal(header.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	var buckets map[string]map[string]any
	if err := json.Unmarshal(raw, &buckets); err != nil {
		t.Fatal(err)
	}
	loops, err := loopruntime.List(buckets)
	if err != nil || len(loops) != 1 || loops[0].Generation() != checkpoint.Activity.ConsumedGeneration || header.CurrentState != "review" {
		t.Fatalf("consumed loop state reset or replayed: loops=%+v expected=%+v stage=%s error=%v", loops, checkpoint.Activity.ConsumedGeneration, header.CurrentState, err)
	}
	rows, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, selected, resultID)
	if err != nil || len(rows) != 1 || rows[0].SubscriberType != "node" || rows[0].SubscriberID != mustRunForkNode("flow_a", "test-node").Key() ||
		rows[0].Status != "delivered" || rows[0].FlowInstance != "flow_a" || rows[0].EntityID != header.EntityID {
		t.Fatalf("exact consumed-result delivery is not terminal: rows=%+v error=%v", rows, err)
	}
}
