package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type activityResultPublicationCut struct {
	*activityJournalProofBus
	fault error
}

func (b *activityResultPublicationCut) Publish(ctx context.Context, event events.Event) error {
	if b.fault != nil {
		return b.fault
	}
	return b.activityJournalProofBus.Publish(ctx, event)
}

func openActivityReplayStore(t *testing.T, backend string) (gateRecoveryStoreCase, func() error, func() gateRecoveryStoreCase) {
	t.Helper()
	if backend == "sqlite" {
		selected, reopen := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
		first := gateRecoveryStoreCase{name: backend, events: selected, cards: selected, lifecycle: selected, persistence: runtimepipeline.NewWorkflowPersistence(selected), trace: selected}
		return first, selected.Close, func() gateRecoveryStoreCase {
			reopened := reopen()
			return gateRecoveryStoreCase{name: backend, events: reopened, cards: reopened, lifecycle: reopened, persistence: runtimepipeline.NewWorkflowPersistence(reopened), trace: reopened}
		}
	}
	if backend != "postgres" {
		t.Fatalf("unknown activity replay backend %q", backend)
	}
	selected, reopen := storetest.StartPostgresRuntimeStoreWithReopen(t)
	first := gateRecoveryStoreCase{name: backend, events: selected, cards: selected, lifecycle: selected, persistence: runtimepipeline.NewWorkflowPersistence(selected), trace: selected}
	return first, selected.Close, func() gateRecoveryStoreCase {
		reopened := reopen()
		return gateRecoveryStoreCase{name: backend, events: reopened, cards: reopened, lifecycle: reopened, persistence: runtimepipeline.NewWorkflowPersistence(reopened), trace: reopened}
	}
}

func TestActivityTerminalResultUsesDurableTimestampBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, status := range []string{runtimepipeline.ActivityAttemptStatusSucceeded, runtimepipeline.ActivityAttemptStatusFailed, runtimepipeline.ActivityAttemptStatusUncertain} {
			for _, cut := range []string{"result_persisted", "journal_committed_result_absent"} {
				t.Run(backend+"/"+status+"/"+cut, func(t *testing.T) {
					selected, closeStore, reopen := openActivityReplayStore(t, backend)
					runID := uuid.NewString()
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						calls.Add(1)
						if status == runtimepipeline.ActivityAttemptStatusUncertain {
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Errorf("interrupt provider response: %v", err)
								return
							}
							_ = conn.Close()
							return
						}
						if status == runtimepipeline.ActivityAttemptStatusFailed {
							w.WriteHeader(http.StatusForbidden)
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"ok": status == runtimepipeline.ActivityAttemptStatusSucceeded})
					}))
					t.Cleanup(server.Close)
					tool := runtimecontracts.MustToolSchemaEntry(
						runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
						runtimecontracts.WithToolEffect(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
						runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
						runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: server.URL}),
					)
					source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool})
					ctx := requireActivityReplayRun(t, selected, source, runID)
					bus := newActivityJournalProofBus(t, selected, source)
					publication := &activityResultPublicationCut{activityJournalProofBus: bus}
					fault := errors.New("process stopped after terminal journal commit before result publication")
					if cut == "journal_committed_result_absent" {
						publication.fault = fault
					}
					pc := newGateRecoveryCoordinator(publication, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
					intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
					intent = seedSelectedActivitySource(t, ctx, selected, pc, intent)
					beforeExecution := time.Now().UTC().Truncate(time.Microsecond)
					err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent)
					afterExecution := time.Now().UTC().Truncate(time.Microsecond)
					if cut == "journal_committed_result_absent" {
						if !errors.Is(err, fault) {
							t.Fatalf("terminal commit cut: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					journal, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
					if err != nil || !found || journal.Status != status || journal.CompletedAt == nil || journal.CompletedAt.IsZero() || calls.Load() != 1 {
						t.Fatalf("terminal receipt: found=%t status=%s completed=%v provider_calls=%d err=%v", found, journal.Status, journal.CompletedAt, calls.Load(), err)
					}
					if journal.CompletedAt.Before(beforeExecution) || journal.CompletedAt.After(afterExecution) || !journal.CompletedAt.Equal(journal.CompletedAt.UTC().Truncate(time.Microsecond)) || !journal.UpdatedAt.Equal(*journal.CompletedAt) {
						t.Fatalf("terminal timestamp %s is not one canonical completion fact within [%s, %s]; updated=%s", journal.CompletedAt, beforeExecution, afterExecution, journal.UpdatedAt)
					}
					prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, journal.ResultEventID)
					if err != nil || found != (cut == "result_persisted") {
						t.Fatalf("pre-restart publication cut: found=%t err=%v", found, err)
					}
					if found && !prepared.Event.Event().CreatedAt().Equal(journal.CompletedAt.UTC().Truncate(time.Microsecond)) {
						t.Fatal("first publication did not use the durable completion timestamp")
					}
					if err := bus.WaitForQuiescence(ctx); err != nil {
						t.Fatal(err)
					}
					if err := closeStore(); err != nil {
						t.Fatal(err)
					}
					selected = reopen()
					bus = newActivityJournalProofBus(t, selected, source)
					pc = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
					if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent); err != nil {
						t.Fatalf("reopened terminal replay: %v", err)
					}
					result := loadActivityResultForProof(t, ctx, selected, journal.ResultEventID)
					if !result.Event.Event().CreatedAt().Equal(journal.CompletedAt.UTC().Truncate(time.Microsecond)) {
						t.Fatal("reconstructed result reminted the completion timestamp")
					}
					if found {
						replayed, present, err := selected.events.LoadPreparedPublishEvent(ctx, journal.ResultEventID)
						if err != nil || !present {
							t.Fatalf("reopened prepared result: found=%t err=%v", present, err)
						}
						verifier := selected.events.(runtimebus.PreparedPublishEventIdentityVerifier)
						if err := verifier.VerifyPreparedPublishEventIdentity(replayed.Event, prepared); err != nil {
							t.Fatalf("reopened result changed immutable event identity: %v", err)
						}
					}
					before := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
					if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent); err != nil {
						t.Fatalf("exact duplicate after reconstruction: %v", err)
					}
					after := loadActivityResultForProof(t, ctx, selected, journal.ResultEventID)
					if !reflect.DeepEqual(result, after) || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
						t.Fatal("exact replay changed immutable result facts or durable side effects")
					}
					ready := make(chan struct{}, 2)
					start := make(chan struct{})
					replayErrors := make(chan error, 2)
					for range 2 {
						go func() {
							ready <- struct{}{}
							<-start
							replayErrors <- runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent)
						}()
					}
					<-ready
					<-ready
					close(start)
					succeeded := 0
					for range 2 {
						if err := <-replayErrors; err == nil {
							succeeded++
						} else if !errors.Is(err, pipelineobligation.ErrBusy) {
							t.Errorf("concurrent terminal replay: %v", err)
						}
					}
					// The existing exact publication claim may refuse the competing
					// writer; neither refusal nor success may mint new result facts.
					if succeeded == 0 {
						t.Fatal("neither concurrent terminal replay acquired publication authority")
					}
					if !reflect.DeepEqual(result, loadActivityResultForProof(t, ctx, selected, journal.ResultEventID)) || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
						t.Fatal("concurrent replay changed result facts or durable side effects")
					}
					stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, journal.RequestEventID)
					if err != nil || !found || !reflect.DeepEqual(journal, stored) || calls.Load() != 1 {
						t.Fatalf("replay altered receipt or redispatched provider: calls=%d found=%t err=%v", calls.Load(), found, err)
					}
					// A genuinely changed same-ID result must still fail before side effects.
					original := bus.publishes[0]
					changed := eventtest.ChildForProducerWithRoutingSource(
						original.ID(), original.Type(), original.Producer(), original.TaskID(), original.Payload(), original.ChainDepth(),
						events.EventLineage{RunID: original.RunID(), ParentEventID: original.ParentEventID(), TaskID: original.TaskID(), ExecutionMode: original.ExecutionMode()},
						original.Envelope(), original.RoutingSource(), original.CreatedAt().Add(time.Second),
					)
					if err := bus.Publish(ctx, changed); !errors.Is(err, events.ErrEventIdentityConflict) {
						t.Fatalf("changed immutable fact accepted: %v", err)
					}
					if !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
						t.Fatal("rejected duplicate added durable side effects")
					}
				})
			}
		}
	}
}

func TestActivityTerminalInvalidTimestampCannotPublishBothStores(t *testing.T) {
	for _, tc := range activityTerminalReplayStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			source := activityJournalProofSource(t, nil)
			ctx := requireActivityReplayRun(t, selected, source, runID)
			bus := newActivityJournalProofBus(t, selected, source)
			pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
			intent = seedSelectedActivitySource(t, ctx, selected, pc, intent)
			started, inserted, err := activityReplayJournal(selected).StartActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent))
			if err != nil || !inserted {
				t.Fatalf("claim real journal: inserted=%t err=%v", inserted, err)
			}
			resultID, resultType, err := runtimepipeline.AdmitActivityResultForTest(intent, intent.SuccessEvent)
			if err != nil {
				t.Fatal(err)
			}
			candidate := runtimepipeline.ActivityAttemptTerminalForTest(started, runtimepipeline.ActivityAttemptStatusSucceeded,
				resultID, resultType,
				runtimepipeline.ActivitySuccessPayloadForTest(intent, map[string]any{"ok": true}), nil)
			if candidate.CompletedAt != nil {
				t.Fatal("precommit candidate unexpectedly supplies completion authority")
			}
			receipt, committed, err := activityReplayJournal(selected).CompleteActivityAttempt(ctx, candidate)
			if err != nil || !committed || receipt.CompletedAt == nil || receipt.CompletedAt.IsZero() {
				t.Fatalf("real terminal commit: committed=%t receipt=%+v err=%v", committed, receipt, err)
			}
			before := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
			zero := time.Time{}
			for _, invalid := range []*time.Time{nil, &zero} {
				broken := receipt
				broken.CompletedAt = invalid
				if err := runtimepipeline.PublishJournaledActivityResultForTest(ctx, pc, intent, broken); err == nil || !strings.Contains(err.Error(), "completion timestamp") {
					t.Fatalf("invalid receipt timestamp accepted: %v", err)
				}
				if len(bus.attempts) != 0 || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
					t.Fatal("invalid timestamp reached publication or added durable side effects")
				}
			}
			if err := runtimepipeline.PublishJournaledActivityResultForTest(ctx, pc, intent, receipt); err != nil {
				t.Fatalf("valid durable receipt cannot recover: %v", err)
			}
			stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, receipt.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(stored, receipt) {
				t.Fatal("timestamp rejection or recovery changed the terminal receipt")
			}
		})
	}
}

func TestActivityRejectedChannelTargetReplaysDurableResultBothStores(t *testing.T) {
	for _, tc := range activityTerminalReplayStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			source := activityJournalProofSource(t, nil)
			ctx := requireActivityReplayRun(t, selected, source, runID)
			bus := newActivityJournalProofBus(t, selected, source)
			pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
			intent.Tool = runtimecontracts.PrivateChannelActivityPrefix + "missing"
			intent = seedSelectedActivitySource(t, ctx, selected, pc, intent)
			var calls atomic.Int32
			client := &http.Client{Transport: activityTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("invalid channel target must never dispatch")
			})}
			if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, client, intent); err != nil {
				t.Fatalf("journal target rejection: %v", err)
			}
			receipt, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
			if err != nil || !found || receipt.Status != runtimepipeline.ActivityAttemptStatusFailed || receipt.CompletedAt == nil || receipt.Failure == nil || receipt.Failure.Detail.Code != "channel_activity_plan_generation_unavailable" {
				t.Fatalf("missing durable rejection: found=%t receipt=%+v err=%v", found, receipt, err)
			}
			before := loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)
			if !before.Event.Event().CreatedAt().Equal(receipt.CompletedAt.UTC().Truncate(time.Microsecond)) {
				t.Fatal("target rejection regenerated completion time")
			}
			counts := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
			pc = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, client, intent); err != nil {
				t.Fatalf("replay target rejection: %v", err)
			}
			after := loadActivityResultForProof(t, ctx, selected, receipt.ResultEventID)
			stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, receipt.RequestEventID)
			if calls.Load() != 0 || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(counts, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) || err != nil || !found || !reflect.DeepEqual(stored, receipt) {
				t.Fatal("replayed rejection changed durable facts or dispatched provider")
			}
		})
	}
}

type activityTerminalRoundTripFunc func(*http.Request) (*http.Response, error)

func (f activityTerminalRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func activityReplayJournal(selected gateRecoveryStoreCase) runtimepipeline.ActivityAttemptJournal {
	return selected.events.(runtimepipeline.ActivityAttemptJournal)
}

func activityJournalProofSource(t *testing.T, tools map[string]runtimecontracts.ToolSchemaEntry) semanticview.Source {
	t.Helper()
	files := canonicalrouting.ActivityTerminalReplayDocuments(t)
	if len(tools) != 0 {
		raw, err := yaml.Marshal(tools)
		if err != nil {
			t.Fatalf("serialize actual activity tool declarations: %v", err)
		}
		files["tools.yaml"] = string(raw)
	}
	bundle := loadPipelineLifecycleFixtureBundle(t, files)
	for name, expected := range tools {
		actual, found := bundle.Tools[name]
		if !found {
			t.Fatalf("compiled source omitted activity tool %s", name)
		}
		// Compiled channel result projections are runtime binding facts, not YAML.
		if projection, present := expected.CompiledResult(); present {
			var err error
			actual, err = actual.WithCompiledResult(projection)
			if err != nil {
				t.Fatalf("compose admitted channel projection: %v", err)
			}
			bundle.Tools[name] = actual
			bundle.FlowTree.Root.Tools[name] = actual
		}
		wantHash, wantErr := expected.CanonicalHash()
		gotHash, gotErr := actual.CanonicalHash()
		if wantErr != nil || gotErr != nil || wantHash != gotHash {
			t.Fatalf("actual activity tool %s drifted through compilation: expected=%s actual=%s errors=%v/%v", name, wantHash, gotHash, wantErr, gotErr)
		}
		resolved, present := bundle.ToolEntryForFlow("research", name)
		resolvedHash, resolvedErr := resolved.CanonicalHash()
		if !present || resolvedErr != nil || resolvedHash != wantHash {
			t.Fatalf("activity node resolves a different tool %s: found=%v hash=%s err=%v", name, present, resolvedHash, resolvedErr)
		}
	}
	return semanticview.Wrap(bundle)
}

func requireActivityReplayRun(t *testing.T, selected gateRecoveryStoreCase, source semanticview.Source, runID string) context.Context {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("activity journal fixture requires its actual compiled artifact")
	}
	fact := sourceartifactfixture.RequireArtifact(t, context.Background(), selected.events.(sourceartifactfixture.Writer), bundle.SourceArtifact)
	ctx := withLiveGateExecution(testAuthorActivityContextForSource(t, correlation.WithRunID(context.Background(), runID), fact))
	storetest.RequireRun(t, ctx, selected.events.(storetest.RunFixtureStore), storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC(),
	})
	return ctx
}

func loadActivityResultForProof(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, eventID string) runtimebus.PreparedPublishEvent {
	t.Helper()
	result, found, err := selected.events.LoadPreparedPublishEvent(ctx, eventID)
	if err != nil || !found {
		t.Fatalf("load durable activity result: found=%t err=%v", found, err)
	}
	return result
}

func activityTerminalReplayStoreCases() []struct {
	name string
	open func(*testing.T) gateRecoveryStoreCase
} {
	return []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{"sqlite", func(t *testing.T) gateRecoveryStoreCase { s, _, _ := openActivityReplayStore(t, "sqlite"); return s }},
		{"postgres", func(t *testing.T) gateRecoveryStoreCase { s, _, _ := openActivityReplayStore(t, "postgres"); return s }},
	}
}

// Observe the real bus and persistence without replacing either semantic owner.
type activityJournalProofBus struct {
	*runtimebus.EventBus
	mu        sync.Mutex
	publishes []events.Event
	attempts  []events.Event
}

func (b *activityJournalProofBus) Publish(ctx context.Context, event events.Event) error {
	b.mu.Lock()
	b.attempts = append(b.attempts, event)
	b.mu.Unlock()
	if err := b.EventBus.Publish(ctx, event); err != nil {
		return err
	}
	b.mu.Lock()
	b.publishes = append(b.publishes, event)
	b.mu.Unlock()
	return nil
}

func newActivityJournalProofBus(t *testing.T, selected gateRecoveryStoreCase, source semanticview.Source) *activityJournalProofBus {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("activity publication bus requires its exact compiled artifact")
	}
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
	},
		"research/entity-1/research.scanner_provider_write.succeeded", "research/entity-1/research.scanner_provider_write.failed",
		"research/entity-1/channel.deliver.succeeded", "research/entity-1/channel.deliver.failed",
		"research/entity-1/telegram.send_message.succeeded", "research/entity-1/telegram.send_message.failed")
	if err != nil {
		t.Fatal(err)
	}
	return &activityJournalProofBus{EventBus: bus}
}

func seedSelectedActivitySource(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, pc *runtimepipeline.PipelineCoordinator, intent runtimeengine.ActivityIntent) runtimeengine.ActivityIntent {
	t.Helper()
	source := pc.SemanticSource()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || correlation.RunIDFromContext(ctx) != intent.SourceRunID {
		t.Fatal("activity construction requires its exact source and run context")
	}
	ctx = effects.WithExecutionMode(ctx, intent.ExecutionMode)
	root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), intent.SourceRunID, intent.SourceRunID, intent.SourceRunID, "")
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: source, SourceArtifactFact: fact, WorkflowInstances: pc,
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	at := time.Now().UTC().Truncate(time.Microsecond)
	rootPlan, err := planner.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: root, OccurredAt: at,
	})
	if err != nil {
		t.Fatalf("prepare activity structural root: %v", err)
	}
	owner := selected.events.(runtimebus.FlowInstanceActivationCommitOwner)
	rootCommit, err := owner.CommitFlowInstanceActivation(ctx, runtimebus.FlowInstanceActivationCommand{Plan: rootPlan})
	if err != nil || !rootCommit.Acknowledged || !rootCommit.Created {
		t.Fatalf("construct activity structural root: result=%+v err=%v", rootCommit, err)
	}
	_, key, present := strings.Cut(intent.FlowInstance, "research/")
	if !present || key == "" {
		t.Fatal("activity source requires its exact research key")
	}
	child, err := flowidentity.KeyedChild(source, root, "research", key)
	if err != nil || child.InstancePath != intent.FlowInstance {
		t.Fatalf("derive actual activity instance: identity=%+v err=%v", child, err)
	}
	payload, err := json.Marshal(map[string]string{"marker": key})
	if err != nil {
		t.Fatal(err)
	}
	creating := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(intent.SourceEventID, "activity.source", "activity-proof", intent.SourceTaskID, payload, intent.ChainDepth, intent.SourceRunID, events.EventEnvelope{}, eventtest.RootRoutingSource(intent.SourceRunID), at, intent.ExecutionMode)
	storetest.CommitSemanticEvent(t, ctx, selected.events, creating)
	plan, err := planner.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: child, ConstructorInput: "activity.source", ResolvedKey: key,
		TriggerEvent: creating, OccurredAt: at,
	})
	if err != nil {
		t.Fatalf("prepare actual activity instance: %v", err)
	}
	committed, err := owner.CommitFlowInstanceActivation(ctx, runtimebus.FlowInstanceActivationCommand{Plan: plan})
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct actual activity instance: result=%+v err=%v", committed, err)
	}
	receiver := flowidentity.RunScopedFlowInstance{RunID: intent.SourceRunID, Route: child.Route()}
	lookup, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, receiver)
	if err != nil {
		t.Fatal(err)
	}
	observed, found, err := selected.events.(runtimepipeline.FlowInstanceIndexReader).LookupFlowInstance(ctx, lookup)
	if err != nil || !found {
		t.Fatalf("read actual activity construction: found=%t err=%v", found, err)
	}
	if err := observed.ValidateSelection(lookup); err != nil {
		t.Fatal(err)
	}
	receipt, err := storetest.ReadReceiverConstructionPublication(ctx, selected.events, receiver, child.EntityID)
	if err != nil || receipt.Identity != child || receipt.BundleHash != fact.BundleHash() ||
		receipt.WorkflowVersion != source.WorkflowVersion() || receipt.CreatingInput != plan.CreatingInput {
		t.Fatalf("activity construction receipt: evidence=%+v err=%v", receipt, err)
	}
	intent.EntityID = identity.NormalizeEntityID(child.EntityID)
	actor := activityidentity.MustNodeOwner(externalPipelineSourceNode(t, source, child.TemplateID, "scanner"))
	if actor.Key() != intent.Owner.Key() {
		t.Fatal("compiled activity owner changed its original node identity")
	}
	intent.Owner = actor
	intent.RoutingSource, err = events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: child.TemplateID, FlowInstance: child.InstancePath, EntityID: child.EntityID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return intent.Normalized()
}
