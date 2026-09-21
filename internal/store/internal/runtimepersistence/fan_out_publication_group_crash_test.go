package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// These cuts reuse Bernoulli's context-qualified real transaction driver, not
// a settlement error pretending to be process death. Other group methods,
// including batch acquisition and prepared-event binding, remain concrete.
type publicationGroupCrashGroup struct {
	pipelineobligation.PublicationGroup
	control *pipelineCrashConnector
	before  func(int)
}

var _ pipelineobligation.PublicationGroup = publicationGroupCrashGroup{}

func (g publicationGroupCrashGroup) ValidateCommittedMembership(claims []pipelineobligation.Claim) error {
	return g.PublicationGroup.ValidateCommittedMembership(claims)
}

func (g publicationGroupCrashGroup) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	g.before(len(members))
	g.control.armed.Store(true)
	return g.PublicationGroup.Settle(context.WithValue(ctx, fanOutCrashCommitKey{}, g.control), members)
}

type publicationGroupCrashOwner struct {
	pipeline.FanOutObligationOwner
	control *pipelineCrashConnector
	cut     string
	before  func(int)
}

func (o publicationGroupCrashOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	group, err := o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil || o.cut == "after_chunk_commit" {
		return group, err
	}
	return publicationGroupCrashGroup{PublicationGroup: group, control: o.control, before: o.before}, nil
}

func (o publicationGroupCrashOwner) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	committed, err := o.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	if err == nil && o.cut == "after_chunk_commit" {
		o.control.barrier()
	}
	return committed, err
}

type publicationGroupCrashExecutor struct {
	*pipeline.PipelineCoordinator
	control *pipelineCrashConnector
	cut     string
	before  func(int)
	errors  chan error
}

type publicationGroupCrashHandlers struct{ started atomic.Int32 }

func (p *publicationGroupCrashHandlers) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.HandlerStarted && signal.SubscriberType == "node" && signal.SubscriberID == mustPersistenceRootNode("item-consumer").Key() {
		p.started.Add(1)
	}
}

type publicationGroupMemberReturnProbe struct {
	after   int
	seen    [3]string
	count   int
	barrier func()
}

func (p *publicationGroupMemberReturnProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if p.after == 0 || signal.Kind != lifecycleprobe.PostCommitDispatchCompleted || signal.Status != "fan_out_group_member_returned" {
		return
	}
	if signal.EventID == "" || p.count == len(p.seen) {
		panic("invalid actual group member return observation")
	}
	for _, id := range p.seen[:p.count] {
		if id == signal.EventID {
			panic("actual group member returned twice")
		}
	}
	p.seen[p.count] = signal.EventID
	p.count++
	if p.count == p.after {
		p.barrier()
	}
}

func publicationGroupCrashMemberPrefix(cut string) int {
	switch cut {
	case "after_chunk_commit":
		return 0
	case "after_member_1":
		return 1
	case "after_member_2":
		return 2
	default:
		return 3
	}
}

func (e *publicationGroupCrashExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	if e.cut != "" {
		owner = publicationGroupCrashOwner{FanOutObligationOwner: owner, control: e.control, cut: e.cut, before: e.before}
	}
	return e.PipelineCoordinator.ServeFanOutCandidate(ctx, owner, key)
}

func (e *publicationGroupCrashExecutor) ReportFanOutServingError(ctx context.Context, err error) {
	select {
	case e.errors <- err:
	default:
	}
	e.PipelineCoordinator.ReportFanOutServingError(ctx, err)
}

func TestIssue2394PublicationGroupSIGKILLRecoveryBothStores(t *testing.T) {
	if mode := os.Getenv("SWARM_PUBLICATION_GROUP_CRASH_CHILD"); mode != "" {
		if os.Getenv("SWARM_PUBLICATION_GROUP_CRASH_RECEIVER") == "agent" {
			runPublicationGroupAgentCrashChild(t, mode)
			return
		}
		runPublicationGroupCrashChild(t, mode)
		return
	}
	runPublicationGroupCrashCases(t, []string{"after_chunk_commit", "before_commit", "after_commit_before_ack"})
}

func TestIssue2394PublicationGroupMemberReturnSIGKILLBothStores(t *testing.T) {
	runPublicationGroupCrashCases(t, []string{"after_member_1", "after_member_2", "after_member_3"})
}

func runPublicationGroupCrashCases(t *testing.T, cuts []string) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range cuts {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "publication-group.db")
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
				}
				fixture := openFanOutCrashStore(t, backend, location)
				root := canonicalrouting.CopyForkFanOutConsumer(t, false, false)
				ctx, seeded, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend, fixture, 3, time.Now().UTC(), false, false, root, nil, nil)
				requirePublicationGroupCrashWorkflowLoad(t, ctx, fixture, seeded.runID)
				env := []string{"SWARM_FAN_OUT_CRASH_BACKEND=" + backend, "SWARM_FAN_OUT_CRASH_LOCATION=" + location,
					"SWARM_FAN_OUT_CRASH_ROOT=" + root, "SWARM_FAN_OUT_CRASH_RUN=" + seeded.runID, "SWARM_FAN_OUT_CRASH_CUT=" + cut}
				cmd, evidence, exited := startPublicationGroupCrashChild(t, "predecessor", env)
				old := awaitFanOutCrashEvidence(t, evidence)
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-exited:
					exit, ok := err.(*exec.ExitError)
					if !ok {
						t.Fatalf("predecessor did not die: %v", err)
					}
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
						t.Fatalf("not actual SIGKILL: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("group predecessor did not exit after SIGKILL")
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, fixture.db, seeded, 3, 3)
				ids := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID)
				if len(ids) != 3 {
					t.Fatalf("committed group membership=%v", ids)
				}
				effects, receipts := publicationGroupCrashMemberPrefix(cut), 0
				if cut == "after_commit_before_ack" {
					receipts = 3
				}
				assertPublicationGroupCrashEffects(t, ctx, fixture, seeded.runID, ids, effects, receipts)
				oldJSON, err := json.Marshal(old.Authority)
				if err != nil {
					t.Fatal(err)
				}
				env = append(env, "SWARM_FAN_OUT_CRASH_PREDECESSOR="+string(oldJSON))
				_, evidence, exited = startPublicationGroupCrashChild(t, "successor", env)
				next := awaitFanOutCrashEvidence(t, evidence)
				if !next.Recovered || next.Authority.PredecessorAuthorityID != old.Authority.AuthorityID {
					t.Fatalf("missing exact successor evidence: old=%+v next=%+v", old, next)
				}
				select {
				case err := <-exited:
					if err != nil {
						t.Fatalf("successor failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("successor did not join after recovery")
				}
				if got := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID); !reflect.DeepEqual(ids, got) {
					t.Fatalf("recovery changed committed identities: before=%v after=%v", ids, got)
				}
				assertPublicationGroupCrashEffects(t, ctx, fixture, seeded.runID, ids, 3, 3)
				t.Logf("real group SIGKILL at %s, exact selected-store takeover, exact ordinal-prefix effects and missing-suffix handler executions, final public recipient effects/receipts, startup candidate discovery; not deferred-agent B15 signal recovery", cut)
			})
		}
	}
}

func requirePublicationGroupCrashWorkflowLoad(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, runID string) {
	t.Helper()
	owner := pipeline.NewWorkflowPersistence(fixture.store.(pipeline.WorkflowPersistenceOwner))
	_, found, err := owner.LoadWorkflowInstance(ctx, flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)})
	if err != nil || !found {
		t.Fatalf("crash root workflow load: found=%v err=%v", found, err)
	}
}

func startPublicationGroupCrashChild(t *testing.T, mode string, env []string) (*exec.Cmd, <-chan fanOutCrashEvidence, <-chan error) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestIssue2394PublicationGroupSIGKILLRecoveryBothStores$", "-test.count=1", "-test.timeout=45s")
	cmd.Env = append(append(os.Environ(), env...), "SWARM_PUBLICATION_GROUP_CRASH_CHILD="+mode)
	cmd.ExtraFiles = []*os.File{write}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		_ = read.Close()
		_ = write.Close()
		t.Fatal(err)
	}
	_ = write.Close()
	wait, done := make(chan error, 1), make(chan struct{})
	go func() { wait <- cmd.Wait(); close(done) }()
	evidence := make(chan fanOutCrashEvidence, 1)
	go func() {
		defer close(evidence)
		defer read.Close()
		var record fanOutCrashEvidence
		if json.NewDecoder(read).Decode(&record) == nil {
			evidence <- record
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = read.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("group crash child did not join")
		}
	})
	return cmd, evidence, wait
}

func runPublicationGroupCrashChild(t *testing.T, mode string) {
	backend, location := os.Getenv("SWARM_FAN_OUT_CRASH_BACKEND"), os.Getenv("SWARM_FAN_OUT_CRASH_LOCATION")
	fixture := openFanOutCrashStore(t, backend, location)
	runID := os.Getenv("SWARM_FAN_OUT_CRASH_RUN")
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, os.Getenv("SWARM_FAN_OUT_CRASH_ROOT"), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	ctx, cancel := context.WithTimeout(correlation.WithRunID(correlation.WithSourceArtifactFact(testAuthorActivityContextForBundle(fact.BundleHash()), fact), runID), 40*time.Second)
	defer cancel()
	request := testStartupAcquireRequest("publication-group-crash-" + mode)
	process, err := fixture.store.(startupownership.Store).AcquireProcessCapability(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	authority, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if mode == "successor" {
		var old startupownership.Authority
		if err := json.Unmarshal([]byte(os.Getenv("SWARM_FAN_OUT_CRASH_PREDECESSOR")), &old); err != nil {
			t.Fatal(err)
		}
		if authority.AuthorityID == old.AuthorityID || authority.PredecessorAuthorityID != old.AuthorityID || authority.AuthorityGeneration != old.AuthorityGeneration+1 || authority.AcquisitionKind != startupownership.AcquisitionCrashTakeover {
			t.Fatalf("not exact process crash takeover: old=%+v next=%+v", old, authority)
		}
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, exists, err := process.CurrentSourceSet(ctx)
	if err != nil || exists != (mode == "successor") || (exists && current.Revision != plan.Revision) {
		t.Fatalf("source-set continuity: exists=%v current=%+v err=%v", exists, current, err)
	}
	if !exists {
		if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
	}
	live, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := live.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := live.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(request.RuntimeInstanceID, fact.BundleHash()))
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := authoractivity.ScopeFromContext(ctx)
	catalog, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	workProcess := worklifetime.NewProcess()
	work, err := workProcess.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: request.RuntimeInstanceID, BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	ctx = worklifetime.WithRuntimeOccurrence(ctx, work)
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.RetireAndWait(deadline); err != nil {
			t.Error(err)
		}
		workProcess.Retire()
		if _, err := workProcess.Join(deadline); err != nil {
			t.Error(err)
		}
	})
	selected := fixture.store.(storeTestDurableEventBusStore)
	deliveryAuthority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, request.RuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, deliveryAuthority); err != nil {
		t.Fatal(err)
	}
	memberProbe := &publicationGroupMemberReturnProbe{}
	if mode == "predecessor" {
		switch cut := os.Getenv("SWARM_FAN_OUT_CRASH_CUT"); cut {
		case "after_member_1", "after_member_2", "after_member_3":
			memberProbe.after = publicationGroupCrashMemberPrefix(cut)
		}
	}
	eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work, RuntimeInstanceID: request.RuntimeInstanceID, DeliveryAuthority: deliveryAuthority, TestLifecycleProbe: memberProbe})
	if err != nil {
		t.Fatal(err)
	}
	workflow := fixture.store.(workflowTestSelectedStore)
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	handlers := &publicationGroupCrashHandlers{}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
		TestLifecycleProbe: handlers,
	})
	if coordinator == nil {
		t.Fatal("real group crash pipeline dependencies incomplete")
	}
	eventBus.SetInterceptors(coordinator)
	continuations, err := deliverycontinuation.New(selected, selected, deliveryAuthority, work, eventBus, func(_ context.Context, err error) { t.Errorf("actual continuation recovery: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	if err := eventBus.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatal(err)
	}
	if err := continuations.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := continuations.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	report := os.NewFile(3, "publication-group-crash-evidence")
	defer report.Close()
	sink := &pipelineGracefulSink{}
	sinkRegistration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: fact.BundleHash()}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sinkRegistration.Release)
	control := &pipelineCrashConnector{cut: os.Getenv("SWARM_FAN_OUT_CRASH_CUT")}
	var beforeSubmit int32
	control.barrier = func() {
		if (control.cut == "before_commit" || control.cut == "after_commit_before_ack") && sink.submits.Load() != beforeSubmit {
			panic("group candidate submission preceded settlement acknowledgement")
		}
		if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: authority}); err != nil {
			panic(err)
		}
		select {} // The parent sends SIGKILL, not graceful cancellation.
	}
	memberProbe.barrier = control.barrier
	serving := &publicationGroupCrashExecutor{PipelineCoordinator: coordinator, control: control, errors: make(chan error, 8)}
	if mode == "predecessor" {
		serving.cut = control.cut
	}
	serving.before = func(members int) {
		if members != 3 {
			panic(fmt.Sprintf("group crash requires actual full three-member settlement, got %d", members))
		}
		beforeSubmit = sink.submits.Load()
	}
	workers := 1
	registration, err := startupownership.StartFanOutServing(ctx, live, work, &workers, serving)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	if mode == "predecessor" {
		select {
		case err := <-serving.errors:
			t.Fatalf("real shared group serving failed before crash cut: %v", err)
		case <-ctx.Done():
			t.Fatal("actual group never reached named SIGKILL cut")
		}
		return
	}
	ids := fanOutCrashOutcomeIDs(t, ctx, fixture.db, runID)
	recovery := pipeline.NewRecoveryManagerWith(eventBus)
	if err := recovery.RecoverToExhaustion(ctx); err != nil {
		t.Fatalf("actual startup pipeline recovery: %v", err)
	}
	if err := continuations.Synchronize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatal(err)
	}
	assertPublicationGroupCrashEffects(t, ctx, fixture, runID, ids, 3, 3)
	before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	if err := recovery.RecoverToExhaustion(ctx); err != nil {
		t.Fatal(err)
	}
	if err := continuations.Synchronize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("duplicate startup recovery mutated acknowledged recipient history")
	}
	wantHandlers := int32(3 - publicationGroupCrashMemberPrefix(control.cut))
	if got := handlers.started.Load(); got != wantHandlers {
		t.Fatalf("successor actual recipient executions=%d want=%d; acknowledged deliveries must not execute again", got, wantHandlers)
	}
	// Remove only the observation sink. The actual startup executor must find
	// the durable candidate without a predecessor's after-commit submission.
	sinkRegistration.Release()
	observed := &pipelineCrashCandidateObserver{CandidateStore: fixture.store.(runlifecycle.CandidateStore), results: make(chan pipelineCrashCandidateResult, 8)}
	lifecycle, err := runlifecycle.NewExecutor(observed, runlifecycle.CandidateScope{BundleHash: fact.BundleHash()}, runlifecycle.NewTerminalCatalog(nil, map[string][]string{".": {"done", "exhausted"}}), work, runlifecycle.ExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lifecycle.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	if err := lifecycle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-observed.results:
		if result.err != nil || result.candidate.RunID != runID || result.outcome.Outcome != runlifecycle.OutcomeAwaitMutation {
			t.Fatalf("actual startup candidate recovery: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not discover durable group candidate")
	}
	if err := lifecycle.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if observed.executions.Load() != 1 {
		t.Fatalf("candidate executed %d times", observed.executions.Load())
	}
	if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: authority, Recovered: true}); err != nil {
		t.Fatal(err)
	}
}

func assertPublicationGroupCrashEffects(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, runID string, ids []string, effects, receipts int) {
	t.Helper()
	if len(ids) != 3 {
		t.Fatalf("expected exact three-publication group, got %v", ids)
	}
	reader := fixture.store.(interface {
		LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	})
	for ordinal, id := range ids {
		wantEffect := 0
		if ordinal < effects {
			wantEffect = 1
		}
		view, err := reader.LoadOperatorEvent(ctx, id)
		if err != nil || view.RunID != runID || view.Payload["value"] != fmt.Sprintf("item-%03d", ordinal) || len(view.Deliveries) != 1 || view.NoDelivery != nil || len(view.DeadLetters) != 0 {
			t.Fatalf("group public publication ordinal%d: %+v err=%v", ordinal, view, err)
		}
		delivery := view.Deliveries[0]
		if delivery.SubscriberType != "node" || delivery.SubscriberID != mustPersistenceRootNode("item-consumer").Key() || delivery.Target.EntityID != runID {
			t.Fatalf("not the real declared recipient: %+v", delivery)
		}
		wantStatus := "pending"
		if wantEffect == 1 {
			wantStatus = "delivered"
		}
		if delivery.Status != wantStatus || delivery.Terminal != (wantEffect == 1) || delivery.Failure != nil || delivery.RetryCount != 0 || delivery.RetryScheduled {
			t.Fatalf("recipient settlement at crash/recovery: %+v want=%s", delivery, wantStatus)
		}
		var count, success int
		if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END),0) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, id).Scan(&count, &success); err != nil || count != receipts/3 || success != count {
			t.Fatalf("exact platform receipt for %s: total=%d success=%d want=%d err=%v", id, count, success, receipts/3, err)
		}
		if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='processed_value'`, runID, id).Scan(&count); err != nil || count != wantEffect {
			t.Fatalf("exact recipient business mutation for %s=%d want=%d err=%v", id, count, wantEffect, err)
		}
		if wantEffect == 1 {
			var raw string
			if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(new_value AS TEXT) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='processed_value'`, runID, id).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var value string
			if err := json.Unmarshal([]byte(raw), &value); err != nil || value != view.Payload["value"] {
				t.Fatalf("recipient effect disagrees with exact ordinal: %s err=%v", raw, err)
			}
		}
	}
}
