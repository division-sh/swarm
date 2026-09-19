package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type fanOutCrashCommitKey struct{}

const fanOutCrashCardinality = 65

// Reuse the existing process-crash driver, but scope its barrier to the actual
// chunk call. A concurrently committing detector read must not consume it.
type fanOutCrashConnector struct{ driver.Connector }
type fanOutCrashConn struct{ *pipelineCrashConn }

func (c fanOutCrashConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &fanOutCrashConn{&pipelineCrashConn{Conn: conn}}, nil
}

func (c *fanOutCrashConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if control, ok := ctx.Value(fanOutCrashCommitKey{}).(*pipelineCrashConnector); ok {
		return &pipelineCrashTx{Tx: tx, owner: control}, nil
	}
	return tx, nil
}

func openFanOutCrashStore(t *testing.T, backend, location string) authorActivityReceiptFixture {
	t.Helper()
	var native driver.Connector = pipelineCrashSQLiteConnector{path: location}
	if backend == "postgres" {
		var err error
		native, err = pq.NewConnector(location)
		if err != nil {
			t.Fatal(err)
		}
	}
	db := sql.OpenDB(fanOutCrashConnector{native})
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	t.Cleanup(func() { _ = db.Close() })
	if backend == "postgres" {
		return authorActivityReceiptFixture{db: db, store: admitTestPostgresStore(t, db)}
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	b, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := storeschema.NewSQLiteWithBackend(b, location)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSQLiteStoreComposition(schema, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapSchema(testAuthorActivityContext(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatal(err)
	}
	store.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
	return authorActivityReceiptFixture{db: db, store: store}
}

type fanOutCrashTurn struct {
	pipeline.FanOutObligationOwner
	control   *pipelineCrashConnector
	beforeCut func() error
}

func (o fanOutCrashTurn) CommitFanOutChunk(ctx context.Context, cmd pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	// A first real turn establishes a durable prefix and a measured local sample.
	// Kill the second chunk, so restart must forget a sample that actually existed.
	if len(cmd.Outcomes) == 0 || cmd.Outcomes[0].Ordinal != 32 {
		return o.FanOutObligationOwner.CommitFanOutChunk(ctx, cmd)
	}
	if err := o.beforeCut(); err != nil {
		return pipeline.CommittedFanOutChunk{}, err
	}
	return o.FanOutObligationOwner.CommitFanOutChunk(context.WithValue(ctx, fanOutCrashCommitKey{}, o.control), cmd)
}

type fanOutCrashExecutor struct {
	*pipeline.PipelineCoordinator
	control   *pipelineCrashConnector
	allow     <-chan struct{}
	errors    chan error
	beforeCut func() error
}

func (e *fanOutCrashExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	select {
	case <-e.allow:
	case <-ctx.Done():
		return pipeline.FanOutTurnResult{}, ctx.Err()
	}
	return e.PipelineCoordinator.ServeFanOutCandidate(ctx, fanOutCrashTurn{owner, e.control, e.beforeCut}, key)
}

func (e *fanOutCrashExecutor) ReportFanOutServingError(ctx context.Context, err error) {
	select {
	case e.errors <- err:
	default:
	}
	e.PipelineCoordinator.ReportFanOutServingError(ctx, err)
}

type fanOutCrashEvidence struct {
	Authority startupownership.Authority
	Recovered bool
}

func TestFanOutProcessSIGKILLRecoveryBothStores(t *testing.T) {
	if mode := os.Getenv("SWARM_FAN_OUT_CRASH_CHILD"); mode != "" {
		runFanOutCrashChild(t, mode)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"before_commit", "after_commit_before_ack"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "fan-out.db")
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
				}
				fixture := openFanOutCrashStore(t, backend, location)
				root := canonicalrouting.CopyForkFanOutCarrier(t, false, false)
				ctx, seeded, _, _ := seedDeclaredForkFanOutGenerationFromSource(t, backend, fixture, fanOutCrashCardinality, time.Now().UTC(), false, false, root, nil, nil)
				env := []string{"SWARM_FAN_OUT_CRASH_BACKEND=" + backend, "SWARM_FAN_OUT_CRASH_LOCATION=" + location,
					"SWARM_FAN_OUT_CRASH_ROOT=" + root, "SWARM_FAN_OUT_CRASH_RUN=" + seeded.runID, "SWARM_FAN_OUT_CRASH_CUT=" + cut}
				cmd, evidence, exited := startFanOutCrashChild(t, "predecessor", env)
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
						t.Fatalf("not SIGKILL: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("predecessor did not exit after SIGKILL")
				}
				prefix := 32
				if cut == "after_commit_before_ack" {
					prefix = 64
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, fixture.db, seeded, prefix, prefix)
				page, err := fixture.store.(interface {
					ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
				}).ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: seeded.runID})
				if err != nil || len(page.Intents) != 1 {
					t.Fatalf("post-death durable read: %+v %v", page, err)
				}
				row := page.Intents[0]
				if !reflect.DeepEqual(row.Runtime, fanoutobligation.UnavailableRuntimeReadback()) || row.Retry != nil || row.ClaimGeneration != 2 {
					t.Fatalf("post-death operational evidence: %+v", row)
				}
				if row.LastServedAt == nil || (prefix == 32 && (row.ClaimOwner == "" || row.LeaseExpiresAt == nil)) || (prefix == 64 && (row.ClaimOwner != "" || row.LeaseExpiresAt != nil)) {
					t.Fatalf("claim/release not atomic with killed chunk: %+v", row)
				}
				before := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID)
				oldJSON, err := json.Marshal(old.Authority)
				if err != nil {
					t.Fatal(err)
				}
				env = append(env, "SWARM_FAN_OUT_CRASH_PREDECESSOR="+string(oldJSON))
				_, evidence, exited = startFanOutCrashChild(t, "successor", env)
				next := awaitFanOutCrashEvidence(t, evidence)
				if !next.Recovered || next.Authority.PredecessorAuthorityID != old.Authority.AuthorityID {
					t.Fatalf("missing exact crash recovery evidence: old=%+v next=%+v", old, next)
				}
				select {
				case err := <-exited:
					if err != nil {
						t.Fatalf("successor failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("successor did not finish")
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, fixture.db, seeded, fanOutCrashCardinality, fanOutCrashCardinality)
				after := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID)
				if len(after) != fanOutCrashCardinality || !reflect.DeepEqual(before, after[:prefix]) {
					t.Fatalf("restart replayed/lost committed prefix: before=%v after=%v", before, after)
				}
				var events int
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='items.child'`, seeded.runID).Scan(&events); err != nil || events != fanOutCrashCardinality {
					t.Fatalf("publication count=%d want%d err=%v", events, fanOutCrashCardinality, err)
				}
				t.Logf("SIGKILL %s: durable prefix %d preserved; exact process takeover, actual shared serving/evaluator/publication and suffix-only completion to %d; prior measured runtime timing forgotten", cut, prefix, fanOutCrashCardinality)
			})
		}
	}
}

func fanOutCrashOutcomeIDs(t *testing.T, ctx context.Context, db *sql.DB, runID string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT o.ordinal,CAST(o.event_id AS TEXT),o.outcome_kind,e.run_id,e.payload
		FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 ORDER BY o.ordinal`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []string{}
	seen := map[string]bool{}
	for rows.Next() {
		var ordinal int
		var id, kind, eventRun string
		var raw []byte
		if err := rows.Scan(&ordinal, &id, &kind, &eventRun, &raw); err != nil {
			t.Fatal(err)
		}
		if ordinal != len(ids) || id == "" || seen[id] || kind != "committed" || eventRun != runID {
			t.Fatalf("noncontiguous/nonpublication outcome: %d %s %s", ordinal, id, kind)
		}
		var payload struct{ Value string }
		if err := json.Unmarshal(raw, &payload); err != nil || payload.Value != fmt.Sprintf("item-%03d", ordinal) {
			t.Fatalf("wrong evaluated immutable item at ordinal%d: %s err=%v", ordinal, raw, err)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func startFanOutCrashChild(t *testing.T, mode string, env []string) (*exec.Cmd, <-chan fanOutCrashEvidence, <-chan error) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFanOutProcessSIGKILLRecoveryBothStores$", "-test.count=1", "-test.timeout=45s")
	cmd.Env = append(append(os.Environ(), env...), "SWARM_FAN_OUT_CRASH_CHILD="+mode)
	cmd.ExtraFiles = []*os.File{write}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		_ = read.Close()
		_ = write.Close()
		t.Fatal(err)
	}
	_ = write.Close()
	wait := make(chan error, 1)
	done := make(chan struct{})
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
			t.Error("child process cleanup failed")
		}
	})
	return cmd, evidence, wait
}

func awaitFanOutCrashEvidence(t *testing.T, evidence <-chan fanOutCrashEvidence) fanOutCrashEvidence {
	t.Helper()
	select {
	case record, ok := <-evidence:
		if !ok {
			t.Fatal("child exited without crash/recovery evidence")
		}
		return record
	case <-time.After(40 * time.Second):
		t.Fatal("no crash/recovery evidence within real lease recovery budget")
	}
	return fanOutCrashEvidence{}
}

func runFanOutCrashChild(t *testing.T, mode string) {
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
	selected := fixture.store.(selectedFanOutOwner)
	request := testStartupAcquireRequest("fan-out-crash-" + mode)
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
			t.Fatalf("not actual crash takeover: old=%+v next=%+v", old, authority)
		}
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, exists, err := process.CurrentSourceSet(ctx)
	if err != nil || exists != (mode == "successor") || (exists && current.Revision != plan.Revision) {
		t.Fatalf("unexpected retained source-set evidence: exists=%v current=%+v err=%v", exists, current, err)
	}
	if !exists {
		if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
	}
	liveGrant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := liveGrant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := liveGrant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	grant, err := liveGrant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(grant.RuntimeInstanceID, fact.BundleHash()))
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
	eventBus, err := newStoreTestEventBus(t, fixture.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work, RuntimeInstanceID: grant.RuntimeInstanceID})
	if err != nil {
		t.Fatal(err)
	}
	workflow := fixture.store.(workflowTestSelectedStore)
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
	})
	if coordinator == nil {
		t.Fatal("real coordinator dependencies incomplete")
	}
	eventBus.SetInterceptors(coordinator)
	report := os.NewFile(3, "fan-out-crash-evidence")
	defer report.Close()
	control := &pipelineCrashConnector{cut: os.Getenv("SWARM_FAN_OUT_CRASH_CUT")}
	control.barrier = func() {
		if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: authority}); err != nil {
			panic(err)
		}
		select {} // SIGKILL, never graceful cancellation or a manufactured error.
	}
	control.armed.Store(mode == "predecessor")
	allow := make(chan struct{})
	executor := &fanOutCrashExecutor{PipelineCoordinator: coordinator, control: control, allow: allow, errors: make(chan error, 8)}
	workers := 1
	registration, err := startupownership.StartFanOutServing(ctx, liveGrant, work, &workers, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	reader := selected.(interface {
		ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
	})
	read := func() fanoutobligation.IntentReadback {
		t.Helper()
		page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: runID})
		if err != nil || len(page.Intents) != 1 {
			t.Fatalf("read real runtime page: %+v %v", page, err)
		}
		page, err = startupownership.ObserveFanOutRuntimePage(ctx, process, page)
		if err != nil {
			t.Fatal(err)
		}
		return page.Intents[0]
	}
	initial := read()
	if initial.Runtime.Availability != "available" || initial.Runtime.ObservedAt == nil || initial.Runtime.LastCommitMS != nil {
		t.Fatalf("new process inherited or fabricated completed-turn timing: %+v", initial.Runtime)
	}
	executor.beforeCut = func() error {
		if mode == "predecessor" {
			row := read()
			if row.Cursor != 32 || row.Runtime.LastCommitMS == nil {
				return fmt.Errorf("pre-crash turn did not establish real timing/prefix: %+v", row)
			}
		}
		return nil
	}
	close(allow)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-executor.errors:
			t.Fatalf("actual shared serving failure: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
			row := read()
			if row.Status != fanoutobligation.StatusClosed || row.Cursor != fanOutCrashCardinality || row.Runtime.LastCommitMS == nil || row.Runtime.ActiveWorkers == nil || *row.Runtime.ActiveWorkers != 0 {
				continue
			}
			if mode != "successor" {
				t.Fatal("predecessor escaped exact chunk commit barrier")
			}
			if len(fanOutCrashOutcomeIDs(t, ctx, fixture.db, runID)) != fanOutCrashCardinality {
				t.Fatal("incomplete durable outcomes after serving")
			}
			if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: authority, Recovered: true}); err != nil {
				t.Fatal(fmt.Errorf("report recovery: %w", err))
			}
			return
		}
	}
}
