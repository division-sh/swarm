package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func permanentForkEvidenceQuery(query string) bool {
	return strings.Contains(query, "FROM run_fork_operations WHERE") ||
		strings.Contains(query, "FROM run_fork_selected_contract_bindings\n") ||
		strings.Contains(query, "JOIN run_fork_operation_live_parents p")
}

type permanentForkEvidenceRows struct {
	driver.Rows
	fault, query string
}

func (r *permanentForkEvidenceRows) Next(values []driver.Value) error {
	if r.fault == "read_error" {
		return errSelectedGrantHistoryRead
	}
	if r.fault == "missing_operation" && strings.Contains(r.query, "SELECT CAST(operation_id AS TEXT)") {
		return io.EOF
	}
	if err := r.Rows.Next(values); err != nil {
		return err
	}
	physical := false
	for _, column := range r.Columns() {
		physical = physical || column == "created_at" && len(r.Columns()) == 6
	}
	for i, column := range r.Columns() {
		switch {
		case r.fault == "unsettled" && column == "state" && strings.Contains(r.query, "SELECT operation_id, actor,"):
			values[i] = "materialized"
		case r.fault == "unsettled" && (column == "result_json" || column == "failure_json") && strings.Contains(r.query, "SELECT operation_id, actor,"):
			values[i] = nil
		case r.fault == "binding_cut" && column == "fork_revision" && len(r.Columns()) == 9:
			values[i] = values[i].(int64) + 1
		case r.fault == "typed_operation_cut" && column == "fork_revision" && strings.Contains(r.query, "SELECT operation_id, actor,"):
			values[i] = values[i].(int64) + 1
		case r.fault == "live_parent" && strings.Contains(r.query, "JOIN run_fork_operation_live_parents p"):
			values[i] = int64(0)
		case physical && r.fault == "physical_source" && column == "source_run_id":
			values[i] = uuid.NewString()
		case physical && (r.fault == "request_bytes" && column == "request_json" || r.fault == "result_bytes" && column == "result_json" || r.fault == "failure_bytes" && column == "failure_json"):
			values[i] = []byte(`{"different":true}`)
		case physical && r.fault == "missing_clock" && column == "created_at":
			values[i] = nil
		case physical && r.fault == "reversed_clock" && column == "updated_at":
			values[i] = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		}
	}
	return nil
}

// Reach: permanent operation/cut capture after genuine selected activation and
// retirement, including read-boundary inconsistencies and row conservation.
// The owed timer is not fired; this is not completed-manifest evidence.
func proveClosedSelectedPermanentForkCohort(t *testing.T, name string) {
	t.Helper()
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		t.Run("permanent_cohort/"+string(kind), func(t *testing.T) {
			control := &activityGrantHistoryConnector{}
			opened := openPermanentCohortNativeStore(t, name, control)
			sqlite := name == "sqlite"
			f := newPermanentCohortTimerFixture(t, opened, kind)
			process, err := agentfixture.ProcessCapability(t, f.ctx, opened.store.(agentfixture.Store))
			if err != nil {
				t.Fatal(err)
			}
			reusePermanentCohortProcess(t, opened.store, process)
			selector := f.eventID
			if kind != runfork.RunForkPointEvent {
				selector = ""
			}
			// Initialize the original execution owner while the recovery inventory
			// is empty, before publishing its separately staged native child.
			gate := selectedStoreActivationGateForTest(t, f.ctx, f.store)
			staged, activation := stageForkContentionFixtureAt(t, f, true, selector, true)
			read := func(readCtx context.Context) (runforkpersistence.ClosedSelectedPermanentForkCohort, error) {
				var captured runforkpersistence.ClosedSelectedPermanentForkCohort
				err := runUnrevisionedEventFixtureTransactionForTest(f.ctx, f.store, func(txctx context.Context, tx *sql.Tx) error {
					if err := readCtx.Err(); err != nil {
						return err
					}
					var err error
					captured, err = runforkpersistence.ReadClosedSelectedPermanentForkCohortTx(txctx, tx, staged.ForkRunID, sqlite)
					return err
				})
				if err != nil {
					return runforkpersistence.ClosedSelectedPermanentForkCohort{}, err
				}
				return captured, nil
			}
			if got, err := read(f.ctx); err == nil || len(got.Generations) != 0 || !strings.Contains(err.Error(), "census is absent") {
				t.Fatalf("materialization without execution history supplied a cohort: %+v err=%v", got, err)
			}
			var attached effects.Authority
			call := func(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
				var found bool
				attached, found = effects.AuthorityFromContext(ctx)
				if !found || attached.Kind != effects.AuthoritySelectedContractFork || attached.SelectedFork.ForkRunID != staged.ForkRunID {
					t.Fatal("native activation lost the original attached execution authority")
				}
				return f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(ctx, request)
			}
			if activated, err := gate(activation, staged.ForkPoint, call); err != nil || !activated.Activated {
				t.Fatalf("activate original permanent operation: %+v err=%v", activated, err)
			}
			store := opened.store.(selectedCompletionAuthorityStore)
			if err := store.QuiesceRunForkSelectedContractRuntimeExecution(f.ctx, attached); err != nil {
				t.Fatalf("quiesce joined original selected executor: %v", err)
			}
			if closed, err := store.CloseRunForkSelectedContractRuntimeExecution(f.ctx, attached.ID); err != nil || !closed {
				t.Fatalf("close joined original selected executor: closed=%t err=%v", closed, err)
			}
			before := historicalContextDatabaseRows(t, f.db, !sqlite)
			for _, scenario := range []string{"complete", "unsettled", "missing_operation", "binding_cut", "typed_operation_cut", "live_parent", "physical_source", "request_bytes", "result_bytes", "failure_bytes", "missing_clock", "reversed_clock", "query_error", "read_error", "cancelled"} {
				t.Run(scenario, func(t *testing.T) {
					control.permanentFault = scenario
					defer func() { control.permanentFault = "" }()
					ctx := f.ctx
					if scenario == "cancelled" {
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						ctx = cancelled
					}
					got, err := read(ctx)
					if scenario != "complete" {
						if err == nil || !reflect.DeepEqual(got, runforkpersistence.ClosedSelectedPermanentForkCohort{}) {
							t.Fatalf("incomplete permanent evidence returned a partial cohort: %+v err=%v", got, err)
						}
						if scenario == "unsettled" && !strings.Contains(err.Error(), "not settled") {
							t.Fatalf("unsettled operation missed its settlement boundary: %v", err)
						}
						if (scenario == "query_error" || scenario == "read_error") && !errors.Is(err, errSelectedGrantHistoryRead) || scenario == "cancelled" && !errors.Is(err, context.Canceled) {
							t.Fatalf("permanent evidence lost its original error: %v", err)
						}
						return
					}
					if err != nil || len(got.Generations) != 1 || len(got.Generations[0].Grants) == 0 || got.Binding.ForkRunID != staged.ForkRunID ||
						got.Operation.Request.OperationID != activation.ForkOperation.OperationID || got.Operation.Status != runfork.ForkOperationActivated ||
						got.Operation.Request.ResolvedPoint == nil || !got.Operation.Request.ResolvedPoint.SameIdentity(staged.ForkPoint) ||
						len(got.RequestJSON) == 0 || len(got.ResultJSON) == 0 || len(got.FailureJSON) != 0 || got.OperationCreatedAt.IsZero() || got.OperationUpdatedAt.IsZero() {
						t.Fatalf("permanent operation/cut evidence changed or lost the real grant history: %+v err=%v", got, err)
					}
				})
			}
			if !reflect.DeepEqual(before, historicalContextDatabaseRows(t, f.db, !sqlite)) {
				t.Fatal("permanent operation evidence capture changed durable rows")
			}
		})
	}
}

// Retain a real initial timer with a native zero-row deployment feed. The
// settled gate-card fixture has no executable event frontier and remains so.
func newPermanentCohortTimerFixture(t *testing.T, opened authorActivityReceiptFixture, kind runfork.RunForkPointKind) forkContentionFixture {
	t.Helper()
	ctx, command := permanentTimerDeploymentCommand(t, opened.store)
	created, err := opened.store.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, command)
	if err != nil || !created.Acknowledged || created.Replay || len(command.Root.Plan.Lifecycle.Timers) != 1 {
		t.Fatalf("create original owed-timer source: %+v err=%v", created, err)
	}
	runID := command.RunCreation.RunID
	f := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{
		store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: ctx, runID: runID, entityID: runID,
	}}
	if kind == runfork.RunForkPointEvent {
		timer := command.Root.Plan.Lifecycle.Timers[0].Activation.Canonical()
		event := eventtest.ExistingRunRootIngress(uuid.NewString(), "c4.later", "ingress", "", []byte(`{}`), 0,
			runID, events.EventEnvelope{}, timer.CreatedAt.Add(time.Second))
		binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
			BundleHash: command.RunCreation.BundleHash, FlowID: ".", EventKey: string(event.Type()),
			SchemaDigest: "sha256:" + strings.Repeat("0", 64), SchemaClass: events.PayloadSchemaSchemaLess,
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
		if _, err := commitAdmittedSemanticEventFixtureOutcome(ctx, opened.store, admitted, nil, pipelineobligation.ScopeDirect); err != nil {
			t.Fatal(err)
		}
		f.eventID = event.ID()
	}
	repo := canonicalrouting.RepoRoot(t)
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: opened.store.(runforkexecution.SourceArtifactSelectedContractSourceStore)}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
		Responsibility: runforkexecution.OriginalRunSource(runID), Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	f.source = loaded.Source
	return f
}

func permanentTimerDeploymentCommand(t *testing.T, selected any) (context.Context, bus.DeploymentRunCreationCommand) {
	t.Helper()
	files := map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":   "pins:\n  outputs:\n    - records.ready\nstages:\n  waiting:\n    timers:\n      - {id: waiting.timeout, after: 1h, emit: timer.elapsed}\n",
		"events.yaml":   "records.ready:\n  body: text\ntimer.elapsed:\n",
	}
	f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, files, nil, ownStoreTestAgentManager, nil)
	catalog, err := contracts.BuildDurableDataCatalog(f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	data := selected.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	if _, err := data.EnsureSourceArtifactWithData(f.ctx, f.bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
		Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte{},
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
	return ctx, bus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
			Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        bus.FlowInstanceActivationCommand{Plan: plan},
	}
}

func reusePermanentCohortProcess(t *testing.T, store any, process runtimeownership.ProcessCapability) {
	t.Helper()
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	work.capabilitiesMu.Lock()
	defer work.capabilitiesMu.Unlock()
	if prior := work.capabilities[store]; prior != nil && prior != process {
		t.Fatal("permanent cohort fixture would replace another construction process")
	}
	if work.capabilities == nil {
		work.capabilities = make(map[any]runtimeownership.ProcessCapability)
	}
	work.capabilities[store] = process
}

// Each point has a distinct native store and its original construction process.
// Never replace an admitted source plan or recover unrelated component work.
func openPermanentCohortNativeStore(t *testing.T, backend string, control *activityGrantHistoryConnector) authorActivityReceiptFixture {
	t.Helper()
	var native driver.Connector
	var location string
	if backend == "postgres" {
		dsn, _, _ := testutil.StartPostgres(t)
		location = dsn
		var err error
		native, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		location = filepath.Join(t.TempDir(), "permanent.db")
		native = pipelineCrashSQLiteConnector{path: location}
	}
	control.Connector = native
	opened, _ := openPipelineCrashStore(t, backend, location, control)
	return opened
}
