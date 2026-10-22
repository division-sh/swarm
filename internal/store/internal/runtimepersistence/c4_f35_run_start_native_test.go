package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
	modernc "modernc.org/sqlite"
)

type c4StartPlanner interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

type c4ForkCreationOwner interface {
	MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	RequireMaterializedRunFork(context.Context, string, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
}

func c4ReopenCreationStore(t *testing.T, backend string, selected any, db *sql.DB, connector *stopCommitConnector, instrumentation ...driver.Connector) (selectedFanOutLifecycleOwner, *sql.DB) {
	t.Helper()
	location := connector.dsn
	if backend == "sqlite" {
		parsed, err := url.Parse(location)
		if err != nil || parsed.Scheme != "file" || parsed.Path == "" {
			t.Fatalf("creation reopen lost its physical SQLite path: %q err=%v", location, err)
		}
		location = parsed.Path
	}
	if err := selected.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _ := openPipelineCrashStore(t, backend, location, instrumentation...)
	return reopened.store.(selectedFanOutLifecycleOwner), reopened.db
}

func c4ReadExactForkWithoutMutation(t *testing.T, ctx context.Context, selected any, backend, child string, request runfork.RunForkMaterializeRequest, present bool, connector *stopCommitConnector) runfork.RunForkMaterialization {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	var commits, writeCommits int32
	if connector != nil {
		commits, writeCommits = connector.commits.Load(), connector.writeCommits.Load()
	}
	got, err := selected.(c4ForkCreationOwner).RequireMaterializedRunFork(ctx, child, request)
	if present {
		if err != nil || got.ForkRunID != child {
			t.Fatalf("exact committed fork readback: got=%+v child=%s err=%v", got, child, err)
		}
	} else if !errors.Is(err, runlifecycle.ErrRunNotFound) || !reflect.DeepEqual(got, runfork.RunForkMaterialization{}) {
		t.Fatalf("rolled-back exact child did not refuse without a result: got=%+v err=%v", got, err)
	}
	if connector != nil {
		wantReadCommits := int32(0)
		if present && backend == "sqlite" {
			wantReadCommits = 1
		}
		if actual := connector.commits.Load() - commits; actual != wantReadCommits || connector.writeCommits.Load() != writeCommits {
			t.Fatalf("exact fork read changed physical commit accounting: commits=%d want=%d write commits=%d want=%d", actual, wantReadCommits, connector.writeCommits.Load(), writeCommits)
		}
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("exact fork read changed complete application storage: %v", err)
	}
	return got
}

func c4RequireAbsentCreation(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
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
}

// The claim and descriptor are acquired before the test arms its creation cut.
func c4CommitCreatingPublication(t *testing.T, ctx context.Context, selected any, admitted events.AdmittedEvent, arm func()) (bus.CommittedPublication, error) {
	t.Helper()
	ctx, releaseCatalog, err := semanticEventFixtureContext(ctx, selected, admitted.Event())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseCatalog()
	owner := pipelineObligationOwnerForFixture(selected)
	claim, err := owner.ClaimPublication(ctx, admitted.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Release(context.WithoutCancel(ctx), claim); err != nil {
			t.Error(err)
		}
	}()
	processed := pipelineobligation.Acknowledged("pipeline_persisted")
	command := bus.PublicationCommand{Commit: bus.CommitPublishRequest{
		Event: admitted, PipelineClaim: claim, ReplayScope: pipelineobligation.ScopeDirect,
		RouteSettlement: testRouteSettlement(admitted.Event(), nil), Disposition: &processed,
	}}
	scope, scoped := authoractivity.ScopeFromContext(ctx)
	descriptor, resolved, err := authoractivity.ResolvedEventDescriptorFromContext(ctx, scope, string(admitted.Event().Type()))
	if err != nil {
		t.Fatal(err)
	}
	if !resolved {
		descriptor, resolved = selected.(semanticFixtureCatalogStore).ResolveAuthorActivityEventDescriptor(scope, string(admitted.Event().Type()))
	}
	if !scoped || !resolved {
		t.Fatalf("creating publication lacks its exact catalog descriptor: scoped=%t resolved=%t", scoped, resolved)
	}
	command.AuthorScope, command.HasAuthorScope = scope, true
	command.AuthorDescriptor, command.HasAuthorDescriptor = descriptor, true
	if arm != nil {
		arm()
	}
	return selected.(bus.CommitPublicationOwner).CommitPublication(ctx, command)
}

func TestC4F35EventStartNativeCommitAccountingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			cut := "rollback"
			if committed {
				cut = "lost_ack"
			}
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, connector := newP16RaceStore(t, backend)
				ctx := testAuthorActivityContext()
				if err := sourceartifactfixture.Ensure(ctx, selected.(sourceartifactfixture.Writer)); err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				input := eventtest.RunCreatingRootIngress(uuid.NewString(), "c4.atomic.birth", "ingress", "", []byte(`{"original":true}`), 0,
					runID, "", events.EventEnvelope{}, time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
				admitted := c4AdmitCreatingInput(t, input, sourceartifactfixture.BundleHash)
				fault := errors.New("C4 event creation physical commit boundary")
				var commits atomic.Int32
				result, err := c4CommitCreatingPublication(t, ctx, selected, admitted, func() {
					connector.arm(func(tx driver.Tx) error {
						commits.Add(1)
						if committed {
							return errors.Join(fault, tx.Commit())
						}
						return errors.Join(fault, tx.Rollback())
					})
				})
				if !errors.Is(err, fault) || result.Acknowledged || result.AppendOutcome != bus.EventAppendOutcomeUnknown || commits.Load() != 1 {
					t.Fatalf("uncertain event creation exposed commit evidence: %+v commits=%d err=%v", result, commits.Load(), err)
				}
				actual, found, err := ReadCanonicalEventRecordForTest(ctx, selected, input.ID())
				if err != nil || found != committed {
					t.Fatalf("event creation physical readback: found=%t want=%t err=%v", found, committed, err)
				}
				if !committed {
					c4RequireAbsentCreation(t, ctx, selected, runID)
				} else {
					c4RequireExclusiveStart(t, ctx, selected, runID, sourceartifactfixture.BundleHash, input.ID())
					c4RequireRevisionCount(t, ctx, selected, runID, 1)
				}
				selected, _ = c4ReopenCreationStore(t, backend, selected, db, connector)
				ctx = testAuthorActivityContext()
				if !committed {
					c4RequireAbsentCreation(t, ctx, selected, runID)
				} else {
					c4RequireExclusiveStart(t, ctx, selected, runID, sourceartifactfixture.BundleHash, input.ID())
					c4RequireRevisionCount(t, ctx, selected, runID, 1)
				}
				retried, err := c4CommitCreatingPublication(t, ctx, selected, admitted, nil)
				want := bus.EventAppendInserted
				if committed {
					want = bus.EventAppendExactDuplicate
				}
				if err != nil || !retried.Acknowledged || retried.AppendOutcome != want {
					t.Fatalf("event creation exact retry: %+v err=%v", retried, err)
				}
				before := c4RequireExclusiveStart(t, ctx, selected, runID, sourceartifactfixture.BundleHash, input.ID())
				_, projection, err := c4LoadStartProjection(t, ctx, selected, runID)
				if err != nil {
					t.Fatal(err)
				}
				for pass := 0; pass < 2; pass++ {
					replay, err := c4CommitCreatingPublication(t, ctx, selected, admitted, nil)
					if err != nil || !replay.Acknowledged || replay.AppendOutcome != bus.EventAppendExactDuplicate {
						t.Fatalf("event creation permanent replay: %+v err=%v", replay, err)
					}
					after := c4RequireExclusiveStart(t, ctx, selected, runID, sourceartifactfixture.BundleHash, input.ID())
					_, got, err := c4LoadStartProjection(t, ctx, selected, runID)
					if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(projection, got) {
						t.Fatalf("event replay changed original creation evidence: %v", err)
					}
					c4RequireRevisionCount(t, ctx, selected, runID, 1)
				}
				actual, found, err = ReadCanonicalEventRecordForTest(ctx, selected, input.ID())
				gotInput, gotErr := events.IntegrityProjection(actual)
				wantInput, wantErr := events.IntegrityProjection(admitted.Event())
				if err != nil || !found || gotErr != nil || wantErr != nil || !reflect.DeepEqual(gotInput, wantInput) {
					t.Fatalf("event creation replay changed its exact input/schema: found=%t err=%v/%v/%v", found, err, gotErr, wantErr)
				}
				origin, err := runlifecycle.EventRunOrigin(input.ID(), string(input.Type()))
				if err != nil {
					t.Fatal(err)
				}
				requireRunOriginHeader(t, ctx, selected.(runOriginReadStore), runID, origin, 1)
			})
		}
	}
}

func TestC4F35ForkStartNativeCommitAccountingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			cut := "rollback"
			if committed {
				cut = "lost_ack"
			}
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, connector := newP16RaceStore(t, backend)
				ctx := testAuthorActivityContext()
				if err := sourceartifactfixture.Ensure(ctx, selected.(sourceartifactfixture.Writer)); err != nil {
					t.Fatal(err)
				}
				sourceRun := uuid.NewString()
				input := eventtest.RunCreatingRootIngress(uuid.NewString(), "c4.fork.atomic.source", "ingress", "", []byte(`{}`), 0,
					sourceRun, "", events.EventEnvelope{}, time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
				admitted := c4AdmitCreatingInput(t, input, sourceartifactfixture.BundleHash)
				processed := pipelineobligation.Acknowledged("pipeline_persisted")
				if _, err := commitAdmittedSemanticEventFixtureOutcomeWithDisposition(ctx, selected, admitted, nil, pipelineobligation.ScopeDirect, &processed); err != nil {
					t.Fatal(err)
				}
				sourceStart := c4RequireExclusiveStart(t, ctx, selected, sourceRun, sourceartifactfixture.BundleHash, input.ID())
				request := runfork.RunForkMaterializeRequest{SourceRunID: sourceRun, At: input.ID()}
				plan, err := selected.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: input.ID()})
				if err != nil {
					t.Fatal(err)
				}
				witness := connector.armForkInsert(sourceRun, plan.ForkPoint, sourceartifactfixture.BundleHash)
				owner := selected.(c4ForkCreationOwner)
				fault := errors.New("C4 fork creation physical commit boundary")
				var commits atomic.Int32
				var intercept func(driver.Tx) error
				intercept = func(tx driver.Tx) error {
					index := commits.Add(1)
					// SQLite planning commits one read-only snapshot before the
					// creation write. PostgreSQL planning rolls its snapshot back.
					if backend == "sqlite" && index == 1 {
						connector.arm(intercept)
						return tx.Commit()
					}
					if committed {
						return errors.Join(fault, tx.Commit())
					}
					return errors.Join(fault, tx.Rollback())
				}
				connector.arm(intercept)
				uncertain, err := owner.MaterializeRunFork(ctx, request)
				wantCommits := int32(1)
				if backend == "sqlite" {
					wantCommits = 2
				}
				if !errors.Is(err, fault) || !reflect.DeepEqual(uncertain, runfork.RunForkMaterialization{}) || commits.Load() != wantCommits {
					t.Fatalf("fork creation fault did not hit its write commit: %+v commits=%d want=%d err=%v", uncertain, commits.Load(), wantCommits, err)
				}
				attempted, err := witness.attempted()
				if err != nil {
					t.Fatal(err)
				}
				forkRun := attempted.RunID
				if !committed {
					c4RequireAbsentCreation(t, ctx, selected, forkRun)
					c4ReadExactForkWithoutMutation(t, ctx, selected, backend, forkRun, request, false, connector)
				} else {
					c4RequireExclusiveStart(t, ctx, selected, forkRun, sourceartifactfixture.BundleHash, "")
					c4RequireRevisionCount(t, ctx, selected, forkRun, 1)
				}
				selected, _ = c4ReopenCreationStore(t, backend, selected, db, connector, p16CommitConnector{connector})
				ctx = testAuthorActivityContext()
				owner = selected.(c4ForkCreationOwner)
				if !committed {
					c4RequireAbsentCreation(t, ctx, selected, forkRun)
					c4ReadExactForkWithoutMutation(t, ctx, selected, backend, forkRun, request, false, connector)
				} else {
					c4RequireExclusiveStart(t, ctx, selected, forkRun, sourceartifactfixture.BundleHash, "")
					c4RequireRevisionCount(t, ctx, selected, forkRun, 1)
				}
				var created runfork.RunForkMaterialization
				if committed {
					created = c4ReadExactForkWithoutMutation(t, ctx, selected, backend, forkRun, request, true, connector)
				} else {
					// The witness stays pinned to the failed invocation, not a new call.
					connector.mu.Lock()
					connector.fork = nil
					connector.mu.Unlock()
					created, err = owner.MaterializeRunFork(ctx, request)
					if err != nil || created.ForkRunID == "" || created.ForkRunID == forkRun {
						t.Fatalf("fresh invocation reused rolled-back attempted child: attempted=%s created=%+v err=%v", forkRun, created, err)
					}
					c4RequireAbsentCreation(t, ctx, selected, forkRun)
					forkRun = created.ForkRunID
				}
				if created.ForkRunID != forkRun || created.ForkRunStatus != runfork.RunForkMaterializedStatus ||
					created.ForkPoint.Kind != runfork.RunForkPointEvent || created.ForkPoint.EventID != input.ID() || created.ForkPoint.Revision != 1 {
					t.Fatalf("fork creation exact reconciliation: %+v err=%v", created, err)
				}
				before := c4RequireExclusiveStart(t, ctx, selected, forkRun, sourceartifactfixture.BundleHash, "")
				_, projection, err := c4LoadStartProjection(t, ctx, selected, forkRun)
				if err != nil {
					t.Fatal(err)
				}
				for pass := 0; pass < 2; pass++ {
					replay := c4ReadExactForkWithoutMutation(t, ctx, selected, backend, forkRun, request, true, connector)
					if !reflect.DeepEqual(replay, created) {
						t.Fatalf("fork creation permanent exact read: got=%+v want=%+v", replay, created)
					}
					after := c4RequireExclusiveStart(t, ctx, selected, forkRun, sourceartifactfixture.BundleHash, "")
					_, got, err := c4LoadStartProjection(t, ctx, selected, forkRun)
					if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(projection, got) {
						t.Fatalf("fork retry changed original creation evidence: %v", err)
					}
					c4RequireRevisionCount(t, ctx, selected, forkRun, 1)
				}
				sourceAfter := c4RequireExclusiveStart(t, ctx, selected, sourceRun, sourceartifactfixture.BundleHash, input.ID())
				if !reflect.DeepEqual(sourceAfter, sourceStart) {
					t.Fatal("fork creation fault/retry changed its source start/input")
				}
				c4RequireRevisionCount(t, ctx, selected, sourceRun, 1)
				origin, err := created.ForkPoint.MaterializationRunOrigin(sourceRun)
				if err != nil {
					t.Fatal(err)
				}
				requireRunOriginHeader(t, ctx, selected.(runOriginReadStore), forkRun, origin, 0)
			})
		}
	}
}

// Only caller intent and committed coordinates cross the process boundary.
// No selected execution authority, process capability or pipeline claim does.
type c4CreationCrashRecord struct {
	Origin      string
	RunID       string
	BundleHash  string
	EventID     string
	SourceRunID string
	FlowPath    string
	At          time.Time
	ForkPoint   runfork.RunForkPoint
	Deployment  *bus.DeploymentRunCreationCommand
}

func c4DecodeCreationCrashRecord(decoder *json.Decoder) (c4CreationCrashRecord, error) {
	var record c4CreationCrashRecord
	if err := decoder.Decode(&record); err != nil {
		return record, err
	}
	if record.Deployment != nil {
		plan := &record.Deployment.Root.Plan
		// JSON maps erase the concrete entry type held in lifecycle bookkeeping.
		entry, present, err := workflowlifecycle.LoadStageEntry(plan.Instance.Bookkeeping)
		if err != nil {
			return record, err
		}
		if !present && plan.Lifecycle.StageEntry != nil {
			return record, errors.New("deployment crash record is missing lifecycle bookkeeping entry")
		}
		if present {
			if plan.Lifecycle.StageEntry == nil || *plan.Lifecycle.StageEntry != entry {
				return record, errors.New("deployment crash record has inconsistent lifecycle entry")
			}
			if err := entry.RequireOwner(plan.Readiness.RunID, plan.Identity.ScopeKey, plan.Identity.InstanceID, plan.Identity.InstancePath, plan.Identity.EntityID, plan.Instance.CurrentState); err != nil {
				return record, err
			}
			if err := workflowlifecycle.StoreStageEntry(plan.Instance.Bookkeeping, entry); err != nil {
				return record, err
			}
		}
	}
	return record, nil
}

func c4RequireCreationCrashRecordRoundTrip(t *testing.T, record c4CreationCrashRecord) {
	t.Helper()
	var wire bytes.Buffer
	if err := json.NewEncoder(&wire).Encode(record); err != nil {
		t.Fatal(err)
	}
	decoded, err := c4DecodeCreationCrashRecord(json.NewDecoder(&wire))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record, decoded) {
		t.Fatal("creation crash record JSON lost exact caller request or typed facts")
	}
	if record.Deployment != nil {
		if err := record.Deployment.Root.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := decoded.Deployment.Root.Validate(); err != nil {
			t.Fatalf("deployment JSON lost its valid typed root plan: %v", err)
		}
		before, err := record.Deployment.Root.Plan.Readiness.Hash()
		if err != nil {
			t.Fatal(err)
		}
		after, err := decoded.Deployment.Root.Plan.Readiness.Hash()
		if err != nil || before != after {
			t.Fatalf("deployment JSON changed readiness hash: %v", err)
		}
	}
}

// This is a pure carrier control; the actual native-built command is checked
// separately before the child arms its physical creation COMMIT.
func TestC4F35CreationCrashRecordJSONRetainsCallerRequest(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 10, 9, 0, 0, 0, 123456000, time.UTC)
	identity := flowidentity.Instance{TemplateID: ".", ScopeKey: ".", InstanceID: runID, InstancePath: runID, EntityID: entityID, HasStoredPath: true}
	routing, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	plan := pipeline.FlowInstanceActivationPlan{
		Identity: identity, OccurredAt: at,
		Instance:  pipeline.WorkflowInstance{InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "c4-start", WorkflowVersion: "1", CurrentState: "waiting", CreatedAt: at, UpdatedAt: at},
		Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: sourceartifactfixture.BundleHash, WorkflowVersion: "1", ExecutionMode: executionmode.Live},
		Lifecycle: pipeline.WorkflowLifecycleMutationPlan{Timers: []pipeline.WorkflowTimerMutation{{Kind: pipeline.WorkflowTimerMutationInsert, Activation: pipeline.WorkflowTimerActivation{
			Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "waiting.timeout", DeclarationRevision: "revision1", Cause: timeridentity.WorkflowTimerActivationCauseInitial},
			RunID: runID, EntityID: entityID, Route: identity.Route(), RoutingSource: routing, OwnerAgent: "timer-owner", EventType: "timer.elapsed",
			ExecutionMode: executionmode.Live, Payload: []byte(`{}`), CreatedAt: at, FireAt: at.Add(time.Hour), Status: "active",
		}}}},
	}
	entry := timeridentity.StageEntryRef{RunID: runID, FlowScope: identity.ScopeKey, InstanceID: identity.InstanceID, InstancePath: identity.InstancePath,
		EntityID: entityID, Stage: "waiting", Cause: "construction"}
	plan.Instance.Bookkeeping = map[string]any{}
	if err := workflowlifecycle.StoreStageEntry(plan.Instance.Bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	plan.Lifecycle.StageEntry = &entry
	command := bus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: sourceartifactfixture.BundleHash, Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: declaration, VersionID: durabledata.VersionID("resource-version-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000")}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        bus.FlowInstanceActivationCommand{Plan: plan},
	}
	record := c4CreationCrashRecord{Origin: "deployment", RunID: runID, BundleHash: sourceartifactfixture.BundleHash, At: at, Deployment: &command}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var unhydrated c4CreationCrashRecord
	if err := json.Unmarshal(raw, &unhydrated); err != nil {
		t.Fatal(err)
	}
	if _, typed := unhydrated.Deployment.Root.Plan.Instance.Bookkeeping["stage_entry"].(timeridentity.StageEntryRef); typed {
		t.Fatal("pure control did not exercise JSON's erased bookkeeping entry type")
	}
	restoredEntry, present, err := workflowlifecycle.LoadStageEntry(unhydrated.Deployment.Root.Plan.Instance.Bookkeeping)
	if err != nil || !present || restoredEntry != entry {
		t.Fatalf("JSON changed the semantic construction entry: present=%t err=%v", present, err)
	}
	unhydrated.Deployment.Root.Plan.Instance.Bookkeeping = record.Deployment.Root.Plan.Instance.Bookkeeping
	if !reflect.DeepEqual(record, unhydrated) {
		t.Fatal("pure deployment JSON changed facts outside the entry representation")
	}
	c4RequireCreationCrashRecordRoundTrip(t, record)
	for _, name := range []string{"missing", "null", "foreign_owner", "missing_lifecycle"} {
		t.Run(name, func(t *testing.T) {
			var invalid c4CreationCrashRecord
			if err := json.Unmarshal(raw, &invalid); err != nil {
				t.Fatal(err)
			}
			bookkeeping := invalid.Deployment.Root.Plan.Instance.Bookkeeping
			switch name {
			case "missing":
				delete(bookkeeping, "stage_entry")
			case "null":
				bookkeeping["stage_entry"] = nil
			case "foreign_owner":
				bookkeeping["stage_entry"].(map[string]any)["run_id"] = uuid.NewString()
			case "missing_lifecycle":
				invalid.Deployment.Root.Plan.Lifecycle.StageEntry = nil
			}
			encoded, err := json.Marshal(invalid)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c4DecodeCreationCrashRecord(json.NewDecoder(bytes.NewReader(encoded))); err == nil {
				t.Fatal("creation crash decoder accepted missing or inconsistent lifecycle facts")
			}
		})
	}
}

func c4CreationReplayContext(t *testing.T, selected any, record c4CreationCrashRecord) context.Context {
	t.Helper()
	for _, id := range []string{record.RunID, record.EventID, record.SourceRunID} {
		if id == "" && id != record.RunID {
			continue
		}
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			t.Fatalf("creation crash record lost a canonical run/input coordinate: %q", id)
		}
	}
	fact, err := selected.(interface {
		RequirePresentRunSource(context.Context, string) (correlation.SourceArtifactFact, error)
	}).RequirePresentRunSource(testAuthorActivityRuntimeContext(), record.RunID)
	if err != nil || fact.BundleHash() != record.BundleHash {
		t.Fatalf("creation retry source disagrees with its committed run: err=%v", err)
	}
	ctx := correlation.WithSourceArtifactFact(testAuthorActivityRuntimeContext(), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	return correlation.WithRunID(effects.WithExecutionMode(ctx, effects.ExecutionModeLive), record.RunID)
}

func TestC4F35CreationCommitSIGKILLReopenBothStores(t *testing.T) {
	if os.Getenv("SWARM_C4_CREATION_CRASH_CHILD") == "1" {
		c4RunCreationCrashChild(t)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, origin := range []string{"event", "deployment", "fork", "standing"} {
			t.Run(backend+"/"+origin, func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "c4-creation.db")
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
				}
				cmd, records, exited := c4StartCreationCrashChild(t, backend, location, origin)
				var record c4CreationCrashRecord
				select {
				case received, ok := <-records:
					if !ok || received.Origin != origin || received.RunID == "" || received.BundleHash == "" {
						t.Fatalf("creation process did not reach its exact committed cut: %+v", received)
					}
					record = received
				case <-time.After(20 * time.Second):
					t.Fatal("creation process did not reach COMMIT-before-ACK within its original bound")
				}
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-exited:
					var exit *exec.ExitError
					if !errors.As(err, &exit) {
						t.Fatalf("creation process did not die by signal: %v", err)
					}
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
						t.Fatalf("creation process did not die by SIGKILL: %v", err)
					}
				case <-time.After(20 * time.Second):
					t.Fatal("creation SIGKILL did not join within its original bound")
				}
				var original runforkrevision.StartProjection
				var originalPlan runfork.RunForkPlan
				for reopen := 0; reopen < 2; reopen++ {
					fixture, _ := openPipelineCrashStore(t, backend, location)
					ctx := c4CreationReplayContext(t, fixture.store, record)
					firstTurn := ""
					if origin == "event" {
						firstTurn = record.EventID
					}
					before := c4RequireExclusiveStart(t, ctx, fixture.store, record.RunID, record.BundleHash, firstTurn)
					c4RequireRevisionCount(t, ctx, fixture.store, record.RunID, 1)
					_, projection, err := c4LoadStartProjection(t, ctx, fixture.store, record.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if reopen == 0 {
						original, originalPlan = projection, before
					} else if !reflect.DeepEqual(projection, original) || !reflect.DeepEqual(before, originalPlan) {
						t.Fatal("fresh reopen changed the committed original start or creating input")
					}
					c4ReplayCreationAfterCrash(t, ctx, fixture, backend, record)
					after := c4RequireExclusiveStart(t, ctx, fixture.store, record.RunID, record.BundleHash, firstTurn)
					_, got, err := c4LoadStartProjection(t, ctx, fixture.store, record.RunID)
					if err != nil || !reflect.DeepEqual(after, originalPlan) || !reflect.DeepEqual(got, original) {
						t.Fatalf("creation retry after process death changed original evidence: %v", err)
					}
					c4RequireRevisionCount(t, ctx, fixture.store, record.RunID, 1)
					if err := fixture.db.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func c4StartCreationCrashChild(t *testing.T, backend, location, origin string) (*exec.Cmd, <-chan c4CreationCrashRecord, <-chan error) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestC4F35CreationCommitSIGKILLReopenBothStores$", "-test.count=1", "-test.timeout=180s")
	cmd.Env = append(os.Environ(), "SWARM_C4_CREATION_CRASH_CHILD=1", "SWARM_C4_CREATION_CRASH_BACKEND="+backend,
		"SWARM_C4_CREATION_CRASH_LOCATION="+location, "SWARM_C4_CREATION_CRASH_ORIGIN="+origin)
	cmd.ExtraFiles = []*os.File{write}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		_ = read.Close()
		_ = write.Close()
		t.Fatal(err)
	}
	_ = write.Close()
	joined := make(chan struct{})
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait(); close(joined) }()
	records := make(chan c4CreationCrashRecord, 1)
	go func() {
		defer close(records)
		defer read.Close()
		record, err := c4DecodeCreationCrashRecord(json.NewDecoder(read))
		if err == nil {
			records <- record
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = read.Close()
		select {
		case <-joined:
		case <-time.After(20 * time.Second):
			t.Error("creation process cleanup did not join within its original bound")
		}
	})
	return cmd, records, exited
}

func c4RunCreationCrashChild(t *testing.T) {
	backend, location, origin := os.Getenv("SWARM_C4_CREATION_CRASH_BACKEND"), os.Getenv("SWARM_C4_CREATION_CRASH_LOCATION"), os.Getenv("SWARM_C4_CREATION_CRASH_ORIGIN")
	connector := &stopCommitConnector{driver: &modernc.Driver{}, dsn: "file:" + location + "?_pragma=foreign_keys(ON)&_pragma=busy_timeout(50)"}
	if backend == "postgres" {
		connector.driver, connector.dsn = &pq.Driver{}, location
	}
	fixture, _ := openPipelineCrashStore(t, backend, location, p16CommitConnector{connector})
	ctx := testAuthorActivityContext()
	if err := sourceartifactfixture.Ensure(ctx, fixture.store.(sourceartifactfixture.Writer)); err != nil {
		t.Fatal(err)
	}
	record := c4CreationCrashRecord{Origin: origin, RunID: uuid.NewString(), EventID: uuid.NewString(), BundleHash: sourceartifactfixture.BundleHash,
		At: time.Now().UTC().Truncate(time.Microsecond)}
	var create func()
	var arm func()
	var forkWitness *stopCommitForkWitness
	arm = func() {
		var commits int
		var intercept func(driver.Tx) error
		intercept = func(tx driver.Tx) error {
			commits++
			if origin == "fork" && backend == "sqlite" && commits == 1 {
				connector.arm(intercept)
				return tx.Commit()
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			if origin == "fork" {
				observed, err := forkWitness.attempted()
				if err != nil || observed.SourceRunID != record.SourceRunID || observed.BundleHash != record.BundleHash ||
					observed.PointKind != record.ForkPoint.Kind || observed.Revision != record.ForkPoint.Revision || observed.EventID != record.ForkPoint.EventID {
					t.Fatalf("committed fork lost its exact attempted child witness: %+v err=%v", observed, err)
				}
				// An attempted INSERT becomes crash-record evidence only after
				// the physical native COMMIT above actually succeeds.
				record.RunID = observed.RunID
			}
			pipe := os.NewFile(3, "c4-creation-commit-evidence")
			if err := json.NewEncoder(pipe).Encode(record); err != nil {
				t.Fatal(err)
			}
			select {} // The parent sends SIGKILL before the native call can ACK.
		}
		connector.arm(intercept)
	}
	switch origin {
	case "event":
		input := eventtest.RunCreatingRootIngress(record.EventID, "c4.atomic.birth", "ingress", "", []byte(`{"original":true}`), 0,
			record.RunID, "", events.EventEnvelope{}, record.At)
		admitted := c4AdmitCreatingInput(t, input, record.BundleHash)
		create = func() {
			if _, err := c4CommitCreatingPublication(t, ctx, fixture.store, admitted, arm); err != nil {
				t.Fatal(err)
			}
		}
	case "deployment":
		var command bus.DeploymentRunCreationCommand
		ctx, command = c4DeploymentStartCommand(t, fixture.store, true)
		record.RunID, record.BundleHash, record.At = command.RunCreation.RunID, command.RunCreation.BundleHash, command.Root.Plan.OccurredAt
		record.EventID, record.Deployment = "", &command
		create = func() {
			arm()
			if _, err := fixture.store.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, command); err != nil {
				t.Fatal(err)
			}
		}
	case "fork":
		record.SourceRunID = uuid.NewString()
		input := eventtest.RunCreatingRootIngress(record.EventID, "c4.fork.atomic.source", "ingress", "", []byte(`{}`), 0,
			record.SourceRunID, "", events.EventEnvelope{}, record.At)
		admitted := c4AdmitCreatingInput(t, input, record.BundleHash)
		processed := pipelineobligation.Acknowledged("pipeline_persisted")
		if _, err := commitAdmittedSemanticEventFixtureOutcomeWithDisposition(ctx, fixture.store, admitted, nil, pipelineobligation.ScopeDirect, &processed); err != nil {
			t.Fatal(err)
		}
		record.RunID = ""
		sourcePlan, err := fixture.store.(c4StartPlanner).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: record.SourceRunID, At: record.EventID})
		if err != nil {
			t.Fatal(err)
		}
		record.ForkPoint = sourcePlan.ForkPoint
		create = func() {
			forkWitness = connector.armForkInsert(record.SourceRunID, record.ForkPoint, record.BundleHash)
			arm()
			if _, err := fixture.store.(c4ForkCreationOwner).MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: record.SourceRunID, At: record.EventID}); err != nil {
				t.Fatal(err)
			}
		}
	case "standing":
		record.FlowPath, record.EventID = "c4-crash-standing", ""
		candidate := pipeline.StandingServiceCandidate{BindingEnabled: true, ServiceID: flowidentity.StandingServiceID(record.FlowPath), FlowPath: record.FlowPath, Source: sourceartifactfixture.Fact()}
		record.RunID = flowidentity.StandingGenerationRunID(candidate.ServiceID, 1)
		ctx = testAuthorActivityRuntimeContext()
		workflow := newWorkflowTestCoordinator(t, fixture.db, pipeline.NewWorkflowPersistence(fixture.store.(workflowTestSelectedStore)), fixture.store.(workflowTestSelectedStore))
		create = func() {
			arm()
			if _, err := workflow.ReconcileStandingService(ctx, candidate); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatalf("unknown creation origin %q", origin)
	}
	c4RequireCreationCrashRecordRoundTrip(t, record)
	create()
	t.Fatal("creation process returned an ACK instead of reaching the SIGKILL boundary")
}

func c4ReplayCreationAfterCrash(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, backend string, record c4CreationCrashRecord) {
	t.Helper()
	var origin runlifecycle.RunOrigin
	var err error
	count := 0
	switch record.Origin {
	case "event":
		input := eventtest.RunCreatingRootIngress(record.EventID, "c4.atomic.birth", "ingress", "", []byte(`{"original":true}`), 0,
			record.RunID, "", events.EventEnvelope{}, record.At)
		replayed, replayErr := c4CommitCreatingPublication(t, ctx, fixture.store, c4AdmitCreatingInput(t, input, record.BundleHash), nil)
		if replayErr != nil || !replayed.Acknowledged || replayed.AppendOutcome != bus.EventAppendExactDuplicate {
			t.Fatalf("fresh owner replayed committed event creation: %+v err=%v", replayed, replayErr)
		}
		origin, err = runlifecycle.EventRunOrigin(record.EventID, string(input.Type()))
		count = 1
	case "deployment":
		if record.Deployment == nil || record.Deployment.RunCreation.RunID != record.RunID || record.Deployment.RunCreation.BundleHash != record.BundleHash {
			t.Fatal("committed deployment lost its exact caller request")
		}
		replayed, replayErr := fixture.store.(bus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, *record.Deployment)
		if replayErr != nil || !replayed.Acknowledged || !replayed.Replay || len(replayed.Activations) != 0 || replayed.Record.Summary.RunID != record.RunID {
			t.Fatalf("fresh owner reconstructed committed deployment instead of replaying: %+v err=%v", replayed, replayErr)
		}
		origin = runlifecycle.DeploymentRunOrigin()
	case "fork":
		replayed := c4ReadExactForkWithoutMutation(t, ctx, fixture.store, backend, record.RunID,
			runfork.RunForkMaterializeRequest{SourceRunID: record.SourceRunID, At: record.EventID}, true, nil)
		point := record.ForkPoint
		if point.Kind != runfork.RunForkPointEvent || point.EventID != record.EventID || point.Revision != 1 ||
			replayed.ForkRunID != record.RunID || replayed.ForkPoint != point {
			t.Fatalf("fresh owner exact read changed committed fork child/cut: %+v", replayed)
		}
		origin, err = point.MaterializationRunOrigin(record.SourceRunID)
		c4RequireExclusiveStart(t, ctx, fixture.store, record.SourceRunID, record.BundleHash, record.EventID)
		c4RequireRevisionCount(t, ctx, fixture.store, record.SourceRunID, 1)
	case "standing":
		fact, present := correlation.SourceArtifactFactFromContext(ctx)
		if !present || fact.BundleHash() != record.BundleHash {
			t.Fatal("standing retry lost its native committed source")
		}
		candidate := pipeline.StandingServiceCandidate{BindingEnabled: true, ServiceID: flowidentity.StandingServiceID(record.FlowPath), FlowPath: record.FlowPath, Source: fact}
		workflow := newWorkflowTestCoordinator(t, fixture.db, pipeline.NewWorkflowPersistence(fixture.store.(workflowTestSelectedStore)), fixture.store.(workflowTestSelectedStore))
		replayed, replayErr := workflow.ReconcileStandingService(testAuthorActivityRuntimeContext(), candidate)
		if replayErr != nil || replayed.RunID != record.RunID || replayed.Generation != 1 {
			t.Fatalf("fresh standing owner duplicated committed generation: %+v err=%v", replayed, replayErr)
		}
		origin, err = runlifecycle.StandingGenerationRunOrigin(candidate.ServiceID, 1)
	default:
		t.Fatalf("unknown creation origin %q", record.Origin)
	}
	if err != nil {
		t.Fatal(err)
	}
	requireRunOriginHeader(t, ctx, fixture.store.(runOriginReadStore), record.RunID, origin, count)
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
				selected, db, connector := newP16RaceStore(t, backend)
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
				capability, err := agentfixture.ProcessCapability(t, ctx, selected.(agentfixture.Store))
				if err != nil {
					t.Fatal(err)
				}
				if err := capability.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				selected, _ = c4ReopenCreationStore(t, backend, selected, db, connector)
				ctx = testAuthorActivityContextForBundle(command.RunCreation.BundleHash)
				owner = selected.(bus.DeploymentRunCreationCommitOwner)
				if !committed {
					c4RequireAbsentCreation(t, ctx, selected, runID)
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
			selected := fixture.store.(c4ForkCreationOwner)
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
				replayed := c4ReadExactForkWithoutMutation(t, ctx, fixture.store, backend, created.ForkRunID, request, true, nil)
				if !reflect.DeepEqual(replayed, created) {
					t.Fatalf("exact fork read changed its original creation: got=%+v want=%+v", replayed, created)
				}
				after := c4RequireExclusiveStart(t, ctx, fixture.store, created.ForkRunID, runLifecycleCandidateParityBundleHash, "")
				if !reflect.DeepEqual(after, before) {
					t.Fatal("fork retry changed child original start")
				}
			}
			c4RequireRevisionCount(t, ctx, fixture.store, created.ForkRunID, 1)
			second, err := selected.MaterializeRunFork(ctx, request)
			if err != nil || second.ForkRunID == "" || second.ForkRunID == created.ForkRunID || second.ForkPoint != created.ForkPoint {
				t.Fatalf("distinct fresh event-cut invocation reused a child or changed the cut: first=%+v second=%+v err=%v", created, second, err)
			}
			c4RequireExclusiveStart(t, ctx, fixture.store, second.ForkRunID, runLifecycleCandidateParityBundleHash, "")
			c4RequireRevisionCount(t, ctx, fixture.store, second.ForkRunID, 1)
			requireRunOriginHeader(t, ctx, fixture.store.(runOriginReadStore), second.ForkRunID, origin, 0)
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
				selected, db = c4ReopenCreationStore(t, backend, selected, db, connector)
				workflow = newWorkflowTestCoordinator(t, db, pipeline.NewWorkflowPersistence(selected.(workflowTestSelectedStore)), selected.(workflowTestSelectedStore))
				reader = selected.(runOriginReadStore)
				reopened, reopenedFound, err := workflow.LoadReconciledStandingService(ctx, candidate)
				if err != nil || reopenedFound != committed || (committed && reopened.RunID != observed.RunID) {
					t.Fatalf("fresh standing owner lost physical commit outcome: %+v found=%t err=%v", reopened, reopenedFound, err)
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
	return ctx, command
}
